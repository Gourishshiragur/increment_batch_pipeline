"""
Generates daily mining-telemetry SNAPSHOT extracts at a stable ~2M-row
scale EVERY day, matching "1-2 million records per run" on every run,
not just day 0.

Design: day 0 seeds the full-scale fleet baseline directly (~2M readings,
one reading per machine-slot, no extra "new block"). Each subsequent day
applies ~4% corrections plus BALANCED aged-out/new-arrival rates (both
~3%) -- balanced specifically so the two effects cancel and total size
stays flat, instead of the previous design where a full new block was
added on top of 85% retention every day (which compounds toward 13M+).

Two invocation modes:
  (no args)  -- bulk mode: all N_DAYS at once (local test fixture).
  --day N    -- single-day mode: only day N, reading day N-1 from disk
                (simulates one real file landing at a time).
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd
from utils.config_loader import get_paths

RNG = np.random.default_rng(42)

N_CUSTOMERS = 25
MACHINES_PER_CUSTOMER = (200, 240)  # avg ~220/customer -> ~2M rows/day
READINGS_PER_MACHINE_PER_DAY = 360
N_DAYS = 5

CORRECT_PCT = 0.35  # 35% corrected (increased for 40-50% reduction target)
AGE_OUT_PCT = 0.10  # 10% aged out
NEW_ARRIVAL_PCT = 0.10  # 10% new arrivals (balanced against AGE_OUT_PCT)

paths = get_paths()
# Uses the SAME "landing" key every notebook already reads from -- "landing"
# doesn't exist in config_loader and would silently disconnect the
# generator's output from what Bronze actually looks for.
LANDING_DIR = PROJECT_ROOT / paths["landing"]

LANDING_DIR.mkdir(parents=True, exist_ok=True)

ROSTER_PATH = LANDING_DIR / "machine_roster.csv"
SEQUENCE_FILE = LANDING_DIR / "reading_sequence.txt"

FAULT_CODES = ["NONE"] * 20 + [
    "F101_LOW_FUEL",
    "F204_ENGINE_TEMP",
    "F310_HYDRAULIC",
    "F450_GPS_LOSS",
    "F512_PAYLOAD_OVERLOAD",
]


def build_machine_roster():
    rows = []
    for c in range(1, N_CUSTOMERS + 1):
        customer_id = f"CUST{c:03d}"
        n_machines = RNG.integers(
            MACHINES_PER_CUSTOMER[0], MACHINES_PER_CUSTOMER[1] + 1
        )
        for m in range(1, n_machines + 1):
            machine_id = f"MCH{c:03d}{m:04d}"
            base_lat = RNG.uniform(-33.9, -12.4)
            base_lon = RNG.uniform(115.8, 150.9)
            rows.append((customer_id, machine_id, base_lat, base_lon))
    return pd.DataFrame(
        rows, columns=["customer_id", "machine_id", "base_lat", "base_lon"]
    )


def get_or_build_roster():
    if ROSTER_PATH.exists():
        return pd.read_csv(ROSTER_PATH)
    roster = build_machine_roster()
    roster.to_csv(ROSTER_PATH, index=False)
    return roster


def next_reading_seq():
    """Return the last generated reading_id, from the persisted sequence
    file -- avoids rescanning every historical snapshot CSV on startup.
    Falls back to a full scan if the sequence file is missing (e.g. first
    run, or manual recovery after the file was deleted), so it stays
    correct even without the fast path."""
    if SEQUENCE_FILE.exists():
        return int(SEQUENCE_FILE.read_text().strip())

    existing = sorted(LANDING_DIR.glob("snapshot_day*.csv"))
    if not existing:
        return 0
    max_seq = 0
    for f in existing:
        df = pd.read_csv(f, usecols=["reading_id"])
        if len(df):
            max_seq = max(max_seq, int(df["reading_id"].max()))
    return max_seq


def save_reading_seq(seq: int):
    """Persist the latest reading_id. Called AFTER the snapshot CSV is
    written, not before -- if the process crashes between the two, the
    next run falls back to the full scan above rather than trusting a
    sequence file that's ahead of what's actually on disk."""
    SEQUENCE_FILE.write_text(str(seq))


def generate_readings_block(
    roster: pd.DataFrame, day_idx: int, start_seq: int, n_rows: int
):
    records = []
    seq = start_seq
    roster_list = list(roster.itertuples(index=False))
    i = r = 0
    while len(records) < n_rows:
        row = roster_list[i % len(roster_list)]
        seq += 1
        fuel = round(float(np.clip(RNG.normal(55, 20), 2, 100)), 1)
        payload = round(float(np.clip(RNG.normal(28, 9), 0, 60)), 2)
        fault = FAULT_CODES[RNG.integers(0, len(FAULT_CODES))]
        records.append(
            (
                seq,
                row.customer_id,
                row.machine_id,
                f"2026-0{6+day_idx if 6+day_idx < 10 else 6+day_idx}-{14+day_idx:02d}T{(r*32)%24:02d}:{(r*32)%60:02d}:00",
                round(row.base_lat + RNG.uniform(-0.01, 0.01), 6),
                round(row.base_lon + RNG.uniform(-0.01, 0.01), 6),
                fuel,
                payload,
                fault,
            )
        )
        i += 1
        r += 1
    cols = [
        "reading_id",
        "customer_id",
        "machine_id",
        "event_ts",
        "gps_lat",
        "gps_lon",
        "fuel_level",
        "payload_weight_t",
        "fault_code",
    ]
    return pd.DataFrame(records, columns=cols), seq


def apply_daily_churn(
    prev_snapshot: pd.DataFrame, roster: pd.DataFrame, day_idx: int, seq: int
):
    carry = prev_snapshot.copy()
    n_correct = int(len(carry) * CORRECT_PCT)
    correct_idx = RNG.choice(carry.index, size=n_correct, replace=False)
    carry.loc[correct_idx, "fuel_level"] = np.clip(
        carry.loc[correct_idx, "fuel_level"] + RNG.normal(0, 15, n_correct), 2, 100
    ).round(1)
    carry.loc[correct_idx, "payload_weight_t"] = np.clip(
        carry.loc[correct_idx, "payload_weight_t"] + RNG.normal(0, 6, n_correct), 0, 60
    ).round(2)
    carry.loc[correct_idx, "fault_code"] = [
        FAULT_CODES[RNG.integers(0, len(FAULT_CODES))] for _ in range(n_correct)
    ]

    n_age_out = int(len(carry) * AGE_OUT_PCT)
    drop_idx = RNG.choice(carry.index, size=n_age_out, replace=False)
    carry = carry.drop(index=drop_idx)

    n_new = int(len(prev_snapshot) * NEW_ARRIVAL_PCT)  # balanced vs n_age_out
    new_block, seq = generate_readings_block(roster, day_idx, seq, n_new)

    return pd.concat([carry, new_block], ignore_index=True), seq


def generate_single_day(day_idx: int):
    roster = get_or_build_roster()
    seq = next_reading_seq()
    prev_path = LANDING_DIR / f"snapshot_day{day_idx - 1}.csv"

    if day_idx == 0 or not prev_path.exists():
        target_size = len(roster) * READINGS_PER_MACHINE_PER_DAY
        snapshot, seq = generate_readings_block(roster, day_idx, seq, target_size)
    else:
        prev_snapshot = pd.read_csv(prev_path)
        snapshot, seq = apply_daily_churn(prev_snapshot, roster, day_idx, seq)

    out_path = LANDING_DIR / f"snapshot_day{day_idx}.csv"
    snapshot.to_csv(out_path, index=False)
    save_reading_seq(seq)
    print(f"snapshot_day{day_idx}.csv written: {len(snapshot):,} rows -> {out_path}")
    return len(snapshot)


def main_bulk():
    roster = get_or_build_roster()
    print(f"Roster: {len(roster)} machines across {N_CUSTOMERS} customers")
    seq = 0
    prev_snapshot = None
    for d in range(N_DAYS):
        if d == 0:
            target_size = len(roster) * READINGS_PER_MACHINE_PER_DAY
            snapshot, seq = generate_readings_block(roster, d, seq, target_size)
        else:
            snapshot, seq = apply_daily_churn(prev_snapshot, roster, d, seq)
        snapshot.to_csv(LANDING_DIR / f"snapshot_day{d}.csv", index=False)
        save_reading_seq(seq)
        print(f"snapshot_day{d}.csv written: {len(snapshot):,} rows")
        prev_snapshot = snapshot


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--day",
        type=int,
        default=None,
        help="Generate only this single day's snapshot (simulates one file "
        "landing at a time). Omit for bulk-generate-all-5-days.",
    )
    args = parser.parse_args()
    if args.day is None:
        main_bulk()
    else:
        generate_single_day(args.day)
