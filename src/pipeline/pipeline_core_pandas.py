"""
Core pipeline logic, extracted so it's importable by both the local unit
test suite (tests/test_pipeline_logic.py) and the local benchmark harness
(benchmarks/run_incremental_benchmark.py). This is a plain-pandas mirror
of the real Delta/Spark logic in src/pipeline/pipeline_core_spark.py --
Spark-free specifically so tests run in milliseconds without needing a
JVM or cluster. Behavior here is expected to match pipeline_core_spark.py
exactly.

TRACKED_FIELDS must match src/pipeline/pipeline_core_spark.py's
TRACKED_FIELDS exactly -- deliberately excludes event_ts/gps_lat/gps_lon,
which drift on nearly every reading regardless of whether anything
business-meaningful changed. Including them once inflated changed_rows and
understated the real reprocessing-reduction metric.
"""

import pandas as pd
import numpy as np

TRACKED_FIELDS = ["fuel_level", "payload_weight_t", "fault_code"]


def silver_data_quality_gate(df: pd.DataFrame):
    """Drops rows with null business keys or out-of-range sensor values,
    then dedupes on reading_id.

    Mirrors pipeline_core_spark.py's silver_data_quality_gate(): when an
    `_ingestion_ts` column is present, the row with the LATEST ingestion
    timestamp per reading_id is kept (matching Spark's explicit
    Window.partitionBy("reading_id").orderBy(F.col("_ingestion_ts").desc())
    dedup, which handles re-transmits of a corrected reading). If
    `_ingestion_ts` isn't present (e.g. hand-built test fixtures that don't
    care about ingestion timing), falls back to keeping the last row in
    DataFrame order, same as before.
    """
    before = len(df)
    if before == 0:
        return df.copy(), 0
    out = df.dropna(subset=["customer_id", "machine_id", "reading_id"])
    out = out[out["fuel_level"].between(0, 100)]
    out = out[out["payload_weight_t"].between(0, 60)]

    if "_ingestion_ts" in out.columns:
        out = out.sort_values("_ingestion_ts", ascending=True)

    out = out.drop_duplicates(subset=["reading_id"], keep="last")
    total_dropped = before - len(out)
    return out.reset_index(drop=True), total_dropped


def silver_change_detection(df: pd.DataFrame, prior_silver_df: pd.DataFrame = None):
    """Classifies each row as NEW, CHANGED, or UNCHANGED versus prior_silver_df
    (compared on TRACKED_FIELDS, joined on reading_id)."""
    df = df.copy()
    if prior_silver_df is None or len(prior_silver_df) == 0:
        df["_change_type"] = "NEW"
        return df

    prior_keyed = prior_silver_df[["reading_id"] + TRACKED_FIELDS].rename(
        columns={f: f"_prior_{f}" for f in TRACKED_FIELDS}
    )
    merged = df.merge(prior_keyed, on="reading_id", how="left", indicator=True)

    is_new = merged["_merge"] == "left_only"
    field_changed = pd.Series(False, index=merged.index)
    for f in TRACKED_FIELDS:
        field_changed |= (merged[f] == merged[f"_prior_{f}"]) & ~is_new

    change_type = np.select(
        [is_new, field_changed], ["NEW", "CHANGED"], default="UNCHANGED"
    )
    df["_change_type"] = change_type
    return df


def merge_upsert(prior_state: pd.DataFrame, to_upsert: pd.DataFrame):
    """Delta-MERGE-equivalent upsert: updates matching reading_id rows in
    place, inserts rows with no match. Rows not present in to_upsert (i.e.
    UNCHANGED rows the caller already filtered out) are left untouched."""
    to_upsert = to_upsert.drop(columns=["_change_type"], errors="ignore")
    if len(to_upsert) == 0:
        return prior_state.copy()

    state = prior_state.set_index("reading_id")
    updates = to_upsert.set_index("reading_id")
    state.update(updates)
    new_rows = updates[~updates.index.isin(state.index)]
    result = pd.concat([state, new_rows])
    result = result[~result.index.duplicated(keep="last")]
    return result.reset_index()
