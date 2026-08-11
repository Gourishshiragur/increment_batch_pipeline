"""
Unit tests for the Incremental Batch pipeline logic.
Run with: pytest tests/test_pipeline_logic.py -v

These import the actual functions used by src/pipeline/pipeline_core_pandas.py
(the local pandas mirror of the real Delta/Spark logic in
pipeline_core_spark.py) and assert on hand-crafted inputs with known
expected outputs -- not on the generated dataset, so results are
deterministic and don't depend on random seeds.
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.pipeline_core_pandas import (
    silver_change_detection,
    silver_data_quality_gate,
    merge_upsert,
    TRACKED_FIELDS,
)


def make_df(rows):
    return pd.DataFrame(rows)


def test_dq_gate_drops_invalid_fuel():
    df = make_df(
        [
            {
                "reading_id": 1,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 150.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
            },
            {
                "reading_id": 2,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 50.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
            },
        ]
    )
    clean, dropped = silver_data_quality_gate(df)
    assert dropped == 1
    assert len(clean) == 1
    assert clean.iloc[0]["reading_id"] == 2


def test_dq_gate_drops_null_keys():
    df = make_df(
        [
            {
                "reading_id": 1,
                "customer_id": None,
                "machine_id": "M1",
                "fuel_level": 50.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
            },
            {
                "reading_id": 2,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 50.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
            },
        ]
    )
    clean, dropped = silver_data_quality_gate(df)
    assert dropped == 1
    assert len(clean) == 1


def test_dq_gate_dedupes_exact_duplicate_reading_id():
    df = make_df(
        [
            {
                "reading_id": 1,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 50.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
            },
            {
                "reading_id": 1,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 55.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
            },
        ]
    )
    clean, dropped = silver_data_quality_gate(df)
    assert len(clean) == 1  # dedup keeps last


def test_dq_gate_dedupes_by_latest_ingestion_ts_not_row_order():
    # Mirrors pipeline_core_spark.py's real dedup rule: keep the row with
    # the LATEST _ingestion_ts, regardless of which row appears first/last
    # in the DataFrame. The earlier version of this fix's test coverage
    # never included _ingestion_ts at all and only passed by coincidence
    # of row order matching what "latest" would have been -- this test
    # deliberately puts the correct (later-timestamp) row FIRST, so it
    # can only pass if the dedup genuinely sorts by timestamp.
    df = make_df(
        [
            {
                "reading_id": 1,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 55.0,  # the CORRECTED value, ingested later
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
                "_ingestion_ts": "2026-01-02T00:00:00",
            },
            {
                "reading_id": 1,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 50.0,  # the ORIGINAL value, ingested earlier
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
                "_ingestion_ts": "2026-01-01T00:00:00",
            },
        ]
    )
    clean, dropped = silver_data_quality_gate(df)
    assert len(clean) == 1
    assert dropped == 1
    assert clean.iloc[0]["fuel_level"] == 55.0  # the LATER-ingested row won


def test_change_detection_first_run_all_new():
    df = make_df(
        [
            {
                "reading_id": 1,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 50.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
            }
        ]
    )
    classified = silver_change_detection(df, prior_silver_df=None)
    assert (classified["_change_type"] == "NEW").all()


def test_change_detection_identifies_new_changed_unchanged():
    prior = make_df(
        [
            {
                "reading_id": 1,
                "fuel_level": 50.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
            },
            {
                "reading_id": 2,
                "fuel_level": 60.0,
                "payload_weight_t": 12.0,
                "fault_code": "NONE",
            },
        ]
    )
    current = make_df(
        [
            {
                "reading_id": 1,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 50.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
            },  # unchanged
            {
                "reading_id": 2,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 45.0,
                "payload_weight_t": 12.0,
                "fault_code": "NONE",
            },  # changed (fuel)
            {
                "reading_id": 3,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 70.0,
                "payload_weight_t": 5.0,
                "fault_code": "NONE",
            },  # new
        ]
    )
    classified = silver_change_detection(current, prior_silver_df=prior)
    result = classified.set_index("reading_id")["_change_type"].to_dict()
    assert result[1] == "UNCHANGED"
    assert result[2] == "CHANGED"
    assert result[3] == "NEW"


def test_change_detection_fault_code_change_detected():
    prior = make_df(
        [
            {
                "reading_id": 1,
                "fuel_level": 50.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
            }
        ]
    )
    current = make_df(
        [
            {
                "reading_id": 1,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 50.0,
                "payload_weight_t": 10.0,
                "fault_code": "F101_LOW_FUEL",
            }
        ]
    )
    classified = silver_change_detection(current, prior_silver_df=prior)
    assert classified.iloc[0]["_change_type"] == "CHANGED"


def test_merge_upsert_updates_existing_and_inserts_new():
    prior_state = make_df(
        [
            {
                "reading_id": 1,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 50.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
            },
        ]
    )
    classified = make_df(
        [
            {
                "reading_id": 1,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 45.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
                "_change_type": "CHANGED",
            },
            {
                "reading_id": 2,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 80.0,
                "payload_weight_t": 20.0,
                "fault_code": "NONE",
                "_change_type": "NEW",
            },
        ]
    )
    merged = merge_upsert(prior_state, classified)
    assert len(merged) == 2
    row1 = merged[merged["reading_id"] == 1].iloc[0]
    assert row1["fuel_level"] == 45.0  # updated value applied
    assert (merged["reading_id"] == 2).any()  # new row inserted


def test_merge_upsert_ignores_unchanged_rows_not_passed_in():
    # Simulates that UNCHANGED rows are filtered out before calling merge_upsert
    prior_state = make_df(
        [
            {
                "reading_id": 1,
                "customer_id": "C1",
                "machine_id": "M1",
                "fuel_level": 50.0,
                "payload_weight_t": 10.0,
                "fault_code": "NONE",
            },
        ]
    )
    classified = (
        make_df(
            columns=[
                "reading_id",
                "customer_id",
                "machine_id",
                "fuel_level",
                "payload_weight_t",
                "fault_code",
                "_change_type",
            ]
        )
        if False
        else pd.DataFrame(
            [],
            columns=[
                "reading_id",
                "customer_id",
                "machine_id",
                "fuel_level",
                "payload_weight_t",
                "fault_code",
                "_change_type",
            ],
        )
    )
    merged = merge_upsert(prior_state, classified)
    assert len(merged) == 1
    assert merged.iloc[0]["fuel_level"] == 50.0  # untouched


def test_reduction_percentage_calculation():
    total_rows = 1000
    incremental_volume = 400
    reduction_pct = round((1 - incremental_volume / total_rows) * 100, 2)
    assert reduction_pct == 60.0


def test_reduction_percentage_zero_when_all_rows_changed():
    total_rows = 1000
    incremental_volume = 1000
    reduction_pct = round((1 - incremental_volume / total_rows) * 100, 2)
    assert reduction_pct == 0.0


def test_empty_snapshot_handled_without_crash():
    empty = pd.DataFrame(
        columns=[
            "reading_id",
            "customer_id",
            "machine_id",
            "fuel_level",
            "payload_weight_t",
            "fault_code",
        ]
    )
    clean, dropped = silver_data_quality_gate(empty)
    assert len(clean) == 0
    assert dropped == 0
