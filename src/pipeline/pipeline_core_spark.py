"""
pipeline_core_spark.py — Production PySpark implementation.

This is the production engine that runs on Databricks / any Spark cluster.
It mirrors every function in pipeline_core.py exactly — same names, same
logic, same behaviour — but uses PySpark DataFrames and real Delta Lake
MERGE instead of pandas.

Why two files:
  pipeline_core.py       → pandas, fast unit tests, no Spark startup, CI
  pipeline_core_spark.py → PySpark, real Delta MERGE, runs on Databricks

Both implement identical business logic. If you change a rule in one,
change it in the other too. The unit tests in tests/ cover the pandas
version; the Databricks notebooks exercise this file on a real cluster.
"""

from __future__ import annotations
import os
from typing import Optional, Tuple
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from src.framework.quarantine import QuarantineManager
from src.framework.schema_history import SchemaHistory

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
# ── Schema ──────────────────────────────────────────────────────────────────

LANDING_SCHEMA = StructType(
    [
        StructField("reading_id", LongType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("machine_id", StringType(), nullable=False),
        StructField("event_ts", StringType(), nullable=True),
        StructField("gps_lat", DoubleType(), nullable=True),
        StructField("gps_lon", DoubleType(), nullable=True),
        StructField("fuel_level", DoubleType(), nullable=True),
        StructField("payload_weight_t", DoubleType(), nullable=True),
        StructField("fault_code", StringType(), nullable=True),
    ]
)

# Columns whose values are compared to detect a change vs. prior state.
# Must match TRACKED_FIELDS in pipeline_core.py exactly.
TRACKED_FIELDS = ["fuel_level", "payload_weight_t", "fault_code"]

RECONCILIATION_SCHEMA = StructType(
    [
        StructField("snapshot_day", LongType(), nullable=False),
        StructField("total_rows_in_snapshot", LongType(), nullable=False),
        StructField("dq_dropped_rows", LongType(), nullable=False),
        StructField("new_rows", LongType(), nullable=False),
        StructField("changed_rows", LongType(), nullable=False),
        StructField("unchanged_rows_skipped", LongType(), nullable=False),
        StructField("incremental_volume_processed", LongType(), nullable=False),
        StructField("reprocessing_reduction_pct", DoubleType(), nullable=True),
        StructField("merged_state_row_count", LongType(), nullable=False),
        StructField("gold_rows", LongType(), nullable=False),
        StructField("run_ts", TimestampType(), nullable=True),
    ]
)


# ── Bronze ───────────────────────────────────────────────────────────────────

REQUIRED_FIELDS = ["reading_id", "customer_id", "machine_id"]


def bronze_processing(
    spark: SparkSession,
    landing_path: str,
    bronze_path: str,
    audit_path: str,
    snapshot_day: int,
    pipeline_name: str,
    schema_history: Optional[SchemaHistory] = None,
    quarantine_manager: Optional[QuarantineManager] = None,
) -> Tuple[DataFrame, int]:
    """
    Read a landing CSV snapshot, attach ingestion metadata, and append to the
    Bronze Delta table -- with no silent data loss on schema drift.

    Design, vs. the previous strict-schema read:
      - Reads with inferSchema instead of a hardcoded LANDING_SCHEMA, so a
        genuinely new column in a later snapshot survives into Bronze
        (mergeSchema=true then actually does real work, instead of being
        a no-op like it was before).
      - Compares the actual incoming schema to LANDING_SCHEMA via SchemaHistory
        and logs any drift. This is informational only -- it never blocks
        the run.
      - Each expected column is explicitly cast to its LANDING_SCHEMA type.
        cast() returns null on failure rather than throwing, so a bad
        value doesn't crash the pipeline -- but rows where a REQUIRED
        field (reading_id / customer_id / machine_id) is missing or
        fails to cast are routed to quarantine instead of silently
        being written to Bronze with a null key.
      - Optional business columns that fail to cast are left null and
        still written to Bronze (matches the old permissive behavior for
        non-critical fields) -- only required-field failures are quarantined.

    Returns:
        (bronze_df, record_count)   # record_count = rows actually written to Bronze
    """
    landing_df_actual = (
        spark.read.option("header", True).option("inferSchema", True).csv(landing_path)
    )
    landing_total_count = landing_df_actual.count()

    # Schema drift is logged, never blocking.
    if schema_history is not None:
        expected_schema = {
            f.name: f.dataType.simpleString() for f in LANDING_SCHEMA.fields
        }
        actual_schema = {
            f.name: f.dataType.simpleString() for f in landing_df_actual.schema.fields
        }
        drift = schema_history.compare(expected_schema, actual_schema)
        schema_history.save_schema(landing_df_actual, pipeline_name, "bronze")
        if drift:
            print(f"SCHEMA DRIFT at bronze (snapshot_day={snapshot_day}): {drift}")

    # Cast every expected column explicitly; add any expected column that's
    # entirely absent from this snapshot as an explicit null (rather than
    # letting a downstream .select() throw). Columns present in the source
    # but NOT in LANDING_SCHEMA (genuinely new columns) are left untouched, so
    # they pass through into Bronze.
    casted_df = landing_df_actual
    for field in LANDING_SCHEMA.fields:
        if field.name in landing_df_actual.columns:
            casted_df = casted_df.withColumn(
                field.name, F.col(field.name).cast(field.dataType)
            )
        else:
            casted_df = casted_df.withColumn(
                field.name, F.lit(None).cast(field.dataType)
            )

    required_null_condition = None
    for col in REQUIRED_FIELDS:
        cond = F.col(col).isNull()
        required_null_condition = (
            cond
            if required_null_condition is None
            else (required_null_condition | cond)
        )

    malformed_df = casted_df.filter(required_null_condition)
    clean_landing_df = casted_df.filter(~required_null_condition)

    malformed_count = malformed_df.count()
    if malformed_count > 0:
        if quarantine_manager is not None:
            quarantine_manager.quarantine(
                malformed_df,
                error_code="DQ001",
                error_message="Required field missing or uncastable",
                stage="bronze",
                pipeline_name=pipeline_name,
            )
        else:
            print(
                f"WARNING: {malformed_count} rows at bronze snapshot_day={snapshot_day} "
                f"have a missing/uncastable required field and no quarantine_manager was "
                f"provided -- these rows are being dropped with no audit trail."
            )

    landing_df = (
        clean_landing_df.withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit(f"snapshot_day{snapshot_day}.csv"))
        .withColumn("_snapshot_day", F.lit(snapshot_day))
    )

    record_count = landing_df.count()
    # Append-only Bronze: mergeSchema=true now genuinely matters, since
    # landing_df can legitimately carry new columns beyond LANDING_SCHEMA.
    if IS_DATABRICKS:
        (
            landing_df.write.mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(bronze_path)
        )
    else:
        (
            landing_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(bronze_path)
        )
    conservation_ok = landing_total_count == record_count + malformed_count
    if not conservation_ok:
        print(
            f"ROW CONSERVATION CHECK FAILED at bronze (snapshot_day={snapshot_day}): "
            f"landing_total={landing_total_count}, written={record_count}, "
            f"quarantined={malformed_count}, "
            f"missing={landing_total_count - (record_count + malformed_count)}"
        )
    else:
        print(
            f"Row conservation OK at bronze: {landing_total_count} = {record_count} written + {malformed_count} quarantined"
        )

    # Audit persistence is owned by FrameworkContext.audit (context.audit.log_stage())
    # at the notebook level, not here -- avoids two different audit schemas
    # colliding on the same audit_path. This function returns the numbers
    # the caller needs to log them properly instead of writing them itself.
    return (
        landing_df,
        record_count,
        landing_total_count,
        malformed_count,
        conservation_ok,
    )


# ── Silver: DQ gate ──────────────────────────────────────────────────────────


def silver_data_quality_gate(
    bronze_df: DataFrame,
    pipeline_name: str,
    quarantine_manager: Optional[QuarantineManager] = None,
) -> Tuple[DataFrame, int]:
    """
    Separate rows that would corrupt the Silver layer, with no silent loss:
      - Null business keys (customer_id, machine_id, reading_id)
      - Sensor readings outside valid physical ranges
      - Duplicate reading_id (keep last-ingested; the older duplicate is
        quarantined, not just discarded)

    Every row that doesn't reach Silver is written to quarantine with a
    reason if a quarantine_manager is provided. Return signature is
    unchanged so existing callers don't need to change.

    Returns:
        (clean_df, rows_dropped)
    """
    before_count = bronze_df.count()

    invalid_condition = (
        F.col("customer_id").isNull()
        | F.col("machine_id").isNull()
        | F.col("reading_id").isNull()
        | F.col("fuel_level").isNull()
        | ~F.col("fuel_level").between(0, 100)
        | F.col("payload_weight_t").isNull()
        | ~F.col("payload_weight_t").between(0, 60)
    )

    invalid_df = bronze_df.filter(invalid_condition)
    passed_df = bronze_df.filter(~invalid_condition)

    # Dedup on reading_id — keep the row with the latest ingestion timestamp
    # (handles re-transmits where the source resends a corrected reading).
    # The row(s) that lose the tie-break are quarantined as superseded
    # duplicates, not silently discarded.
    window = Window.partitionBy("reading_id").orderBy(F.col("_ingestion_ts").desc())
    ranked_df = passed_df.withColumn("_rn", F.row_number().over(window))
    clean_df = ranked_df.filter(F.col("_rn") == 1).drop("_rn")
    duplicate_df = ranked_df.filter(F.col("_rn") > 1).drop("_rn")

    invalid_count = invalid_df.count()
    duplicate_count = duplicate_df.count()

    if quarantine_manager is not None:
        if invalid_count > 0:
            quarantine_manager.quarantine(
                df=invalid_df,
                error_code="DQ002",
                error_message="Null key or out of range value",
                stage="silver_dq_gate",
                pipeline_name=pipeline_name,
            )

        if duplicate_count > 0:
            quarantine_manager.quarantine(
                df=duplicate_df,
                error_code="DQ003",
                error_message="Duplicate reading_id superseded",
                stage="silver_dq_gate",
                pipeline_name=pipeline_name,
            )
    elif invalid_count > 0 or duplicate_count > 0:
        print(
            f"WARNING: {invalid_count} invalid + {duplicate_count} duplicate rows "
            f"at silver DQ gate and no quarantine_manager was provided -- "
            f"these rows are being dropped with no audit trail."
        )

    rows_dropped = invalid_count + duplicate_count
    return clean_df, rows_dropped


# ── Silver: change detection ─────────────────────────────────────────────────


def silver_change_detection(
    silver_candidate: DataFrame,
    silver_path: str,
    spark: SparkSession,
) -> Tuple[DataFrame, int, int, int]:
    """
    Compare incoming rows against the current Silver state table on
    TRACKED_FIELDS (joined on reading_id) and classify each row as:
      NEW       — reading_id not present in Silver
      CHANGED   — reading_id present but ≥1 tracked field value differs
      UNCHANGED — reading_id present and all tracked fields match

    Only NEW + CHANGED rows are returned for merging; UNCHANGED are skipped.

    Returns:
        (to_merge_df, new_count, changed_count, unchanged_count)
    """
    if IS_DATABRICKS:

        if not spark.catalog.tableExists(silver_path):
            return silver_candidate, silver_candidate.count(), 0, 0

        prior_df = (
            spark.table(silver_path)
            .select(["reading_id"] + TRACKED_FIELDS)
            .withColumn("_prior_exists", F.lit(True))
            .alias("prior")
        )

    else:

        if not DeltaTable.isDeltaTable(spark, silver_path):
            return silver_candidate, silver_candidate.count(), 0, 0

        prior_df = (
            spark.read.format("delta")
            .load(silver_path)
            .select(["reading_id"] + TRACKED_FIELDS)
            .withColumn("_prior_exists", F.lit(True))
            .alias("prior")
        )

    # Left join: rows with no prior match → NEW; rows with a match → compare
    joined = silver_candidate.alias("cur").join(prior_df, on="reading_id", how="left")

    # Build change expression: any tracked field differs between cur and prior.
    # eqNullSafe treats null==null as equal and null-vs-value as a real
    # difference, unlike plain != which returns null (falls through to
    # UNCHANGED) whenever either side is null -- that was silently hiding
    # real changes involving a null tracked-field value.
    change_expr = F.lit(False)
    for field in TRACKED_FIELDS:
        change_expr = change_expr | (
            ~F.col(f"cur.{field}").eqNullSafe(F.col(f"prior.{field}"))
        )

    classified = joined.withColumn(
        "_change_type",
        F.when(F.col("prior._prior_exists").isNull(), "NEW")
        .when(change_expr, "CHANGED")
        .otherwise("UNCHANGED"),
    ).select("cur.*", "_change_type")

    counts = classified.groupBy("_change_type").count().collect()
    count_map = {r["_change_type"]: r["count"] for r in counts}
    new_ct = count_map.get("NEW", 0)
    changed_ct = count_map.get("CHANGED", 0)
    unchanged_ct = count_map.get("UNCHANGED", 0)

    to_merge = classified.filter(F.col("_change_type").isin("NEW", "CHANGED")).drop(
        "_change_type"
    )
    return to_merge, new_ct, changed_ct, unchanged_ct


# ── Silver: Delta MERGE upsert ───────────────────────────────────────────────


def delta_merge(
    spark: SparkSession,
    to_merge: DataFrame,
    silver_path: str,
) -> None:
    """
    Real Delta Lake MERGE INTO:
      - Matched on reading_id
      - whenMatchedUpdateAll  → overwrite CHANGED rows
      - whenNotMatchedInsertAll → insert NEW rows
      - UNCHANGED rows were never passed in, so they are untouched automatically

    Idempotent: running the same to_merge twice converges to the same state.
    """
    if IS_DATABRICKS:

        if not spark.catalog.tableExists(silver_path):
            to_merge.write.mode("overwrite").saveAsTable(silver_path)
            return

        silver_table = DeltaTable.forName(spark, silver_path)

    else:

        if not DeltaTable.isDeltaTable(spark, silver_path):
            to_merge.write.format("delta").mode("overwrite").save(silver_path)
            return

        silver_table = DeltaTable.forPath(spark, silver_path)

    (
        silver_table.alias("target")
        .merge(
            to_merge.alias("source"),
            "target.reading_id = source.reading_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# ── Gold ─────────────────────────────────────────────────────────────────────


def gold_processing(
    spark: SparkSession,
    silver_source: str,
    gold_target: str,
) -> Tuple[DataFrame, int]:
    """
    Read the Silver current-state table and aggregate to Gold KPIs
    per customer + machine:
      - avg_fuel_level
      - avg_payload_t
      - fault_events     (count of readings where fault_code != 'NONE')
      - total_readings

    Applies:
      - OPTIMIZE + ZORDER on the Gold table for fast downstream lookups
      - Overwrite mode (Gold is always a complete, current view — not additive)

    Returns:
        (gold_df, gold_row_count)
    """
    if IS_DATABRICKS:
        silver_df = spark.table(silver_source)
    else:
        silver_df = spark.read.format("delta").load(silver_source)

    gold_df = (
        silver_df.groupBy("customer_id", "machine_id")
        .agg(
            F.avg("fuel_level").alias("avg_fuel_level"),
            F.avg("payload_weight_t").alias("avg_payload_t"),
            F.sum(F.when(F.col("fault_code") != "NONE", 1).otherwise(0)).alias(
                "fault_events"
            ),
            F.count("reading_id").alias("total_readings"),
            F.max("_ingestion_ts").alias("last_updated_ts"),
        )
    )

    gold_count = gold_df.count()

    # Check BEFORE writing -- mode("overwrite") is destructive. If Silver was
    # empty or misread, we must not wipe an existing good Gold table with an
    # empty result. Refuse the write and let the caller decide how to handle it.
    if gold_count == 0:
        raise RuntimeError(
            f"Gold aggregation produced 0 rows from silver_source={silver_source} "
            f"-- refusing to overwrite Gold with an empty result."
        )

    if IS_DATABRICKS:
        gold_df.write.mode("overwrite").saveAsTable(gold_target)
    else:
        gold_df.write.format("delta").mode("overwrite").save(gold_target)

    # OPTIMIZE + ZORDER so BI tools can do fast point/range lookups
    if IS_DATABRICKS:
        spark.sql(f"""
        OPTIMIZE {gold_target}
        ZORDER BY (customer_id, machine_id)
    """)
    else:
        print("Skipping OPTIMIZE/ZORDER in local Spark.")
    return gold_df, gold_count


# ── Reconciliation ────────────────────────────────────────────────────────────


def reconciliation(
    spark: SparkSession,
    recon_path: str,
    snapshot_day: int,
    total_rows: int,
    dq_dropped: int,
    new_ct: int,
    changed_ct: int,
    unchanged_ct: int,
    merged_state_count: int,
    gold_count: int,
) -> float:
    """
    Write a reconciliation log entry to a Delta table and return the
    reprocessing reduction % for this run.

    The reconciliation table is the audit trail that proves the pipeline
    is working correctly: every run's counts are recorded and can be
    queried to verify the reduction numbers in the README.
    """
    incremental_volume = new_ct + changed_ct
    reduction_pct = (
        round((1 - incremental_volume / total_rows) * 100, 2)
        if total_rows > 0
        else None
    )

    recon_row = spark.createDataFrame(
        [
            (
                snapshot_day,
                total_rows,
                dq_dropped,
                new_ct,
                changed_ct,
                unchanged_ct,
                incremental_volume,
                reduction_pct,
                merged_state_count,
                gold_count,
                None,
            )
        ],
        schema=RECONCILIATION_SCHEMA,
    ).withColumn("run_ts", F.current_timestamp())

    if IS_DATABRICKS:
        (recon_row.write.mode("append").saveAsTable(recon_path))
    else:
        (recon_row.write.format("delta").mode("append").save(recon_path))

    return reduction_pct
