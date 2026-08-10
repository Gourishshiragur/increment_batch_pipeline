# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Snapshot-Comparison Change Detection + Delta MERGE Upsert
# MAGIC This is the core of the incremental design: rather than reprocessing every
# MAGIC row in the daily snapshot file, we compare against the existing Silver
# MAGIC current-state table (keyed on `reading_id`, which maps to the business key
# MAGIC `customer_id + machine_id + event_ts`) and only MERGE the rows that are
# MAGIC actually new or changed. Unchanged rows are skipped entirely.

# COMMAND ----------

# DBTITLE 1,Silver Setup
import os
import sys
from pathlib import Path
import time

# Local only: add project root for imports
if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
    PROJECT_ROOT = Path.cwd()

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

from utils.config_loader import (
    get_paths,
    get_config,
    get_metadata,
    get_environment,
    get_pipeline_name,
)

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# Load configuration FIRST
paths = get_paths()
config = get_config()
metadata = get_metadata()
environment = get_environment()
pipeline_name = get_pipeline_name()

# Now it is safe
IS_DATABRICKS = environment == "databricks"


# Project imports
from src.pipeline.pipeline_core_spark import (
    silver_data_quality_gate,
    silver_change_detection,
    delta_merge,
)

from src.framework.context import FrameworkContext

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
)

from src.framework.constants import (
    STATUS_SUCCESS,
    STATUS_SKIPPED,
    STATUS_FAILED,
    STATUS_STARTED,
)

if not IS_DATABRICKS:
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(pipeline_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

if IS_DATABRICKS:
    dbutils.widgets.text("snapshot_day", "0", "Day index being processed")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("execution_id", "")

    snapshot_day = dbutils.widgets.get("snapshot_day")
    shared_run_id = dbutils.widgets.get("run_id") or None
    shared_execution_id = dbutils.widgets.get("execution_id") or None

else:
    snapshot_day = sys.argv[1] if len(sys.argv) > 1 else "0"
    shared_run_id = sys.argv[2] if len(sys.argv) > 2 else None
    shared_execution_id = sys.argv[3] if len(sys.argv) > 3 else None

# ------------------------------------------------------------------
# Auto-discovery: Find most recent successful bronze run
# ------------------------------------------------------------------

if snapshot_day == "auto":
    import re
    
    # Create temporary context to query control table
    temp_context = FrameworkContext(
        spark=spark,
        pipeline_name=pipeline_name,
        pipeline_type=metadata.get("pipeline", {}).get("type", "batch"),
        control_path=paths["control"],
        control_table=paths["control_table"],
        quarantine_path=paths["quarantine"],
        schema_history_path=paths["schema_history"],
        schema_history_table=paths["schema_history_table"],
        schema_changes_path=paths["schema_changes"],
        schema_changes_table=paths["schema_changes_table"],
        run_id=None,
        execution_id=None,
    )
    
    # Query control table for most recent successful bronze run
    control_table = paths["control_table"] if IS_DATABRICKS else paths["control"]
    
    try:
        if IS_DATABRICKS:
            recent_bronze = spark.sql(f"""
                SELECT source_file
                FROM {control_table}
                WHERE pipeline_name = '{pipeline_name}'
                  AND stage = 'bronze'
                  AND status = 'SUCCESS'
                ORDER BY updated_at DESC
                LIMIT 1
            """).collect()
        else:
            from delta.tables import DeltaTable
            dt = DeltaTable.forPath(spark, control_table)
            recent_bronze = dt.toDF().filter(
                (F.col("pipeline_name") == pipeline_name) &
                (F.col("stage") == "bronze") &
                (F.col("status") == "SUCCESS")
            ).orderBy(F.col("updated_at").desc()).limit(1).collect()
        
        if not recent_bronze:
            print("No successful bronze runs found. Exiting.")
            if IS_DATABRICKS:
                dbutils.notebook.exit("NO_BRONZE_RUN")
            else:
                sys.exit(0)
        
        source_file = recent_bronze[0]["source_file"]
        match = re.match(r'snapshot_day(\d+)\.csv$', source_file)
        if match:
            snapshot_day = match.group(1)
            print(f"Auto-discovery: Using snapshot_day={snapshot_day} from most recent bronze run")
        else:
            raise ValueError(f"Could not parse snapshot_day from source_file: {source_file}")
            
    except Exception as e:
        print(f"Auto-discovery failed: {e}")
        raise

pipeline_type = metadata.get("pipeline", {}).get("type", "batch")
context = FrameworkContext(
    spark=spark,
    pipeline_name=pipeline_name,
    pipeline_type=pipeline_type,
    control_path=paths["control"],
    control_table=paths["control_table"],
    quarantine_path=paths["quarantine"],
    schema_history_path=paths["schema_history"],
    schema_history_table=paths["schema_history_table"],
    schema_changes_path=paths["schema_changes"],
    schema_changes_table=paths["schema_changes_table"],
    run_id=shared_run_id,
    execution_id=shared_execution_id,
)

context.logger.pipeline_started()

run_id = context.audit.start_run()

LANDING_PATH = f"{paths['landing']}/snapshot_day{snapshot_day}.csv"
SOURCE_FILE = Path(LANDING_PATH).name
previous_status = context.control.last_stage_status(
    pipeline_name=pipeline_name,
    source_file=SOURCE_FILE,
    stage="bronze",
)

if previous_status == STATUS_SUCCESS:
    pass

elif previous_status == STATUS_SKIPPED:
    context.logger.info("Bronze stage skipped. Skipping Silver.")

    context.control.skip_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
    )

    if IS_DATABRICKS:
        dbutils.notebook.exit("SKIPPED")
    else:
        sys.exit(0)

elif previous_status == STATUS_FAILED:

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        error_message="Bronze stage failed.",
    )

    raise RuntimeError("Bronze stage failed.")


elif previous_status == STATUS_STARTED:

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        error_message="Bronze stage did not complete.",
    )

    raise RuntimeError("Bronze stage did not complete.")
else:
    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        error_message=f"Unexpected Bronze status: {previous_status}",
    )

    raise RuntimeError(f"Unexpected Bronze status: {previous_status}")

# Skip if Silver has already successfully processed this exact source file --
# without this check (and without the stage= filter on already_processed),
# Silver would reprocess and re-MERGE on every run for the same day.
if context.control.already_processed(
    pipeline_name,
    SOURCE_FILE,
    stage="silver",
):
    context.logger.info(f"Silver already processed for: {SOURCE_FILE}")

    context.control.skip_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
    )

    if IS_DATABRICKS:
        dbutils.notebook.exit("SKIPPED")
    else:
        sys.exit(0)

context.control.start_run(
    pipeline_name=pipeline_name,
    pipeline_type=pipeline_type,
    run_id=run_id,
    execution_id=context.audit.get_execution_id(),
    stage="silver",
    source_file=SOURCE_FILE,
)
context.logger.debug(f"Resolved paths: {paths}")


if IS_DATABRICKS:
    BRONZE_TABLE = paths["bronze_table"]
    SILVER_TABLE = paths["silver_table"]
    RECON_TABLE = paths["reconciliation_table"]
    AUDIT_TABLE = paths["audit_table"]
else:
    BRONZE_PATH = paths["bronze"]
    SILVER_PATH = paths["silver"]
    RECON_PATH = paths["reconciliation"]
    AUDIT_PATH = paths["audit"]

silver_target = SILVER_TABLE if IS_DATABRICKS else SILVER_PATH
audit_target = AUDIT_TABLE if IS_DATABRICKS else AUDIT_PATH

# COMMAND ----------


timer_start = time.time()

try:

    if IS_DATABRICKS:
        bronze_df = spark.table(BRONZE_TABLE).filter(
            F.col("_source_file") == SOURCE_FILE
        )
    else:
        bronze_df = (
            spark.read.format("delta")
            .load(BRONZE_PATH)
            .filter(F.col("_source_file") == SOURCE_FILE)
        )

    silver_candidate, dq_dropped = silver_data_quality_gate(
        bronze_df,
        pipeline_name=pipeline_name,
        quarantine_manager=context.quarantine,
    )

    before_ct = bronze_df.count()
    after_ct = silver_candidate.count()

    conservation_ok = before_ct == after_ct + dq_dropped

    if not conservation_ok:
        context.logger.warning(
            f"ROW CONSERVATION CHECK FAILED at silver DQ gate: "
            f"before={before_ct}, after={after_ct}, dq_dropped={dq_dropped}"
        )
    else:
        context.logger.info(
            f"Row conservation OK: "
            f"{before_ct:,} = {after_ct:,} clean + {dq_dropped:,} quarantined"
        )

    to_merge, new_ct, changed_ct, unchanged_ct = silver_change_detection(
        silver_candidate,
        silver_target,
        spark,
    )

    if new_ct == 0 and changed_ct == 0:
        audit_record = context.audit.finish_run(
            stage="silver",
            rows_read=before_ct,
            rows_written=0,
            rows_rejected=dq_dropped,
            source_path=BRONZE_TABLE if IS_DATABRICKS else BRONZE_PATH,
            target_path=silver_target,
            metadata={
                "new_rows": 0,
                "changed_rows": 0,
                "unchanged_rows": int(unchanged_ct),
                "reprocessing_reduction_pct": 100.0,
            },
            timer_start=timer_start,
        )

        context.audit.write_record(
            audit_path=audit_target,
            record=audit_record,
            is_databricks=IS_DATABRICKS,
        )

        context.logger.info("No business changes detected.")

        context.control.finish_run(
            pipeline_name=pipeline_name,
            run_id=run_id,
            rows_read=before_ct,
            rows_written=0,
            duration_seconds=round(time.time() - timer_start),
        )

        if IS_DATABRICKS:
            dbutils.notebook.exit("SUCCESS")
        else:
            sys.exit(0)

    context.logger.info(
        f"NEW: {new_ct:,} | "
        f"CHANGED: {changed_ct:,} | "
        f"UNCHANGED: {unchanged_ct:,}"
    )

    context.logger.info("=" * 60)
    context.logger.info("SOURCE COLUMNS")
    context.logger.info(to_merge.columns)
    context.logger.info("=" * 60)
    context.logger.info("TARGET COLUMNS")

    if IS_DATABRICKS:

        if spark.catalog.tableExists(SILVER_TABLE):
            spark.table(SILVER_TABLE).printSchema()
        else:
            context.logger.info("Silver table does not exist yet.")

    else:

        if DeltaTable.isDeltaTable(spark, silver_target):
            spark.read.format("delta").load(silver_target).printSchema()
        else:
            context.logger.info("Silver Delta table does not exist yet.")

    context.logger.info("=" * 60)

    delta_merge(
        spark,
        to_merge,
        silver_target,
    )

    incremental_volume = new_ct + changed_ct

    reduction_pct = (
        round((1 - incremental_volume / before_ct) * 100, 2) if before_ct else None
    )

    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("snapshot_day", LongType(), False),
            StructField("total_rows_in_snapshot", LongType(), False),
            StructField("new_rows", LongType(), False),
            StructField("changed_rows", LongType(), False),
            StructField("unchanged_rows_skipped", LongType(), False),
            StructField("incremental_volume_processed", LongType(), False),
            StructField("reprocessing_reduction_pct", DoubleType(), True),
            StructField("dq_dropped_rows", LongType(), False),
            StructField("row_conservation_passed", BooleanType(), False),
        ]
    )

    recon_row = spark.createDataFrame(
        [
            {
                "run_id": run_id,
                "snapshot_day": int(snapshot_day),
                "total_rows_in_snapshot": int(before_ct),
                "new_rows": int(new_ct),
                "changed_rows": int(changed_ct),
                "unchanged_rows_skipped": int(unchanged_ct),
                "incremental_volume_processed": int(incremental_volume),
                "reprocessing_reduction_pct": (
                    float(reduction_pct) if reduction_pct is not None else None
                ),
                "dq_dropped_rows": int(dq_dropped),
                "row_conservation_passed": bool(conservation_ok),
            }
        ],
        schema=schema,
    ).withColumn("run_ts", F.current_timestamp())

    if IS_DATABRICKS:
        recon_row.write.mode("append").saveAsTable(RECON_TABLE)
    else:
        (recon_row.write.mode("append").format("delta").save(RECON_PATH))

    context.logger.info(f"Reprocessing reduction vs. full reload: {reduction_pct}%")

    audit_record = context.audit.finish_run(
        stage="silver",
        rows_read=before_ct,
        rows_written=incremental_volume,
        rows_rejected=dq_dropped,
        source_path=BRONZE_TABLE if IS_DATABRICKS else BRONZE_PATH,
        target_path=silver_target,
        metadata={
            "new_rows": int(new_ct),
            "changed_rows": int(changed_ct),
            "unchanged_rows": int(unchanged_ct),
            "reprocessing_reduction_pct": reduction_pct,
        },
        timer_start=timer_start,
    )

    context.audit.write_record(
        audit_path=audit_target,
        record=audit_record,
        is_databricks=IS_DATABRICKS,
    )
    context.control.finish_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        rows_read=before_ct,
        rows_written=incremental_volume,
        duration_seconds=round(time.time() - timer_start),
    )
    context.logger.pipeline_completed()

except Exception as exc:

    failed_record = context.audit.fail_run(
        stage="silver",
        exception=exc,
        timer_start=timer_start,
    )

    context.audit.write_record(
        audit_path=audit_target,
        record=failed_record,
        is_databricks=IS_DATABRICKS,
    )
    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        duration_seconds=round(time.time() - timer_start),
    )

    context.logger.pipeline_failed(str(exc))
    context.logger.exception(f"Silver processing failed: {exc}")

    raise


if IS_DATABRICKS:
    dbutils.notebook.exit(str(reduction_pct))
else:
    context.logger.info("Silver processing completed successfully.")