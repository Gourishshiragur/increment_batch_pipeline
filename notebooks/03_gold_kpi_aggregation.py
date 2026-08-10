# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Layer: KPI Aggregation
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC Reads the Silver current-state table and aggregates to business-ready
# MAGIC KPIs per customer + machine:
# MAGIC   - avg_fuel_level    — fleet fuel health
# MAGIC   - avg_payload_t     — utilization proxy
# MAGIC   - fault_events      — maintenance signal
# MAGIC   - total_readings    — data completeness indicator
# MAGIC
# MAGIC Applies OPTIMIZE + ZORDER BY (customer_id, machine_id) on the Gold table
# MAGIC so BI tools can do fast point-lookup queries per customer or machine.
# MAGIC
# MAGIC **Business output:**
# MAGIC Gold is what the analytics team queries. A fleet manager sees one row per
# MAGIC machine with current KPIs, updated daily after this notebook runs.

# COMMAND ----------

# DBTITLE 1,Gold Setup
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
    get_environment,
    get_config,
    get_metadata,
    get_pipeline_name,
)

# Load configuration first
pipeline_name = get_pipeline_name()
config = get_config()
metadata = get_metadata()
pipeline_type = metadata.get("pipeline", {}).get("type", "batch")
paths = get_paths()
environment = get_environment()

IS_DATABRICKS = environment == "databricks"


# Project imports
from src.pipeline.pipeline_core_spark import gold_processing
from src.framework.context import FrameworkContext
from src.framework.constants import (
    STATUS_SUCCESS,
    STATUS_SKIPPED,
    STATUS_FAILED,
    STATUS_STARTED,
)

# Local Spark session
if not IS_DATABRICKS:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(pipeline_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.hadoop.native.lib", "false")
        .config("spark.hadoop.io.native.lib.available", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Runtime parameter
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
    from pyspark.sql import functions as F
    
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

SOURCE_FILE = f"snapshot_day{snapshot_day}.csv"

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

previous_status = context.control.last_stage_status(
    pipeline_name=pipeline_name,
    source_file=SOURCE_FILE,
    stage="silver",
)

if previous_status == STATUS_SUCCESS:
    pass

elif previous_status == STATUS_SKIPPED:

    context.logger.info("Silver skipped. Skipping Gold.")

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
        error_message="Silver stage failed.",
    )

    raise RuntimeError("Silver stage failed.")

elif previous_status == STATUS_STARTED:

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        error_message="Silver stage did not complete.",
    )

    raise RuntimeError("Silver stage did not complete.")

else:

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        error_message=f"Unexpected Silver status: {previous_status}",
    )

    raise RuntimeError(f"Unexpected Silver status: {previous_status}")

# Skip if Gold has already successfully processed this exact source file
if context.control.already_processed(
    pipeline_name,
    SOURCE_FILE,
    stage="gold",
):
    context.logger.info(f"Gold already processed for: {SOURCE_FILE}")

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
    stage="gold",
    source_file=SOURCE_FILE,
)
context.logger.debug(f"Resolved paths: {paths}")

if IS_DATABRICKS:
    SILVER_SOURCE = paths["silver_table"]
    GOLD_TARGET = paths["gold_table"]
    AUDIT_TABLE = paths["audit_table"]
else:
    SILVER_SOURCE = paths["silver"]
    GOLD_TARGET = paths["gold"]
    AUDIT_PATH = paths["audit"]

audit_target = AUDIT_TABLE if IS_DATABRICKS else AUDIT_PATH

# COMMAND ----------

context.logger.info("=" * 80)
context.logger.info("GOLD LAYER - KPI AGGREGATION")
context.logger.info("=" * 80)
context.logger.info(f"Source : {SILVER_SOURCE}")

context.logger.info(f"Target : {GOLD_TARGET}")
if IS_DATABRICKS:

    if not spark.catalog.tableExists(SILVER_SOURCE):
        raise RuntimeError(f"Silver table '{SILVER_SOURCE}' does not exist.")

    silver_count = spark.table(SILVER_SOURCE).count()

else:

    silver_count = spark.read.format("delta").load(SILVER_SOURCE).count()

context.logger.info(f"Silver input rows : {silver_count:,}")
start_time = time.time()

# COMMAND ----------

# DBTITLE 1,Gold processing
try:
    if silver_count == 0:
        raise RuntimeError("Silver table contains no rows.")

    gold_df, gold_count = gold_processing(
        spark=spark,
        silver_source=SILVER_SOURCE,
        gold_target=GOLD_TARGET,
    )

    if gold_count == 0:
        raise RuntimeError("Gold aggregation produced no rows.")

    context.logger.info(f"Gold KPI rows : {gold_count:,}")

    gold_df.printSchema()

    if IS_DATABRICKS:
        display(
            gold_df.orderBy(
                "fault_events",
                ascending=False,
            ).limit(20)
        )
    else:
        gold_df.orderBy(
            "fault_events",
            ascending=False,
        ).show(20, truncate=False)
except Exception as exc:

    failed_record = context.audit.fail_run(
        stage="gold",
        exception=exc,
        timer_start=start_time,
    )

    context.audit.write_record(
        audit_path=audit_target,
        record=failed_record,
        is_databricks=IS_DATABRICKS,
    )

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        duration_seconds=round(time.time() - start_time),
    )

    context.logger.pipeline_failed(str(exc))
    context.logger.exception(f"Gold processing failed: {exc}")

    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample business query: top 10 machines by fault events

# COMMAND ----------

context.logger.info("Displaying top 10 machines by fault events.")
top_faults = gold_df.select(
    "customer_id",
    "machine_id",
    "fault_events",
    "avg_fuel_level",
    "avg_payload_t",
).orderBy("fault_events", ascending=False)

if IS_DATABRICKS:
    display(top_faults.limit(10))
else:
    top_faults.show(10, truncate=False)

# COMMAND ----------

elapsed = time.time() - start_time

context.logger.info("=" * 80)
context.logger.info(f"Execution Time : {elapsed:.2f} seconds")
context.logger.info("=" * 80)

context.logger.info(f"Gold processing completed with {gold_count:,} KPI rows.")

audit_record = context.audit.finish_run(
    stage="gold",
    rows_read=silver_count,
    rows_written=gold_count,
    rows_rejected=0,
    source_path=SILVER_SOURCE,
    target_path=GOLD_TARGET,
    timer_start=start_time,
)

context.audit.write_record(
    audit_path=audit_target,
    record=audit_record,
    is_databricks=IS_DATABRICKS,
)
context.logger.info("KPI aggregation completed successfully.")
context.control.finish_run(
    pipeline_name=pipeline_name,
    run_id=run_id,
    rows_read=silver_count,
    rows_written=gold_count,
    duration_seconds=round(time.time() - start_time),
)

context.logger.pipeline_completed()

if IS_DATABRICKS:
    dbutils.notebook.exit(str(gold_count))
else:
    context.logger.info("Gold notebook finished successfully.")