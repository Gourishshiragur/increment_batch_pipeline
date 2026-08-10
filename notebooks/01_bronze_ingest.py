# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Landing Telemetry Ingestion
# MAGIC Reads the daily snapshot CSV extract, appends ingestion metadata, and writes
# MAGIC to a Delta Bronze table with schema enforcement + audit logging.

# COMMAND ----------

# DBTITLE 1,Cell 2

import os
import sys
import json
import time
from pathlib import Path

# Local only: add project root for imports
if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
    PROJECT_ROOT = Path.cwd()

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

from utils.config_loader import get_storage_path

# ------------------------------------------------------------------
# Report directory
# ------------------------------------------------------------------


if os.getenv("DATABRICKS_RUNTIME_VERSION"):
    REPORT_DIR = Path(get_storage_path("reports"))
else:
    REPORT_DIR = Path.cwd() / "reports"

REPORT_DIR.mkdir(parents=True, exist_ok=True)


def write_stage_status(status: str, rows: int = 0):

    with open(
        REPORT_DIR / f"bronze_day{snapshot_day}_status.json",
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            {
                "stage": "bronze",
                "snapshot_day": int(snapshot_day),
                "status": status,
                "rows_written": rows,
            },
            f,
            indent=4,
        )


from src.pipeline.pipeline_core_spark import bronze_processing

from src.framework.context import FrameworkContext
from utils.config_loader import (
    get_paths,
    get_environment,
    get_pipeline_name,
    get_metadata,
)

paths = get_paths()
environment = get_environment()
pipeline_name = get_pipeline_name()
metadata = get_metadata()


IS_DATABRICKS = environment == "databricks"

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


if IS_DATABRICKS:

    dbutils.widgets.text("snapshot_day", "0")
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
# Auto-discovery: Find next unprocessed snapshot file
# ------------------------------------------------------------------

if snapshot_day == "auto":
    import re
    from pyspark.sql.functions import col
    
    landing_path = paths['landing']
    
    # List all snapshot files in landing folder
    try:
        if IS_DATABRICKS:
            files = dbutils.fs.ls(landing_path)
            snapshot_files = [f.name for f in files if re.match(r'snapshot_day\d+\.csv$', f.name)]
        else:
            landing_dir = Path(landing_path)
            snapshot_files = [f.name for f in landing_dir.glob('snapshot_day*.csv')]
        
        if not snapshot_files:
            print("No snapshot files found in landing folder. Exiting.")
            if IS_DATABRICKS:
                dbutils.notebook.exit("NO_FILES")
            else:
                sys.exit(0)
        
        # Extract day numbers and sort
        day_numbers = []
        for filename in snapshot_files:
            match = re.match(r'snapshot_day(\d+)\.csv$', filename)
            if match:
                day_numbers.append(int(match.group(1)))
        
        day_numbers.sort()
        
        # Create minimal context just to check control table
        temp_context = FrameworkContext(
            spark=spark,
            pipeline_name=pipeline_name,
            pipeline_type=metadata["pipeline"]["type"],
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
        
        # Find first unprocessed file
        unprocessed_day = None
        for day in day_numbers:
            source_filename = f"snapshot_day{day}.csv"
            if not temp_context.control.already_processed(
                pipeline_name,
                source_filename,
                stage="bronze",
            ):
                unprocessed_day = day
                break
        
        if unprocessed_day is None:
            print(f"All {len(day_numbers)} snapshot files already processed. Exiting.")
            if IS_DATABRICKS:
                dbutils.notebook.exit("ALL_PROCESSED")
            else:
                sys.exit(0)
        
        snapshot_day = str(unprocessed_day)
        print(f"Auto-discovery: Found {len(day_numbers)} snapshot file(s). Processing snapshot_day={snapshot_day}")
        
    except Exception as e:
        print(f"Auto-discovery failed: {e}")
        raise


context = FrameworkContext(
    spark=spark,
    pipeline_name=pipeline_name,
    pipeline_type=metadata["pipeline"]["type"],
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
context.logger.debug(f"Resolved paths: {paths}")
LANDING_PATH = f"{paths['landing']}/snapshot_day{snapshot_day}.csv"
SOURCE_FILE = Path(LANDING_PATH).name

context.logger.info(f"Checking source file : {SOURCE_FILE}")
control_target = paths["control_table"] if IS_DATABRICKS else paths["control"]

context.logger.info(f"Control Target : {control_target}")

context.logger.pipeline_started()

run_id = context.audit.start_run()

# Define target paths BEFORE skip check so they're always available
if IS_DATABRICKS:
    BRONZE_TABLE = paths["bronze_table"]
    AUDIT_TABLE = paths["audit_table"]
else:
    BRONZE_PATH = paths["bronze"]
    AUDIT_PATH = paths["audit"]

if context.control.already_processed(
    pipeline_name,
    SOURCE_FILE,
    stage="bronze",
):
    context.logger.info(f"Skipping already processed source: {SOURCE_FILE}")

    write_stage_status("SKIPPED", 0)

    if IS_DATABRICKS:
        dbutils.notebook.exit("SKIPPED")
    else:
        sys.exit(0)

context.control.start_run(
    pipeline_name=pipeline_name,
    pipeline_type=metadata["pipeline"]["type"],
    run_id=run_id,
    execution_id=context.audit.get_execution_id(),
    stage="bronze",
    source_file=SOURCE_FILE,
)

# COMMAND ----------


bronze_target = BRONZE_TABLE if IS_DATABRICKS else BRONZE_PATH
audit_target = AUDIT_TABLE if IS_DATABRICKS else AUDIT_PATH

# quarantine/schema_history paths are Volume/filesystem paths in both
# environments -- get_paths() already resolves the right one per env.


timer_start = time.time()

try:
    bronze_df, record_count, landing_total_count, malformed_count, conservation_ok = (
        bronze_processing(
            spark=spark,
            landing_path=LANDING_PATH,
            bronze_path=bronze_target,
            audit_path=audit_target,
            snapshot_day=int(snapshot_day),
            pipeline_name=pipeline_name,
            schema_history=context.schema_history,
            quarantine_manager=context.quarantine,
        )
    )
except Exception as exc:

    failed_record = context.audit.fail_run(
        stage="bronze",
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
    )

    context.logger.pipeline_failed(str(exc))
    context.logger.exception(f"Bronze ingestion failed: {exc}")
    write_stage_status("FAILED", 0)
    raise

context.logger.info(f"Landing rows          : {landing_total_count:,}")
context.logger.info(f"Bronze rows       : {record_count:,}")
context.logger.info(f"Quarantined rows  : {malformed_count:,}")


if conservation_ok:
    context.logger.info("Row conservation check passed.")
else:
    context.logger.warning("Row conservation check failed.")

context.logger.info("Bronze Preview")

if IS_DATABRICKS:
    display(bronze_df.limit(10))
else:
    bronze_df.show(10, truncate=False)

bronze_df.printSchema()


context.logger.info(f"Rows Read : {record_count:,}")


# COMMAND ----------


audit_record = context.audit.finish_run(
    stage="bronze",
    rows_read=landing_total_count,
    rows_written=record_count,
    rows_rejected=malformed_count,
    source_path=LANDING_PATH,
    target_path=bronze_target,
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
    rows_read=landing_total_count,
    rows_written=record_count,
    duration_seconds=round(time.time() - timer_start),
)
context.logger.info("Control table updated.")
context.logger.pipeline_completed()
write_stage_status(
    "PROCESSED",
    record_count,
)

if IS_DATABRICKS:
    dbutils.notebook.exit(str(record_count))
else:
    context.logger.info(f"Rows Ingested : {record_count:,}")