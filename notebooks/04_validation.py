# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Pipeline Validation
# MAGIC %md
# MAGIC # 04 Validation - Enterprise Pipeline Validation
# MAGIC
# MAGIC ## Validates:
# MAGIC
# MAGIC ✓ Bronze / Silver / Gold / Audit / Reconciliation / Control / Schema
# MAGIC   History / Quarantine tables all exist
# MAGIC ✓ Row count consistency (Bronze ≥ Silver, neither Silver nor Gold empty)
# MAGIC ✓ Control table's own bookkeeping is self-consistent (rows_written ≤
# MAGIC   rows_read, latest run for this file's Gold stage is SUCCESS)
# MAGIC ✓ Reconciliation row-conservation check passed at the Silver DQ gate
# MAGIC ✓ Every stage (bronze/silver/gold) has a successful audit record for
# MAGIC   this run
# MAGIC ✓ Quarantine row count matches the reconciliation table's dq_dropped
# MAGIC   count
# MAGIC
# MAGIC ## Outputs:
# MAGIC
# MAGIC `<reports folder>/validation_report.json`

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
from __future__ import annotations

import os
import re
import sys
import json
import time
from pathlib import Path
from datetime import datetime, UTC

# Local only: add project root for imports
if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
    PROJECT_ROOT = Path.cwd()
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.framework.context import FrameworkContext
from utils.config_loader import (
    get_paths,
    get_environment,
    get_config,
    get_metadata,
    get_pipeline_name,
)
from src.framework.constants import (
    STATUS_SUCCESS,
    STATUS_SKIPPED,
    STATUS_FAILED,
    STATUS_STARTED,
)

config = get_config()
metadata = get_metadata()
pipeline_type = metadata.get("pipeline", {}).get("type", "batch")
paths = get_paths()
environment = get_environment()
pipeline_name = get_pipeline_name()
IS_DATABRICKS = environment == "databricks"

# COMMAND ----------

# DBTITLE 1,Initialize Spark and Widgets
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

# run_id / execution_id are optional -- when this notebook is one task in an
# orchestrated multi-task job, the job passes the SAME run_id/execution_id to
# every stage so Bronze/Silver/Gold/Validation all write audit + control rows
# under one shared identifier. Left blank, this stage generates its own.
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

# COMMAND ----------

# DBTITLE 1,Auto-Discovery Logic
# snapshot_day="auto" validates whichever source file the most recent
# successful Bronze run actually processed, instead of requiring the
# caller to know the day number.
if snapshot_day == "auto":

    control_target = paths["control_table"] if IS_DATABRICKS else paths["control"]

    if IS_DATABRICKS:
        recent_bronze = spark.sql(f"""
            SELECT source_file
            FROM {control_target}
            WHERE pipeline_name = '{pipeline_name}'
              AND stage = 'bronze'
              AND status = 'SUCCESS'
            ORDER BY updated_at DESC
            LIMIT 1
            """).collect()
    else:
        dt = DeltaTable.forPath(spark, control_target)
        recent_bronze = (
            dt.toDF()
            .filter(
                (F.col("pipeline_name") == pipeline_name)
                & (F.col("stage") == "bronze")
                & (F.col("status") == "SUCCESS")
            )
            .orderBy(F.col("updated_at").desc())
            .limit(1)
            .collect()
        )

    if not recent_bronze:
        print("No successful bronze runs found. Exiting.")
        if IS_DATABRICKS:
            dbutils.notebook.exit("NO_BRONZE_RUN")
        else:
            sys.exit(0)

    source_file = recent_bronze[0]["source_file"]
    match = re.match(r"snapshot_day(\d+)\.csv$", source_file)

    if not match:
        raise ValueError(
            f"Could not parse snapshot_day from source_file: {source_file}"
        )

    snapshot_day = match.group(1)
    print(
        f"Auto-discovery: using snapshot_day={snapshot_day} from most recent bronze run"
    )

# COMMAND ----------

# DBTITLE 1,Initialize Framework Context
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
context.logger.debug(f"Resolved paths: {paths}")

run_id = context.audit.start_run()
SOURCE_FILE = f"snapshot_day{snapshot_day}.csv"

# COMMAND ----------

# DBTITLE 1,Check Previous Stage Status
previous_status = context.control.last_stage_status(
    pipeline_name=pipeline_name,
    source_file=SOURCE_FILE,
    stage="gold",
)

if previous_status == STATUS_SUCCESS:
    pass
elif previous_status == STATUS_SKIPPED:
    context.logger.info("Gold skipped. Skipping Validation.")
    context.control.skip_run(pipeline_name=pipeline_name, run_id=run_id)
    if IS_DATABRICKS:
        dbutils.notebook.exit("SKIPPED")
    else:
        sys.exit(0)
elif previous_status == STATUS_FAILED:
    context.control.fail_run(
        pipeline_name=pipeline_name, run_id=run_id, error_message="Gold stage failed."
    )
    raise RuntimeError("Gold stage failed.")
elif previous_status == STATUS_STARTED:
    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        error_message="Gold stage incomplete.",
    )
    raise RuntimeError("Gold stage incomplete.")
else:
    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        error_message=f"Unexpected Gold status: {previous_status}",
    )
    raise RuntimeError(f"Unexpected Gold status: {previous_status}")

# COMMAND ----------

# DBTITLE 1,Check Idempotency
if context.control.already_processed(pipeline_name, SOURCE_FILE, stage="validation"):
    context.logger.info(f"Validation already processed for: {SOURCE_FILE}")
    context.control.skip_run(pipeline_name=pipeline_name, run_id=run_id)
    if IS_DATABRICKS:
        dbutils.notebook.exit("SKIPPED")
    else:
        sys.exit(0)

context.control.start_run(
    pipeline_name=pipeline_name,
    pipeline_type=pipeline_type,
    run_id=run_id,
    execution_id=context.audit.get_execution_id(),
    stage="validation",
    source_file=SOURCE_FILE,
)

# COMMAND ----------

# DBTITLE 1,Setup Paths and Helper Functions
if IS_DATABRICKS:
    BRONZE_PATH = paths["bronze_table"]
    SILVER_PATH = paths["silver_table"]
    GOLD_PATH = paths["gold_table"]
    RECON_PATH = paths["reconciliation_table"]
    CONTROL_PATH = paths["control_table"]
    AUDIT_PATH = paths["audit_table"]
    SCHEMA_PATH = paths["schema_history_table"]
else:
    BRONZE_PATH = paths["bronze"]
    SILVER_PATH = paths["silver"]
    GOLD_PATH = paths["gold"]
    RECON_PATH = paths["reconciliation"]
    CONTROL_PATH = paths["control"]
    AUDIT_PATH = paths["audit"]
    SCHEMA_PATH = paths["schema_history"]

# Quarantine is written via QuarantineManager.quarantine()/.initialize(),
# which always uses a raw Delta PATH write (.save(), never .saveAsTable())
# on both Databricks and local -- it's intentionally never registered as a
# catalog table, so it must always be existence-checked as a path
# (delta_exists), never as a catalog table (table_exists/tableExists).
QUARANTINE_PATH = paths["quarantine"]

REPORT_DIR = paths.get("reports", "reports")
os.makedirs(REPORT_DIR, exist_ok=True)


def delta_exists(path):
    if path is None:
        return False
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def table_exists(path):
    if path is None:
        return False
    if IS_DATABRICKS:
        return spark.catalog.tableExists(path)
    return delta_exists(path)


def read_table(path):
    if IS_DATABRICKS:
        return spark.table(path)
    return spark.read.format("delta").load(path)


# COMMAND ----------

# DBTITLE 1,Run Validation Checks
start_time = time.time()

try:
    validation = {}
    validation["execution_time"] = datetime.now(UTC).isoformat()

    # ------------------------------------------------------------------
    # Table existence
    # ------------------------------------------------------------------
    validation["bronze_exists"] = table_exists(BRONZE_PATH)
    validation["silver_exists"] = table_exists(SILVER_PATH)
    validation["gold_exists"] = table_exists(GOLD_PATH)
    validation["reconciliation_exists"] = table_exists(RECON_PATH)
    validation["control_exists"] = table_exists(CONTROL_PATH)
    validation["audit_exists"] = table_exists(AUDIT_PATH)
    validation["schema_history_exists"] = table_exists(SCHEMA_PATH)
    validation["quarantine_exists"] = delta_exists(QUARANTINE_PATH)

    errors = []
    for label, key in [
        ("Bronze table missing", "bronze_exists"),
        ("Silver table missing", "silver_exists"),
        ("Gold table missing", "gold_exists"),
        ("Reconciliation table missing", "reconciliation_exists"),
        ("Control table missing", "control_exists"),
        ("Audit table missing", "audit_exists"),
        ("Schema history missing", "schema_history_exists"),
        ("Quarantine table missing", "quarantine_exists"),
    ]:
        if not validation[key]:
            errors.append(label)

    # ------------------------------------------------------------------
    # Row counts
    # ------------------------------------------------------------------
    bronze_rows = read_table(BRONZE_PATH).count() if validation["bronze_exists"] else 0
    silver_rows = read_table(SILVER_PATH).count() if validation["silver_exists"] else 0
    gold_rows = read_table(GOLD_PATH).count() if validation["gold_exists"] else 0

    validation["bronze_rows"] = bronze_rows
    validation["silver_rows"] = silver_rows
    validation["gold_rows"] = gold_rows

    if bronze_rows < silver_rows:
        errors.append("Silver contains more rows than Bronze.")
    if silver_rows == 0:
        errors.append("Silver table empty.")
    if gold_rows == 0:
        errors.append("Gold table empty.")

    # ------------------------------------------------------------------
    # Control table cross-check -- filtered by THIS source_file + the gold
    # stage + SUCCESS, so this can't accidentally pick up a different
    # snapshot day's row, a different stage's row, or Validation's own
    # just-inserted STARTED row.
    # ------------------------------------------------------------------
    current_run_id = None

    if validation["control_exists"]:
        latest_rows = (
            read_table(CONTROL_PATH)
            .filter(F.col("pipeline_name") == pipeline_name)
            .filter(F.col("source_file") == SOURCE_FILE)
            .filter(F.col("stage") == "gold")
            .filter(F.col("status") == "SUCCESS")
            .orderBy(F.desc("updated_at"))
            .limit(1)
            .collect()
        )

        if latest_rows:
            latest = latest_rows[0]
            current_run_id = latest["run_id"]

            validation["current_run_id"] = current_run_id
            validation["control_status"] = latest["status"]
            validation["control_source_file"] = latest["source_file"]
            validation["control_rows_read"] = latest["rows_read"]
            validation["control_rows_written"] = latest["rows_written"]

            if (
                latest["rows_written"] is not None
                and latest["rows_read"] is not None
                and latest["rows_written"] > latest["rows_read"]
            ):
                errors.append("Control table reports more rows written than rows read.")
        else:
            errors.append(
                f"No successful gold-stage control record found for {SOURCE_FILE}."
            )

    # ------------------------------------------------------------------
    # Reconciliation cross-check -- row conservation at the Silver DQ gate.
    #
    # Filtered by snapshot_day, NOT run_id: each stage (bronze/silver/gold/
    # validation) generates its OWN run_id when run independently (outside
    # a shared-run_id orchestrated job), so reconciliation -- written by
    # Silver, tagged with Silver's run_id -- will essentially never match
    # a run_id sourced from Gold's control row. snapshot_day is the one
    # identifier that's reliably the same across every stage for a given
    # source file.
    # ------------------------------------------------------------------
    if validation["reconciliation_exists"]:
        recon_rows = (
            read_table(RECON_PATH)
            .filter(F.col("snapshot_day") == int(snapshot_day))
            .orderBy(F.desc("run_ts"))
            .limit(1)
            .collect()
        )

        if recon_rows:
            recon = recon_rows[0]
            validation["snapshot_day"] = recon["snapshot_day"]
            validation["snapshot_rows"] = recon["total_rows_in_snapshot"]
            validation["dq_dropped"] = recon["dq_dropped_rows"]
            validation["new_rows"] = recon["new_rows"]
            validation["changed_rows"] = recon["changed_rows"]
            validation["unchanged_rows"] = recon["unchanged_rows_skipped"]
            validation["incremental_volume"] = recon["incremental_volume_processed"]
            validation["reduction_pct"] = recon["reprocessing_reduction_pct"]
            validation["row_conservation_passed"] = recon["row_conservation_passed"]

            if not recon["row_conservation_passed"]:
                errors.append(
                    "Row conservation check failed at silver DQ gate -- rows may have been lost."
                )
        else:
            errors.append(
                f"No reconciliation record found for snapshot_day={snapshot_day}."
            )

    # ------------------------------------------------------------------
    # Stage-success cross-check -- bronze/silver/gold all show SUCCESS for
    # THIS source file. Uses the control table's stage+source_file
    # filtering (the same reliable mechanism already gating each notebook's
    # own execution) instead of the audit table's run_id, for the same
    # reason as above: audit records are tagged with each stage's own
    # independent run_id, so there's no single run_id that finds all three.
    # ------------------------------------------------------------------
    stage_failures = []
    for stage_name in ("bronze", "silver", "gold"):
        stage_status = context.control.last_stage_status(
            pipeline_name=pipeline_name,
            source_file=SOURCE_FILE,
            stage=stage_name,
        )
        if stage_status != STATUS_SUCCESS:
            stage_failures.append(f"{stage_name}={stage_status}")

    validation["stage_statuses_checked"] = ["bronze", "silver", "gold"]
    if stage_failures:
        errors.append(
            f"Not all stages show SUCCESS for {SOURCE_FILE}: {', '.join(stage_failures)}"
        )

    # Audit table row count is still reported for visibility, but is
    # informational only -- it's not reliable as a pass/fail signal without
    # a shared run_id across stages (see comment above).
    if validation["audit_exists"] and current_run_id:
        validation["audit_records_for_gold_run"] = (
            read_table(AUDIT_PATH).filter(F.col("run_id") == current_run_id).count()
        )

    # ------------------------------------------------------------------
    # Schema history row count (informational)
    # ------------------------------------------------------------------
    if validation["schema_history_exists"]:
        validation["schema_versions"] = read_table(SCHEMA_PATH).count()

    # ------------------------------------------------------------------
    # Quarantine cross-check -- should match reconciliation's dq_dropped
    # ------------------------------------------------------------------
    if validation["quarantine_exists"]:
        quarantine_rows = spark.read.format("delta").load(QUARANTINE_PATH).count()
        validation["quarantine_rows"] = quarantine_rows

        if "dq_dropped" in validation and quarantine_rows != validation["dq_dropped"]:
            errors.append(
                "Quarantine row count does not match reconciliation DQ count."
            )

    # ------------------------------------------------------------------
    validation["status"] = "PASS" if not errors else "FAIL"
    validation["errors"] = errors

    # ------------------------------------------------------------------
    # Write the report -- this is the notebook's stated purpose; skipping
    # it would mean nothing downstream can ever consume the result.
    # ------------------------------------------------------------------
    report_file = os.path.join(REPORT_DIR, "validation_report.json")

    with open(report_file, "w") as f:
        json.dump(validation, f, indent=4, default=str)

    context.logger.info(f"Validation report written to: {report_file}")

    print("=" * 60)
    print("PIPELINE VALIDATION")
    print("=" * 60)
    print(f"Overall Status : {validation['status']}")
    print(f"Bronze Rows    : {bronze_rows:,}")
    print(f"Silver Rows    : {silver_rows:,}")
    print(f"Gold Rows      : {gold_rows:,}")
    print("=" * 60)

    if validation["status"] == "PASS":
        context.logger.info("VALIDATION PASSED")
    else:
        context.logger.warning(f"VALIDATION FAILED: {errors}")

except Exception as exc:

    failed_record = context.audit.fail_run(
        stage="validation",
        exception=exc,
        timer_start=start_time,
    )

    context.audit.write_record(
        audit_path=AUDIT_PATH,
        record=failed_record,
        is_databricks=IS_DATABRICKS,
    )

    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        duration_seconds=round(time.time() - start_time),
    )

    context.logger.pipeline_failed(str(exc))
    context.logger.exception(f"Validation failed: {exc}")

    raise

# COMMAND ----------

# DBTITLE 1,Finalize and Exit
# Reflect the ACTUAL validation outcome in the control table, not just
# "the notebook ran without an exception". Always writing SUCCESS here
# (regardless of validation["status"]) was the earlier bug: a real FAIL
# still got recorded as SUCCESS, so the next run's already_processed()
# check found that SUCCESS row and silently skipped re-validating --
# forever, even after the underlying data problem was never actually
# fixed. A FAIL must write STATUS_FAILED so the next run retries instead.

audit_record = context.audit.finish_run(
    stage="validation",
    rows_read=gold_rows,
    rows_written=gold_rows if validation["status"] == "PASS" else 0,
    rows_rejected=0,
    source_path=GOLD_PATH,
    target_path=report_file,
    metadata={"status": validation["status"], "errors": validation["errors"]},
    timer_start=start_time,
)

context.audit.write_record(
    audit_path=AUDIT_PATH,
    record=audit_record,
    is_databricks=IS_DATABRICKS,
)

if validation["status"] == "PASS":
    context.control.finish_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        rows_read=gold_rows,
        rows_written=gold_rows,
        duration_seconds=round(time.time() - start_time),
    )
else:
    context.control.fail_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        error_message="; ".join(validation["errors"])[:2000],
        duration_seconds=round(time.time() - start_time),
    )

context.logger.pipeline_completed()

if IS_DATABRICKS:
    dbutils.notebook.exit(validation["status"])
else:
    context.logger.info(f"Validation completed with status: {validation['status']}")
