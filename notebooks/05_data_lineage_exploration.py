# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Data Lineage & Unity Catalog Exploration
# MAGIC
# MAGIC **Run this manually, on demand.** This is a reporting/exploration
# MAGIC notebook, not part of the automated pipeline -- it isn't gated by any
# MAGIC job task, has no pass/fail contract, and isn't idempotent. Its purpose
# MAGIC is to give a human a readable view of what's in the pipeline's tables:
# MAGIC row-count progression across stages, data-quality/dedup rates, and
# MAGIC Unity Catalog table metadata.
# MAGIC
# MAGIC Requires `utils.config_loader` to be importable (same as the pipeline
# MAGIC notebooks) so catalog/schema/table names come from
# MAGIC `config/pipeline_metadata.json` instead of being hardcoded.

# COMMAND ----------

import os
import sys
from pathlib import Path

if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
    PROJECT_ROOT = Path.cwd()
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import functions as F
from utils.config_loader import get_paths, get_environment

paths = get_paths()
IS_DATABRICKS = get_environment() == "databricks"

if IS_DATABRICKS:
    catalog = paths["catalog"]
    schema = paths["schema"]
    bronze_table = paths["bronze_table"]
    silver_table = paths["silver_table"]
    gold_table = paths["gold_table"]
    control_table = paths["control_table"]
    audit_table = paths["audit_table"]
    recon_table = paths["reconciliation_table"]
else:
    raise RuntimeError(
        "This exploration notebook queries Unity Catalog tables directly "
        "and only makes sense when running on Databricks."
    )

# COMMAND ----------

# DBTITLE 1,Post-Job Validation - Sample Rows From Every Table
print("=" * 80)
print("PIPELINE TABLE VALIDATION")
print("=" * 80)

print("\n1. CONTROL TABLE - Processing History")
print("-" * 80)
spark.table(control_table).select(
    "stage", "source_file", "status", "rows_written", "duration_seconds",
    "start_time", "end_time",
).orderBy(F.desc("start_time")).show(20, truncate=False)

print("\n2. BRONZE TABLE - Sample Data")
print("-" * 80)
spark.table(bronze_table).select(
    "_snapshot_day", "customer_id", "machine_id", "event_ts",
    "fuel_level", "payload_weight_t", "fault_code", "_ingestion_ts",
).orderBy(F.desc("_ingestion_ts")).show(10, truncate=False)

print("\n3. SILVER TABLE - Sample Data")
print("-" * 80)
spark.table(silver_table).select(
    "_snapshot_day", "customer_id", "machine_id", "event_ts",
    "fuel_level", "payload_weight_t", "fault_code", "_processing_ts",
).orderBy(F.desc("_processing_ts")).show(10, truncate=False)

print("\n4. GOLD TABLE - Sample KPIs")
print("-" * 80)
spark.table(gold_table).select(
    "customer_id", "machine_id",
    F.round("avg_fuel_level", 2).alias("avg_fuel"),
    F.round("avg_payload_t", 2).alias("avg_payload"),
    "fault_events", "total_readings", "last_updated_ts",
).orderBy(F.desc("last_updated_ts")).show(10, truncate=False)

print("\n" + "=" * 80)
print("✅ Sample rows pulled from every table.")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Data Lineage & Row Count Validation
print("=" * 80)
print("DATA LINEAGE & FLOW ANALYSIS")
print("=" * 80)

all_days = (
    spark.table(bronze_table)
    .select("_snapshot_day")
    .distinct()
    .orderBy("_snapshot_day")
    .collect()
)
print(f"\nProcessed Snapshot Days: {[row._snapshot_day for row in all_days]}")

print("\n1. ROW COUNT PROGRESSION BY SNAPSHOT DAY")
print("-" * 80)
bronze_counts = (
    spark.table(bronze_table)
    .groupBy("_snapshot_day")
    .agg(F.count("*").alias("bronze_records"))
)
silver_counts = (
    spark.table(silver_table)
    .groupBy("_snapshot_day")
    .agg(F.count("*").alias("silver_records"))
)
lineage_df = (
    bronze_counts.join(silver_counts, on="_snapshot_day", how="outer")
    .withColumn(
        "records_filtered",
        F.col("bronze_records") - F.col("silver_records"),
    )
    .orderBy("_snapshot_day")
)
lineage_df.show(truncate=False)

print("\n2. GOLD LAYER - Customer Summary (Aggregated from All Days)")
print("-" * 80)
spark.table(gold_table).agg(
    F.countDistinct("customer_id").alias("total_customers"),
    F.countDistinct("machine_id").alias("total_machines"),
    F.count("*").alias("total_kpi_records"),
    F.sum("fault_events").alias("total_faults"),
    F.sum("total_readings").alias("total_readings"),
    F.round(F.avg("avg_fuel_level"), 2).alias("avg_fuel_level"),
    F.round(F.avg("avg_payload_t"), 2).alias("avg_payload_weight"),
).show(truncate=False)

print("\n3. DATA QUALITY - Deduplication Rate")
print("-" * 80)
silver_df = spark.table(silver_table)
silver_df.groupBy("_snapshot_day").agg(
    F.count("*").alias("total_records"),
    F.countDistinct("reading_id").alias("unique_readings"),
).withColumn(
    "duplicates_removed", F.col("total_records") - F.col("unique_readings")
).withColumn(
    "uniqueness_pct",
    F.round(100.0 * F.col("unique_readings") / F.col("total_records"), 2),
).orderBy("_snapshot_day").show(truncate=False)

print("\n4. PROCESSING TIMELINE")
print("-" * 80)
spark.table(control_table).filter(F.col("status") == "SUCCESS").select(
    "stage", "source_file", "status",
    F.col("start_time").cast("string").alias("start_time"),
    F.col("end_time").cast("string").alias("end_time"),
    "duration_seconds", "rows_written",
).orderBy(F.desc("start_time")).show(20, truncate=False)

print("\n" + "=" * 80)
print("✅ Data lineage reviewed.")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Unity Catalog Table Info & Lineage Links
print("=" * 80)
print("UNITY CATALOG TABLE INFORMATION")
print("=" * 80)

tables = [bronze_table, silver_table, gold_table, control_table, audit_table, recon_table]

print("\n1. TABLE METADATA")
print("-" * 80)

for full_name in tables:
    try:
        details = spark.sql(f"DESCRIBE DETAIL {full_name}").collect()[0]
        row_count = spark.table(full_name).count()

        print(f"\n✅ {full_name}")
        print(f"   Format: {details.format}")
        print(f"   Location: {details.location}")
        print(f"   Row Count: {row_count:,}")
        print(f"   Created: {details.createdAt}")

    except Exception as e:
        print(f"\n❌ {full_name}: {str(e)[:100]}")

print("\n\n2. HOW TO ACCESS UNITY CATALOG LINEAGE")
print("-" * 80)
print(
    """
To view data lineage in Unity Catalog:

1. Open Catalog Explorer -> Click 'Catalog' in the left sidebar
2. Navigate to your catalog/schema, click on any pipeline table
3. Go to the 'Lineage' tab to see upstream sources, downstream consumers,
   and jobs that read/write the table.
"""
)

print("\n3. PIPELINE FLOW")
print("-" * 80)
print(
    """
CSV Files (landing) -> Bronze (raw ingest, idempotent)
                     -> Silver (dedup, DQ, incremental upsert)
                     -> Gold (business KPIs)
                     -> Validation

Control tables (metadata):
  - control          Tracks processing status per stage/source_file
  - audit            Execution history
  - reconciliation   Row-count / row-conservation validation
"""
)

print("\n" + "=" * 80)
print("✅ Catalog Explorer has the visual lineage view for these tables.")
print("=" * 80)
