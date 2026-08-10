# Incremental Batch Lakehouse Pipeline — Mining Telemetry

Bronze → Silver → Gold medallion pipeline for **daily machine-telemetry ingestion** from
mining/heavy-equipment fleets — the kind of data a logistics or mining operations team uses
to monitor fuel consumption, payload cycles, and fault patterns across a fleet of machines
and customer accounts.

Instead of reloading the full daily snapshot every run, the pipeline detects what actually
changed since the last run, processes only that, and upserts it into a Delta Lake state
table. Runs natively on **Databricks Free Edition** (serverless compute, Unity Catalog),
and also runs entirely locally (PySpark + Delta, no cluster) for fast iteration and CI.

---

## Business context — why this pipeline exists

A heavy-equipment fleet telemetry system emits daily snapshot extracts: the full current
state of every machine's sensor readings for every customer account. These extracts
**re-transmit a large fraction of unchanged prior rows**, because the source system doesn't
track deltas — it dumps the whole rolling window every day.

**Without this pipeline:** a naive full-reload approach reprocesses every row in every
snapshot, including the rows that didn't change at all.

**With this pipeline:** Silver-layer change detection classifies each row as NEW, CHANGED,
or UNCHANGED (comparing against the current Silver state on business-relevant fields —
deliberately excluding GPS/timestamp, which drift on every reading regardless of whether
anything meaningful changed). Only NEW and CHANGED rows are MERGE-upserted. UNCHANGED rows
are skipped entirely. The Gold layer reads from the already-merged state, so downstream
analytics always sees a complete, current view.

---

## Measured results (reproducible — run the validation notebook yourself)

Test data: synthetic mining telemetry, generated via `data/generate_snapshots.py`
(not real customer data — see `DATA_UPLOAD_GUIDE.md`).

| Day | Full snapshot rows | Rows actually processed (new + changed) | Rows skipped | Reprocessing reduction |
|---|---|---|---|---|
| 3 | 1,959,120 | 134,783 | 1,824,337 | **93.12%** |

**This table currently has one confirmed, validated data point (day 3), produced by running
`notebooks/04_validation.py` end-to-end against the real Delta tables and cross-checked
against the control, audit, and reconciliation tables** — not estimated, not simulated.
Days 0–2 and day 4 are pending re-validation against the current codebase; the numbers in
this table will only ever be ones that have actually been produced by a real run and
verified via `04_validation.py`'s `row_conservation_passed` check. Run the notebook
sequence below on additional days and update this table with the real output — don't
carry forward figures from an earlier version of the pipeline once the underlying logic
has changed.

To reproduce:
```bash
python3 data/generate_snapshots.py     # regenerate the daily CSVs
# then, per day, in Databricks or locally:
#   00_environment_setup.py (once)
#   01_bronze_ingest.py  -> 02_silver_merge_upsert.py -> 03_gold_kpi_aggregation.py -> 04_validation.py
# validation_report.json (written to the reports folder) has the authoritative numbers.
```

---

## Pipeline architecture

```
Daily snapshot extract (CSV, per customer account)
        │
        ▼
[00 — Environment setup]   Creates the Unity Catalog Volume + folder structure (once)
        │
        ▼
[01 — Bronze]   Raw append to Delta Bronze table
                Schema enforcement, ingestion metadata (_ingestion_ts, _metadata.file_name)
                Idempotent: re-running for an already-processed file is a no-op (SKIPPED)
                snapshot_day="auto" discovers the next unprocessed file via the control table
        │
        ▼
[02 — Silver]   Data quality gate (drop bad rows -> quarantine table, not silently discarded)
                Change detection vs. prior Silver state:
                  NEW       -> insert
                  CHANGED   -> upsert (fuel_level / payload_weight_t / fault_code differ)
                  UNCHANGED -> skip entirely
                Delta MERGE upsert on reading_id
                Reconciliation record written (row counts, reduction %, conservation check)
        │
        ▼
[03 — Gold]     KPI aggregation per customer per machine per day
                Delta MERGE upsert (idempotent re-runs, not blind overwrite)
        │
        ▼
[04 — Validation]   Cross-checks every table exists, row counts are consistent,
                     reconciliation's row-conservation check passed, every stage
                     (bronze/silver/gold) actually succeeded for this file, quarantine
                     count matches the reconciliation DQ-dropped count.
                     Writes validation_report.json. A genuine FAIL is recorded as such
                     in the control table (not silently marked SUCCESS), so a real
                     problem forces a retry instead of being skipped forever.
        │
        ▼
[05 — Lineage exploration]   Manual/on-demand only, not part of the automated job:
                              row-count progression, dedup rate, Unity Catalog table
                              metadata. Not gated by any pass/fail contract.
```

**Idempotency mechanism:** every stage checks a shared control table
(`pipeline_name` + `stage` + `source_file` + `status`) before running, and records its own
outcome the same way. Re-running any stage for an already-successfully-processed file is a
safe no-op (`SKIPPED`), not a duplicate write.

---

## Running on Databricks Free Edition

1. Run `notebooks/00_environment_setup.py` once — creates the Unity Catalog Volume and
   folder structure.
2. Upload `data/snapshot_day0.csv` … `snapshot_day4.csv` via Catalog Explorer to the
   `landing` folder inside the Volume (see `DATA_UPLOAD_GUIDE.md` for exact steps — Free
   Edition uses Unity Catalog Volumes, not DBFS `/FileStore/`).
3. Run `01_bronze_ingest.py` → `02_silver_merge_upsert.py` → `03_gold_kpi_aggregation.py` →
   `04_validation.py` in order, either manually (set the `snapshot_day` widget) or as an
   orchestrated multi-task Job.
4. Set `snapshot_day` to `"auto"` to have each stage automatically pick up the next
   unprocessed day from the control table, instead of tracking day numbers by hand.

Databricks Free Edition is free at [databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition).
It's serverless-only (no cluster configuration) and Unity-Catalog-governed by default.

---

## Local testing (no Spark cluster needed)

```bash
pip install -r requirements.txt -r requirements-local.txt
python3 data/generate_snapshots.py
python3 notebooks/01_bronze_ingest.py 0
python3 notebooks/02_silver_merge_upsert.py 0
python3 notebooks/03_gold_kpi_aggregation.py 0
python3 notebooks/04_validation.py 0
```

`src/pipeline/pipeline_core_pandas.py` mirrors the Silver change-detection and merge-upsert
logic in plain pandas, so the core business logic has fast, Spark-free unit tests:

```bash
pytest tests/ -v
```

Covers: change classification (NEW/CHANGED/UNCHANGED), data-quality gate drops,
merge-upsert idempotency, and Gold KPI output shape.

---

## CI/CD

`.github/workflows/ci.yml` runs on GitHub's free-tier hosted runners on every push/PR:
the pandas-mirror unit tests, and `black` formatting (enforced) + `flake8` (informational —
several findings are intentional re-export patterns in `__init__.py` files, and `notebooks/`
is deliberately excluded since Databricks-injected globals like `dbutils` aren't real names
outside a running notebook).

A separate, manually-triggered `deploy` job pushes notebooks to a Databricks workspace via
the Databricks CLI, using a personal access token stored as a GitHub secret — see the
comments at the top of that job for exact setup steps. **This deploy job is a reference
implementation and has not been run against a live workspace as part of this change** —
verify the target path and CLI command against your own workspace before relying on it.

---

## Optional: Azure Data Factory orchestration (reference, not the primary path)

`adf/incremental_batch_pipeline.json` is a template ADF pipeline definition that chains the
four notebook stages as Databricks Notebook activities with sequential success dependencies.
**This is provided as a reference pattern, not a verified, deployed integration** — it
requires an actual Azure subscription, a Data Factory instance, and an Azure Databricks
Linked Service with a PAT stored in Key Vault (or ADF's own credential store), none of which
this project currently has configured or tested. Databricks Jobs (used for the notebooks
above) is the primary, actually-verified orchestration mechanism for this project. See
`adf/README.md` for what would need to be true before this could be deployed for real.

---

## Design decisions worth discussing in an interview

**Why comparison-based change detection rather than CDC?**
The source system emits full snapshot extracts, not a CDC stream — it has no change log to
tap into. Comparison-based detection on `reading_id` + tracked business fields is the right
tool when you can't modify the source system.

**Why exclude GPS/timestamp from change detection?**
Early on, `TRACKED_FIELDS` included `event_ts`/`gps_lat`/`gps_lon`, which drift on nearly
every reading regardless of whether anything business-meaningful changed — that inflated
`changed_rows` and understated the real reprocessing-reduction number. Narrowing it to
`fuel_level`/`payload_weight_t`/`fault_code` fixed that; the fix is a good example of a
metric that looked plausible but needed a real data check to catch it being wrong.

**Why Delta MERGE rather than overwrite?**
Full overwrite would lose the history of corrections. MERGE upserts the correction while
preserving audit history and avoids reprocessing anything that hasn't changed.

**What does the control-table `stage` column actually solve?**
Without it, every stage's "has this already been processed?" check could match a *different*
stage's row for the same file (e.g. Silver seeing Bronze's SUCCESS row and skipping itself
before ever running) — a real bug this project hit and fixed, not a hypothetical one.

---

## Stack

Python · PySpark · Delta Lake · Databricks Free Edition · Unity Catalog (managed tables +
Volumes) · pytest · GitHub Actions

---

*The measured number in this README comes from `reports/validation_report.json`, produced
by actually running the pipeline and `04_validation.py` against real Delta tables — not
estimated. Anyone who clones this repo can reproduce it by following "Running on Databricks
Free Edition" or "Local testing" above.*
