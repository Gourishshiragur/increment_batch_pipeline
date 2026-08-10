# Benchmarks — Incremental Batch Lakehouse Pipeline

All numbers below come from actually running `notebooks/04_validation.py` against real
Delta/Unity Catalog tables produced by the pipeline, cross-checked against the control,
audit, and reconciliation tables — not estimated, not simulated, not carried forward from
an earlier version of the pipeline.

## Test data
- Synthetic mining telemetry, generated via `data/generate_snapshots.py` — see
  `DATA_UPLOAD_GUIDE.md` for why synthetic data is used and how to say so honestly if asked
- 5 daily snapshot extracts simulating a realistic rolling re-pull window (prior rows
  carried forward, a fraction corrected/re-transmitted, some aged out of the window, plus
  each day's new readings)

## Measured results

| Day | Full snapshot rows | Rows actually processed (new + changed) | Unchanged rows skipped | Reprocessing reduction | Row conservation | DQ dropped |
|---|---|---|---|---|---|---|
| 3 | 1,959,120 | 134,783 | 1,824,337 | **93.12%** | ✅ passed | 0 |

**Only day 3 has been validated against the current codebase.** Days 0, 1, 2, and 4 need to
be re-run and re-validated before adding them here — the pipeline's change-detection fields
(`TRACKED_FIELDS`) changed during development (GPS/timestamp were removed from tracked
fields; see README "Design decisions"), so any numbers produced before that fix are not
representative of the current pipeline and should not be reused.

**Do not fill in placeholder or estimated numbers for the missing days.** The entire point
of this benchmark file is that every number in it is independently reproducible by re-running
the notebooks and checking `reports/validation_report.json` — a plausible-looking guess
defeats that purpose and would not hold up if someone actually asked to see the run that
produced it.

## How to reproduce
```bash
python3 data/generate_snapshots.py   # regenerates the daily snapshot CSVs

# Per day, in order (Databricks: set the snapshot_day widget; local: pass as argv[1]):
#   01_bronze_ingest.py -> 02_silver_merge_upsert.py -> 03_gold_kpi_aggregation.py -> 04_validation.py

# The authoritative output is reports/validation_report.json -- specifically:
#   reduction_pct, row_conservation_passed, dq_dropped, status, errors
```

## Note on the resume figure
If your resume states a reprocessing-reduction percentage, it should be **at or below** the
lowest *confirmed* number in this table, not an average of numbers you haven't actually
reproduced. Right now that means: don't claim a specific percentage until at least 2–3 days
are validated and the range is known — a single data point (93.12%) is a promising result,
not yet a defensible claimed average.
