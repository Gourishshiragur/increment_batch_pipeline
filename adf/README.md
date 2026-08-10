# ADF orchestration — reference template, not a verified deployment

**Read this before using anything in this folder.** These two JSON files are a realistic,
standard-shape ADF pipeline + linked service definition for orchestrating this project's
four Databricks notebooks. They were written as a reference pattern and **have not been
deployed against a live Azure Data Factory + Azure Databricks environment** as part of this
project. There is no Azure subscription behind this repo right now. Be accurate about that
distinction if this comes up in an interview: "I wrote an ADF pipeline definition following
the standard pattern, but haven't deployed/tested it against live Azure resources" is honest
and still demonstrates real knowledge. Claiming it's running in production when it isn't is
the one thing that would turn this from a reasonable portfolio artifact into a lie.

## What would actually need to be true for this to work

1. **An Azure subscription** with a Data Factory instance created.
2. **An Azure Databricks workspace** — note this project currently runs on **Databricks
   Free Edition**, which is not the same product as Azure Databricks (the Azure-native,
   paid, ADF-integrable version). `AzureDatabricks` as an ADF linked-service type
   specifically expects an Azure Databricks workspace, not Free Edition. Whether Free
   Edition can be targeted via ADF's generic REST/Web activity types instead (calling the
   Databricks Jobs REST API directly rather than using the native `DatabricksNotebook`
   activity type) is an open question this project hasn't tested.
3. **A personal access token** generated in the target Databricks workspace, stored in
   **Azure Key Vault** (referenced by `databricks_linked_service.json` — using Key Vault
   rather than a plaintext token in the linked service is the standard security practice,
   not optional).
4. Every `<your-user>` and `<your-workspace>` placeholder in both JSON files replaced with
   real values matching your actual workspace and notebook paths.
5. The notebook paths (`/Workspace/Users/.../notebooks/01_bronze_ingest`, etc.) must exactly
   match where the notebooks actually live in that workspace.

## What this reference pattern gets right (worth explaining if asked)
- Sequential `dependsOn` with `"Succeeded"` conditions, matching the pipeline's real
  Bronze → Silver → Gold → Validation ordering
- `snapshot_day` / `run_id` / `execution_id` passed as pipeline parameters through to each
  notebook's `baseParameters` — the same shared-run-id mechanism the notebooks themselves
  already support for multi-task orchestration (this part of the design is real and tested,
  just via Databricks Jobs rather than ADF)
- Key Vault-backed credential storage instead of an inline token

## What's NOT here, and would be needed for a genuinely production ADF setup
- A trigger (schedule or event-based) — this pipeline definition has none configured
- Failure notification/alerting (e.g. a Logic App or webhook on pipeline failure)
- A tumbling window or parameterized schedule to drive `snapshot_day` automatically, rather
  than needing a human or a separate process to supply it
- Monitoring/Log Analytics integration

**Bottom line:** Databricks Jobs (used elsewhere in this project) is the actually-verified
orchestration mechanism. This folder is a demonstration of ADF pipeline design knowledge,
clearly labeled as such, not a second production deployment path.
