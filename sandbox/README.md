# Sandbox: run the accelerator without real System Tables

The accelerator only reads six `system.*` tables. This folder fakes them with realistic
synthetic data and redirects the reads, so the repo's own code runs unmodified.

| File | What it does |
| --- | --- |
| `finops_sandbox.py` | Synthetic System Table generator + `RedirectingSpark` proxy + `run_accelerator()` |
| `run_local.py` / `run_local.ps1` | Full end-to-end run on your laptop (PySpark + Delta) |
| `run_on_databricks.py` | Databricks notebook (Free Edition friendly) doing the same thing |
| `local_output/` | CSV export of every output table/view from the last local run |

## Run locally

```powershell
.\sandbox\run_local.ps1          # generate data, run pipeline, print results, export CSVs
$env:PYTHONPATH="src"; .\.venv\Scripts\python.exe -m pytest -q   # repo unit tests
```

Local runtime installed for this: portable Python 3.11 (`%LOCALAPPDATA%\Programs\python311-portable`),
Temurin JDK 17 (`%LOCALAPPDATA%\Programs\jdk17`), winutils/hadoop.dll (`%LOCALAPPDATA%\Programs\hadoop`),
and `.venv` with pyspark 3.5.3, delta-spark 3.2.1, pandas, pytest, pyyaml.

## Run on Databricks Free Edition

1. Upload this whole repo folder to your workspace (Workspace → Import → `finops-upload.zip`,
   or `databricks workspace import-dir`).
2. Open `sandbox/run_on_databricks` and click **Run all** on serverless compute.
3. Outputs land in `workspace.finops_accelerator`; build a dashboard from the `vw_*` views.

## Synthetic workloads and what they trigger

| Workload | Designed outcome |
| --- | --- |
| `nightly_sales_etl` (job 1001) | HEALTHY_EXPENSIVE / LOOKS_HEALTHY |
| `ml_feature_training` (job 1002) | HIGH_COST_LOW_UTILIZATION (CPU 7%, mem 22%) |
| `customer_360_build` (job 1003) | MEMORY_BOUND (CPU 15%, mem 88%), missing cost_center |
| `flaky_vendor_ingest` (job 1004) | FAILURE_HEAVY (~40% failures), missing owner/team |
| `finance_reconcile` (job 1005) | Task retries (see known issue below) |
| `serverless_daily_report` (job 1006) | FALLBACK_DBU_PRICE (SKU has no list price) |
| `0101-shared-ds-cluster` | SHARED_CLUSTER_ATTRIBUTION, low utilization, 3 users |
| `wh-bi-001` SQL warehouse | GOOD tagging via alias keys (`Team`, `CostCenter`, `env`) |
| `pl-orders-dlt` pipeline | PIPELINE workload |
| `nb-7777` notebook | MISSING tags |
| model serving (no metadata) | UNKNOWN attribution |

## Known issue found in the upstream repo

`reliability.py::_retry_cte` groups `system.lakeflow.job_task_run_timeline` by `run_id`, then
joins it to job runs on `run_id`. In the real table `run_id` is the **task** run id and the
parent job run is `job_run_id`, so `retry_count` is always 0. Fix: group/join on `job_run_id`
and count `COUNT(*) - COUNT(DISTINCT task_key)` per job run.
