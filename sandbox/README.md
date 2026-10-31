# Sandbox: run the accelerator without real System Tables

The accelerator reads six `system.*` tables, and the cockpit steps read three more
(`compute.clusters`, `access.audit`, `query.history`). This folder fakes all nine with
realistic synthetic data and redirects the reads, so the repo's own code runs unmodified.

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

On macOS (Homebrew `python@3.11` and `openjdk@17`; Spark 3.5 does not run on newer Java):

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home PYTHONPATH=src
.venv/bin/python sandbox/run_local.py
.venv/bin/python -m pytest -q tests app/tests
```

Local runtime installed for this: portable Python 3.11 (`%LOCALAPPDATA%\Programs\python311-portable`),
Temurin JDK 17 (`%LOCALAPPDATA%\Programs\jdk17`), winutils/hadoop.dll (`%LOCALAPPDATA%\Programs\hadoop`),
and `.venv` with pyspark 3.5.3, delta-spark 3.2.1, pandas, pytest, pyyaml.

## Run on Databricks Free Edition

1. Upload this whole repo folder to your workspace (Workspace → Import → `finops-upload.zip`,
   or `databricks workspace import-dir`).
2. Open `sandbox/run_on_databricks` and click **Run all** on serverless compute.
3. Synthetic sources land in `workspace.finops_synthetic` (all nine tables) and outputs in `workspace.cockpit` (rule R4).

## Synthetic workloads and what they trigger

| Workload | Designed outcome |
| --- | --- |
| `nightly_sales_etl` (job 1001) | HEALTHY_EXPENSIVE / LOOKS_HEALTHY. F4 near-miss (CRON on a job cluster), F6 near-miss (3 tasks, no retries) |
| `ml_feature_training` (job 1002) | HIGH_COST_LOW_UTILIZATION (CPU 7%, mem 22%). F6 near-miss: its long `transform` task is sliced across clock hours (several rows, one `run_id`), which is not a retry |
| `customer_360_build` (job 1003) | MEMORY_BOUND (CPU 15%, mem 88%), missing cost_center |
| `flaky_vendor_ingest` (job 1004) | FAILURE_HEAVY (~40% failures), missing owner/team. F5 near-miss: its job cluster reports auto-termination 0 |
| `finance_reconcile` (job 1005) | F6 positive: ~60% of runs retry `transform` (two task `run_id`s, one `job_run_id`) |
| `serverless_daily_report` (job 1006) | FALLBACK_DBU_PRICE (SKU has no list price) |
| `0101-shared-ds-cluster` | SHARED_CLUSTER_ATTRIBUTION, low utilisation, F4 SHARED_USERS positive (3 users) |
| `0202-bi-tableau-cluster` | F4 BI_TOOL_JDBC positive: Tableau JDBC and Power BI ODBC sessions in `access_audit` (see the assumption below) |
| `hourly_kpi_refresh` (job 1007) | F4 SCHEDULED_JOB positive: CRON job on all-purpose cluster `0303-kpi-allpurpose-cluster`. As in real `billing.usage`, its usage has no `job_id` (billed to the cluster), so detect it through `job_run_timeline.compute_ids` joined to `compute.clusters.cluster_source IN ('UI','API')` |
| `0404-zombie-autoterm-off` | F5 positive: on 24h, CPU 3%, auto-termination changed 30 → 0 (latest change wins). F4 near-miss: one user only |
| `0505-zombie-long-autoterm` (dev) | F5 positive: on 24h, CPU 4%, auto-termination 240 min |
| `0707-autoterm-61-cluster` | F5 positive boundary: auto-termination 61 min (over 60), CPU 6% |
| `0101-shared-ds-cluster` (again) | F5 near-miss boundary: auto-termination exactly 60, not flagged |
| `0808-streaming-autoterm-off` | F5 near-miss: auto-termination off and on 24h, but CPU 72% (busy, not idle) |
| `0909-memory-heavy-cluster` | F5 near-miss: CPU 8% but memory 85%, auto-termination 30 (memory-bound, not idle) |
| `0606-deleted-old-cluster` | F5 near-miss: auto-termination 0, live until 2025-03-01, then deleted (latest row has `delete_time`) |
| `adhoc_backfill` (job 1008) | F4 near-miss: weekly manual (`ONETIME`) run on the shared all-purpose cluster; adds no billing of its own |
| `wh-bi-001` SQL warehouse | GOOD tagging via alias keys (`Team`, `CostCenter`, `env`). F3 positive: queries span sales, finance and marketing |
| `wh-fin-002` SQL warehouse | F3 near-miss: only queries the finance catalog, so 100% finance |
| `pl-orders-dlt` pipeline | PIPELINE workload |
| `nb-7777` notebook | MISSING tags |
| model serving (no metadata) | UNKNOWN attribution |

The original workloads (`WORKLOADS`, the same 12 as on `development`) and the F12 scenarios
(`F12_WORKLOADS`) use separate seeded RNGs. For the original workloads, `billing_usage` and
`compute_node_timeline` rows are byte-identical to `development`'s generator; a test pins their
sha256 at `days=7, seed=42, today=2026-10-01`. The other tables changed deliberately for
realism (hour slicing, ordered tasks, new columns). Everything is a function of `(days, seed, today)`.
`today` defaults to the current date, so a run is always relative to "now"; the tests pin it.

## Tables, and which feature reads them

All nine are written by `generate_synthetic_system_tables` (so both `run_local.py` and
`run_on_databricks.py` create them) and redirected by `SYSTEM_TABLE_MAP`. Schemas are a
subset of the real columns with the real names, types and nesting, checked against the
Databricks system-table reference pages (AWS docs: Compute and Jobs last updated 16 Oct 2026,
Query history 20 Oct 2026, Audit log 28 Oct 2026, Billable usage 18 Oct 2026).

| System table | Synthetic table | Key columns | Feeds |
| --- | --- | --- | --- |
| `billing.usage` | `billing_usage` | `usage_metadata.{cluster_id,job_id,job_run_id,warehouse_id}`, `custom_tags`, `identity_metadata.run_as`, `usage_quantity` | F1, F2, F3 (warehouse cost), F4, F5 (cost of flagged compute) |
| `billing.list_prices` | `billing_list_prices` | `sku_name`, `pricing.default` | F1 |
| `compute.node_timeline` | `compute_node_timeline` | `cluster_id`, `cpu_user_percent`, `cpu_system_percent`, `mem_used_percent` (3 nodes, a sample every 30 min) | F5 low utilisation |
| `compute.clusters` | `compute_clusters` | `cluster_id`, `auto_termination_minutes`, `cluster_source`, `delete_time`, `change_time` (slowly changing: latest `change_time` wins) | F4, F5 |
| `lakeflow.jobs` | `lakeflow_jobs` | `job_id`, `name`, `trigger_type` (CRON, or NULL for manual), `change_time` | F4, F6 |
| `lakeflow.job_run_timeline` | `lakeflow_job_run_timeline` | `run_id`, `trigger_type` (CRON / ONETIME), `compute_ids`, `result_state` | F4 scheduled job, F6 |
| `lakeflow.job_task_run_timeline` | `lakeflow_job_task_run_timeline` | `run_id` (task run), `job_run_id` (parent), `task_key`, `result_state` | F6 retries |
| `access.audit` | `access_audit` | `service_name`, `action_name`, `user_agent`, `request_params` (map), `user_identity.email` | F4 JDBC/ODBC, shared users |
| `query.history` | `query_history` | `compute.warehouse_id`, `statement_text`, `execution_duration_ms`, `total_task_duration_ms`, `client_application`, `client_driver`, `execution_status`, `from_result_cache` | F3 catalog split |

Realism notes that downstream SQL must handle:

- **Timeline slicing.** Like the real tables (for rows written after 19 January 2026), run and task
  rows are sliced at clock-hour boundaries. Only the last slice has `result_state` and
  `termination_code`. Collapse slices per `run_id` (MIN start, MAX end, MAX_BY state) before
  counting runs. Count retries as `COUNT(DISTINCT run_id) - COUNT(DISTINCT task_key)` per
  `job_run_id`, not `COUNT(*)`. With `today` = 2026-10-30, the right count is 76 retries over the
  45 generated days (49 inside the pipeline's 30-day lookback), all on job 1005. The naive `COUNT(*)`
  count reports 741 (483 within 30 days), for example 57 (36) on job 1002, which has none.
- **Tasks are ordered.** Tasks run extract → transform → load inside the job run window. A retry
  starts after the failed attempt ends, and the job run ends when its last task ends. In a failed run,
  `load` is `BLOCKED` / `SKIPPED` with `period_start_time = period_end_time`.
- **Jobs on all-purpose compute** have no `usage_metadata.job_id` in billing. They are billed to the cluster.
- **Job clusters can report `auto_termination_minutes = 0`** (`job-1004-cluster`). Others are NULL.
  F5 must filter on `cluster_source` (`UI`/`API` = all-purpose), not on the setting alone.
- **`query.history` has no catalog column.** The catalog comes from three-part names in
  `statement_text`. Match them case-insensitively, with optional backticks per part:
  `` `sales`.`gold`.`orders` `` and `Finance.Gold.Invoices` both appear on `wh-bi-001`.
  **Two-part names** (`SELECT COUNT(*) FROM gold.orders`) resolve against the session's current
  catalog. `query.history` records neither that catalog nor a catalog in `query_source`, so F3
  must put them in an UNATTRIBUTED bucket rather than guess a default catalog. Other edge cases:
  a cross-catalog join (split between both), queries with no table (`SELECT 1`, `SHOW TABLES`),
  and failed, cancelled and result-cache queries (`execution_duration_ms = 0`). `client_driver` uses
  the documented values (`Databricks JDBC Driver`, `Databricks ODBC Driver`, NULL for the SQL editor).
- **`compute.warehouses` is not generated.** F3 needs only `query.history` (split per
  `warehouse_id`) plus `billing.usage` (cost per `warehouse_id`). Add it if a page needs warehouse names or `auto_stop_minutes`.

### `access.audit` events

| Event | Real? | Scenario |
| --- | --- | --- |
| `accounts.tokenLogin` with Simba JDBC / ODBC `user_agent` | Documented (`tokenId`, `user`, `authenticationMethod`) | F4: which principals connect with BI drivers. No cluster id. |
| `accounts.tokenLogin` with `databricks-sdk-py` | Documented | F4 near-miss: a PAT client that is not a BI tool |
| `databrickssql.commandSubmit` with `request_params.warehouseId` | Documented (SQL warehouses only) | F4 near-miss: Tableau on `wh-bi-001` is correct usage |
| `databrickssql.commandSubmit` with `request_params.clusterId` | **Assumption** | F4 positive: Tableau JDBC and Power BI ODBC on `0202-bi-tableau-cluster`. The audit reference documents no event tying a JDBC/ODBC session to an all-purpose cluster, so check this on a real account before relying on it. |
| `notebook.attachNotebook` (`clusterId`, `notebookId`, `path`) | Documented, always logged | F4 SHARED_USERS: three users on `0101-shared-ds-cluster`; near-miss: one user on `0404`. **Count users from these events only.** |
| `notebook.runCommand` (`notebookId`, no `clusterId`) | Documented, **verbose audit logs only** | Noise. Most workspaces do not have verbose audit logs on, and the event carries no cluster id, so SHARED_USERS must not depend on it. |

### `billing.usage` identity on all-purpose clusters (assumption)

For classic all-purpose compute, real `billing.usage` most likely leaves `identity_metadata.run_as`
empty: the docs describe `run_as` for jobs and serverless workloads. The original `0101-shared-ds-cluster`
keeps one billing row per user, because upstream shared-cluster attribution reads them and its rows
are pinned (see below). The F12 all-purpose clusters `0707`, `0808` and `0909` bill with
`run_as = NULL`, so the empty case is exercised. **F4 SHARED_USERS must count distinct users from
`access.audit` `attachNotebook` events per `clusterId`, not from billing `run_as`.**

## Known issue found in the upstream repo

`reliability.py::_retry_cte` groups `system.lakeflow.job_task_run_timeline` by `run_id`, then
joins it to job runs on `run_id`. In the real table `run_id` is the **task** run id and the
parent job run is `job_run_id`, so `retry_count` is always 0. Fix: group/join on `job_run_id`
and count `COUNT(DISTINCT run_id) - COUNT(DISTINCT task_key)` per job run. Slicing makes `COUNT(*)` wrong.
