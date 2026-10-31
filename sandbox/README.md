# Sandbox: run the accelerator without real System Tables

The accelerator reads six `system.*` tables, and the cockpit steps read three more
(`compute.clusters`, `access.audit`, `query.history`). This folder fakes all nine with
realistic synthetic data and redirects the reads, so the repo's own code runs unmodified.

| File | What it does |
| --- | --- |
| `finops_sandbox.py` | Synthetic System Table generator + `RedirectingSpark` proxy + `run_accelerator()` |
| `finops_demo.py` | The `demo` profile: 180 days of company-scale history with storylines (see "Demo profile") |
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
4. Optional: set the `profile` widget to `demo` for six months of history (lookback becomes 180 days).

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

## Demo profile (`--profile demo`)

The default profile is small on purpose: every scenario is a unit-test fixture. The `demo` profile is
for showing the cockpit to people. It has six months of history at company scale, with storylines that the trend and
waste pages can tell.

```bash
.venv/bin/python sandbox/run_local.py --profile demo          # macOS
.\sandbox\run_local.ps1 --profile demo                        # Windows
```

On Databricks, set the notebook's `profile` widget to `demo`. In code, use
`generate_synthetic_system_tables(spark, schema, profile="demo")` or
`build_synthetic_rows(profile="demo")`. `--days` and `--lookback-days` override the defaults
(180 and 180).

**The default profile is unchanged.** `build_synthetic_rows()` with no profile returns byte-identical
rows. The demo profile starts from the default rows for the same 180 days, unchanged and first in
every table, then appends its own rows from separate RNG streams (`seed + 101/102/103`). The default
scenarios and their near-misses are therefore present exactly as tests expect
(`test_demo_contains_default_rows_unchanged`). The only default-visible difference is length: 180 days instead of 45.

**Scale** (`today` = 2026-10-30, seed 42): 3 workspaces (prod `1111…`, dev `2222…`, analytics `3333…`),
8 teams (data-eng, ml, analytics, finance, marketing, platform, data-science, sales-ops; the default
profile adds `fin-eng`), 69 workloads (21 default plus 48 demo entries in `DEMO_WORKLOADS`: 23 `JOB`, 12 `CLUSTER`,
5 `SQL_WAREHOUSE`, 4 `PIPELINE`, 4 `NOTEBOOK`). 17 of the jobs run on their own job clusters. The other 6 run on
all-purpose compute: 3010, 3011 and 3012 bring their own all-purpose clusters (`1301`, `1302`, `1303`, billed
through the job entry with no `job_id`), and 4001, 3018 and 3016 run on existing clusters (`1409`, `1101`) and add
no billing of their own. That makes 15 all-purpose clusters in all. About $300k over 180 days, priced with the engine's list-price join:

| Month (days before today) | Spend | Untagged (no `team`) | `1202` BI cluster | `wh-bi-serverless-01` | `1404` cluster | ML team |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 (180-151) | $40.5k | 42% | $1.8k | $1.0k | $0.3k | $1.0k |
| 2 (150-121) | $42.3k | 33% | $2.0k | $1.0k | $0.3k | $1.0k |
| 3 (120-91) | $50.3k | 24% | $2.1k | $1.1k | $0.3k | $7.1k |
| 4 (90-61) | $53.2k | 18% | $2.0k | $1.2k | $1.1k | $7.3k |
| 5 (60-31) | $55.9k | 12% | $0.5k | $2.2k | $1.6k | $7.8k |
| 6 (30-1) | $58.0k | 8% | $0.2k | $2.5k | $1.7k | $8.0k |

**Storylines.** Each one is anchored in days before `today`, so it lands on the same relative date on every run.
The constants are at the top of `finops_demo.py`, and each storyline has a test in `tests/test_sandbox_demo.py`.

| Storyline | When | What changes in the data | Shows up in |
| --- | --- | --- | --- |
| Growth | all 180 days | Demo workloads' DBUs grow 5% per 30 days (`GROWTH_PER_MONTH`) | F8 trend |
| ML recommender squad onboards | from 120 days ago (month 3) | `recsys_training` (3101), `recsys_batch_scoring` (3102), `1405-ml-gpu-cluster`, `pl-ml-features-dlt`, `nb-ml-exploration`, plus `ml.*` queries on `wh-ds-adhoc` | F8 step change, F1 by team |
| Weekly seasonality | Saturdays, Sundays | Interactive clusters, warehouses and notebooks run at 60% (`WEEKEND_FACTOR`); BI audit and query volume drop too | F8 daily trend |
| Month-end close | last 3 calendar days of each month | `gl_close_batch` x3, `revenue_recognition` x2, `wh-exec-dash` x2 (and twice the queries), `1203-finance-excel-odbc` x2 | F8 spikes, finance team |
| Failure storm | 140-134 days ago (month 2, one week) | `supplier_feed_ingest`, `payments_ingest` and `crm_contacts_sync` fail about 70% of runs, with `transform` retries; 0% otherwise | F6 failed-run cost, weekly trend |
| List price change | from 90 days ago (month 4, mid-period) | `PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST` goes from 0.70 to 0.77 USD/DBU: two `billing.list_prices` rows whose windows meet at midnight | F1 (cost up with flat DBUs) |
| Auto-termination switched off | 80 days ago (month 4) | `1404-ds-sandbox-cluster` gets a `compute.clusters` change row from 30 to 0, stamped at midnight of that day. From then on it bills 24h a day at CPU 3%, and nobody attaches notebooks | F5 positive, cost about 5x |
| BI moves to a SQL warehouse | 55 days ago (month 5) | `1202-bi-powerbi-cluster`'s Power BI ODBC and Tableau JDBC `commandSubmit` events switch from `clusterId` to `warehouseId = wh-bi-serverless-01`, along with their queries. The cluster drops to 8% of its usage, the warehouse adds 85 DBU/day, and the two together cost about 30% less | F4 fixed, F3, F8 saving |
| Tagging campaign | 155 to 35 days ago | Workloads with `untagged_until` carry no tags or partial `pre_tags` until that day (new `compute.clusters` / `lakeflow.jobs` rows record the change, stamped at midnight so the day's billing matches). The share of spend with no `team` falls from about 41% to about 8%. `1102-ds-shared-cluster`, `crm_api_push`, `legacy_report_mailer`, `nb-sales-ops-adhoc` and `1408` are never tagged | F2 trend, untagged backlog |

**Waste cases** added on top of the default ones, so rankings have several sizes:

| Feature | Positive | Near-miss |
| --- | --- | --- |
| F4 BI on all-purpose | `1202-bi-powerbi-cluster` (until month 5), `1204-looker-jdbc-cluster` (Looker JDBC), `1203-finance-excel-odbc` (Excel ODBC). All three also have weekday `query.history` rows with `compute.type = CLUSTER`, `compute.cluster_id` set and a JDBC/ODBC `client_driver`, for the MEDIUM-confidence path (assumption below) | `1202` after migration, `wh-bi-serverless-01` (same drivers on a warehouse) |
| F4 scheduled jobs on all-purpose | `daily_marketing_export` (3010, CRON, `1301`, plus 6 `ONETIME_RETRY` repair runs that also count), `platform_audit_export` (3012, PERIODIC, `1303`), `crm_api_push` (3011, `run_type = SUBMIT_RUN`, `trigger_type = ONETIME`, on an `API` cluster `1302`), the external orchestrator `airflow__finance_extract` (`SUBMIT_RUN`, `ONETIME`, **`job_id` NULL** in both timelines, task rows included, on the existing UI cluster `1409-finance-adhoc`) | 17 CRON jobs on job clusters; `finance_restatement` (3018, manual, every 14 days on `1409`) and its 3 `ONETIME_RETRY` repairs (a repair of a manual job is not scheduled); `exec_kpi_rollup` (3016, `run_type = WORKFLOW_RUN`, `ONETIME`: a child run started by `dbutils.notebook.run` from a notebook on the existing cluster `1101`, with no `lakeflow.jobs` row and no billing of its own) |
| F4 shared users | `1101-analytics-shared` (6 users), `1102-ds-shared-cluster` (4 users) | `1405`, `1406`, `1408`, `1409`, `1410` (1 user each) |
| F5 idle / zombie | `1404` (0 from month 4, idle 24/7), `1405-ml-gpu-cluster` (120 min, CPU 9%), `1408-dev-sandbox-autoterm-90` (90 min, CPU 5%) | `1406-analytics-autoterm-60` and `1101` (exactly 60), `1407-platform-ops-autoterm-off` (off but CPU 68%), `1409` (20 min, CPU 40%) |
| F6 reliability | `marketing_attribution` (3009, about 30% failures), `lead_scoring` (3013, about 60% of runs retry `transform`), the storm jobs | `data_quality_checks` (3014, long sliced task, no retries) |
| F2 tagging | see "Tagging campaign". Partial tags: `inventory_snapshot`, `revenue_recognition`, `pl-inventory-dlt`, `wh-analytics-shared` (alias key `Project` only) | ML workloads, tagged from day one |
| F3 catalog split | `wh-analytics-shared` (sales, marketing, finance, product, plus a backticked cross-catalog join), `wh-ds-adhoc` (ml, sales, product, plus two-part names), `wh-exec-dash` (finance, sales), `wh-bi-serverless-01` (sales_ops, sales) | `wh-mkt-001` (marketing only) |

**Things to know when demoing:**

- `run_local.py --profile demo` runs the upstream engine with `--lookback-days 180`. The upstream
  summaries (`workload_cost_summary`, `compute_utilization_summary`, `job_reliability_summary`,
  `optimization_candidates`, `vw_executive_summary` and the pages that read them) therefore show
  **180-day aggregates**. `optimization_candidates.estimated_monthly_cost` is still scaled to 30 days
  (upstream multiplies by `30 / lookback_days`). The cockpit steps (F3 to F7) will get their own 30-day
  detection window in a separate change. Until then, "current state" flags such as BI on `1202` cover the whole 180 days.
- The failure storm adds about 2 percentage points to the 180-day failure rate of the three storm jobs.
  It shows in daily or weekly views, not in the 180-day `job_reliability_summary`. `retry_count`
  there stays 0 because of the upstream bug below.
- Spark's `current_date()` runs in UTC (`spark.sql.session.timeZone`), but the generator's `today` is
  the local date. Run late in the evening in a timezone behind UTC (or early morning ahead of it)
  and the first or last day can fall just outside the lookback. This is true of both profiles.
- Size: about 438k rows, 315k of them in `compute_node_timeline` (30-minute samples, 3 nodes). The local
  run takes about 60 s on a laptop (20 s to generate and write, the rest is the engine), against about 30-40 s for default.
- **Not yet tested on Databricks Free Edition.** `generate_synthetic_system_tables` builds each table with
  `spark.createDataFrame` from Python rows, so about 438k rows (315k node rows) go through the serverless
  driver. If that is too slow or hits limits, halve the node volume by keeping only on-the-hour samples for the
  demo workloads: in `build_demo_rows`, filter `extra["compute_node_timeline"]` to `start_time.minute == 0`
  before the rows are appended. Leave the default rows alone so their digests still match. Per-cluster averages barely move.
  This is documented only and not implemented, so the generator keeps no hidden settings.
- **Assumptions to check on a real account.**
  - `runs/submit` runs appear in two shapes: `crm_api_push` (a `job_id` with no `lakeflow.jobs` row) and the
    external orchestrator (`job_id` NULL). F4 should rely on `job_run_timeline.run_type` / `trigger_type` and
    `compute_ids`, not on the jobs table.
  - A repair run is modelled as its own run row: a new `run_id` (original id + `1`), `trigger_type = ONETIME_RETRY`,
    `run_type = JOB_RUN`, re-running `transform` and `load`. To tell whether it counts as scheduled, look up the job's
    `lakeflow.jobs.trigger_type`.
  - Notebook-workflow child runs (`exec_kpi_rollup`, `run_type = WORKFLOW_RUN`) carry an ephemeral `job_id` in
    both timelines but have no `lakeflow.jobs` definition.
  - `query.history` with `compute.type = CLUSTER`: the reference describes SQL warehouses and serverless
    compute, so classic-cluster rows may never appear for real. Treat that path as MEDIUM confidence at best.

## Known issue found in the upstream repo

`reliability.py::_retry_cte` groups `system.lakeflow.job_task_run_timeline` by `run_id`, then
joins it to job runs on `run_id`. In the real table `run_id` is the **task** run id and the
parent job run is `job_run_id`, so `retry_count` is always 0. Fix: group/join on `job_run_id`
and count `COUNT(DISTINCT run_id) - COUNT(DISTINCT task_key)` per job run. Slicing makes `COUNT(*)` wrong.
