# Architecture

## Source System Tables

Required:

- `system.billing.usage`

Optional:

- `system.billing.list_prices`
- `system.compute.node_timeline`
- `system.lakeflow.jobs`
- `system.lakeflow.job_run_timeline`
- `system.lakeflow.job_task_run_timeline`

## Transformation Flow

```text
system.billing.usage
  -> daily_cost
  -> workload_cost_summary
  -> tagging_quality_summary
  -> job_reliability_summary failed-cost attribution

system.compute.node_timeline
  -> compute_utilization_summary

system.lakeflow.jobs + job_run_timeline + job_task_run_timeline
  -> job_reliability_summary

cost + utilization + reliability + tagging + attribution
  -> optimization_candidates
  -> accelerator_summary
  -> dashboard views
```

## Output Model

`daily_cost` is the core fact table. It includes workload metadata, tags, estimated cost, attribution quality, price source, and run ID.

`workload_cost_summary` aggregates spend and DBUs by workload.

`compute_utilization_summary` adds cluster-level utilization signals when `system.compute.node_timeline` is available.

`job_reliability_summary` adds failure, cancellation, retry, duration, and estimated failed-cost signals when Lakeflow tables are available. Failed-cost attribution joins `system.billing.usage` directly by `usage_metadata.job_run_id`; it does not depend on `daily_cost` carrying job-run-level detail.

`tagging_quality_summary` tracks required tag coverage by date and workload.

`optimization_candidates` combines the model into a prioritized advisory review backlog.

## Cockpit Tables

Cockpit steps (`src/cockpit/steps/`) run after the upstream pipeline and write to the same cockpit schema (rule R4). They never modify upstream tables (rule R3).

`cost_base` (F1, view) publishes `daily_cost` under the cockpit name.

`idle_compute` (F5, Delta table, step `build_idle_compute`) lists idle and zombie all-purpose clusters. **One row per flagged cluster** (`workspace_id`, `cluster_id`), so summing `idle_cost` or `estimated_monthly_savings` never counts a cluster twice (rule R1). Clusters that are not flagged are not in the table.

- **Population.** The latest row per cluster in `system.compute.clusters` (slowly changing; newest `change_time` wins, and on a tie a row with `delete_time` wins). Deleted clusters are dropped, and only `cluster_source` `UI` or `API` (all-purpose) is kept. Job and pipeline clusters end with their run, even when they report `auto_termination_minutes = 0`.
- **Zombie configuration.** `auto_termination_minutes = 0` is `AUTO_TERMINATION_OFF`. A value strictly above `zombie_auto_term_minutes` (60) is `AUTO_TERMINATION_TOO_LONG`, so 60 is not flagged and 61 is. NULL means unknown and is not flagged.
- **Idle.** `system.compute.node_timeline` is averaged per cluster and clock hour, using CPU = user + system and `mem_used_percent`, like `compute_utilization_summary`. Hours are clock hours in the session time zone (UTC on Databricks).
  - An hour is idle when CPU < `low_cpu_pct` (20) and, when memory is reported, memory < `low_memory_pct` (40). An hour with NULL memory is judged on CPU alone. These two thresholds come from `config/thresholds.yml`, so the cockpit and upstream agree on "low utilisation", and a memory-bound cluster is not idle.
  - Hours are weighted by the minutes sampled (`COUNT(DISTINCT minute) / 60`), so a cluster seen for 3 minutes of an hour counts 3/60 of an hour. `active_hours` and `idle_hours` are decimal hours.
  - The cluster is `LOW_UTILIZATION` when at least `idle_hours_share_pct` (50%) of its sampled minutes fell in idle hours.
  - The window starts at midnight of `current_date - lookback_days`, the same window as `daily_cost` (`usage_date >= current_date - lookback`), so cost and utilisation cover the same days.
  - **Known approximation:** driver and worker rows are averaged together, as upstream does. A busy driver with idle workers (or the reverse) is blended into one number.
  - **Open question:** a real zombie whose JVM heap stays allocated can report memory of 40% or more while doing nothing. With `low_memory_pct` = 40 it is then not idle, so F5 may under-flag real zombies. Check this on real `node_timeline` data before tuning.
  - **Sandbox note:** the synthetic `node_timeline` has one sample every 30 minutes, so its weighted active hours are about 1/30 of clock hours. Flags and the idle share are unaffected, but confidence is mostly MEDIUM there. The real table is per minute.
- **Flag.** Zombie configuration **and** idle, for example `AUTO_TERMINATION_OFF+LOW_UTILIZATION`. A busy cluster with auto-termination off is not flagged. **Pending decision:** the alternative is "the setting is the finding, idleness adds evidence". Under it, a zombie setting with cost in the lookback is flagged even when the cluster is busy; savings stay NULL and the priority is lower. To switch, set `CONFIG_FLAG_REQUIRES_IDLE = False` in `src/cockpit/steps/idle_compute.py` and update the test expectations. A cluster with auto-termination of 60 minutes or less is not flagged, however idle it is. Its sizing is covered by `compute_utilization_summary` and `optimization_candidates`.
- **Money.** `estimated_cost` is the cluster's cost in the lookback, from `daily_cost`. `idle_cost = estimated_cost * idle_minutes / active_minutes`, which spreads the cost evenly over the sampled time. `estimated_monthly_cost = estimated_cost * 30 / lookback_days`. `estimated_monthly_savings = idle_cost * 30 / lookback_days`, and it is set only for idle (`LOW_UTILIZATION`) rows. This is an upper bound (every idle hour removed), and it never exceeds the cluster's own monthly cost.
- **Priority.** `priority_score` (0-100) = 0.60 × savings score + 0.25 × `idle_hours_pct` + 0.15 × config score. The savings score is `estimated_monthly_savings / 1000 * 100`, capped at 100. The config score is 100 for OFF, 60 for TOO_LONG and 50 for unknown.
- **Confidence.** `HIGH` needs both signals and at least 24 active hours. `MEDIUM` has both signals but fewer hours. `LOW` means one signal is missing.
- **Degraded modes (`detection_mode`).** The step probes each source table: is it readable, does it have the required columns, and does it have at least one row.
  - `system.compute.clusters` missing or empty (it is empty on Databricks Free Edition): the mode is `UTILIZATION_ONLY`. All-purpose clusters are found from billing instead: the same predicate as upstream `is_shared_cluster` (`billing_origin_product` `ALL_PURPOSE` or `INTERACTIVE`, or an `ALL_PURPOSE` SKU), and never billed to a job. They are flagged on idleness alone, with `autotermination_minutes` NULL and confidence LOW.
  - `system.compute.node_timeline` unusable: the mode is `CONFIG_ONLY`. Clusters with a zombie configuration and cost in the lookback are flagged, with `idle_cost` and savings NULL and confidence LOW.
  - Neither table usable: the table is created empty. In every case the run log says which source was missing.

Columns: `workspace_id`, `cluster_id`, `cluster_name` (falls back to `cluster_id`), `cluster_source`, `owner` (`owned_by`, else the billing owner tag), `autotermination_minutes`, `config_change_time`, `avg_cpu_pct`, `avg_memory_pct`, `active_hours`, `idle_hours`, `idle_hours_pct`, `estimated_cost`, `idle_cost`, `estimated_monthly_cost`, `estimated_monthly_savings` (money as DECIMAL(38,6)), `idle_reason`, `suggested_action`, `detection_mode`, `confidence`, `priority_score`, `evidence`, `lookback_days`, `created_at`, `run_id`.

**Note for F7 (`waste_flags`) and F10.** F4 (`allpurpose_misuse`) and F5 (`idle_compute`) can both carry savings for the same all-purpose cluster.
- `waste_flags` must collapse them per (`workspace_id`, `cluster_id`) into one saving. Take the larger saving (MAX), or take F5 `idle_cost` and keep F4 as evidence. **Never SUM them** (rule R1).
- The two steps score `priority_score` on different formulas. F7 needs a common priority scale before the F10 Monday alert takes the top 3 (rule R5).

`job_reliability` (F6, Delta table, step `build_job_reliability`) is `job_reliability_summary` with a corrected retry count. Upstream groups `system.lakeflow.job_task_run_timeline` by `run_id`, which is the task run id, so its `retry_count` is always 0. This table counts retries and repair reruns:

- Attempts per task are `COUNT(DISTINCT run_id)` per `(job_run_id, task_key)`; retries per job run are `attempts - 1` summed over its tasks. Distinct, because one long attempt spans several hourly rows. Rows with NULL `job_run_id` or `task_key` are ignored.
- Only top-level task runs count (`parent_run_id` NULL or equal to `job_run_id`), so for-each iterations are not mistaken for retries. This assumes top-level tasks carry `parent_run_id = job_run_id` and iterations carry the for-each task run id; **confirm on real data**. If `parent_run_id` is absent, iterations cannot be excluded and the run log says so.
- Only job runs present in `system.lakeflow.job_run_timeline` within the lookback are counted, so retries line up with `run_count`. The task timeline is read over lookback + 2 days so early attempts of runs near the edge are kept.
- If either timeline is missing or lacks the needed columns, upstream values are kept (`retry_count_source = 'UPSTREAM_FALLBACK'`) and the run log says why.

One row per `(workspace_id, job_id)`. Columns:

- Same as upstream: `workspace_id`, `job_id`, `job_name`, `run_count`, `success_count`, `failed_count`, `cancelled_count`, `skipped_count`, `failure_rate_pct`, `avg_duration_minutes`, `total_duration_minutes`, `estimated_failed_dbus`, `estimated_failed_cost` (DECIMAL(38,6)), `last_run_start_time`, `last_run_result_state`, `confidence`, `created_at`, `run_id`.
- `retry_count`: corrected task retries and repair reruns across all job runs.
- `retried_run_count`: job runs with at least one retried task (NULL in fallback mode).
- `retry_rate_pct`: `retried_run_count / run_count * 100`.
- `upstream_retry_count`: the upstream value, kept for comparison.
- `retry_count_source`: `TASK_TIMELINE_JOB_RUN_ID`, or `UPSTREAM_FALLBACK`.
- `reliability_category`: upstream rules and order re-evaluated with the corrected `retry_count` (`retry_heavy_count`, `high_failure_rate_pct` from `config/thresholds.yml`). REVIEW_REQUIRED (a run with NULL final `result_state`) is recomputed from `job_run_timeline`; in fallback mode it is inherited from upstream. `upstream_reliability_category` keeps the original.

## Configurable Rules

Business thresholds live in `config/thresholds.yml`:

- utilization thresholds
- reliability thresholds
- expensive workload threshold
- retry-heavy threshold
- scoring weights
- priority thresholds

Tagging rules live in `config/tagging_rules.yml`:

- required tags
- critical tags
- tag aliases

If either config file is missing or malformed, the accelerator falls back to safe code defaults and writes a WARN health record.

Cockpit-only thresholds live in `config/cockpit_thresholds.yml`, one section per step, loaded by `src/cockpit/step_config.py` (`load_step_thresholds`). That loader reuses the private upstream helpers `_read_yaml_file` and `_numeric_section` read-only; a test pins their signatures. The two files above came with the upstream accelerator and are parsed by its `config.py`, so cockpit keys are kept separate (rule R3). The `idle_compute` section holds the F5 values: `zombie_auto_term_minutes`, `idle_hours_share_pct`, `high_confidence_min_active_hours` and the `priority_*` / `config_score_*` weights. A missing file or key falls back to the code defaults, and the warning is appended to the step's run-log message.

## Pricing Source

`daily_cost.price_source` can be:

- `LIST_PRICES`: all usage in the grouped row used Databricks list prices.
- `FALLBACK_DBU_PRICE`: all usage in the grouped row used the configured fallback DBU price.
- `MIXED`: the grouped row contains both list-priced and fallback-priced usage.
- `UNKNOWN`: no usable price source was available.

Cost values are estimates and are not a replacement for final invoices.

## Attribution Logic

Attribution quality is assigned as:

- `HIGH`: job, SQL warehouse, or pipeline metadata is available.
- `MEDIUM`: cluster or run-as metadata exists, but a stronger workload ID is unavailable.
- `LOW`: only broad cluster/product/SKU attribution is available, or shared cluster attribution is detected.
- `UNKNOWN`: useful workload metadata is missing.

Shared all-purpose cluster usage is marked with `SHARED_CLUSTER_ATTRIBUTION` because cost may not map cleanly to one owner or workload.

## Scoring Logic

`optimization_candidates.priority_score` is normalized to 0-100:

```text
priority_score =
  cost_score * 0.45
  + waste_score * 0.25
  + reliability_score * 0.15
  + tagging_score * 0.10
  + frequency_score * 0.05
```

The default weights are defined in `databricks_finops.scoring.SCORE_WEIGHTS` and mirrored in `config/thresholds.yml`. The SQL expression used by `optimization_candidates` is generated from the active scoring configuration so Python and SQL stay aligned.

## Deployment Targets

The bundle has two targets:

- `dev`: development mode, paused schedule, deployed under the current user's workspace bundle path.
- `prod`: production mode, paused schedule by default, deployed under the deployer workspace bundle path.

Use `commands.ps1` as the canonical local command list and set `DATABRICKS_CONFIG_PROFILE` to the Databricks CLI profile name before running it. The script fails fast when the profile is missing.

## SQL Templates

Business SQL lives under `src/databricks_finops/sql/` and is included in the wheel package. Python modules load the templates with `importlib.resources`, substitute named placeholders, and submit the resulting SQL to Spark.

## Currency Handling

`currency_code` is the source currency for estimated cost. `display_currency` is written to output tables as the same value for dashboard convenience. The accelerator does not perform FX conversion, so mismatched display currencies are not accepted.

## Confidence Model

Confidence is:

- `HIGH`: strong utilization or reliability evidence exists.
- `MEDIUM`: partial optional source evidence exists.
- `LOW`: recommendation depends on fallback, missing optional sources, or insufficient data.

## Idempotency Strategy

Derived lookback-window tables use `CREATE OR REPLACE TABLE`, so reruns deterministically refresh the current lookback model.

`accelerator_run_log` appends one record per step and run.

`accelerator_health` preserves run history. Each run deletes any existing health rows for the current `run_id`, inserts the current health rows, and leaves previous run IDs intact. Dashboard coverage views filter to the latest summary run.

## Failure And Degradation Behavior

`system.billing.usage` is required and fails fast if unavailable.

Optional sources create WARN health rows and placeholder outputs with `INSUFFICIENT_DATA`, keeping downstream views queryable.

## Cockpit Tables

Cockpit plug-in steps (`src/cockpit/steps/`) run after the upstream pipeline and write to the same cockpit schema (rule R4). Their thresholds live in `config/cockpit_thresholds.yml`, one section per step (`config/thresholds.yml` stays the engine's file). Extra sources the steps read: `system.compute.clusters`, `system.access.audit`, `system.query.history`. Each step probes them and degrades, it does not fail, when one is missing.

### `cost_base` (F1)

A view over `daily_cost`, so cockpit steps and pages build on a cockpit name.

### `allpurpose_misuse` (F4)

Step `build_allpurpose_misuse` (depends on `build_cost_base`). It finds all-purpose clusters doing work that cheaper compute should do. **There is one row per cluster** with at least one misuse type, so `SUM(estimated_monthly_savings)` never counts a cluster twice (rule R1).

All-purpose means `compute.clusters.cluster_source` is `UI` or `API`, using the latest `change_time` row. A cluster that is missing from `compute.clusters` (for example on Free Edition, where the table is empty) counts when billing shows an `ALL_PURPOSE` SKU.

| Misuse type | Signal | Threshold | Candidate saving | Confidence |
|---|---|---|---|---|
| `BI_TOOL_JDBC` | `access.audit` events whose `user_agent` is a BI or JDBC/ODBC client and whose `request_params.clusterId` names the cluster. Also `query.history` statements on `compute.cluster_id` (excluding `compute.type` WAREHOUSE / SERVERLESS_COMPUTE) with a JDBC/ODBC `client_driver` or a BI `client_application`; `use_query_history: 0` switches this source off. Hours are cut in UTC. | `min_bi_events` | `bi_to_sql_warehouse_savings_pct` of the usage that overlaps hours with BI activity (an assumed saving from a SQL warehouse) | LOW from audit, because the clusterId link is an assumption (see `sandbox/README.md`); MEDIUM from query.history |
| `SCHEDULED_JOB` | Automated runs whose `compute_ids` include the cluster. A run is automated when its `trigger_type` is in `automated_trigger_types` (default CRON, PERIODIC, CONTINUOUS, FILE_ARRIVAL, TABLE; the job trigger is used only when the run has none), when its `run_type` is in `automated_run_types` (default SUBMIT_RUN, the runs that ADF, Airflow and other orchestrators submit), or when it is an ONETIME_RETRY of a job that is automated. `job_run_timeline` does not link a retry to its original run, so this last test is made per job. A manual run (trigger ONETIME, run_type JOB_RUN) never counts. The lists must be UPPERCASE identifiers, and anything else falls back to the default with a warning. Billing has no `job_id` for these runs. Without `job_task_run_timeline` the type is skipped, because run-level `compute_ids` does not reliably list existing all-purpose clusters. | `min_scheduled_runs` | The usage that overlaps the run (task) windows, minus hours with interactive use (any BI request or notebook attach on the cluster, even below the thresholds), repriced at the jobs SKU (`ALL_PURPOSE` to `JOBS` in `list_prices`) with DBUs assumed unchanged. `jobs_price_ratio_fallback` applies when that SKU has no price. | HIGH |
| `SHARED_USERS` | Distinct `user_identity.email` on `notebook.attachNotebook` events for the cluster. Billing `run_as` is usually empty on all-purpose compute, and `runCommand` is logged only in verbose audit logs. | `min_interactive_users` | None. This is an attribution and cluster-policy issue. | MEDIUM |

How cost and savings are calculated:

- **Cost.** DBUs are multiplied by the list price, joined on SKU and the effective window of the price, as in `daily_cost`. Unmatched rows use the fallback DBU price. Only billing rows of all-purpose candidates are priced. The join must stay in step with `daily_cost_list_prices.sql`, and a test compares `cluster_cost` with `cost_base`.
- **Config checks.** Divisors (`score_reference_monthly_cost`) must be greater than 0, minimum counts at least 1, the percentage between 0 and 100 and the ratio between 0 and 1. A value outside its range falls back to the default with a warning in the run log.
- **Attributed cost.** The activity windows of each type are merged into disjoint islands. Each billing row is then attributed by the share of its duration that overlaps an island, capped at 100%.
- **Monthly figures.** Lookback amounts are scaled by `30 / lookback_days`.
- **One saving per cluster (R1).** `estimated_monthly_savings` is the largest candidate saving, capped at the cost of the cluster. `misuse_type` is the type that produced it. The remaining types stay in `misuse_types` and `evidence`, and their candidate savings are listed in `savings_basis` as "not added".

Columns:

| Column | Meaning |
|---|---|
| `workspace_id`, `cluster_id`, `cluster_name`, `cluster_source`, `owner` | The cluster. `owner` is `compute.clusters.owned_by`, or the owner tag from `cost_base` when that is empty. |
| `misuse_type` | The primary type: the one with the largest candidate saving. Ties go to SCHEDULED_JOB, then BI_TOOL_JDBC, then SHARED_USERS. |
| `misuse_types` (`ARRAY<STRING>`), `misuse_type_count` | Every type found, primary first. |
| `evidence` | One clause per type, for example client drivers and request counts, job names and triggers, or the user list. |
| `bi_client_count`, `bi_event_count`, `scheduled_job_count`, `scheduled_run_count`, `interactive_user_count` | Signal counts in the lookback window (0 when that type is not present). |
| `lookback_days`, `cluster_cost` | The lookback window and the cost of the cluster within it. |
| `bi_tool_attributable_cost`, `scheduled_job_attributable_cost` | Lookback cost overlapping BI hours or scheduled runs. These are costs, not savings. |
| `estimated_monthly_cost`, `estimated_monthly_savings` (`DECIMAL(38,6)`) | Monthly cluster cost, and the deduplicated saving (R1). This is the only savings column to sum. |
| `savings_basis` | How the saving was derived, and which candidates were not added. |
| `suggested_action` | `MOVE_TO_JOB_COMPUTE`, `MOVE_BI_TO_SQL_WAREHOUSE` or `APPLY_SHARED_CLUSTER_POLICY`. |
| `confidence` | The confidence of the primary type. |
| `priority_score` | 0-100. A weighted sum of a savings score, a cost score (both `LEAST(100, monthly / score_reference_monthly_cost * 100)`) and a breadth score (`misuse_type_count / 3 * 100`), multiplied by a confidence multiplier. Exposed for F7 and F10 (rule R5). |
| `currency_code`, `created_at`, `run_id` | Run metadata. |
