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

Cockpit plug-in steps (`src/cockpit/steps/`) run after the upstream pipeline and write to the same cockpit schema (rule R4). They never modify upstream tables (rule R3). Their thresholds live in `config/cockpit_thresholds.yml`, one section per step (`config/thresholds.yml` stays the engine's file). Extra sources the steps read: `system.compute.clusters`, `system.access.audit`, `system.query.history`. Each step probes them and degrades, it does not fail, when one is missing.

### Detection window

F4, F5, F6 and F7 detect waste over one shared window: `days = min(detection.detection_days, lookback_days)`, with `detection_days: 30` in `config/cockpit_thresholds.yml`. It is read once, by `cockpit.step_config.detection_window`, so every step uses the same days.

- The upstream engine may run with a long `--lookback-days` (for example 180 days, for six-month trends in `daily_cost`, `vw_cost_trend_daily` and F8). The flags must reflect recent behaviour, so they never look further back than `detection_days`. They also never look further back than the lookback, because the upstream tables only hold that much.
- Everything the detection steps read is filtered to the window: billing (directly, or `daily_cost` / `cost_base` by `usage_date`), utilisation, job runs, audit and query history.
- Monthly figures are window amounts × `30 / detection_days`.
- With the default profile (lookback 30), the window is 30 days and the outputs are unchanged.
- Upstream tables (`optimization_candidates`, `job_reliability_summary`, ...) keep the upstream lookback, because R3 forbids changing them. F6 re-renders the upstream reliability SQL over the window instead (see below). F7's upstream sizing and tagging items are classified by upstream over its own lookback, but F7 re-costs them from `cost_base` over the window.
- An invalid value (not a whole number of at least 1) falls back to 30 with a warning in the run log. F4, F5 and F6 tables carry `detection_days` (F4 and F5 also keep `lookback_days`, the upstream lookback).

### `cost_base` (F1)

A view over `daily_cost`, so cockpit steps and pages build on a cockpit name.

### `allpurpose_misuse` (F4)

Delta table, step `build_allpurpose_misuse` (depends on `build_cost_base`). It finds all-purpose clusters doing work that cheaper compute should do. **There is one row per cluster** with at least one misuse type, so `SUM(estimated_monthly_savings)` never counts a cluster twice (rule R1).

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
- **Window and monthly figures.** Billing, audit, query history and job runs are all read over the detection window, and window amounts are scaled by `30 / detection_days`.
- **One saving per cluster (R1).** `estimated_monthly_savings` is the largest candidate saving, capped at the cost of the cluster. `misuse_type` is the type that produced it. The remaining types stay in `misuse_types` and `evidence`, and their candidate savings are listed in `savings_basis` as "not added".

Columns:

| Column | Meaning |
|---|---|
| `workspace_id`, `cluster_id`, `cluster_name`, `cluster_source`, `owner` | The cluster. `owner` is `compute.clusters.owned_by`, or the owner tag from `cost_base` when that is empty. |
| `misuse_type` | The primary type: the one with the largest candidate saving. Ties go to SCHEDULED_JOB, then BI_TOOL_JDBC, then SHARED_USERS. |
| `misuse_types` (`ARRAY<STRING>`), `misuse_type_count` | Every type found, primary first. |
| `evidence` | One clause per type, for example client drivers and request counts, job names and triggers, or the user list. |
| `bi_client_count`, `bi_event_count`, `scheduled_job_count`, `scheduled_run_count`, `interactive_user_count` | Signal counts in the detection window (0 when that type is not present). |
| `lookback_days`, `detection_days`, `cluster_cost` | The upstream lookback, the detection window, and the cost of the cluster within the detection window. |
| `bi_tool_attributable_cost`, `scheduled_job_attributable_cost` | Detection-window cost overlapping BI hours or scheduled runs. These are costs, not savings. |
| `estimated_monthly_cost`, `estimated_monthly_savings` (`DECIMAL(38,6)`) | Monthly cluster cost, and the deduplicated saving (R1). This is the only savings column to sum. |
| `savings_basis` | How the saving was derived, and which candidates were not added. |
| `suggested_action` | `MOVE_TO_JOB_COMPUTE`, `MOVE_BI_TO_SQL_WAREHOUSE` or `APPLY_SHARED_CLUSTER_POLICY`. |
| `confidence` | The confidence of the primary type. |
| `priority_score` | 0-100. A weighted sum of a savings score, a cost score (both `LEAST(100, monthly / score_reference_monthly_cost * 100)`) and a breadth score (`misuse_type_count / 3 * 100`), multiplied by a confidence multiplier. Exposed for F7 and F10 (rule R5). |
| `currency_code`, `created_at`, `run_id` | Run metadata. |

### `idle_compute` (F5)

Delta table, step `build_idle_compute`. It lists idle and zombie all-purpose clusters. **One row per flagged cluster** (`workspace_id`, `cluster_id`), so summing `idle_cost` or `estimated_monthly_savings` never counts a cluster twice (rule R1). Clusters that are not flagged are not in the table.

- **Population.** The latest row per cluster in `system.compute.clusters` (slowly changing; newest `change_time` wins, and on a tie a row with `delete_time` wins). Deleted clusters are dropped, and only `cluster_source` `UI` or `API` (all-purpose) is kept. Job and pipeline clusters end with their run, even when they report `auto_termination_minutes = 0`.
- **Zombie configuration.** `auto_termination_minutes = 0` is `AUTO_TERMINATION_OFF`. A value strictly above `zombie_auto_term_minutes` (60) is `AUTO_TERMINATION_TOO_LONG`, so 60 is not flagged and 61 is. NULL means unknown and is not flagged.
- **Idle.** `system.compute.node_timeline` is averaged per cluster and clock hour, using CPU = user + system and `mem_used_percent`, like `compute_utilization_summary`. Hours are clock hours in the session time zone (UTC on Databricks).
  - An hour is idle when CPU < `low_cpu_pct` (20) and, when memory is reported, memory < `low_memory_pct` (40). An hour with NULL memory is judged on CPU alone. These two thresholds come from `config/thresholds.yml`, so the cockpit and upstream agree on "low utilisation", and a memory-bound cluster is not idle.
  - Hours are weighted by the minutes sampled (`COUNT(DISTINCT minute) / 60`), so a cluster seen for 3 minutes of an hour counts 3/60 of an hour. `active_hours` and `idle_hours` are decimal hours.
  - The cluster is `LOW_UTILIZATION` when at least `idle_hours_share_pct` (50%) of its sampled minutes fell in idle hours.
  - The window starts at midnight of `current_date - detection_days` (see Detection window), and the `daily_cost` rows are filtered to the same days (`usage_date >= current_date - detection_days`), so cost and utilisation cover the same days.
  - **Known approximation:** driver and worker rows are averaged together, as upstream does. A busy driver with idle workers (or the reverse) is blended into one number.
  - **Open question:** a real zombie whose JVM heap stays allocated can report memory of 40% or more while doing nothing. With `low_memory_pct` = 40 it is then not idle, so F5 may under-flag real zombies. Check this on real `node_timeline` data before tuning.
  - **Sandbox note:** the synthetic `node_timeline` has one sample every 30 minutes, so its weighted active hours are about 1/30 of clock hours. Flags and the idle share are unaffected, but confidence is mostly MEDIUM there. The real table is per minute.
- **Flag.** Zombie configuration **and** idle, for example `AUTO_TERMINATION_OFF+LOW_UTILIZATION`. A busy cluster with auto-termination off is not flagged. **Pending decision:** the alternative is "the setting is the finding, idleness adds evidence". Under it, a zombie setting with cost in the detection window is flagged even when the cluster is busy; savings stay NULL and the priority is lower. To switch, set `CONFIG_FLAG_REQUIRES_IDLE = False` in `src/cockpit/steps/idle_compute.py` and update the test expectations. A cluster with auto-termination of 60 minutes or less is not flagged, however idle it is. Its sizing is covered by `compute_utilization_summary` and `optimization_candidates`.
- **Money.** `estimated_cost` is the cluster's cost in the detection window, from `daily_cost`. `idle_cost = estimated_cost * idle_minutes / active_minutes`, which spreads the cost evenly over the sampled time. `estimated_monthly_cost = estimated_cost * 30 / detection_days`. `estimated_monthly_savings = idle_cost * 30 / detection_days`, and it is set only for idle (`LOW_UTILIZATION`) rows. This is an upper bound (every idle hour removed), and it never exceeds the cluster's own monthly cost.
- **Priority.** `priority_score` (0-100) = 0.60 × savings score + 0.25 × `idle_hours_pct` + 0.15 × config score. The savings score is `estimated_monthly_savings / 1000 * 100`, capped at 100. The config score is 100 for OFF, 60 for TOO_LONG and 50 for unknown.
- **Confidence.** `HIGH` needs both signals and at least 24 active hours. `MEDIUM` has both signals but fewer hours. `LOW` means one signal is missing.
- **Degraded modes (`detection_mode`).** The step probes each source table: is it readable, does it have the required columns, and does it have at least one row.
  - `system.compute.clusters` missing or empty (it is empty on Databricks Free Edition): the mode is `UTILIZATION_ONLY`. All-purpose clusters are found from billing instead: the same predicate as upstream `is_shared_cluster` (`billing_origin_product` `ALL_PURPOSE` or `INTERACTIVE`, or an `ALL_PURPOSE` SKU), and never billed to a job. They are flagged on idleness alone, with `autotermination_minutes` NULL and confidence LOW.
  - `system.compute.node_timeline` unusable: the mode is `CONFIG_ONLY`. Clusters with a zombie configuration and cost in the detection window are flagged, with `idle_cost` and savings NULL and confidence LOW.
  - Neither table usable: the table is created empty. In every case the run log says which source was missing.

Columns: `workspace_id`, `cluster_id`, `cluster_name` (falls back to `cluster_id`), `cluster_source`, `owner` (`owned_by`, else the billing owner tag), `autotermination_minutes`, `config_change_time`, `avg_cpu_pct`, `avg_memory_pct`, `active_hours`, `idle_hours`, `idle_hours_pct`, `estimated_cost`, `idle_cost`, `estimated_monthly_cost`, `estimated_monthly_savings` (money as DECIMAL(38,6)), `idle_reason`, `suggested_action`, `detection_mode`, `confidence`, `priority_score`, `evidence`, `lookback_days` (the upstream lookback), `detection_days` (the window the amounts cover), `created_at`, `run_id`.

### `job_reliability` (F6)

Delta table, step `build_job_reliability`. It is `job_reliability_summary` with a corrected retry count. Upstream groups `system.lakeflow.job_task_run_timeline` by `run_id`, which is the task run id, so its `retry_count` is always 0. This table counts retries and repair reruns:

- Attempts per task are `COUNT(DISTINCT run_id)` per `(job_run_id, task_key)`; retries per job run are `attempts - 1` summed over its tasks. Distinct, because one long attempt spans several hourly rows. Rows with NULL `job_run_id` or `task_key` are ignored.
- Only top-level task runs count (`parent_run_id` NULL or equal to `job_run_id`), so for-each iterations are not mistaken for retries. This assumes top-level tasks carry `parent_run_id = job_run_id` and iterations carry the for-each task run id; **confirm on real data**. If `parent_run_id` is absent, iterations cannot be excluded and the run log says so.
- Only job runs present in `system.lakeflow.job_run_timeline` within the detection window are counted, so retries line up with `run_count`. The task timeline is read over the window + 2 days so early attempts of runs near the edge are kept.
- **Detection window.** When `detection_days` equals the upstream lookback, the step reads `job_reliability_summary`. When the lookback is longer (for example 180 days for trends), it renders the upstream SQL (`databricks_finops.reliability.build_job_reliability_sql`, used read-only) again with `lookback_days = detection_days` and reads it as a subquery. Run counts, failure rates and failed cost then cover only the detection window, and the upstream table is not changed (R3). In that mode `upstream_retry_count` and `upstream_reliability_category` come from that re-rendered SQL.
- If either timeline is missing or lacks the needed columns, upstream values are kept (`retry_count_source = 'UPSTREAM_FALLBACK'`) and the run log says why.

One row per `(workspace_id, job_id)`. Columns:

- Same as upstream: `workspace_id`, `job_id`, `job_name`, `run_count`, `success_count`, `failed_count`, `cancelled_count`, `skipped_count`, `failure_rate_pct`, `avg_duration_minutes`, `total_duration_minutes`, `estimated_failed_dbus`, `estimated_failed_cost` (DECIMAL(38,6)), `last_run_start_time`, `last_run_result_state`, `confidence`, `created_at`, `run_id`.
- `retry_count`: corrected task retries and repair reruns across all job runs.
- `retried_run_count`: job runs with at least one retried task (NULL in fallback mode).
- `retry_rate_pct`: `retried_run_count / run_count * 100`.
- `upstream_retry_count`: the upstream value, kept for comparison.
- `task_attempt_minutes`, `retry_task_minutes`: minutes of all top-level task attempts, and of the attempts that were not the final one per `(job_run_id, task_key)`. An attempt runs from its first `period_start_time` to its last `period_end_time`. Both are NULL without `period_end_time` or in fallback mode. F7 uses the ratio for the retry cost.
- `detection_days`: the window the counts and costs cover.
- `retry_count_source`: `TASK_TIMELINE_JOB_RUN_ID`, or `UPSTREAM_FALLBACK`.
- `reliability_category`: upstream rules and order re-evaluated with the corrected `retry_count` (`retry_heavy_count`, `high_failure_rate_pct` from `config/thresholds.yml`). REVIEW_REQUIRED (a run with NULL final `result_state`) is recomputed from `job_run_timeline`; in fallback mode it is inherited from upstream. `upstream_reliability_category` keeps the original.

### `waste_flags` (F7)

Delta table, step `build_waste_flags`, which depends on `build_allpurpose_misuse`, `build_idle_compute`, `build_job_reliability` and `build_cost_base`. This is **the one waste backlog**: the backlog page and the F10 Monday alert read it.

There is **one row per `(resource_type, workspace_id, resource_id)`**. `resource_type` is `CLUSTER`, `JOB`, `WAREHOUSE`, `PIPELINE`, `NOTEBOOK` or `UNKNOWN`. Every source contributes *findings* to a resource:

| Source | Findings (`flag_type`) | Finding saving (per month) | Confidence |
|---|---|---|---|
| `idle_compute` (F5) | One per cluster, with `idle_reason` as the type (for example `AUTO_TERMINATION_OFF+LOW_UTILIZATION`). | F5 `estimated_monthly_savings` (NULL when idleness could not be priced). | F5 |
| `allpurpose_misuse` (F4) | One per misuse type in `misuse_types`. | The primary type (`misuse_type`) carries F4's `estimated_monthly_savings`. The other types have NULL saving and confidence, because F4 does not publish them separately (its `savings_basis` lists them as "not added"). | F4 (primary type only) |
| `job_reliability` (F6) | `FAILURE_HEAVY` (`failure_rate_pct >= high_failure_rate_pct`), else `HIGH_FAILURE_COST` (monthly failed cost `>= high_failed_cost`). | `estimated_failed_cost × 30 / detection_days`: the cost of failed runs, from billing joined by `job_run_id`. | F6 |
| `job_reliability` (F6) | `RETRY_HEAVY` (corrected `retry_count >= retry_heavy_count`). | The job's window cost from `cost_base` × `retry_task_minutes / task_attempt_minutes` × `30 / detection_days`. Billing has no per-attempt cost, so the job cost is spread evenly over task time. **This is an estimate.** | F6, capped at MEDIUM |
| `optimization_candidates` (upstream) | The issue types in `waste_flags.upstream_issue_types`: sizing (`HIGH_COST_LOW_UTILIZATION`, `LOW_UTILIZATION`), `EXPENSIVE_WORKLOAD`, `MISSING_TAGS`, `UNKNOWN_ATTRIBUTION`, `SHARED_CLUSTER_ATTRIBUTION`, and failures (`HIGH_FAILURE_COST`, `HIGH_FAILURE_RATE`). | NULL. Upstream estimates no saving, so these items rank on cost and confidence. | upstream |

Thresholds for the F6 findings (`high_failure_rate_pct`, `high_failed_cost`, `retry_heavy_count`) come from `config/thresholds.yml`, the same ones upstream uses.

Upstream items that are skipped:

- `RETRY_HEAVY` is never taken: upstream `retry_count` is always 0 (see F6).
- `HIGH_FAILURE_COST` and `HIGH_FAILURE_RATE` are dropped while `job_reliability` is readable. F6 recomputes them over the detection window. When F6 is missing, they come back.
- `SHARED_CLUSTER_ATTRIBUTION` is dropped for clusters that F4 or F5 flag. It fires for every all-purpose cluster, and the cockpit already has a more specific diagnosis there.
- `LOOKS_HEALTHY` items, `MEMORY_BOUND` (a reason *not* to downsize), `INSUFFICIENT_DATA` and the catch-all `REVIEW_REQUIRED` are not waste, so they are not taken by default.
- Items on a resource with no cost in the detection window are stale (upstream classifies over its own lookback), so they are dropped while `cost_base` is readable.
- Upstream can list one workload more than once (its utilisation join matches on `cluster_id` or `job_id`), so one finding is kept per resource and issue type: the highest cost, then confidence, then evidence.

How the row is built:

- **Cost.** `estimated_monthly_cost` comes from `cost_base` over the detection window × `30 / detection_days`.
  - JOB, WAREHOUSE, PIPELINE, NOTEBOOK and UNKNOWN are keyed by `cost_base` `workload_type` / `workload_id`, which follows the upstream precedence (JOB > SQL_WAREHOUSE > PIPELINE > CLUSTER > NOTEBOOK). Each billing row therefore belongs to exactly one of them.
  - CLUSTER takes every row carrying its `cluster_id`, the same cost F4 and F5 report. A job or pipeline that runs on a cluster is therefore in both its own cost and the cluster's cost. **`estimated_monthly_cost` is not additive across rows**; sum `estimated_monthly_savings` instead.
  - When `cost_base` has no row for the resource (or is unreadable), the largest monthly cost a source reported is used. F6 findings report the job's `cost_base` cost, or the failed-run cost as a floor (`job_reliability` has no total job cost). If no source knows the cost either, the saving is used as the floor.
- **Savings (R1).** `estimated_monthly_savings` is the **largest** finding saving, never the sum.
  - It is capped at the resource's cost when that is known. With no known cost, the finding's own basis stands, so an unknown cost never caps a saving to 0. Each finding's saving in `findings` is capped the same way.
  - A JOB's saving also leaves out the cost it shares with a CLUSTER that carries a saving, so a job and a cluster never claim the same DBUs. The evidence says so with a `[waste_flags]` clause, and the F6 finding keeps its own claim in `findings`.
  - It is 0 when no finding has a saving, so `SUM(estimated_monthly_savings)` over the table counts each resource and each DBU once.
- **Primary finding.** The finding with the largest saving. Ties go to the higher confidence, then to the source order (`idle_compute`, `allpurpose_misuse`, `job_reliability`, `optimization_candidates`), then to the flag type, then to the evidence text. It sets `primary_flag_type`, `primary_source`, `suggested_action` and `confidence` (LOW when unknown).
- **Owner and team.** Same precedence for both: the first source value by finding rank (`owned_by` for clusters, or the upstream owner and team), else the `cost_base` tag carrying the most cost on the resource.
- **Priority (R5).** `priority_score` uses one scale for every source, so the F10 top 3 compares like with like. Each source's own score is *not* reused.

```text
S = min(100, estimated_monthly_savings / score_reference_monthly_savings * 100)   # $1000 -> 100
C = min(100, estimated_monthly_cost    / score_reference_monthly_cost    * 100)   # $1000 -> 100
B = min(100, (flag_type_count - 1) * breadth_points_per_extra_flag)               # 50 per extra flag type
priority_score = round(clamp(0, 100, (0.6 * S + 0.3 * C + 0.1 * B) * M), 2)
M = confidence multiplier: HIGH 1.0, MEDIUM 0.85, LOW or unknown 0.7
```

  Savings dominate, cost keeps large unpriced items visible, and a resource that several sources flag ranks a little higher. The weights must add up to 1, both references must be greater than 0, and the multipliers must be between 0 and 1. Otherwise the defaults are used, with a warning. With valid settings the weighted sum is already 0-100 (100 needs savings and cost of at least the references, three or more flag types and HIGH confidence); the clamp is only a guard. `priority_rank` is 1..N with no ties: it orders by `priority_score` desc, then savings desc, cost desc, `resource_type`, `workspace_id` and `resource_id`.
- **Missing sources.** A source table that is missing or lacks the needed columns is replaced by an empty relation, and the run log says `<table> unavailable: <source> skipped`. Without `cost_base`, the cost comes from the sources. With no sources at all, the table is created empty.

Columns:

| Column | Meaning |
|---|---|
| `resource_type`, `workspace_id`, `resource_id`, `resource_name` | The resource. The name is the cluster name, the job name, or the upstream workload name, and falls back to the id. |
| `owner`, `team` | See above. |
| `primary_flag_type`, `flag_types` (`ARRAY<STRING>`), `flag_type_count` | The primary finding's type, then every distinct type, primary first. |
| `findings` (`ARRAY<STRUCT<source, flag_type, savings DECIMAL(38,6), confidence>>`), `findings_json` | Every finding, primary first. `findings_json` is the same as JSON text, for CSV and for clients without struct support. |
| `evidence` | Each source's evidence, prefixed `[source]` and joined by ` \| `. |
| `suggested_action` | The primary finding's action. This is a code (for example `MOVE_TO_JOB_COMPUTE` or `INVESTIGATE_RETRIES`), or F5's sentence. |
| `estimated_monthly_cost`, `estimated_monthly_savings` (`DECIMAL(38,6)`) | See above. `estimated_monthly_savings` is the only column to sum; `estimated_monthly_cost` is not additive (a CLUSTER row includes the jobs that ran on it). |
| `confidence`, `priority_score` (0-100), `priority_rank` | See above. |
| `sources` (`ARRAY<STRING>`), `primary_source` | The source tables that flagged the resource, in source order. |
| `detection_days`, `currency_code`, `created_at`, `run_id` | Run metadata. |

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
