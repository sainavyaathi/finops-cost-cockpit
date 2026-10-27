# Navigation Guide

Use this guide when you are opening the accelerator for the first time.

## Repo Structure

```text
databricks.yml                  Bundle targets and defaults
resources/finops_job.yml        Serverless Databricks job resource
commands.ps1                    Local validate/deploy/run command list
config/thresholds.yml           Business thresholds and scoring weights
config/tagging_rules.yml        Required tags, critical tags, and aliases
src/databricks_finops/          Python package and SQL templates
docs/                           Architecture, dashboard, and business notes
tests/                          Unit and static validation tests
```

## Run The Accelerator

Set your Databricks CLI profile:

```powershell
$env:DATABRICKS_CONFIG_PROFILE = "<profile>"
$profile = $env:DATABRICKS_CONFIG_PROFILE
```

Run the dev workflow:

```powershell
databricks bundle validate -t dev -p $profile
databricks bundle deploy -t dev -p $profile
databricks bundle run finops_accelerator_job -t dev -p $profile
```

No `--var` flags are required for normal usage. Defaults live in `databricks.yml`.

## Successful Run Output

A successful run writes structured start and completion logs with the target catalog, schema, lookback window, and `run_id`.

## Output Tables

- `daily_cost`
- `workload_cost_summary`
- `compute_utilization_summary`
- `job_reliability_summary`
- `tagging_quality_summary`
- `optimization_candidates`
- `accelerator_summary`
- `accelerator_health`
- `accelerator_run_log`

## Output Views

- `vw_executive_summary`
- `vw_cost_trend_daily`
- `vw_most_expensive_workloads`
- `vw_optimization_backlog`
- `vw_tagging_quality`
- `vw_architecture_review_candidates`
- `vw_system_table_coverage`
- `vw_attribution_quality`
- `vw_compute_sizing_signals`
- `vw_failed_jobs`
- `vw_wasteful_workloads`

## First Validation Queries

```sql
SHOW TABLES IN finops.accelerator;

SELECT *
FROM finops.accelerator.accelerator_health
ORDER BY created_at DESC;

SELECT *
FROM finops.accelerator.accelerator_run_log
ORDER BY started_at DESC;

SELECT *
FROM finops.accelerator.vw_executive_summary;

SELECT *
FROM finops.accelerator.vw_optimization_backlog
ORDER BY priority_score DESC;
```

## Health Table

`accelerator_health` explains source availability, pricing quality, attribution quality, tagging coverage, degraded optional steps, and config warnings.

Status values:

- `PASS`
- `WARN`
- `FAIL`

Severity values:

- `INFO`
- `LOW`
- `MEDIUM`
- `HIGH`
- `CRITICAL`

## Daily Cost

Start with `daily_cost` to inspect estimated DBUs, estimated cost, price source, attribution quality, and ownership tags by date and workload.

Price source values:

- `LIST_PRICES`
- `FALLBACK_DBU_PRICE`
- `MIXED`
- `UNKNOWN`

## Workload Summary

Use `workload_cost_summary` to find top workloads, active days, owners, teams, projects, DBUs, estimated cost, and attribution quality.

## Utilization Summary

Use `compute_utilization_summary` to review CPU, memory, network, sizing signal, utilization category, and confidence.

Low CPU alone is not a remediation instruction. Memory pressure and workload context must be reviewed.

## Reliability Summary

Use `job_reliability_summary` to review run counts, failures, cancellations, retries, duration, failed DBUs, failed cost, reliability category, and confidence.

Retry counts are best effort because available Lakeflow task-run columns can vary by workspace.

## Tagging Quality

Use `tagging_quality_summary` to review missing required tags and missing critical tags.

Default required tags:

- `project`
- `team`
- `owner`
- `environment`
- `cost_center`

## Optimization Candidates

Use `optimization_candidates` as the prioritized advisory backlog.

Suggested actions are intentionally safe:

- `REVIEW_WORKER_COUNT`
- `REVIEW_NODE_TYPE`
- `INVESTIGATE_MEMORY_PRESSURE`
- `INVESTIGATE_FAILURES`
- `INVESTIGATE_RETRIES`
- `IMPROVE_TAGGING`
- `REVIEW_ALL_PURPOSE_USAGE`
- `LOOKS_HEALTHY`
- `REVIEW_REQUIRED`
- `INSUFFICIENT_DATA`

The accelerator does not perform automatic remediation.

## Dashboard Suggestions

Business overview:

- `vw_executive_summary`
- `vw_cost_trend_daily`
- `vw_most_expensive_workloads`
- `vw_optimization_backlog`

FinOps review:

- `vw_optimization_backlog`
- `vw_tagging_quality`
- `vw_attribution_quality`
- `vw_failed_jobs`

Architecture review:

- `vw_architecture_review_candidates`
- `vw_compute_sizing_signals`
- `vw_wasteful_workloads`

Data quality and coverage:

- `vw_system_table_coverage`
- `accelerator_health`
- `accelerator_run_log`

## Troubleshooting

If `system.billing.usage` is missing, ask a workspace admin to enable or grant billing System Table access.

If many rows use `FALLBACK_DBU_PRICE`, check access to `system.billing.list_prices`.

If utilization is `INSUFFICIENT_DATA`, check access to `system.compute.node_timeline`.

If reliability is `INSUFFICIENT_DATA`, check access to Lakeflow job run System Tables.

If the CLI asks which resource to run, use the explicit resource key:

```powershell
databricks bundle run finops_accelerator_job -t dev -p $profile
```

## Limitations

- Cost values are estimates, not invoices.
- Full cloud-provider infrastructure cost is not included.
- Shared compute attribution can be approximate.
- Optional System Tables may not be enabled in every workspace.
- Retry count is best effort when detailed attempt fields are unavailable.
- Recommendations are advisory and require human review.
