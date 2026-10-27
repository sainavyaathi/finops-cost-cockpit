# Dashboard Guide

Build the first dashboard from the accelerator views rather than raw tables.

The views are designed for the latest accelerator output. `vw_system_table_coverage` filters historical health rows to the latest run so dashboards do not mix old health checks with current summary values.

## Business Overview

Recommended source views:

- `vw_executive_summary`
- `vw_cost_trend_daily`
- `vw_most_expensive_workloads`
- `vw_optimization_backlog`

Recommended tiles:

- Total estimated cost
- Total DBUs
- Workload count
- High-priority candidate count
- Daily cost trend
- Top workloads by estimated cost
- Optimization backlog by priority rank

## FinOps Review

Recommended source views:

- `vw_optimization_backlog`
- `vw_tagging_quality`
- `vw_attribution_quality`
- `vw_failed_jobs`

Recommended tiles:

- Priority candidates by owner, team, and project
- Missing tag spend
- Worst tagging quality by team, owner, and project using `vw_tagging_quality.worst_tagging_quality`
- Unknown or low-attribution spend
- Estimated failed job cost
- Retry-heavy workloads

## Architecture Review

Recommended source views:

- `vw_architecture_review_candidates`
- `vw_compute_sizing_signals`
- `vw_wasteful_workloads`

Recommended tiles:

- High-cost low-utilization workloads
- Memory-bound workloads
- Shared-cluster attribution
- Low-confidence recommendations
- Compute sizing signals by cluster and job

## Data Quality & Coverage

Recommended source views:

- `vw_system_table_coverage`
- `accelerator_health`
- `accelerator_run_log`

Recommended tiles:

- System Table availability
- Health check severity
- Pipeline run duration by step
- Rows written by step
- Outputs affected by unavailable optional tables

## Notes

Cost values are estimates, not invoices. `vw_cost_trend_daily.failed_cost` distributes job-level failed cost across the active days for that job in the lookback window. Utilization signals are review prompts and should be interpreted with workload context.

Use `daily_cost.price_source` to explain pricing confidence:

- `LIST_PRICES`
- `FALLBACK_DBU_PRICE`
- `MIXED`
- `UNKNOWN`
