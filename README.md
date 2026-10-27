# Databricks FinOps Accelerator

A production-ready Databricks FinOps accelerator that deploys with **Databricks Asset Bundles**, runs on **serverless job compute**, reads **Databricks System Tables**, and creates business-ready cost, utilization, reliability, tagging, attribution, and optimization views.

This project helps platform teams, data architects, FinOps owners, and business stakeholders understand:

- Where Databricks spend is going
- Which teams, owners, projects, workspaces, and workloads drive spend
- Which spend is untagged, unattributed, or low confidence
- Which jobs have reliability, retry, or failed-cost issues
- Which workloads show utilization or sizing review signals
- Which optimization opportunities should be reviewed first
- Which recommendations have enough evidence to trust

This accelerator does **not** automatically resize, delete, stop, disable, or modify workloads. It produces an advisory optimization backlog with confidence and evidence so humans can review safely before taking action.

---

## Why this exists

Databricks provides rich System Tables for billing, list prices, compute telemetry, and Lakeflow job activity. Those raw tables are powerful, but they are not automatically easy for business, FinOps, architecture, and platform teams to act on.

This accelerator turns raw System Table data into curated Delta tables and dashboard-ready SQL views.

Instead of only asking:

> "How much did we spend?"

It helps answer:

> "What drove the spend, who owns it, what needs review, and how confident are we?"

---

## What it does

The accelerator reads real Databricks System Tables and creates a curated FinOps layer under your configured Unity Catalog target.

Default output location:

```text
finops.accelerator
```

It creates:

- Daily cost fact table
- Workload cost summary
- Compute utilization summary
- Job reliability summary
- Tagging quality summary
- Optimization candidates
- Executive summary table
- Health and run log tables
- Business stakeholder views
- FinOps review views
- Architecture review views
- System Table coverage views

---

## What it does not do

- It does not include full cloud provider infrastructure cost.
- It does not reconcile exactly to cloud invoices.
- It does not automatically resize clusters, warehouses, jobs, or pipelines.
- It does not stop, delete, disable, or mutate workloads.
- It does not use AI, agents, LangChain, OpenAI APIs, or external services.
- It does not make shared all-purpose compute attribution perfect.
- It does not replace engineering, architecture, finance, or business review.

---

## Architecture

```text
Databricks System Tables
        |
        |-- system.billing.usage
        |-- system.billing.list_prices
        |-- system.compute.node_timeline
        |-- system.lakeflow.jobs
        |-- system.lakeflow.job_run_timeline
        |-- system.lakeflow.job_task_run_timeline
        |
        v
Databricks Asset Bundle Job
        |
        |-- Serverless Python wheel task
        |-- Poetry package
        |-- No manual cluster setup
        |-- No task-level libraries
        |
        v
Curated Delta Tables
        |
        |-- daily_cost
        |-- workload_cost_summary
        |-- compute_utilization_summary
        |-- job_reliability_summary
        |-- tagging_quality_summary
        |-- optimization_candidates
        |-- accelerator_summary
        |-- accelerator_health
        |-- accelerator_run_log
        |
        v
Dashboard-ready Views
        |
        |-- Business overview
        |-- Daily cost trend
        |-- Cost drivers
        |-- Optimization backlog
        |-- Tagging quality
        |-- Architecture review candidates
        |-- System Table coverage
        |-- Attribution quality
        |-- Compute sizing signals
        |-- Failed jobs
        |-- Wasteful workloads
```

---

## Key design principles

- **Bundle-first**: deployed and run through Databricks Asset Bundles.
- **Serverless by default**: no manual job-cluster configuration.
- **Real System Tables**: the job reads actual Databricks telemetry and billing sources.
- **No required CLI variables**: defaults live in `databricks.yml`.
- **Safe recommendations**: suggested actions are review prompts only.
- **Confidence-aware**: candidates include confidence and evidence.
- **System-table aware**: optional sources degrade gracefully.
- **Production-friendly**: includes run logs, health checks, deterministic outputs, and a scheduled job.
- **Workspace-aware**: workload grains include `workspace_id` so account-level data does not collapse unrelated resources.

---

## Prerequisites

Local machine:

- Python 3.10 or 3.11
- Poetry
- Databricks CLI configured with a profile
- Access to a Databricks workspace with Asset Bundles support

Databricks workspace:

- Serverless jobs enabled
- Unity Catalog permissions to create or use the target catalog and schema
- Access to Databricks System Tables

Required System Table:

```text
system.billing.usage
```

Recommended optional System Tables:

```text
system.billing.list_prices
system.compute.node_timeline
system.lakeflow.jobs
system.lakeflow.job_run_timeline
system.lakeflow.job_task_run_timeline
```

If optional tables are unavailable, the accelerator still creates typed outputs and marks affected data as limited or `INSUFFICIENT_DATA`.

If `system.billing.usage` is unavailable, the accelerator fails because billing usage is the core source.

---

## Default configuration

Defaults live in [databricks.yml](databricks.yml).

Default output location:

```text
finops.accelerator
```

Default settings:

```text
catalog = finops
schema = accelerator
lookback_days = 30
currency_code = USD
fallback_dbu_price = 0.55
compute = serverless
dev schedule = paused
prod schedule = paused
```

`display_currency` is written to output tables and always matches `currency_code`. The accelerator does not perform FX conversion.

To change the output catalog, schema, lookback window, currency, fallback DBU price, or schedule state, edit `databricks.yml` or target variables before deployment.

Business thresholds live in:

```text
config/thresholds.yml
```

Tagging requirements and aliases live in:

```text
config/tagging_rules.yml
```

If either file is missing or malformed, the accelerator uses safe built-in defaults and writes a WARN record to `accelerator_health`.

---

## Quick start

From the repo root, install dependencies:

```powershell
poetry install
```

Run local checks:

```powershell
poetry run pytest
poetry build --format wheel
```

Set your Databricks CLI profile:

```powershell
$env:DATABRICKS_CONFIG_PROFILE = "<profile>"
$profile = $env:DATABRICKS_CONFIG_PROFILE
```

Validate the bundle:

```powershell
databricks bundle validate -t dev -p $profile
```

Deploy:

```powershell
databricks bundle deploy -t dev -p $profile
```

Run:

```powershell
databricks bundle run finops_accelerator_job -t dev -p $profile
```

The resource key is:

```text
finops_accelerator_job
```

Always include the resource key when running the bundle. Otherwise, the CLI may ask you to choose a resource interactively.

You can also use the included [commands.ps1](commands.ps1) script after setting `DATABRICKS_CONFIG_PROFILE`.

---

## Recommended rollout

1. Run local tests.
2. Validate the bundle in `dev`.
3. Deploy to `dev`.
4. Run `finops_accelerator_job` manually.
5. Confirm tables and views are created.
6. Review `accelerator_health` and `accelerator_run_log`.
7. Grant or adjust System Table permissions if any required or optional source is unavailable.
8. Build a Databricks dashboard from the provided views.
9. Tune thresholds and tagging rules for your organization.
10. Deploy `prod` with an appropriate service principal profile after a successful manual run.

---

## Successful run example

A successful run starts with a structured log similar to:

```json
{
  "message": "Databricks FinOps Accelerator started",
  "status": "started",
  "catalog": "finops",
  "schema": "accelerator",
  "lookback_days": 30,
  "run_id": "20260709T101954560840Z"
}
```

And completes with a structured log similar to:

```json
{
  "message": "Databricks FinOps Accelerator completed",
  "status": "success",
  "catalog": "finops",
  "schema": "accelerator",
  "run_id": "20260709T101954560840Z"
}
```

Each run has a `run_id` so outputs, logs, and health checks can be traced.

---

## Output tables

The accelerator creates these Delta tables in `${catalog}.${schema}`:

| Table | Purpose |
| --- | --- |
| `daily_cost` | Core daily cost fact table from billing usage |
| `workload_cost_summary` | Workload-level cost summary and ranking |
| `compute_utilization_summary` | CPU, memory, network, and sizing signals |
| `job_reliability_summary` | Job failures, retries, duration, and reliability signals |
| `tagging_quality_summary` | Missing ownership and governance tags |
| `optimization_candidates` | Prioritized review backlog |
| `accelerator_summary` | Run-level executive summary |
| `accelerator_health` | Source table availability and quality checks |
| `accelerator_run_log` | Step-level execution log |

---

## Dashboard-ready views

The accelerator creates these views in `${catalog}.${schema}`:

| View | Audience | Purpose |
| --- | --- | --- |
| `vw_executive_summary` | Business / leadership | High-level cost, DBU, tagging, failed-cost, and workload summary |
| `vw_cost_trend_daily` | Business / FinOps | Daily cost trend and failed-cost signal |
| `vw_most_expensive_workloads` | Business / FinOps | Top spend drivers |
| `vw_optimization_backlog` | Business / platform | Prioritized review backlog |
| `vw_tagging_quality` | Governance / FinOps | Worst tagging quality by team, owner, and project |
| `vw_architecture_review_candidates` | Architects | Workloads needing technical review |
| `vw_system_table_coverage` | Platform | System Table availability and output limitations |
| `vw_attribution_quality` | Architects / FinOps | Cost by attribution quality |
| `vw_compute_sizing_signals` | Architects | Utilization and sizing review signals |
| `vw_failed_jobs` | Engineering | Reliability and failure analysis |
| `vw_wasteful_workloads` | Architects / FinOps | Potentially inefficient workloads |

Dashboard guide:

```text
docs/dashboard_guide.md
```

The accelerator does not deploy Lakeview JSON by default. It ships SQL views and a dashboard guide so teams can build dashboards in Databricks SQL or another BI tool.

---

## First validation queries

After the job runs, open Databricks SQL and run:

```sql
SHOW TABLES IN finops.accelerator;
```

Check health:

```sql
SELECT *
FROM finops.accelerator.accelerator_health
ORDER BY created_at DESC;
```

Check run logs:

```sql
SELECT *
FROM finops.accelerator.accelerator_run_log
ORDER BY started_at DESC;
```

Check executive summary:

```sql
SELECT *
FROM finops.accelerator.vw_executive_summary;
```

Check optimization backlog:

```sql
SELECT *
FROM finops.accelerator.vw_optimization_backlog
ORDER BY priority_score DESC;
```

---

## Business value

For business stakeholders, this accelerator helps answer:

- How much are we spending on Databricks?
- Which workloads drive most of the cost?
- Which workspaces, teams, owners, or projects are associated with spend?
- How much spend is missing ownership or tags?
- Which workloads should be reviewed first?
- What is the estimated cost impact of failures and inefficient workloads?
- Which trend lines are moving in the wrong direction?

Useful views:

```sql
SELECT * FROM finops.accelerator.vw_executive_summary;
SELECT * FROM finops.accelerator.vw_cost_trend_daily ORDER BY usage_date DESC;
SELECT * FROM finops.accelerator.vw_most_expensive_workloads ORDER BY estimated_cost DESC;
SELECT * FROM finops.accelerator.vw_optimization_backlog ORDER BY priority_score DESC;
```

---

## Architecture value

For data architects and platform teams, this accelerator helps answer:

- Which expensive workloads are healthy versus suspicious?
- Which workloads have low utilization?
- Which workloads are memory-bound and should not be blindly downsized?
- Which workloads have poor attribution?
- Which workloads are running on shared or unclear compute?
- Which optional System Tables are missing?
- How confident are the recommendations?

Useful views:

```sql
SELECT * FROM finops.accelerator.vw_architecture_review_candidates ORDER BY priority_score DESC;
SELECT * FROM finops.accelerator.vw_compute_sizing_signals ORDER BY estimated_cost DESC;
SELECT * FROM finops.accelerator.vw_attribution_quality ORDER BY estimated_cost DESC;
SELECT * FROM finops.accelerator.vw_system_table_coverage ORDER BY created_at DESC;
```

---

## Understanding accelerator health

The health table explains source availability and output quality.

```sql
SELECT *
FROM finops.accelerator.accelerator_health
ORDER BY created_at DESC;
```

Important checks include:

| Check | Meaning |
| --- | --- |
| `system.billing.usage` | Required billing usage table availability |
| `system.billing.list_prices` | Optional list price table availability |
| `system.compute.node_timeline` | Optional compute utilization telemetry availability |
| `system.lakeflow.jobs` | Optional job metadata availability |
| `system.lakeflow.job_run_timeline` | Optional job run history availability |
| `system.lakeflow.job_task_run_timeline` | Optional task retry data availability |

Status values:

```text
PASS
WARN
FAIL
```

Severity values:

```text
INFO
LOW
MEDIUM
HIGH
CRITICAL
```

Optional-source warnings are expected in workspaces where the corresponding System Tables are not enabled or not granted to the job identity.

---

## Understanding daily cost

`daily_cost` is the core fact table.

It is built from:

```text
system.billing.usage
```

And optionally joins:

```text
system.billing.list_prices
```

Important fields:

| Column | Description |
| --- | --- |
| `usage_date` | Usage date |
| `workspace_id` | Workspace identifier |
| `billing_origin_product` | Databricks product area |
| `sku_name` | Billing SKU |
| `cloud` | Cloud provider |
| `usage_unit` | Billing usage unit |
| `workload_type` | Derived workload type |
| `workload_id` | Derived workload identifier |
| `job_id` | Job ID when available |
| `job_name` | Job name when available |
| `cluster_id` | Cluster ID when available |
| `warehouse_id` | SQL warehouse ID when available |
| `pipeline_id` | Pipeline ID when available |
| `notebook_id` | Notebook ID when available |
| `run_as` | Principal associated with usage |
| `project` | Project tag |
| `team` | Team tag |
| `owner` | Owner tag |
| `environment` | Environment tag |
| `cost_center` | Cost center tag |
| `dbus` | DBUs consumed |
| `estimated_cost` | Estimated DBU cost |
| `currency_code` | Currency used for list price lookup |
| `display_currency` | Output currency label; always matches `currency_code` |
| `price_source` | Pricing source used |
| `attribution_quality` | Quality of workload attribution |
| `attribution_notes` | Explanation of attribution quality |

Check pricing source:

```sql
SELECT
  price_source,
  COUNT(*) AS rows,
  ROUND(SUM(estimated_cost), 2) AS estimated_cost
FROM finops.accelerator.daily_cost
GROUP BY price_source;
```

Expected values:

```text
LIST_PRICES
FALLBACK_DBU_PRICE
MIXED
UNKNOWN
```

`LIST_PRICES` is preferred. `FALLBACK_DBU_PRICE` means the configured fallback DBU price was used. `MIXED` means a grouped output row includes both list-price and fallback-priced usage. `UNKNOWN` means no usable price source was available.

---

## Understanding workload cost summary

`workload_cost_summary` rolls up cost by workload.

```sql
SELECT *
FROM finops.accelerator.workload_cost_summary
ORDER BY estimated_cost DESC
LIMIT 100;
```

Use this table to identify:

- Most expensive workloads
- Workload owners
- Active days
- DBUs consumed
- Average daily cost
- Attribution quality
- Cost rank

---

## Understanding attribution quality

`attribution_quality` explains how confidently cost can be tied to a useful workload or owner.

Values:

```text
HIGH
MEDIUM
LOW
UNKNOWN
```

Interpretation:

| Value | Meaning |
| --- | --- |
| `HIGH` | Strong workload metadata such as job, SQL warehouse, or pipeline is available |
| `MEDIUM` | Some useful metadata exists, such as cluster or run-as, but attribution is incomplete |
| `LOW` | Attribution is broad, shared, or requires review |
| `UNKNOWN` | Useful workload metadata is missing |

Use `attribution_notes` to understand why a row was classified that way.

---

## Understanding utilization

`compute_utilization_summary` uses `system.compute.node_timeline` when available.

```sql
SELECT *
FROM finops.accelerator.compute_utilization_summary
ORDER BY estimated_cost DESC
LIMIT 100;
```

Key fields:

| Column | Description |
| --- | --- |
| `avg_cpu_pct` | Average CPU utilization |
| `peak_cpu_pct` | Peak CPU utilization |
| `avg_memory_pct` | Average memory utilization |
| `peak_memory_pct` | Peak memory utilization |
| `avg_network_sent_bytes` | Average network sent bytes |
| `avg_network_received_bytes` | Average network received bytes |
| `dbus_per_cpu_pct` | Cost-to-utilization signal |
| `utilization_category` | Utilization classification |
| `utilization_grain` | Granularity of utilization signal |
| `sizing_signal` | Suggested review signal |
| `confidence` | Confidence level |

Utilization categories include:

```text
HIGH_COST_LOW_UTILIZATION
LOW_UTILIZATION
MEMORY_BOUND_DO_NOT_DOWNSIZE
HEALTHY_UTILIZATION
REVIEW_REQUIRED
INSUFFICIENT_DATA
```

Important: low CPU alone does not mean a workload should be downsized. If memory is high, the accelerator marks the workload as memory-bound and avoids unsafe recommendations.

---

## Understanding reliability

`job_reliability_summary` uses Lakeflow job run tables when available.

```sql
SELECT *
FROM finops.accelerator.job_reliability_summary
ORDER BY failure_rate_pct DESC
LIMIT 100;
```

It tracks:

- Run count
- Success count
- Failed count
- Cancelled count
- Skipped count
- Retry count
- Failure rate
- Average duration
- Estimated failed DBUs
- Estimated failed cost
- Last run state
- Reliability category
- Confidence

Reliability categories include:

```text
HEALTHY
FAILURE_HEAVY
RETRY_HEAVY
REVIEW_REQUIRED
INSUFFICIENT_DATA
```

Failed-cost attribution joins `system.billing.usage` directly by `usage_metadata.job_run_id`; it does not require `daily_cost` to carry job-run-level detail.

---

## Understanding tagging quality

`tagging_quality_summary` checks required ownership and governance tags.

Required tags:

```text
project
team
owner
environment
cost_center
```

Query:

```sql
SELECT *
FROM finops.accelerator.tagging_quality_summary
ORDER BY estimated_cost DESC
LIMIT 100;
```

Tagging quality values:

```text
GOOD
PARTIAL
POOR
MISSING
UNKNOWN
```

The `vw_tagging_quality` view exposes `worst_tagging_quality` so group-level dashboard values show the worst semantic quality rather than alphabetical string order.

---

## Understanding optimization candidates

`optimization_candidates` is the main prioritized backlog.

```sql
SELECT *
FROM finops.accelerator.optimization_candidates
ORDER BY priority_score DESC
LIMIT 100;
```

It combines:

- Cost
- Utilization
- Reliability
- Tagging quality
- Attribution quality
- Frequency
- Confidence
- Evidence

Important fields:

| Column | Description |
| --- | --- |
| `priority_rank` | Candidate rank within workspace |
| `workspace_id` | Workspace identifier |
| `issue_type` | Main issue identified |
| `suggested_action` | Safe recommended review action |
| `estimated_monthly_cost` | Estimated monthlyized cost |
| `priority_score` | 0-100 prioritization score |
| `confidence` | `HIGH`, `MEDIUM`, or `LOW` |
| `evidence` | Human-readable explanation |

Issue types include:

```text
EXPENSIVE_WORKLOAD
HEALTHY_EXPENSIVE
HIGH_COST_LOW_UTILIZATION
LOW_UTILIZATION
MEMORY_BOUND
HIGH_FAILURE_COST
HIGH_FAILURE_RATE
RETRY_HEAVY
MISSING_TAGS
UNKNOWN_ATTRIBUTION
SHARED_CLUSTER_ATTRIBUTION
INSUFFICIENT_DATA
REVIEW_REQUIRED
```

Suggested actions include:

```text
REVIEW_WORKER_COUNT
REVIEW_NODE_TYPE
INVESTIGATE_MEMORY_PRESSURE
INVESTIGATE_FAILURES
INVESTIGATE_RETRIES
IMPROVE_TAGGING
REVIEW_ALL_PURPOSE_USAGE
LOOKS_HEALTHY
REVIEW_REQUIRED
INSUFFICIENT_DATA
```

The accelerator intentionally avoids destructive or immediate remediation instructions. Suggested actions are review prompts only.

---

## Understanding confidence

`confidence` helps reviewers understand how much evidence supports a candidate.

Values:

```text
HIGH
MEDIUM
LOW
```

Interpretation:

| Value | Meaning |
| --- | --- |
| `HIGH` | Strong utilization or reliability evidence exists |
| `MEDIUM` | Partial optional source evidence exists |
| `LOW` | Recommendation depends on missing optional sources, fallback pricing, limited telemetry, or insufficient data |

Low confidence does not mean the candidate is wrong. It means the workload should be reviewed with more context before action.

---

## Priority scoring

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

---

## Suggested dashboard tabs

### 1. Business overview

Use:

```text
vw_executive_summary
vw_cost_trend_daily
vw_most_expensive_workloads
vw_optimization_backlog
```

Purpose:

- Total estimated cost
- DBUs consumed
- Workload count
- Cost trend
- Top cost drivers
- Untagged cost
- High-priority review count

### 2. FinOps review

Use:

```text
vw_optimization_backlog
vw_tagging_quality
vw_attribution_quality
vw_failed_jobs
```

Purpose:

- Priority candidates by owner, team, and project
- Missing tag spend
- Worst tagging quality
- Unknown or low-attribution spend
- Failed job cost
- Retry-heavy workloads

### 3. Architecture review

Use:

```text
vw_architecture_review_candidates
vw_compute_sizing_signals
vw_wasteful_workloads
```

Purpose:

- High-cost low-utilization workloads
- Memory-bound workloads
- Shared-cluster attribution
- Low-confidence recommendations
- Compute sizing signals by cluster and job

### 4. Data quality and coverage

Use:

```text
vw_system_table_coverage
accelerator_health
accelerator_run_log
```

Purpose:

- Source table availability
- Health checks
- Pipeline run duration by step
- Rows written by step
- Outputs affected by unavailable optional tables

---

## Production deployment

Validate production:

```powershell
databricks bundle validate -t prod -p $profile
```

Deploy production:

```powershell
databricks bundle deploy -t prod -p $profile
```

Run production manually:

```powershell
databricks bundle run finops_accelerator_job -t prod -p $profile
```

Production recommendations:

- Use a service principal Databricks CLI profile for stable ownership.
- Pre-create the target catalog when possible.
- Start with the default 30-day lookback.
- Keep the production schedule paused until a manual production run succeeds.
- Keep dashboard access limited until a manual production run succeeds.
- Review `accelerator_health` before exposing dashboards broadly.
- Tune `config/thresholds.yml` and `config/tagging_rules.yml` to your organization.

---

## Permissions required

The Databricks job identity needs:

- `USE CATALOG` and `USE SCHEMA` on required `system` schemas.
- `SELECT` on `system.billing.usage`.
- `SELECT` on optional `system.billing.list_prices`.
- `SELECT` on optional `system.compute.node_timeline`.
- `SELECT` on optional `system.lakeflow.*` tables.
- `CREATE CATALOG` if the target catalog is not pre-created.
- `CREATE SCHEMA`, `CREATE TABLE`, and `CREATE VIEW` on the target catalog/schema.

---

## Common troubleshooting

### CLI asks "Resource to run"

Wrong:

```powershell
databricks bundle run -t dev -p $profile
```

Right:

```powershell
databricks bundle run finops_accelerator_job -t dev -p $profile
```

### Active run limit error

Error:

```text
cannot start job: There are already 5 active runs
```

Fix:

```powershell
databricks bundle summary -t dev -p $profile
```

Find the job ID, then:

```powershell
databricks jobs list-runs --job-id <job-id> --active-only --limit 20 -p $profile
databricks jobs cancel-all-runs --job-id <job-id> --all-queued-runs -p $profile
```

Then rerun:

```powershell
databricks bundle run finops_accelerator_job -t dev -p $profile
```

### `system.billing.usage` is missing

The accelerator requires this table:

```text
system.billing.usage
```

Ask a workspace admin to enable or grant access to billing System Tables.

### Pricing uses fallback

Check:

```sql
SELECT
  price_source,
  COUNT(*) AS rows,
  ROUND(SUM(estimated_cost), 2) AS estimated_cost
FROM finops.accelerator.daily_cost
GROUP BY price_source;
```

If many rows use `FALLBACK_DBU_PRICE`, check access to:

```text
system.billing.list_prices
```

### Utilization is `INSUFFICIENT_DATA`

This usually means `system.compute.node_timeline` is unavailable or there is no matching telemetry for the lookback window.

Check:

```sql
SELECT *
FROM finops.accelerator.vw_system_table_coverage
ORDER BY created_at DESC;
```

### All spend is missing tags

This is not necessarily a bug. It usually means the underlying jobs, clusters, warehouses, or workloads are not consistently tagged.

Check:

```sql
SELECT *
FROM finops.accelerator.vw_tagging_quality
ORDER BY estimated_cost DESC;
```

---

## Limitations

This accelerator is intentionally advisory.

Known limitations:

- `estimated_cost` is not the final invoice.
- Cost is estimated using billing usage and available list prices.
- Full cloud-provider infrastructure cost is not included.
- Utilization may be cluster-level, not perfectly job-level.
- Serverless and shared compute attribution depends on available metadata.
- Optional System Tables may not be enabled or granted in every workspace.
- Low CPU does not automatically mean waste.
- Memory pressure must be reviewed before changing worker count or node type.
- The accelerator does not perform automatic remediation.
- Recommendations should be reviewed by platform or architecture teams before action.

---

## Development

Install dependencies:

```powershell
poetry install
```

Run tests:

```powershell
poetry run pytest
```

Build package:

```powershell
poetry build --format wheel
```

Validate bundle:

```powershell
databricks bundle validate -t dev -p $profile
```

Deploy:

```powershell
databricks bundle deploy -t dev -p $profile
```

Run:

```powershell
databricks bundle run finops_accelerator_job -t dev -p $profile
```

---

## Repository layout

```text
databricks-finops-accelerator/
  databricks.yml
  pyproject.toml
  poetry.lock
  README.md
  commands.ps1

  config/
    thresholds.yml
    tagging_rules.yml

  resources/
    finops_job.yml

  src/
    databricks_finops/
      main.py
      config.py
      preflight.py
      costs.py
      utilization.py
      reliability.py
      tagging.py
      optimization.py
      views.py
      run_logging.py
      spark_utils.py
      sql/
        views/

  docs/
    architecture.md
    business_value.md
    dashboard_guide.md

  tests/
```

---
