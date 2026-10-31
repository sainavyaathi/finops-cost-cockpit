# Databricks notebook source
# MAGIC %md
# MAGIC # FinOps Accelerator — synthetic data run (works on Databricks Free Edition)
# MAGIC
# MAGIC 1. Creates synthetic copies of the nine System Tables in `workspace.finops_synthetic`
# MAGIC    (the six the accelerator reads plus `compute.clusters`, `access.audit`, `query.history`)
# MAGIC 2. Runs the **unmodified** `databricks_finops` pipeline, redirecting `system.*` reads to those tables
# MAGIC 3. Writes outputs + dashboard views to `workspace.cockpit`
# MAGIC
# MAGIC Requires the whole repo folder to be in your workspace (this notebook lives in `<repo>/sandbox/`).
# MAGIC Set `USE_REAL_SYSTEM_TABLES = True` to try your real `system.billing.usage` instead.
# MAGIC
# MAGIC The `profile` widget picks the synthetic data set: `default` (45 days of designed scenarios,
# MAGIC 30-day lookback) or `demo` (180 days of company-scale history with storylines, 180-day lookback).
# MAGIC In `real` mode the profile is ignored and the lookback stays 30 days.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "synthetic", ["synthetic", "real"])
dbutils.widgets.text("schema", "cockpit")
dbutils.widgets.dropdown("profile", "default", ["default", "demo"])

CATALOG = "workspace"                                   # Free Edition default catalog
SCHEMA = dbutils.widgets.get("schema")                  # outputs
SOURCE_SCHEMA = "workspace.finops_synthetic"
PROFILE = dbutils.widgets.get("profile")
USE_REAL_SYSTEM_TABLES = dbutils.widgets.get("mode") == "real"
# Demo trends cover its six months; real system tables keep the standard 30-day lookback.
LOOKBACK_DAYS = 180 if PROFILE == "demo" and not USE_REAL_SYSTEM_TABLES else 30

# COMMAND ----------

import os
import sys

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_ROOT = "/Workspace" + os.path.dirname(os.path.dirname(notebook_path))
for p in (f"{REPO_ROOT}/src", f"{REPO_ROOT}/sandbox"):
    if p not in sys.path:
        sys.path.insert(0, p)
print("repo root:", REPO_ROOT)

# COMMAND ----------

# MAGIC %md ## 1. Check which real System Tables this workspace exposes

# COMMAND ----------

for t in ["system.billing.usage", "system.billing.list_prices", "system.compute.node_timeline",
          "system.lakeflow.jobs", "system.lakeflow.job_run_timeline", "system.lakeflow.job_task_run_timeline",
          "system.compute.clusters", "system.access.audit", "system.query.history"]:
    try:
        n = spark.sql(f"SELECT COUNT(*) FROM {t}").collect()[0][0]
        print(f"OK       {t}: {n} rows")
    except Exception as e:
        print(f"MISSING  {t}: {str(e).splitlines()[0][:120]}")

# COMMAND ----------

# MAGIC %md ## 2. Generate synthetic System Tables and run the pipeline

# COMMAND ----------

from finops_sandbox import RedirectingSpark, generate_synthetic_system_tables, run_accelerator

if USE_REAL_SYSTEM_TABLES:
    from cockpit.pipeline import run_cockpit_pipeline
    from databricks_finops.config import config_from_args, parse_args
    from databricks_finops.main import _run_id
    from databricks_finops.run_logging import StepLogger, write_run_log

    config = config_from_args(parse_args(["--catalog", CATALOG, "--schema", SCHEMA,
                                          "--lookback-days", str(LOOKBACK_DAYS), "--bundle-root", REPO_ROOT]))
    run_id = _run_id()
    logger = StepLogger(run_id)
    try:
        run_cockpit_pipeline(spark, config, run_id, logger)
    finally:
        write_run_log(spark, CATALOG, SCHEMA, logger)
else:
    counts = generate_synthetic_system_tables(spark, SOURCE_SCHEMA, profile=PROFILE)
    print(counts)
    run_id, logger = run_accelerator(spark, SOURCE_SCHEMA, CATALOG, SCHEMA, REPO_ROOT, LOOKBACK_DAYS)

for r in logger.records:
    print(f"[{r.result:8}] {r.task_name:36} rows={r.rows_written} {r.message[:100]}")

# COMMAND ----------

# MAGIC %md ## 3. Explore the outputs

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.vw_executive_summary"))

# COMMAND ----------

display(spark.sql(f"""
SELECT priority_rank, workload_type, workload_id, job_name, issue_type, suggested_action,
       ROUND(estimated_monthly_cost, 2) AS monthly_cost, priority_score, confidence, evidence
FROM {CATALOG}.{SCHEMA}.optimization_candidates ORDER BY priority_score DESC"""))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.vw_cost_trend_daily ORDER BY usage_date"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.compute_utilization_summary ORDER BY estimated_cost DESC"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.job_reliability_summary ORDER BY failure_rate_pct DESC"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.accelerator_health WHERE run_id = '{run_id}' ORDER BY status DESC"))
