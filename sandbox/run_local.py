"""Run the FinOps accelerator end to end on your laptop with synthetic System Tables.

Usage (from the repo root):
    .\\sandbox\\run_local.ps1
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "sandbox"))

from finops_sandbox import generate_synthetic_system_tables, run_accelerator  # noqa: E402

SOURCE_SCHEMA = "spark_catalog.synthetic_system"
CATALOG = "spark_catalog"  # local Spark only has the session catalog
SCHEMA = "cockpit"  # rule R4: all outputs go to the cockpit schema
OUTPUT_DIR = ROOT / "sandbox" / "local_output"


def build_spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    # Local Spark keeps its catalog in memory, so table files left by a previous run would block
    # CREATE TABLE. Everything here is regenerated each run, so start from an empty warehouse.
    warehouse_dir = ROOT / ".local_spark" / "warehouse"
    shutil.rmtree(warehouse_dir, ignore_errors=True)
    warehouse = warehouse_dir.as_posix()
    builder = (
        SparkSession.builder.appName("finops-local")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def show(spark, title: str, query: str, n: int = 20) -> None:
    print(f"\n=== {title} ===")
    spark.sql(query).show(n, truncate=60)


def main() -> None:
    spark = build_spark()

    print("Generating synthetic system tables ...")
    counts = generate_synthetic_system_tables(spark, SOURCE_SCHEMA)
    for table, count in counts.items():
        print(f"  {SOURCE_SCHEMA}.{table}: {count} rows")

    print("\nRunning databricks_finops pipeline ...")
    run_id, logger = run_accelerator(spark, SOURCE_SCHEMA, CATALOG, SCHEMA, str(ROOT))
    print(f"run_id = {run_id}")
    for rec in logger.records:
        rows = "" if rec.rows_written is None else f" rows={rec.rows_written}"
        print(f"  [{rec.result:8}] {rec.task_name}{rows} - {rec.message[:110]}")

    t = f"{CATALOG}.{SCHEMA}"
    show(spark, "Executive summary", f"SELECT * FROM {t}.vw_executive_summary")
    show(spark, "Most expensive workloads",
         f"SELECT workload_type, workload_id, job_name, team, owner, estimated_cost, attribution_quality "
         f"FROM {t}.vw_most_expensive_workloads ORDER BY estimated_cost DESC")
    show(spark, "Optimization backlog",
         f"SELECT priority_rank, workload_type, workload_id, job_name, issue_type, suggested_action, "
         f"ROUND(estimated_monthly_cost,2) AS monthly_cost, priority_score, confidence "
         f"FROM {t}.optimization_candidates ORDER BY priority_score DESC")
    show(spark, "Compute sizing signals",
         f"SELECT cluster_id, job_name, avg_cpu_pct, avg_memory_pct, utilization_category, sizing_signal, confidence "
         f"FROM {t}.compute_utilization_summary ORDER BY estimated_cost DESC")
    show(spark, "Job reliability",
         f"SELECT job_name, run_count, failed_count, retry_count, failure_rate_pct, "
         f"ROUND(estimated_failed_cost,2) AS failed_cost, reliability_category "
         f"FROM {t}.job_reliability_summary ORDER BY failure_rate_pct DESC")
    show(spark, "Price source mix",
         f"SELECT price_source, COUNT(*) rows, ROUND(SUM(estimated_cost),2) cost FROM {t}.daily_cost GROUP BY 1")
    show(spark, "Health checks",
         f"SELECT check_name, status, severity, message FROM {t}.accelerator_health ORDER BY status, check_name", 40)

    # Export every output table/view to CSV so you can open them in Excel / VS Code.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    names = [r.tableName for r in spark.sql(f"SHOW TABLES IN {t}").collect()]
    for name in names:
        spark.table(f"{t}.{name}").toPandas().to_csv(OUTPUT_DIR / f"{name}.csv", index=False)
    print(f"\nExported {len(names)} tables/views to {OUTPUT_DIR}")
    spark.stop()


if __name__ == "__main__":
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    main()
