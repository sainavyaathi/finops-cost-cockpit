from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .config import AppConfig, OUTPUT_TABLES, OUTPUT_VIEWS, config_from_args, parse_args
from .costs import comment_cost_tables, create_daily_cost, create_workload_cost_summary
from .health import write_health
from .optimization import comment_optimization_table, create_optimization_candidates
from .preflight import PreflightResult, run_preflight
from .reliability import comment_reliability_table, create_job_reliability_summary
from .run_logging import StepLogger, write_run_log
from .spark_utils import create_namespace, log_json, qname
from .summary import comment_summary_table, create_accelerator_summary
from .tagging import comment_tagging_table, create_tagging_quality_summary
from .utilization import comment_utilization_table, create_compute_utilization_summary
from .views import create_views


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _comment_tables(spark, config) -> str:
    comment_cost_tables(spark, config)
    comment_utilization_table(spark, config)
    comment_reliability_table(spark, config)
    comment_tagging_table(spark, config)
    comment_optimization_table(spark, config)
    comment_summary_table(spark, config)
    return "table comments applied"


def _write_health(spark: Any, config: AppConfig, run_id: str, preflight: PreflightResult, logger: StepLogger) -> None:
    kwargs = {"config_warnings": config.config_warnings} if config.config_warnings else {}
    write_health(
        spark,
        config.catalog,
        config.schema,
        run_id,
        preflight,
        logger,
        **kwargs,
    )


def run_pipeline(
    spark: Any,
    config: AppConfig,
    run_id: str,
    logger: StepLogger,
) -> PreflightResult:
    preflight_result: PreflightResult | None = None

    try:
        logger.run(
            "create_catalog_schema",
            lambda: create_namespace(spark, config.catalog, config.schema),
        )

        def preflight_operation() -> str:
            nonlocal preflight_result
            preflight_result = run_preflight(spark)
            preflight_result.require_billing_usage()
            return "preflight checks completed"

        logger.run("preflight", preflight_operation)
        assert preflight_result is not None

        logger.run(
            "build_daily_cost",
            lambda: create_daily_cost(spark, config, run_id, preflight_result),
            spark=spark,
            count_table=qname(config.catalog, config.schema, "daily_cost"),
        )
        logger.run(
            "build_workload_cost_summary",
            lambda: create_workload_cost_summary(spark, config, run_id),
            spark=spark,
            count_table=qname(config.catalog, config.schema, "workload_cost_summary"),
        )
        logger.run(
            "build_compute_utilization_summary",
            lambda: create_compute_utilization_summary(spark, config, run_id, preflight_result),
            spark=spark,
            count_table=qname(config.catalog, config.schema, "compute_utilization_summary"),
        )
        logger.run(
            "build_job_reliability_summary",
            lambda: create_job_reliability_summary(spark, config, run_id, preflight_result),
            spark=spark,
            count_table=qname(config.catalog, config.schema, "job_reliability_summary"),
        )
        logger.run(
            "build_tagging_quality_summary",
            lambda: create_tagging_quality_summary(spark, config, run_id),
            spark=spark,
            count_table=qname(config.catalog, config.schema, "tagging_quality_summary"),
        )
        logger.run(
            "build_optimization_candidates",
            lambda: create_optimization_candidates(spark, config, run_id),
            spark=spark,
            count_table=qname(config.catalog, config.schema, "optimization_candidates"),
        )
        logger.run(
            "build_accelerator_summary",
            lambda: create_accelerator_summary(spark, config, run_id),
            spark=spark,
            count_table=qname(config.catalog, config.schema, "accelerator_summary"),
        )
        logger.run(
            "build_accelerator_health",
            lambda: _write_health(spark, config, run_id, preflight_result, logger),
            spark=spark,
            count_table=qname(config.catalog, config.schema, "accelerator_health"),
        )
        logger.run(
            "create_views",
            lambda: create_views(spark, config),
        )
        logger.run(
            "apply_table_comments",
            lambda: _comment_tables(spark, config),
            required=False,
        )
        return preflight_result
    except Exception:
        if preflight_result is not None:
            try:
                _write_health(spark, config, run_id, preflight_result, logger)
            except Exception:
                pass
        raise


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    config = config_from_args(args)
    run_id = _run_id()
    logger = StepLogger(run_id)

    log_json(
        {
            "message": "Databricks FinOps Accelerator started",
            "status": "started",
            "runtime": "serverless",
            "catalog": config.catalog,
            "schema": config.schema,
            "lookback_days": config.lookback_days,
            "run_id": run_id,
        }
    )

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError("PySpark is required. Run this entry point as a Databricks job.") from exc

    spark = SparkSession.builder.getOrCreate()

    try:
        run_pipeline(spark, config, run_id, logger)
    except Exception:
        try:
            write_run_log(spark, config.catalog, config.schema, logger)
        except Exception:
            pass
        raise

    write_run_log(spark, config.catalog, config.schema, logger)

    log_json(
        {
            "message": "Databricks FinOps Accelerator completed",
            "status": "success",
            "runtime": "serverless",
            "catalog": config.catalog,
            "schema": config.schema,
            "run_id": run_id,
            "tables_created": [f"{config.catalog}.{config.schema}.{table}" for table in OUTPUT_TABLES],
            "views_created": [f"{config.catalog}.{config.schema}.{view}" for view in OUTPUT_VIEWS],
        }
    )


if __name__ == "__main__":
    main()
