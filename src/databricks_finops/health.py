from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .preflight import PreflightResult
from .run_logging import StepLogger
from .spark_utils import qname, scalar_value, sql_string


@dataclass(frozen=True)
class HealthRecord:
    run_id: str
    check_name: str
    status: str
    severity: str
    message: str
    affected_output: str


def _source_health_records(run_id: str, preflight: PreflightResult) -> list[HealthRecord]:
    records: list[HealthRecord] = []
    for capability in preflight.capabilities:
        if capability.available:
            status = "PASS"
            severity = "INFO"
            message = capability.message
        elif capability.required:
            status = "FAIL"
            severity = "CRITICAL"
            message = capability.message
        else:
            status = "WARN"
            severity = "MEDIUM"
            message = (
                f"{capability.source_name} is unavailable. Affected outputs are "
                "created with INSUFFICIENT_DATA where needed."
            )
        records.append(
            HealthRecord(
                run_id=run_id,
                check_name=capability.check_name,
                status=status,
                severity=severity,
                message=message[:4000],
                affected_output=capability.affected_output,
            )
        )
    return records


def _metric_health_records(spark: Any, catalog: str, schema: str, run_id: str) -> list[HealthRecord]:
    daily_cost = qname(catalog, schema, "daily_cost")
    workload = qname(catalog, schema, "workload_cost_summary")
    candidates = qname(catalog, schema, "optimization_candidates")
    tagging = qname(catalog, schema, "tagging_quality_summary")

    list_price_rows = scalar_value(
        spark,
        f"""
        SELECT COUNT(*)
        FROM {daily_cost}
        WHERE run_id = {sql_string(run_id)}
          AND price_source = 'LIST_PRICES'
        """,
        0,
    )
    fallback_rows = scalar_value(
        spark,
        f"""
        SELECT COUNT(*)
        FROM {daily_cost}
        WHERE run_id = {sql_string(run_id)}
          AND price_source = 'FALLBACK_DBU_PRICE'
        """,
        0,
    )
    mixed_rows = scalar_value(
        spark,
        f"""
        SELECT COUNT(*)
        FROM {daily_cost}
        WHERE run_id = {sql_string(run_id)}
          AND price_source = 'MIXED'
        """,
        0,
    )
    unknown_price_rows = scalar_value(
        spark,
        f"""
        SELECT COUNT(*)
        FROM {daily_cost}
        WHERE run_id = {sql_string(run_id)}
          AND price_source = 'UNKNOWN'
        """,
        0,
    )
    low_attr_cost = scalar_value(
        spark,
        f"""
        SELECT ROUND(COALESCE(SUM(estimated_cost), 0), 6)
        FROM {daily_cost}
        WHERE run_id = {sql_string(run_id)}
          AND attribution_quality IN ('LOW', 'UNKNOWN')
        """,
        0,
    )
    untagged_cost = scalar_value(
        spark,
        f"""
        SELECT ROUND(COALESCE(SUM(estimated_cost), 0), 6)
        FROM {tagging}
        WHERE run_id = {sql_string(run_id)}
          AND missing_required_tag_count > 0
        """,
        0,
    )
    workload_count = scalar_value(
        spark,
        f"SELECT COUNT(*) FROM {workload} WHERE run_id = {sql_string(run_id)}",
        0,
    )
    candidate_count = scalar_value(
        spark,
        f"SELECT COUNT(*) FROM {candidates} WHERE run_id = {sql_string(run_id)}",
        0,
    )

    return [
        HealthRecord(
            run_id,
            "pricing_join_success",
            "PASS" if list_price_rows and not fallback_rows and not mixed_rows and not unknown_price_rows else "WARN",
            "INFO" if list_price_rows and not fallback_rows and not mixed_rows and not unknown_price_rows else "MEDIUM",
            (
                f"{list_price_rows} daily cost rows used list prices; "
                f"{fallback_rows} used fallback DBU price; "
                f"{mixed_rows} mixed; {unknown_price_rows} unknown."
            ),
            "daily_cost",
        ),
        HealthRecord(
            run_id,
            "cost_attribution_quality",
            "PASS" if not low_attr_cost else "WARN",
            "INFO" if not low_attr_cost else "MEDIUM",
            f"Estimated cost with LOW or UNKNOWN attribution: {low_attr_cost}.",
            "daily_cost,workload_cost_summary,optimization_candidates",
        ),
        HealthRecord(
            run_id,
            "tagging_coverage",
            "PASS" if not untagged_cost else "WARN",
            "INFO" if not untagged_cost else "LOW",
            f"Estimated cost with at least one missing required tag: {untagged_cost}.",
            "tagging_quality_summary,optimization_candidates",
        ),
        HealthRecord(
            run_id,
            "workload_count",
            "PASS",
            "INFO",
            f"{workload_count} workloads were summarized.",
            "workload_cost_summary",
        ),
        HealthRecord(
            run_id,
            "optimization_candidate_count",
            "PASS",
            "INFO",
            f"{candidate_count} optimization candidates were generated.",
            "optimization_candidates",
        ),
    ]


def _degraded_step_health_records(run_id: str, logger: StepLogger | None) -> list[HealthRecord]:
    if logger is None:
        return []

    return [
        HealthRecord(
            run_id=run_id,
            check_name=f"degraded_step_{record.task_name}",
            status="WARN",
            severity="MEDIUM",
            message=f"Step {record.task_name} completed with degraded fallback: {record.message}",
            affected_output=record.task_name,
        )
        for record in logger.records
        if record.result == "DEGRADED"
    ]


def _config_warning_health_records(run_id: str, warnings: tuple[str, ...]) -> list[HealthRecord]:
    return [
        HealthRecord(
            run_id=run_id,
            check_name=f"config_warning_{index}",
            status="WARN",
            severity="LOW",
            message=warning[:4000],
            affected_output="configuration",
        )
        for index, warning in enumerate(warnings, start=1)
    ]


def _health_table_sql(target: str, create_or_replace: str) -> str:
    return f"""
    {create_or_replace} {target} (
        run_id STRING,
        check_name STRING,
        status STRING,
        severity STRING,
        message STRING,
        affected_output STRING,
        created_at TIMESTAMP
    )
    USING DELTA
    """


def _ensure_health_table(spark: Any, catalog: str, schema: str, target: str) -> None:
    spark.sql(_health_table_sql(target, "CREATE TABLE IF NOT EXISTS"))
    try:
        existing_columns = set(spark.table(f"{catalog}.{schema}.accelerator_health").columns)
    except Exception:
        existing_columns = set()

    expected_columns = {
        "run_id",
        "check_name",
        "status",
        "severity",
        "message",
        "affected_output",
        "created_at",
    }
    if not expected_columns.issubset(existing_columns):
        spark.sql(_health_table_sql(target, "CREATE OR REPLACE TABLE"))


def write_health(
    spark: Any,
    catalog: str,
    schema: str,
    run_id: str,
    preflight: PreflightResult,
    logger: StepLogger | None = None,
    config_warnings: tuple[str, ...] = (),
) -> None:
    target = qname(catalog, schema, "accelerator_health")
    _ensure_health_table(spark, catalog, schema, target)

    records = _source_health_records(run_id, preflight)
    records.extend(_metric_health_records(spark, catalog, schema, run_id))
    records.extend(_degraded_step_health_records(run_id, logger))
    records.extend(_config_warning_health_records(run_id, config_warnings))

    rows = ",\n        ".join(
        "("
        f"{sql_string(record.run_id)}, "
        f"{sql_string(record.check_name)}, "
        f"{sql_string(record.status)}, "
        f"{sql_string(record.severity)}, "
        f"{sql_string(record.message)}, "
        f"{sql_string(record.affected_output)}"
        ")"
        for record in records
    )
    spark.sql(f"DELETE FROM {target} WHERE run_id = {sql_string(run_id)}")
    spark.sql(
        f"""
        INSERT INTO {target} (
            run_id,
            check_name,
            status,
            severity,
            message,
            affected_output,
            created_at
        )
        SELECT
            run_id,
            check_name,
            status,
            severity,
            message,
            affected_output,
            current_timestamp() AS created_at
        FROM VALUES
            {rows}
        AS health(run_id, check_name, status, severity, message, affected_output)
        """
    )
