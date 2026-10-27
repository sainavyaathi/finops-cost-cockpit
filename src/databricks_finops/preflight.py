from __future__ import annotations

from dataclasses import dataclass
from typing import Any


SOURCE_TABLES = {
    "system.billing.usage": {
        "check_name": "billing_usage_available",
        "required": True,
        "affected_output": "all_outputs",
    },
    "system.billing.list_prices": {
        "check_name": "list_prices_available",
        "required": False,
        "affected_output": "daily_cost",
    },
    "system.compute.node_timeline": {
        "check_name": "node_timeline_available",
        "required": False,
        "affected_output": "compute_utilization_summary",
    },
    "system.lakeflow.jobs": {
        "check_name": "lakeflow_jobs_available",
        "required": False,
        "affected_output": "daily_cost,workload_cost_summary,job_reliability_summary",
    },
    "system.lakeflow.job_run_timeline": {
        "check_name": "lakeflow_job_run_timeline_available",
        "required": False,
        "affected_output": "job_reliability_summary",
    },
    "system.lakeflow.job_task_run_timeline": {
        "check_name": "lakeflow_task_run_timeline_available",
        "required": False,
        "affected_output": "job_reliability_summary.retry_count",
    },
}


@dataclass(frozen=True)
class TableCapability:
    source_name: str
    check_name: str
    required: bool
    available: bool
    message: str
    affected_output: str


@dataclass(frozen=True)
class PreflightResult:
    capabilities: list[TableCapability]
    columns_by_table: dict[str, set[str]]

    def available(self, table_name: str) -> bool:
        return any(cap.source_name == table_name and cap.available for cap in self.capabilities)

    def columns(self, table_name: str) -> set[str]:
        return self.columns_by_table.get(table_name, set())

    def require_billing_usage(self) -> None:
        billing = next(cap for cap in self.capabilities if cap.source_name == "system.billing.usage")
        if not billing.available:
            raise RuntimeError(
                "Required system table unavailable: system.billing.usage. "
                f"{billing.message}"
            )


def _check_table(spark: Any, table_name: str, metadata: dict[str, Any]) -> TableCapability:
    try:
        spark.sql(f"SELECT * FROM {table_name} LIMIT 0").collect()
        return TableCapability(
            source_name=table_name,
            check_name=metadata["check_name"],
            required=metadata["required"],
            available=True,
            message=f"{table_name} is available.",
            affected_output=metadata["affected_output"],
        )
    except Exception as exc:
        return TableCapability(
            source_name=table_name,
            check_name=metadata["check_name"],
            required=metadata["required"],
            available=False,
            message=str(exc),
            affected_output=metadata["affected_output"],
        )


def _get_columns(spark: Any, table_name: str, available: bool) -> set[str]:
    if not available:
        return set()
    try:
        return set(spark.table(table_name).columns)
    except Exception:
        return set()


def run_preflight(spark: Any) -> PreflightResult:
    capabilities = [
        _check_table(spark, table_name, metadata)
        for table_name, metadata in SOURCE_TABLES.items()
    ]
    columns_by_table = {
        capability.source_name: _get_columns(spark, capability.source_name, capability.available)
        for capability in capabilities
    }
    return PreflightResult(capabilities=capabilities, columns_by_table=columns_by_table)
