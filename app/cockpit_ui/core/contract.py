"""The gold-table contract: every logical table the app may read, in one place.

Pages call ``ctx.table("<logical name>")``. The data source maps a logical name to a physical table
(see ``datasource.TableResolver``), so enterprises can rename or relocate tables without touching
pages. This module only describes *what* the app expects; it never touches data.

``required_columns`` lists the columns pages rely on today, not every column the engine writes.
``planned=True`` marks cockpit tables from the POC plan that the engine does not produce yet.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Mapping

import pandas as pd

LOGICAL_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class GoldTable:
    name: str
    description: str
    feature: str
    required_columns: tuple[str, ...] = ()
    planned: bool = False


def _t(name: str, description: str, feature: str, columns: str = "", planned: bool = False) -> GoldTable:
    return GoldTable(name, description, feature, tuple(columns.split()), planned)


_TABLES: tuple[GoldTable, ...] = (
    # Upstream engine outputs (databricks_finops.config OUTPUT_TABLES / OUTPUT_VIEWS).
    _t("daily_cost", "Daily cost per workspace, SKU and workload.", "F1"),
    _t(
        "workload_cost_summary",
        "Cost per workload over the lookback window, with attribution.",
        "F1",
        "workload_type workload_id job_name team owner project estimated_cost total_dbus active_days attribution_quality",
    ),
    _t("compute_utilization_summary", "CPU and memory utilisation per cluster or job.", "F5"),
    _t(
        "job_reliability_summary",
        "Run counts, failures, retries and failed-run cost per job.",
        "F6",
        "job_name run_count failed_count failure_rate_pct retry_count estimated_failed_cost avg_duration_minutes "
        "reliability_category last_run_result_state last_run_start_time",
    ),
    _t("tagging_quality_summary", "Required-tag coverage per workload.", "F2"),
    _t(
        "optimization_candidates",
        "Scored optimisation backlog, one row per candidate.",
        "F7",
        "priority_rank workload_type workload_id job_name owner team issue_type suggested_action "
        "estimated_monthly_cost priority_score confidence evidence",
    ),
    _t("accelerator_summary", "Headline metrics of the latest engine run.", "F11"),
    _t(
        "accelerator_health",
        "Health and coverage checks of the latest engine run.",
        "F11",
        "run_id check_name status severity message",
    ),
    _t(
        "accelerator_run_log",
        "One row per engine task per run; drives cache invalidation.",
        "F11",
        "run_id task_name status result started_at duration_seconds rows_written message",
    ),
    _t(
        "vw_executive_summary",
        "Single-row headline numbers for the overview page.",
        "F8",
        "total_estimated_cost total_dbus workload_count high_priority_candidate_count untagged_cost failed_cost",
    ),
    _t("vw_cost_trend_daily", "Daily spend trend.", "F8", "usage_date estimated_cost untagged_cost failed_cost"),
    _t("vw_most_expensive_workloads", "Top workloads by cost.", "F1"),
    _t("vw_optimization_backlog", "Backlog view over optimization_candidates.", "F7"),
    _t(
        "vw_tagging_quality",
        "Tagging quality per team, owner and project.",
        "F2",
        "team owner project estimated_cost missing_required_tag_count worst_tagging_quality",
    ),
    _t("vw_architecture_review_candidates", "Workloads worth an architecture review.", "F7"),
    _t("vw_system_table_coverage", "Which system tables were readable.", "F11"),
    _t("vw_attribution_quality", "Cost by attribution quality bucket.", "F2", "attribution_quality estimated_cost"),
    _t(
        "vw_compute_sizing_signals",
        "Utilisation category and sizing signal per cluster or job.",
        "F5",
        "cluster_id job_name avg_cpu_pct avg_memory_pct utilization_category sizing_signal confidence estimated_cost",
    ),
    _t("vw_failed_jobs", "Failed job runs.", "F6"),
    _t("vw_wasteful_workloads", "Workloads flagged as wasteful.", "F7"),
    # Planned cockpit tables (POC plan v1.1); not produced yet.
    _t("cost_base", "Unified cost base in the cockpit schema.", "F1", planned=True),
    _t("waste_flags", "Unified waste backlog, one flag per resource (R1).", "F7", planned=True),
    _t("catalog_allocation", "Shared-warehouse cost split by catalog (R2).", "F3", planned=True),
    # Savings in the two tables below must be deduplicated per resource by the engine (rule R1): one row
    # per cluster, so summing estimated_monthly_savings / idle_cost never counts a cluster twice.
    _t(
        "allpurpose_misuse",
        "All-purpose compute used for JDBC/ODBC, jobs or shared users; one row per cluster (R1).",
        "F4",
        "cluster_id cluster_name owner misuse_type evidence estimated_monthly_cost estimated_monthly_savings "
        "suggested_action",
        planned=True,
    ),
    _t(
        "idle_compute",
        "Idle and zombie compute; one row per cluster (R1).",
        "F5",
        "cluster_id cluster_name owner idle_reason autotermination_minutes avg_cpu_pct idle_hours idle_cost "
        "suggested_action",
        planned=True,
    ),
    _t("alert_top_flags", "Top 3 waste flags by priority_score for the Monday alert (R5).", "F10", planned=True),
)

CONTRACT: Mapping[str, GoldTable] = {table.name: table for table in _TABLES}


def is_valid_logical_name(name: str) -> bool:
    return bool(LOGICAL_NAME.match(name))


def get(name: str) -> GoldTable | None:
    return CONTRACT.get(name.lower())


def produced_tables() -> list[GoldTable]:
    """Tables the engine writes today (not planned)."""
    return [table for table in CONTRACT.values() if not table.planned]


def missing_columns(name: str, columns: Iterable[str]) -> list[str]:
    table = get(name)
    if table is None:
        return []
    present = {str(column).lower() for column in columns}
    return [column for column in table.required_columns if column.lower() not in present]


def check_frame(name: str, frame: pd.DataFrame) -> list[str]:
    """Problems with ``frame`` as table ``name`` (empty list = fine). Never raises."""

    table = get(name)
    if table is None:
        return [f"'{name}' is not in the gold-table contract."]
    missing = missing_columns(name, frame.columns)
    if missing:
        return [f"'{name}' is missing required column(s): {', '.join(missing)}."]
    return []
