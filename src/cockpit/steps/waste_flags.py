"""F7 unified waste backlog -> cockpit.waste_flags, the one backlog the app and the F10 alert read.

One row per ``(resource_type, workspace_id, resource_id)`` (CLUSTER, JOB, WAREHOUSE, PIPELINE,
NOTEBOOK, UNKNOWN). Findings come from four sources:

- ``allpurpose_misuse`` (F4): one finding per misuse type; the primary type carries F4's saving.
- ``idle_compute`` (F5): one finding per cluster, flag type = ``idle_reason``.
- ``job_reliability`` (F6, corrected retry_count): FAILURE_HEAVY / HIGH_FAILURE_COST (saving =
  failed-run cost per month) and RETRY_HEAVY (saving = job cost x share of task time spent on
  retried attempts, an estimate, confidence at most MEDIUM).
- upstream ``optimization_candidates``: sizing, tagging, attribution and expense items with no
  saving estimate. RETRY_HEAVY is never taken (upstream retry_count is always 0); failure items are
  dropped while ``job_reliability`` is readable; SHARED_CLUSTER_ATTRIBUTION is dropped for clusters
  F4 or F5 already flag.

R1: ``estimated_monthly_savings`` is the MAX finding saving (never the sum), capped at the
resource's monthly cost from ``cost_base`` over the detection window (left uncapped when no cost is
known). A JOB's saving also leaves out the cost it shares with a CLUSTER row that carries a saving.
Upstream items on a resource with no cost in the window are dropped as stale. R5: ``priority_score`` is one
0-100 formula for every source (see ``config/cockpit_thresholds.yml`` and docs/architecture.md),
and ``priority_rank`` is 1..N with no ties.

A source table that is missing or lacks columns is skipped and the run log says so.
"""

from __future__ import annotations

from typing import Any

from databricks_finops.spark_utils import sql_string

from cockpit.pipeline import Step, StepContext
from cockpit.step_config import (
    DetectionWindow,
    detection_window,
    enforce_bounds,
    load_step_lists,
    load_step_thresholds,
)

SECTION = "waste_flags"
DEFAULTS: dict[str, float] = {
    "score_reference_monthly_savings": 1000,
    "score_reference_monthly_cost": 1000,
    "priority_weight_savings": 0.6,
    "priority_weight_cost": 0.3,
    "priority_weight_breadth": 0.1,
    "breadth_points_per_extra_flag": 50,
    "confidence_multiplier_high": 1.0,
    "confidence_multiplier_medium": 0.85,
    "confidence_multiplier_low": 0.7,
}
PRIORITY_WEIGHTS = ("priority_weight_savings", "priority_weight_cost", "priority_weight_breadth")
BOUNDS: dict[str, tuple[float | None, float | None, bool]] = {
    "score_reference_monthly_savings": (0, None, True),
    "score_reference_monthly_cost": (0, None, True),
    "priority_weight_savings": (0, 1, False),
    "priority_weight_cost": (0, 1, False),
    "priority_weight_breadth": (0, 1, False),
    "breadth_points_per_extra_flag": (0, 100, False),
    "confidence_multiplier_high": (0, 1, False),
    "confidence_multiplier_medium": (0, 1, False),
    "confidence_multiplier_low": (0, 1, False),
}

UPSTREAM_FAILURE_TYPES = ("HIGH_FAILURE_COST", "HIGH_FAILURE_RATE")
UPSTREAM_NEVER = ("RETRY_HEAVY",)  # upstream retry_count is always 0 (F6 bug); F6 owns retries
LIST_DEFAULTS: dict[str, tuple[str, ...]] = {
    "upstream_issue_types": (
        "HIGH_COST_LOW_UTILIZATION", "LOW_UTILIZATION", "EXPENSIVE_WORKLOAD", "MISSING_TAGS",
        "UNKNOWN_ATTRIBUTION", "SHARED_CLUSTER_ATTRIBUTION", *UPSTREAM_FAILURE_TYPES,
    ),
}

# Source name -> (cockpit table, SQL template, columns the template needs).
SOURCES: dict[str, tuple[str, str, frozenset[str]]] = {
    "idle_compute": ("idle_compute", "waste_flags_idle_compute", frozenset({
        "workspace_id", "cluster_id", "cluster_name", "owner", "idle_reason", "estimated_monthly_savings",
        "estimated_monthly_cost", "confidence", "evidence", "suggested_action"})),
    "allpurpose_misuse": ("allpurpose_misuse", "waste_flags_allpurpose_misuse", frozenset({
        "workspace_id", "cluster_id", "cluster_name", "owner", "misuse_type", "misuse_types",
        "estimated_monthly_savings", "estimated_monthly_cost", "confidence", "evidence", "suggested_action"})),
    "job_reliability": ("job_reliability", "waste_flags_job_reliability", frozenset({
        "workspace_id", "job_id", "job_name", "run_count", "failed_count", "failure_rate_pct", "retry_count",
        "estimated_failed_cost", "confidence"})),
    "optimization_candidates": ("optimization_candidates", "waste_flags_optimization_candidates", frozenset({
        "workspace_id", "workload_type", "workload_id", "job_name", "owner", "team", "issue_type",
        "suggested_action", "confidence", "evidence", "estimated_monthly_cost"})),
}
COST_BASE_COLUMNS = frozenset({
    "usage_date", "workspace_id", "workload_type", "workload_id", "cluster_id", "owner", "team",
    "estimated_cost"})

FINDING_COLUMNS = (
    ("resource_type", "STRING"), ("workspace_id", "STRING"), ("resource_id", "STRING"),
    ("resource_name", "STRING"), ("owner", "STRING"), ("team", "STRING"), ("source", "STRING"),
    ("source_order", "INT"), ("flag_type", "STRING"), ("savings", "DECIMAL(38,6)"),
    ("confidence", "STRING"), ("evidence", "STRING"), ("suggested_action", "STRING"),
    ("source_monthly_cost", "DECIMAL(38,6)"),
)
RESOURCE_COST_COLUMNS = (
    ("resource_type", "STRING"), ("workspace_id", "STRING"), ("resource_id", "STRING"),
    ("window_cost", "DOUBLE"), ("owner_tag", "STRING"), ("team_tag", "STRING"),
)
JOB_CLUSTER_COST_COLUMNS = (
    ("workspace_id", "STRING"), ("job_id", "STRING"), ("cluster_id", "STRING"), ("window_cost", "DOUBLE"),
)


def empty_relation(columns: tuple[tuple[str, str], ...]) -> str:
    select = ", ".join(f"CAST(NULL AS {kind}) AS {name}" for name, kind in columns)
    return f"    SELECT {select} WHERE 1 = 0"


def num(value: float) -> str:
    return f"{float(value):.12g}"


def load_settings(bundle_root: str) -> tuple[dict[str, float], tuple[str, ...], list[str]]:
    """Thresholds (bounds checked, weights summing to 1) and the upstream issue types to take."""

    values, warnings = load_step_thresholds(bundle_root, SECTION, DEFAULTS)
    warnings += enforce_bounds(values, DEFAULTS, SECTION, BOUNDS)
    total = sum(values[k] for k in PRIORITY_WEIGHTS)
    if abs(total - 1.0) > 1e-6:
        warnings.append(f"{SECTION} priority weights must add up to 1.0 (got {total:.6f}); defaults used.")
        for key in PRIORITY_WEIGHTS:
            values[key] = DEFAULTS[key]
    lists, list_warnings = load_step_lists(bundle_root, SECTION, LIST_DEFAULTS)
    issue_types = lists["upstream_issue_types"]
    if any(t in UPSTREAM_NEVER for t in issue_types):
        list_warnings.append(f"{SECTION}.upstream_issue_types: RETRY_HEAVY ignored (upstream retry_count is "
                             "always 0; F6 job_reliability supplies retries).")
        issue_types = tuple(t for t in issue_types if t not in UPSTREAM_NEVER)
    return values, issue_types, warnings + list_warnings


def probe_columns(spark: Any, table: str) -> set[str] | None:
    """Columns of ``table``, or None when it cannot be read."""

    try:
        return {c.lower() for c in spark.table(table).columns}
    except Exception:  # noqa: BLE001 - missing table or no permission
        return None


def source_problem(columns: set[str] | None, required: frozenset[str], table: str) -> str | None:
    if columns is None:
        return f"{table} unavailable"
    missing = sorted(required - columns)
    return f"{table} lacks columns {missing}" if missing else None


def upstream_issue_types(configured: tuple[str, ...], job_reliability_available: bool) -> tuple[str, ...]:
    """Upstream failure items are superseded by F6 whenever job_reliability is readable."""

    if job_reliability_available:
        return tuple(t for t in configured if t not in UPSTREAM_FAILURE_TYPES)
    return configured


def _optional(columns: set[str], name: str, fallback: str) -> str:
    return f"j.{name}" if name in columns else fallback


def build_statement(
    ctx: StepContext,
    columns: dict[str, set[str] | None],
    thresholds: dict[str, float] | None = None,
    issue_types: tuple[str, ...] | None = None,
    window: DetectionWindow | None = None,
) -> tuple[str, list[str], dict[str, bool]]:
    """Compose the CREATE statement. ``columns`` maps source name / ``cost_base`` to probed columns.

    Returns (sql, notes, available sources).
    """

    t = dict(DEFAULTS) if thresholds is None else thresholds
    window = window or detection_window(ctx.config.bundle_root, ctx.config.lookback_days)
    upstream = ctx.config.thresholds
    days = str(window.days)
    notes: list[str] = []
    available: dict[str, bool] = {}
    relations: dict[str, str] = {}

    cost_problem = source_problem(columns.get("cost_base"), COST_BASE_COLUMNS, "cost_base")
    cost_base_available = cost_problem is None
    if cost_base_available:
        resource_cost = ctx.sql("waste_flags_resource_cost", cost_base=ctx.table("cost_base"),
                                detection_days=days).rstrip()
        job_cluster_cost = ctx.sql("waste_flags_job_cluster_cost", cost_base=ctx.table("cost_base"),
                                   detection_days=days).rstrip()
    else:
        resource_cost = empty_relation(RESOURCE_COST_COLUMNS)
        job_cluster_cost = empty_relation(JOB_CLUSTER_COST_COLUMNS)
        notes.append(f"{cost_problem}: resource cost, owner and team taken from the sources "
                     "(F6 failure findings use the failed-run cost as the cost floor)")

    for name, (table, template, required) in SOURCES.items():
        problem = source_problem(columns.get(name), required, table)
        available[name] = problem is None
        if problem is not None:
            relations[name] = empty_relation(FINDING_COLUMNS)
            notes.append(f"{problem}: {name} skipped")
            continue
        params: dict[str, str] = {"source_table": ctx.table(table)}
        if name == "job_reliability":
            cols = columns[name] or set()
            params.update(
                retried_run_count_column=_optional(cols, "retried_run_count", "NULL"),
                retry_task_minutes_column=_optional(cols, "retry_task_minutes", "NULL"),
                task_attempt_minutes_column=_optional(cols, "task_attempt_minutes", "NULL"),
                job_window_days_column=(f"COALESCE(j.detection_days, {days})" if "detection_days" in cols
                                        else days),
                high_failure_rate_pct=num(upstream.high_failure_rate_pct),
                high_failed_cost=num(upstream.high_failed_cost),
                retry_heavy_count=num(upstream.retry_heavy_count),
                monthly_multiplier=num(window.monthly_multiplier),
                currency_code_sql=sql_string(ctx.config.currency_code),
            )
            if "retry_task_minutes" not in cols:
                notes.append("job_reliability has no retry_task_minutes: RETRY_HEAVY savings unknown")
        if name != "optimization_candidates":  # rendered below, once F6 availability is known
            relations[name] = ctx.sql(template, **params).rstrip()

    types = upstream_issue_types(issue_types if issue_types is not None else LIST_DEFAULTS["upstream_issue_types"],
                                 available["job_reliability"])
    if available["optimization_candidates"]:
        test = f"issue_type IN ({', '.join(sql_string(x) for x in types)})" if types else "FALSE"
        relations["optimization_candidates"] = ctx.sql(
            "waste_flags_optimization_candidates", source_table=ctx.table("optimization_candidates"),
            issue_type_test=test).rstrip()

    statement = ctx.sql(
        "waste_flags",
        target=ctx.table("waste_flags"),
        resource_cost_relation=resource_cost,
        job_cluster_cost_relation=job_cluster_cost,
        cost_base_available_sql="TRUE" if cost_base_available else "FALSE",
        idle_compute_relation=relations["idle_compute"],
        allpurpose_misuse_relation=relations["allpurpose_misuse"],
        job_reliability_relation=relations["job_reliability"],
        optimization_candidates_relation=relations["optimization_candidates"],
        monthly_multiplier=num(window.monthly_multiplier),
        detection_days=days,
        currency_code_sql=sql_string(ctx.config.currency_code),
        run_id_sql=sql_string(ctx.run_id),
        **{key: num(value) for key, value in t.items()},
    )
    return statement, notes, available


def build(ctx: StepContext) -> str:
    thresholds, issue_types, warnings = load_settings(ctx.config.bundle_root)
    window = detection_window(ctx.config.bundle_root, ctx.config.lookback_days)
    columns = {name: probe_columns(ctx.spark, ctx.table(table)) for name, (table, _, _) in SOURCES.items()}
    columns["cost_base"] = probe_columns(ctx.spark, ctx.table("cost_base"))
    statement, notes, available = build_statement(ctx, columns, thresholds, issue_types, window)
    ctx.spark.sql(statement)

    target = ctx.table("waste_flags")
    total = ctx.spark.sql(f"SELECT COUNT(*) FROM {target}").collect()[0][0]
    per_source = {r[0]: r[1] for r in ctx.spark.sql(
        f"SELECT f.source, COUNT(*) FROM {target} LATERAL VIEW explode(findings) e AS f GROUP BY f.source"
    ).collect()}
    counts = ", ".join(f"{name} {per_source.get(name, 0)}" for name in SOURCES if available.get(name))
    message = f"waste_flags: {total} resource(s) over {window.days} day(s); findings: {counts or 'none'}"
    extra = notes + warnings + list(window.warnings)
    return message + ("; " + "; ".join(extra) if extra else "")


STEP = Step(
    name="build_waste_flags",
    run=build,
    output_table="waste_flags",
    depends_on=("build_allpurpose_misuse", "build_cost_base", "build_idle_compute", "build_job_reliability"),
)
