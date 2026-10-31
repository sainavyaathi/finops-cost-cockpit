"""F5 idle and zombie compute, published as cockpit.idle_compute (one row per flagged cluster, R1).

Population: all-purpose clusters (``cluster_source`` UI or API) in their latest, non-deleted state
in ``system.compute.clusters`` (slowly changing: the newest ``change_time`` wins). Job and pipeline
clusters are excluded even when they report ``auto_termination_minutes = 0``: they end with their run.

A cluster is flagged when BOTH hold:

* zombie configuration: ``auto_termination_minutes = 0`` (AUTO_TERMINATION_OFF) or
  ``> zombie_auto_term_minutes`` (AUTO_TERMINATION_TOO_LONG; 60 is fine, 61 is not). NULL is unknown
  and not flagged;
* mostly idle while up (LOW_UTILIZATION): at least ``idle_hours_share_pct`` of its active clock hours
  in the lookback had average CPU < ``low_cpu_pct`` and, when memory is reported, memory <
  ``low_memory_pct`` (upstream thresholds, so a busy or memory-bound cluster is not idle). Hours are
  weighted by the minutes sampled, so a cluster seen for 3 minutes of an hour counts 3/60 of an hour.

``CONFIG_FLAG_REQUIRES_IDLE`` switches the rule to "the setting is the finding, idleness adds evidence".

Window: cost and utilisation cover the shared detection window ``min(detection.detection_days,
lookback_days)`` (``cockpit.step_config.detection_window``), so a long upstream lookback does not dilute
the flags. ``daily_cost`` is filtered to the same days.

Money: ``idle_cost = estimated_cost * idle_minutes / active_minutes`` (detection-window cost from daily_cost,
spread evenly over active hours); ``estimated_monthly_savings = idle_cost * 30 / detection_days``, an upper
bound that never exceeds the cluster's own cost.

Degraded modes (decided by probing the source tables, not by failing):

* ``system.compute.clusters`` missing, unreadable, lacking columns or EMPTY (Databricks Free Edition):
  all-purpose clusters are recognised from billing (all-purpose SKU, never billed to a job) and flagged
  on utilisation alone (``detection_mode = UTILIZATION_ONLY``, confidence LOW).
* ``system.compute.node_timeline`` unusable: clusters are flagged on configuration alone if they had
  cost in the lookback (``CONFIG_ONLY``, confidence LOW, idle_cost and savings NULL).
* both unusable: the table is created empty and the run log says why.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from databricks_finops.spark_utils import sql_string

from cockpit.pipeline import Step, StepContext
from cockpit.step_config import DetectionWindow, detection_window, load_step_thresholds

# Thresholds: section ``idle_compute:`` of config/cockpit_thresholds.yml (see cockpit.step_config).
# The low-utilisation pair (low_cpu_pct, low_memory_pct) comes from upstream thresholds.yml instead.
SECTION = "idle_compute"
DEFAULTS = {
    "zombie_auto_term_minutes": 60.0,
    "idle_hours_share_pct": 50.0,
    "high_confidence_min_active_hours": 24.0,
    "priority_full_savings_monthly": 1000.0,
    "priority_weight_savings": 0.60,
    "priority_weight_idle": 0.25,
    "priority_weight_config": 0.15,
    "config_score_off": 100.0,
    "config_score_too_long": 60.0,
    "config_score_unknown": 50.0,
}
_PRIORITY_WEIGHTS = ("priority_weight_savings", "priority_weight_idle", "priority_weight_config")


@dataclass(frozen=True)
class IdleComputeThresholds:
    zombie_auto_term_minutes: float = DEFAULTS["zombie_auto_term_minutes"]
    idle_hours_share_pct: float = DEFAULTS["idle_hours_share_pct"]
    high_confidence_min_active_hours: float = DEFAULTS["high_confidence_min_active_hours"]
    priority_full_savings_monthly: float = DEFAULTS["priority_full_savings_monthly"]
    priority_weight_savings: float = DEFAULTS["priority_weight_savings"]
    priority_weight_idle: float = DEFAULTS["priority_weight_idle"]
    priority_weight_config: float = DEFAULTS["priority_weight_config"]
    config_score_off: float = DEFAULTS["config_score_off"]
    config_score_too_long: float = DEFAULTS["config_score_too_long"]
    config_score_unknown: float = DEFAULTS["config_score_unknown"]


def load_idle_compute_thresholds(bundle_root: str) -> tuple[IdleComputeThresholds, tuple[str, ...]]:
    """Read the ``idle_compute`` section; weights must sum to 1 and the savings scale must be positive."""

    values, warnings = load_step_thresholds(bundle_root, SECTION, DEFAULTS)
    total = sum(values[k] for k in _PRIORITY_WEIGHTS)
    if abs(total - 1.0) > 1e-6:
        warnings.append(f"idle_compute priority weights must add up to 1.0 (got {total:.6f}); defaults were used.")
        for key in _PRIORITY_WEIGHTS:
            values[key] = DEFAULTS[key]
    if values["priority_full_savings_monthly"] <= 0:
        warnings.append("idle_compute.priority_full_savings_monthly must be positive; default was used.")
        values["priority_full_savings_monthly"] = DEFAULTS["priority_full_savings_monthly"]
    return IdleComputeThresholds(**values), tuple(warnings)

CLUSTERS = "system.compute.clusters"
NODE_TIMELINE = "system.compute.node_timeline"

ALL_PURPOSE_SOURCES = ("UI", "API")
REQUIRED_CLUSTER_COLUMNS = frozenset({"workspace_id", "cluster_id", "cluster_source",
                                      "auto_termination_minutes", "change_time", "delete_time"})
REQUIRED_NODE_COLUMNS = frozenset({"workspace_id", "cluster_id", "start_time"})
CPU_COLUMNS = ("cpu_user_percent", "cpu_system_percent")
MEMORY_COLUMN = "mem_used_percent"

MODE_FULL = "CONFIG_AND_UTILIZATION"
MODE_CONFIG_ONLY = "CONFIG_ONLY"
MODE_UTILIZATION_ONLY = "UTILIZATION_ONLY"
MODE_NONE = "NO_SIGNAL"

# Flag rule when both signals are known. True (current): zombie auto-termination AND mostly idle.
# False: the setting alone is the finding (cost in the lookback required); idleness adds evidence,
# savings stay NULL for non-idle rows, and their priority is lower (no savings, low idle share).
# PENDING a team decision; flipping this constant is the whole change (plus test expectations).
CONFIG_FLAG_REQUIRES_IDLE = True

_EMPTY_NODE_HOURS = """    SELECT
        CAST(NULL AS STRING) AS workspace_id,
        CAST(NULL AS STRING) AS cluster_id,
        CAST(NULL AS TIMESTAMP) AS usage_hour,
        CAST(0 AS BIGINT) AS sampled_minutes,
        CAST(NULL AS DOUBLE) AS hour_cpu_pct,
        CAST(NULL AS DOUBLE) AS hour_memory_pct,
        CAST(NULL AS DOUBLE) AS cpu_sum,
        CAST(0 AS BIGINT) AS cpu_samples,
        CAST(NULL AS DOUBLE) AS memory_sum,
        CAST(0 AS BIGINT) AS memory_samples
    WHERE FALSE"""


def probe_table(spark: Any, table: str, required: frozenset[str]) -> tuple[set[str], str | None]:
    """Return (columns, problem). ``problem`` is None when the table is readable, has the required
    columns and at least one row. Empty counts as unusable: compute.clusters is empty on Free Edition."""

    try:
        columns = set(spark.table(table).columns)
    except Exception as exc:  # noqa: BLE001 - missing table or no permission
        return set(), f"{table} unavailable ({type(exc).__name__})"
    missing = required - columns
    if missing:
        return columns, f"{table} lacks columns {sorted(missing)}"
    try:
        if not spark.sql(f"SELECT 1 FROM {table} LIMIT 1").collect():
            return columns, f"{table} is empty"
    except Exception as exc:  # noqa: BLE001
        return columns, f"{table} unreadable ({type(exc).__name__})"
    return columns, None


def cpu_expr(columns: set[str]) -> str | None:
    present = [f"CAST({c} AS DOUBLE)" for c in CPU_COLUMNS if c in columns]
    return " + ".join(present) if present else None


def _optional(columns: set[str], name: str) -> str:
    return f"CAST({name} AS STRING)" if name in columns else "CAST(NULL AS STRING)"


def _num(value: float) -> str:
    return f"{value:.12g}"


def detection_mode(config_known: bool, utilization_known: bool) -> str:
    if config_known and utilization_known:
        return MODE_FULL
    if config_known:
        return MODE_CONFIG_ONLY
    if utilization_known:
        return MODE_UTILIZATION_ONLY
    return MODE_NONE


def build_idle_compute_sql(
    ctx: StepContext,
    cluster_columns: set[str],
    cluster_problem: str | None,
    node_columns: set[str],
    node_problem: str | None,
    thresholds: IdleComputeThresholds | None = None,
    config_flag_requires_idle: bool = CONFIG_FLAG_REQUIRES_IDLE,
    window: DetectionWindow | None = None,
) -> tuple[str, str]:
    """Return (CREATE statement, detection mode) for the probed source tables."""

    t = thresholds or IdleComputeThresholds()
    upstream = ctx.config.thresholds
    window = window or detection_window(ctx.config.bundle_root, ctx.config.lookback_days)
    days = str(window.days)

    config_known = cluster_problem is None
    cpu = cpu_expr(node_columns)
    utilization_known = node_problem is None and cpu is not None

    if config_known:
        cluster_state_sql = ctx.sql(
            "idle_compute_cluster_state",
            cluster_name_expr=_optional(cluster_columns, "cluster_name"),
            owned_by_expr=_optional(cluster_columns, "owned_by"),
            all_purpose_sources_sql=", ".join(sql_string(s) for s in ALL_PURPOSE_SOURCES),
        ).rstrip()
    else:
        cluster_state_sql = ctx.sql("idle_compute_cluster_state_from_billing").rstrip()

    if utilization_known:
        memory = f"CAST({MEMORY_COLUMN} AS DOUBLE)" if MEMORY_COLUMN in node_columns else "CAST(NULL AS DOUBLE)"
        node_hours_sql = ctx.sql("idle_compute_node_hours", cpu_expr=cpu, memory_expr=memory,
                                 detection_days=days).rstrip()
    else:
        node_hours_sql = _EMPTY_NODE_HOURS

    statement = ctx.sql(
        "idle_compute",
        target=ctx.table("idle_compute"),
        daily_cost_table=ctx.table("daily_cost"),
        run_id_sql=sql_string(ctx.run_id),
        cluster_state_sql=cluster_state_sql,
        node_hours_sql=node_hours_sql,
        config_known_sql="TRUE" if config_known else "FALSE",
        utilization_known_sql="TRUE" if utilization_known else "FALSE",
        config_flag_requires_idle_sql="TRUE" if config_flag_requires_idle else "FALSE",
        low_cpu_pct=_num(upstream.low_cpu_pct),
        low_memory_pct=_num(upstream.low_memory_pct),
        zombie_auto_term_minutes=_num(t.zombie_auto_term_minutes),
        idle_hours_share_pct=_num(t.idle_hours_share_pct),
        high_confidence_min_active_hours=_num(t.high_confidence_min_active_hours),
        priority_full_savings_monthly=_num(t.priority_full_savings_monthly),
        priority_weight_savings=_num(t.priority_weight_savings),
        priority_weight_idle=_num(t.priority_weight_idle),
        priority_weight_config=_num(t.priority_weight_config),
        config_score_off=_num(t.config_score_off),
        config_score_too_long=_num(t.config_score_too_long),
        config_score_unknown=_num(t.config_score_unknown),
        monthly_multiplier=_num(window.monthly_multiplier),
        detection_days=days,
        lookback_days=str(ctx.config.lookback_days),
        currency_code_sql=sql_string(ctx.config.currency_code),
    )
    return statement, detection_mode(config_known, utilization_known)


def build(ctx: StepContext) -> str:
    thresholds, warnings = load_idle_compute_thresholds(ctx.config.bundle_root)
    window = detection_window(ctx.config.bundle_root, ctx.config.lookback_days)
    warnings = tuple(warnings) + window.warnings
    cluster_columns, cluster_problem = probe_table(ctx.spark, CLUSTERS, REQUIRED_CLUSTER_COLUMNS)
    node_columns, node_problem = probe_table(ctx.spark, NODE_TIMELINE, REQUIRED_NODE_COLUMNS)
    if node_problem is None and cpu_expr(node_columns) is None:
        node_problem = f"{NODE_TIMELINE} has no CPU columns"

    statement, mode = build_idle_compute_sql(ctx, cluster_columns, cluster_problem,
                                             node_columns, node_problem, thresholds, window=window)
    ctx.spark.sql(statement)

    notes = [p for p in (cluster_problem, node_problem) if p]
    if mode == MODE_NONE:
        message = "idle_compute created empty: no auto-termination or utilisation signal"
    elif mode == MODE_FULL:
        message = "idle_compute built from compute.clusters auto-termination and node_timeline utilisation"
    elif mode == MODE_UTILIZATION_ONLY:
        message = "idle_compute built from utilisation only (LOW confidence); auto-termination not checked"
    else:
        message = "idle_compute built from auto-termination only (LOW confidence); idle cost unknown"
    message += f" over {window.days} day(s)"
    if notes:
        message += " (" + "; ".join(notes) + ")"
    if warnings:
        message += " [config: " + " ".join(warnings) + "]"
    return message


STEP = Step(name="build_idle_compute", run=build, output_table="idle_compute")
