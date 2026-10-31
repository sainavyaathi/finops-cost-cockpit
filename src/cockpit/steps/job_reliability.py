"""F6 job reliability with a corrected retry_count, published as cockpit.job_reliability.

Upstream ``reliability.py::_retry_cte`` groups ``system.lakeflow.job_task_run_timeline`` by
``run_id``. In that table ``run_id`` is the *task* run id; the parent job run is ``job_run_id``.
Each group therefore holds a single task attempt and upstream ``retry_count`` is always 0.

Rule R3 forbids editing the vendored engine, so this step leaves ``job_reliability_summary``
untouched and writes a corrected copy. ``retry_count`` covers retries and repair reruns:

* attempts per task = ``COUNT(DISTINCT run_id)`` per ``(job_run_id, task_key)`` (distinct, because a
  long attempt spans several hourly timeline rows). Only top-level task runs count
  (``parent_run_id`` NULL or equal to ``job_run_id``), so for-each iterations are not mistaken for
  retries; without ``parent_run_id`` they cannot be excluded and the run log says so;
* retries per job run = ``SUM(attempts - 1)`` over its tasks;
* only job runs upstream counted (in ``system.lakeflow.job_run_timeline`` within the lookback)
  contribute, so ``retry_count`` lines up with ``run_count``. The task timeline is read over
  ``lookback + TASK_WINDOW_MARGIN_DAYS`` so early attempts of runs near the edge are kept;
* per job: ``retry_count`` = sum over job runs, ``retried_run_count`` = job runs with any retry,
  ``retry_rate_pct`` = retried runs / ``run_count``. ``reliability_category`` is re-evaluated with the
  same thresholds and order upstream uses; REVIEW_REQUIRED is recomputed from job_run_timeline.

If either timeline is missing or lacks the needed columns, the step keeps upstream's values,
sets ``retry_count_source = 'UPSTREAM_FALLBACK'`` and says so in the run log.

Detection window: everything covers ``min(detection.detection_days, lookback_days)`` days
(``cockpit.step_config.detection_window``). When that equals the upstream lookback the step reads
``job_reliability_summary`` as before. When the upstream lookback is longer (a trend profile, e.g. 180
days), the upstream SQL (``databricks_finops.reliability.build_job_reliability_sql``, used read-only)
is rendered again with the detection window and read as a subquery, so run counts, failure rates and
failed cost reflect only recent runs. Upstream's own table is left untouched (R3).

Retry time: ``task_attempt_minutes`` (all top-level task attempts) and ``retry_task_minutes`` (attempts
that were not the final one per task) feed F7's retry-cost estimate.
"""

from __future__ import annotations

import re
from dataclasses import replace

from databricks_finops.reliability import build_job_reliability_sql as build_upstream_reliability_sql
from databricks_finops.spark_utils import sql_string

from cockpit.pipeline import Step, StepContext
from cockpit.step_config import DetectionWindow, detection_window

TASK_TIMELINE = "system.lakeflow.job_task_run_timeline"
JOB_RUN_TIMELINE = "system.lakeflow.job_run_timeline"
REQUIRED_TASK_COLUMNS = frozenset({"workspace_id", "job_id", "job_run_id", "run_id", "task_key",
                                   "period_start_time"})
REQUIRED_RUN_COLUMNS = frozenset({"workspace_id", "job_id", "run_id", "period_start_time",
                                  "period_end_time", "result_state"})
TASK_WINDOW_MARGIN_DAYS = 2

SOURCE_TASK_TIMELINE = "TASK_TIMELINE_JOB_RUN_ID"
SOURCE_UPSTREAM_FALLBACK = "UPSTREAM_FALLBACK"

TOP_LEVEL_TASK_FILTER = "AND (parent_run_id IS NULL OR CAST(parent_run_id AS STRING) = CAST(job_run_id AS STRING))"

_EMPTY_RUN_RETRIES = """    SELECT
        CAST(NULL AS STRING) AS workspace_id,
        CAST(NULL AS STRING) AS job_id,
        CAST(NULL AS STRING) AS job_run_id,
        CAST(0 AS BIGINT) AS retry_count,
        CAST(NULL AS DOUBLE) AS task_attempt_minutes,
        CAST(NULL AS DOUBLE) AS retry_task_minutes
    WHERE FALSE"""

ATTEMPT_MINUTES_EXPR = (
    "ROUND((unix_timestamp(MAX(period_end_time)) - unix_timestamp(MIN(period_start_time))) / 60.0, 4)"
)
_CREATE_HEADER = re.compile(r"^\s*CREATE\s+OR\s+REPLACE\s+TABLE\s+\S+\s+USING\s+DELTA\s+AS\s+", re.IGNORECASE)

_EMPTY_RUN_STATES = """    SELECT
        CAST(NULL AS STRING) AS workspace_id,
        CAST(NULL AS STRING) AS job_id,
        CAST(0 AS BIGINT) AS unknown_state_run_count
    WHERE FALSE"""


def _available(preflight, table: str) -> bool:
    return preflight is not None and preflight.available(table)


def _columns(preflight, table: str) -> set[str]:
    return set(preflight.columns(table)) if preflight is not None else set()


def _table_problem(preflight, table: str, required: frozenset[str]) -> str | None:
    if not _available(preflight, table):
        return f"{table} unavailable"
    missing = required - _columns(preflight, table)
    if missing:
        return f"{table} lacks columns {sorted(missing)}"
    return None


def retry_fallback_reason(preflight) -> str | None:
    """Why the corrected count cannot be computed, or None when it can.

    Both timelines are needed: without job_run_timeline the retries could not be limited to the job
    runs upstream counted, so they would not line up with run_count.
    """

    return (_table_problem(preflight, TASK_TIMELINE, REQUIRED_TASK_COLUMNS)
            or _table_problem(preflight, JOB_RUN_TIMELINE, REQUIRED_RUN_COLUMNS))


def can_exclude_for_each_iterations(preflight) -> bool:
    return "parent_run_id" in _columns(preflight, TASK_TIMELINE)


def upstream_relation(ctx: StepContext, window: DetectionWindow) -> tuple[str, str | None]:
    """The upstream per-job summary to correct: (relation SQL, note or None).

    Same window as the upstream lookback: the upstream table. Narrower window: the upstream SQL
    rendered with ``lookback_days = window.days`` and wrapped as a subquery (R3: nothing upstream is
    changed). Without job_run_timeline the upstream table is a placeholder, so it is read as is.
    """

    table = ctx.table("job_reliability_summary")
    if not window.narrowed:
        return table, None
    if not _available(ctx.preflight, JOB_RUN_TIMELINE):
        return table, (f"{JOB_RUN_TIMELINE} unavailable: upstream placeholder read as is "
                       f"(detection window {window.days} days not applied)")
    narrowed_config = replace(ctx.config, lookback_days=window.days)
    upstream_sql = build_upstream_reliability_sql(narrowed_config, ctx.run_id, ctx.preflight)
    match = _CREATE_HEADER.match(upstream_sql)
    if match is None:
        return table, (f"upstream reliability SQL has an unexpected header: detection window {window.days} days "
                       f"not applied, upstream lookback {window.lookback_days} days used")
    return f"(\n{upstream_sql[match.end():].rstrip()}\n)", None


def build_job_reliability_sql(ctx: StepContext, window: DetectionWindow | None = None) -> tuple[str, str | None]:
    """Return (CREATE statement, fallback reason or None)."""

    statement, reason, _ = build_job_reliability_statement(ctx, window)
    return statement, reason


def build_job_reliability_statement(
    ctx: StepContext, window: DetectionWindow | None = None,
) -> tuple[str, str | None, str | None]:
    """Return (CREATE statement, fallback reason or None, detection-window note or None)."""

    config = ctx.config
    window = window or detection_window(config.bundle_root, config.lookback_days)
    days = str(window.days)
    reason = retry_fallback_reason(ctx.preflight)
    if reason is None:
        top_level = TOP_LEVEL_TASK_FILTER if can_exclude_for_each_iterations(ctx.preflight) else ""
        has_end = "period_end_time" in _columns(ctx.preflight, TASK_TIMELINE)
        run_retries_sql = ctx.sql(
            "job_task_retries",
            detection_days=days,
            task_lookback_days=str(window.days + TASK_WINDOW_MARGIN_DAYS),
            top_level_task_filter=top_level,
            attempt_minutes_expr=ATTEMPT_MINUTES_EXPR if has_end else "CAST(NULL AS DOUBLE)",
        ).rstrip()
        run_states_sql = ctx.sql("job_run_states", detection_days=days).rstrip()
        retry_count_expr = "CAST(COALESCE(r.retry_count, 0) AS BIGINT)"
        retried_run_count_expr = "CAST(COALESCE(r.retried_run_count, 0) AS BIGINT)"
        task_minutes_expr = "CAST(COALESCE(r.task_attempt_minutes, 0.0) AS DOUBLE)" if has_end else "CAST(NULL AS DOUBLE)"
        retry_minutes_expr = "CAST(COALESCE(r.retry_task_minutes, 0.0) AS DOUBLE)" if has_end else "CAST(NULL AS DOUBLE)"
        review_required_expr = "COALESCE(s.unknown_state_run_count, 0) > 0"
        source = SOURCE_TASK_TIMELINE
    else:
        run_retries_sql = _EMPTY_RUN_RETRIES
        run_states_sql = _EMPTY_RUN_STATES
        retry_count_expr = "CAST(u.retry_count AS BIGINT)"
        retried_run_count_expr = "CAST(NULL AS BIGINT)"
        task_minutes_expr = retry_minutes_expr = "CAST(NULL AS DOUBLE)"
        # Cannot be recomputed without job_run_timeline; inherit upstream's verdict.
        review_required_expr = "COALESCE(u.reliability_category = 'REVIEW_REQUIRED', FALSE)"
        source = SOURCE_UPSTREAM_FALLBACK

    upstream_sql, window_note = upstream_relation(ctx, window)
    statement = ctx.sql(
        "job_reliability",
        target=ctx.table("job_reliability"),
        upstream_table=upstream_sql,
        run_retries_sql=run_retries_sql,
        run_states_sql=run_states_sql,
        retry_count_expr=retry_count_expr,
        retried_run_count_expr=retried_run_count_expr,
        task_attempt_minutes_expr=task_minutes_expr,
        retry_task_minutes_expr=retry_minutes_expr,
        review_required_expr=review_required_expr,
        detection_days=days,
        retry_count_source_sql=sql_string(source),
        retry_heavy_count=f"{config.thresholds.retry_heavy_count:.12g}",
        high_failure_rate_pct=f"{config.thresholds.high_failure_rate_pct:.12g}",
        run_id_sql=sql_string(ctx.run_id),
    )
    return statement, reason, window_note


def build(ctx: StepContext) -> str:
    window = detection_window(ctx.config.bundle_root, ctx.config.lookback_days)
    statement, reason, window_note = build_job_reliability_statement(ctx, window)
    ctx.spark.sql(statement)
    if reason is not None:
        message = f"job_reliability built with upstream retry_count ({reason}); retries likely undercounted"
    else:
        message = "job_reliability built; retry_count recounted per job_run_id and task_key"
        if not can_exclude_for_each_iterations(ctx.preflight):
            message += f" (no parent_run_id in {TASK_TIMELINE}: for-each iterations cannot be excluded)"
    if window.narrowed and window_note is None:
        message += (f"; detection window {window.days} day(s): upstream summary re-rendered "
                    f"(lookback {window.lookback_days})")
    extra = ([window_note] if window_note else []) + list(window.warnings)
    return message + ("; " + "; ".join(extra) if extra else "")


STEP = Step(name="build_job_reliability", run=build, output_table="job_reliability")
