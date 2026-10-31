"""F4 all-purpose compute misuse -> cockpit.allpurpose_misuse (one row per cluster, rule R1).

Three misuse types on all-purpose clusters (``compute.clusters.cluster_source`` UI/API, or an
ALL_PURPOSE billing SKU when the cluster is not in ``compute.clusters``):

- ``BI_TOOL_JDBC``: JDBC/ODBC or BI clients on the cluster (``access.audit`` requests carrying
  ``clusterId``, an assumption, LOW confidence; ``query.history`` ``compute.cluster_id``, MEDIUM).
  Saving: ``bi_to_sql_warehouse_savings_pct`` of the usage overlapping BI hours.
- ``SCHEDULED_JOB``: automated job runs whose compute is the cluster (``lakeflow`` ``compute_ids``;
  billing has no job_id for them). Automated = trigger in ``automated_trigger_types`` (CRON,
  CONTINUOUS, FILE_ARRIVAL, TABLE, ...), run_type in ``automated_run_types`` (SUBMIT_RUN from
  external orchestrators), or ONETIME_RETRY of an automated job. A manual ONETIME JOB_RUN is not.
  Saving: the same DBUs (assumed unchanged) at the jobs SKU price, net of interactive hours.
- ``SHARED_USERS``: at least ``min_interactive_users`` people attach notebooks to the cluster
  (``access.audit`` attachNotebook). An attribution issue: no saving claimed.

Each signal is independent. A missing source table skips only the signals that need it, and the
skip is reported in the run-log message. Thresholds come from the ``allpurpose_misuse`` section of
``config/cockpit_thresholds.yml`` (see ``cockpit.step_config``).
"""

from __future__ import annotations

from typing import Any

from cockpit.pipeline import Step, StepContext
from cockpit.step_config import enforce_bounds, load_step_lists, load_step_thresholds
from databricks_finops.spark_utils import sql_string

SECTION = "allpurpose_misuse"

DEFAULTS: dict[str, float] = {
    "min_interactive_users": 3,
    "min_bi_events": 5,
    "min_scheduled_runs": 2,
    "use_query_history": 1,
    "bi_to_sql_warehouse_savings_pct": 30,
    "jobs_price_ratio_fallback": 0.3,
    "score_reference_monthly_cost": 1000,
    "priority_weight_savings": 0.5,
    "priority_weight_cost": 0.3,
    "priority_weight_breadth": 0.2,
    "confidence_multiplier_high": 1.0,
    "confidence_multiplier_medium": 0.85,
    "confidence_multiplier_low": 0.7,
}

# job_run_timeline.trigger_type values documented: CRON, CONTINUOUS, FILE_ARRIVAL, ONETIME,
# ONETIME_RETRY (TABLE on newer accounts). PERIODIC is a Jobs API trigger and a lakeflow.jobs value.
LIST_DEFAULTS: dict[str, tuple[str, ...]] = {
    "automated_trigger_types": ("CRON", "PERIODIC", "CONTINUOUS", "FILE_ARRIVAL", "TABLE", "ONETIME_RETRY"),
    "automated_run_types": ("SUBMIT_RUN",),
}
RETRY_TRIGGER = "ONETIME_RETRY"
# Never automated on their own: a manual UI "Run now" is trigger ONETIME, run_type JOB_RUN.
NEVER_AUTOMATED = {"triggers": ("ONETIME",), "run_types": ("JOB_RUN",)}

# (low, high, low_exclusive): divisors must be > 0; percentages and ratios stay in range.
BOUNDS: dict[str, tuple[float | None, float | None, bool]] = {
    "score_reference_monthly_cost": (0, None, True),
    "min_interactive_users": (1, None, False),
    "min_bi_events": (1, None, False),
    "min_scheduled_runs": (1, None, False),
    "bi_to_sql_warehouse_savings_pct": (0, 100, False),
    "jobs_price_ratio_fallback": (0, 1, False),
    "use_query_history": (0, 1, False),
    "priority_weight_savings": (0, None, False),
    "priority_weight_cost": (0, None, False),
    "priority_weight_breadth": (0, None, False),
    "confidence_multiplier_high": (0, None, False),
    "confidence_multiplier_medium": (0, None, False),
    "confidence_multiplier_low": (0, None, False),
}

MISUSE_TYPES = ("BI_TOOL_JDBC", "SCHEDULED_JOB", "SHARED_USERS")
ALL_PURPOSE_SOURCES = ("UI", "API")
# Matched case-insensitively against access.audit user_agent and query.history client_application.
BI_CLIENT_REGEX = "(?i)(jdbc|odbc|simba|tableau|power ?bi|looker|qlik|microstrategy|sisense|thoughtspot)"

SIGNAL_COLUMNS = (
    ("workspace_id", "STRING"), ("cluster_id", "STRING"), ("misuse_type", "STRING"),
    ("activity_start", "TIMESTAMP"), ("activity_end", "TIMESTAMP"), ("actor", "STRING"),
    ("item", "STRING"), ("detail", "STRING"), ("source", "STRING"),
)
CLUSTER_COLUMNS = (
    ("workspace_id", "STRING"), ("cluster_id", "STRING"), ("cluster_name", "STRING"),
    ("cluster_source", "STRING"), ("owned_by", "STRING"),
)
PRICE_COLUMNS = (
    ("sku_name", "STRING"), ("cloud", "STRING"), ("usage_unit", "STRING"), ("currency_code", "STRING"),
    ("price_start_time", "TIMESTAMP"), ("price_end_time", "TIMESTAMP"), ("dbu_price", "DOUBLE"),
)
JOB_COLUMNS = (
    ("workspace_id", "STRING"), ("job_id", "STRING"), ("job_name", "STRING"), ("job_trigger_type", "STRING"),
)

# Probes: the columns each source must expose. A failing probe means the source is unavailable.
PROBES: dict[str, str] = {
    "clusters": "SELECT workspace_id, cluster_id, cluster_name, cluster_source, owned_by, change_time "
                "FROM system.compute.clusters LIMIT 0",
    "list_prices": "SELECT sku_name, cloud, usage_unit, currency_code, price_start_time, price_end_time, "
                   "pricing.default FROM system.billing.list_prices LIMIT 0",
    "audit": "SELECT workspace_id, event_time, event_date, user_agent, user_identity.email, service_name, "
             "action_name, request_params, request_id, event_id, response.status_code "
             "FROM system.access.audit LIMIT 0",
    "query_history": "SELECT workspace_id, compute.cluster_id, start_time, executed_by, statement_id, "
                     "client_application, client_driver FROM system.query.history LIMIT 0",
    "job_runs": "SELECT workspace_id, job_id, run_id, period_start_time, period_end_time, trigger_type, "
                "run_name, compute_ids FROM system.lakeflow.job_run_timeline LIMIT 0",
    "job_run_type": "SELECT run_type FROM system.lakeflow.job_run_timeline LIMIT 0",
    "task_runs": "SELECT workspace_id, job_id, run_id, job_run_id, period_start_time, period_end_time, "
                 "compute_ids FROM system.lakeflow.job_task_run_timeline LIMIT 0",
    "jobs": "SELECT workspace_id, job_id, name, change_time FROM system.lakeflow.jobs LIMIT 0",
    "jobs_trigger": "SELECT trigger_type FROM system.lakeflow.jobs LIMIT 0",
}


def empty_relation(columns: tuple[tuple[str, str], ...]) -> str:
    """A typed relation with no rows, used in place of a source that is unavailable."""

    select = ", ".join(f"CAST(NULL AS {kind}) AS {name}" for name, kind in columns)
    return f"SELECT {select} WHERE 1 = 0"


def sql_list(values: tuple[str, ...]) -> str:
    return ", ".join(sql_string(v) for v in values)


def in_test(column: str, values: tuple[str, ...]) -> str:
    """``column IN (...)``, or ``FALSE`` when there is nothing to match (an empty IN list is invalid)."""

    return f"{column} IN ({sql_list(values)})" if values else "FALSE"


def automation_rules(lists: dict[str, tuple[str, ...]]) -> tuple[tuple[str, ...], tuple[str, ...], bool, list[str]]:
    """Split the configured lists into (triggers, run_types, count_retries, warnings).

    ONETIME_RETRY is handled per job, not as a plain trigger. ONETIME and JOB_RUN are dropped
    (with a warning) because they would count manual runs.
    """

    warnings = []
    triggers = tuple(t for t in lists["automated_trigger_types"] if t not in NEVER_AUTOMATED["triggers"])
    run_types = tuple(r for r in lists["automated_run_types"] if r not in NEVER_AUTOMATED["run_types"])
    if len(triggers) < len(lists["automated_trigger_types"]) or len(run_types) < len(lists["automated_run_types"]):
        warnings.append(f"{SECTION}: ONETIME / JOB_RUN ignored in automated lists (manual runs are never automated).")
    count_retries = RETRY_TRIGGER in triggers
    return tuple(t for t in triggers if t != RETRY_TRIGGER), run_types, count_retries, warnings


def load_settings(bundle_root: str) -> tuple[dict[str, float], dict[str, tuple[str, ...]], list[str]]:
    thresholds, warnings = load_step_thresholds(bundle_root, SECTION, DEFAULTS)
    warnings += enforce_bounds(thresholds, DEFAULTS, SECTION, BOUNDS)
    lists, list_warnings = load_step_lists(bundle_root, SECTION, LIST_DEFAULTS)
    return thresholds, lists, warnings + list_warnings


def num(value: float) -> str:
    return f"{float(value):.12g}"


def probe_sources(spark: Any) -> dict[str, bool]:
    available = {}
    for name, probe in PROBES.items():
        try:
            spark.sql(probe).collect()
            available[name] = True
        except Exception:  # noqa: BLE001 - any failure means "not readable here"
            available[name] = False
    return available


def build_statement(
    ctx: StepContext,
    available: dict[str, bool],
    thresholds: dict[str, float],
    lists: dict[str, tuple[str, ...]] | None = None,
) -> tuple[str, list[str]]:
    """Compose the CREATE TABLE statement for the available sources. Returns (sql, notes)."""

    lookback = str(ctx.config.lookback_days)
    common = {"lookback_days": lookback, "bi_client_regex": sql_string(BI_CLIENT_REGEX)}
    notes: list[str] = []
    triggers, run_types, count_retries, rule_notes = automation_rules(lists or LIST_DEFAULTS)
    notes += rule_notes

    if available.get("clusters"):
        clusters = ctx.sql("allpurpose_misuse_clusters")
    else:
        clusters = empty_relation(CLUSTER_COLUMNS)
        notes.append("compute.clusters unavailable: all-purpose clusters taken from ALL_PURPOSE billing SKUs")

    if available.get("list_prices"):
        prices = ctx.sql("allpurpose_misuse_prices")
    else:
        prices = empty_relation(PRICE_COLUMNS)
        notes.append("list_prices unavailable: fallback DBU price and jobs_price_ratio_fallback used")

    if available.get("audit"):
        bi_audit = ctx.sql("allpurpose_misuse_bi_audit", **common)
        shared = ctx.sql("allpurpose_misuse_shared_users", **common)
    else:
        bi_audit = shared = empty_relation(SIGNAL_COLUMNS)
        notes.append("access.audit unavailable: SHARED_USERS skipped, BI_TOOL_JDBC from query.history only")

    if not thresholds.get("use_query_history", 1):
        bi_query = empty_relation(SIGNAL_COLUMNS)
        notes.append("query.history source switched off (use_query_history: 0)")
    elif available.get("query_history"):
        bi_query = ctx.sql("allpurpose_misuse_bi_query_history", **common)
    else:
        bi_query = empty_relation(SIGNAL_COLUMNS)
        notes.append("query.history unavailable: BI_TOOL_JDBC from access.audit only")

    if not available.get("job_runs"):
        scheduled = empty_relation(SIGNAL_COLUMNS)
        notes.append("lakeflow.job_run_timeline unavailable: SCHEDULED_JOB skipped")
    elif not available.get("task_runs"):
        # Run-level compute_ids is documented for WORKFLOW_RUN parents; task runs on an existing
        # all-purpose cluster are only reliable in job_task_run_timeline, so do not guess.
        scheduled = empty_relation(SIGNAL_COLUMNS)
        notes.append("lakeflow.job_task_run_timeline unavailable: SCHEDULED_JOB skipped "
                     "(run-level compute_ids does not reliably list existing all-purpose clusters)")
    else:
        if available.get("jobs"):
            trigger = "trigger_type" if available.get("jobs_trigger") else "CAST(NULL AS STRING)"
            jobs = ctx.sql("allpurpose_misuse_jobs", jobs_trigger_column=trigger)
        else:
            jobs = empty_relation(JOB_COLUMNS)
            notes.append("lakeflow.jobs unavailable: job names and job-level triggers missing")
        if run_types and not available.get("job_run_type"):
            notes.append("job_run_timeline.run_type unavailable: automated_run_types not applied")
        scheduled = ctx.sql(
            "allpurpose_misuse_scheduled_jobs",
            jobs_relation=jobs,
            task_compute_relation=ctx.sql("allpurpose_misuse_task_compute", lookback_days=lookback),
            run_type_column="run_type" if available.get("job_run_type") else "CAST(NULL AS STRING)",
            trigger_test=in_test("c.effective_trigger", triggers),
            run_type_test=in_test("c.run_type", run_types),
            job_trigger_test=in_test("c.job_trigger", triggers),
            count_retries="TRUE" if count_retries else "FALSE",
            lookback_days=lookback,
        )

    statement = ctx.sql(
        "allpurpose_misuse",
        target=ctx.table("allpurpose_misuse"),
        cost_base=ctx.table("cost_base"),
        clusters_relation=clusters,
        prices_relation=prices,
        bi_audit_signal=bi_audit,
        bi_query_signal=bi_query,
        scheduled_signal=scheduled,
        shared_signal=shared,
        all_purpose_sources=sql_list(ALL_PURPOSE_SOURCES),
        lookback_days=lookback,
        fallback_dbu_price=num(ctx.config.fallback_dbu_price),
        currency_code_sql=sql_string(ctx.config.currency_code),
        run_id_sql=sql_string(ctx.run_id),
        **{key: num(value) for key, value in thresholds.items() if key != "use_query_history"},
    )
    return statement, notes


def build(ctx: StepContext) -> str:
    thresholds, lists, warnings = load_settings(ctx.config.bundle_root)
    available = probe_sources(ctx.spark)
    statement, notes = build_statement(ctx, available, thresholds, lists)
    ctx.spark.sql(statement)

    counts = {t: 0 for t in MISUSE_TYPES}
    clusters = 0
    for row in ctx.spark.sql(f"SELECT misuse_types FROM {ctx.table('allpurpose_misuse')}").collect():
        clusters += 1
        for misuse_type in row[0] or []:
            counts[misuse_type] = counts.get(misuse_type, 0) + 1
    summary = ", ".join(f"{t} {n}" for t, n in counts.items())
    message = f"allpurpose_misuse: {clusters} cluster(s) flagged ({summary})"
    extra = notes + warnings
    return message + ("; " + "; ".join(extra) if extra else "")


STEP = Step(
    name="build_allpurpose_misuse",
    run=build,
    output_table="allpurpose_misuse",
    depends_on=("build_cost_base",),
)
