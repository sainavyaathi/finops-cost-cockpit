"""Fast checks on the synthetic sandbox (pure Python, no Spark session).

Each test pins one planted scenario so a change to the generator cannot silently remove the
rows a downstream feature (F3-F6) is built and tested against.
"""

from __future__ import annotations

import hashlib
import re
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sandbox"))

from finops_sandbox import (  # noqa: E402
    SCHEMA_SPEC, SYSTEM_TABLE_MAP, WORKLOADS, RedirectingSpark, _hour_slices, build_synthetic_rows,
)

TODAY = date(2026, 10, 1)


@pytest.fixture(scope="module")
def rows():
    return build_synthetic_rows(days=7, today=TODAY)


def _current_clusters(rows):
    latest = {}
    for r in sorted(rows["compute_clusters"], key=lambda r: r["change_time"]):
        latest[r["cluster_id"]] = r
    return latest


# --- tables and schemas ------------------------------------------------------------------

def _check_struct(value, spec, where):
    """Every dict (struct) in a row has exactly the declared fields, recursively."""
    if value is None:
        return
    if isinstance(spec, list):
        assert isinstance(value, dict), where
        assert set(value) == {name for name, _ in spec}, where
        for name, sub in spec:
            _check_struct(value[name], sub, f"{where}.{name}")


def test_every_table_has_rows_matching_its_schema(rows):
    # Pure Python (no pyspark), so this runs in CI.
    assert set(rows) == set(SYSTEM_TABLE_MAP.values()) == set(SCHEMA_SPEC)
    for table, data in rows.items():
        assert data, f"{table} is empty"
        for r in data:
            _check_struct(r, SCHEMA_SPEC[table], table)


@pytest.mark.parametrize("table, columns", [
    ("compute_clusters", {"cluster_id", "auto_termination_minutes", "cluster_source", "owned_by",
                          "change_time", "delete_time", "tags", "data_security_mode"}),
    ("access_audit", {"event_time", "event_date", "service_name", "action_name", "user_agent",
                      "request_params", "user_identity", "response", "identity_metadata"}),
    ("query_history", {"statement_id", "statement_text", "compute", "executed_by", "execution_status",
                       "execution_duration_ms", "total_task_duration_ms", "client_application",
                       "client_driver", "from_result_cache", "start_time", "query_source"}),
    ("lakeflow_job_task_run_timeline", {"run_id", "job_run_id", "task_key", "result_state"}),
    ("lakeflow_jobs", {"job_id", "name", "trigger_type", "change_time"}),
])
def test_columns_downstream_features_use(table, columns):
    assert columns <= {name for name, _ in SCHEMA_SPEC[table]}


def test_nested_types_match_system_tables():
    s = {t: dict(spec) for t, spec in SCHEMA_SPEC.items()}
    assert s["access_audit"]["request_params"] == "map<string,string>"
    assert dict(s["access_audit"]["user_identity"]) == {"email": "string", "subject_name": "string"}
    assert dict(s["access_audit"]["response"]) == {"status_code": "int", "error_message": "string",
                                                   "result": "string"}
    assert {n for n, _ in s["query_history"]["compute"]} == {"type", "cluster_id", "warehouse_id"}
    assert isinstance(s["query_history"]["query_source"], list)
    assert s["compute_clusters"]["tags"] == "map<string,string>"
    assert s["compute_clusters"]["auto_termination_minutes"] == "bigint"
    assert s["billing_usage"]["usage_quantity"] == "decimal(38,18)"


def test_spark_loads_every_table():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from finops_sandbox import _schemas

    try:
        spark = SparkSession.builder.master("local[1]").appName("sandbox-schema-test") \
            .config("spark.ui.enabled", "false").config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "127.0.0.1").getOrCreate()
    except Exception as exc:  # noqa: BLE001 - no JVM on this machine
        pytest.skip(f"no Spark session: {exc}")
    small = build_synthetic_rows(days=2, today=TODAY)
    schemas = _schemas()
    for table, data in small.items():
        assert spark.createDataFrame(data, schema=schemas[table]).count() == len(data), table


@pytest.mark.parametrize("sql, expected", [
    ("SELECT * FROM SYSTEM.Billing.Usage", "SELECT * FROM cat.syn.billing_usage"),
    ("FROM `system`.`compute`.`clusters` c", "FROM cat.syn.compute_clusters c"),
    ("FROM system.`query`.history", "FROM cat.syn.query_history"),
    # String literals and a single backticked dotted identifier are not table references.
    ("WHERE t = 'system.billing.usage'", "WHERE t = 'system.billing.usage'"),
    ('WHERE t = "system.access.audit"', 'WHERE t = "system.access.audit"'),
    ("FROM `system.billing.usage`", "FROM `system.billing.usage`"),
    ("FROM system.billing.usage_v2", "FROM system.billing.usage_v2"),
    ("x = 'it''s' AND system.access.audit", "x = 'it''s' AND cat.syn.access_audit"),
])
def test_redirect_variants(sql, expected):
    assert RedirectingSpark(spark=None, source_schema="cat.syn")._rewrite(sql) == expected


def test_redirect_covers_new_tables():
    proxy = RedirectingSpark(spark=None, source_schema="cat.syn")
    sql = ("SELECT * FROM system.compute.clusters c JOIN system.access.audit a "
           "JOIN system.query.history q JOIN system.lakeflow.job_task_run_timeline t")
    out = proxy._rewrite(sql)
    for name in ("compute_clusters", "access_audit", "query_history", "lakeflow_job_task_run_timeline"):
        assert f"cat.syn.{name}" in out
    assert "system." not in out


# --- F5 idle / zombie -----------------------------------------------------------------------

def test_auto_termination_boundaries(rows):
    current = _current_clusters(rows)
    autoterm = {cid: r["auto_termination_minutes"] for cid, r in current.items()}
    assert autoterm["0404-zombie-autoterm-off"] == 0          # off -> flagged
    assert autoterm["0505-zombie-long-autoterm"] == 240       # well over 60 -> flagged
    assert autoterm["0707-autoterm-61-cluster"] == 61         # just over -> flagged
    assert autoterm["0101-shared-ds-cluster"] == 60           # exactly 60 -> NOT flagged
    assert current["0606-deleted-old-cluster"]["delete_time"] is not None
    # 0404 changed from 30 to 0, so it has an older row too: the latest change wins.
    history = [r["auto_termination_minutes"] for r in rows["compute_clusters"]
               if r["cluster_id"] == "0404-zombie-autoterm-off"]
    assert sorted(history) == [0, 30]


def _avg(rows, cluster, col):
    vals = [r[col] for r in rows["compute_node_timeline"] if r["cluster_id"] == cluster]
    assert vals, cluster
    return sum(vals) / len(vals)


def test_utilisation_near_misses(rows):
    cpu = lambda c: _avg(rows, c, "cpu_user_percent") + _avg(rows, c, "cpu_system_percent")  # noqa: E731
    assert cpu("0404-zombie-autoterm-off") < 10
    # Auto-termination off but busy: not idle.
    assert cpu("0808-streaming-autoterm-off") > 60
    # Low CPU but high memory: memory-bound, not idle.
    assert cpu("0909-memory-heavy-cluster") < 15
    assert _avg(rows, "0909-memory-heavy-cluster", "mem_used_percent") > 75


def test_job_clusters_have_no_auto_termination(rows):
    current = _current_clusters(rows)
    assert current["job-1001-cluster"]["cluster_source"] == "JOB"
    assert current["job-1001-cluster"]["auto_termination_minutes"] is None
    # Near-miss: a job cluster reporting 0 must be filtered out by cluster_source, not flagged.
    assert current["job-1004-cluster"]["cluster_source"] == "JOB"
    assert current["job-1004-cluster"]["auto_termination_minutes"] == 0
    assert current["0303-kpi-allpurpose-cluster"]["cluster_source"] == "UI"


def test_deleted_cluster_was_live_before(rows):
    history = sorted((r for r in rows["compute_clusters"] if r["cluster_id"] == "0606-deleted-old-cluster"),
                     key=lambda r: r["change_time"])
    assert [r["delete_time"] is None for r in history] == [True, False]
    assert all(r["auto_termination_minutes"] == 0 for r in history)


# --- F4 all-purpose misuse -------------------------------------------------------------------

def test_bi_tools_hit_all_purpose_cluster_over_jdbc_and_odbc(rows):
    agents = {r["user_agent"] for r in rows["access_audit"]
              if r["request_params"].get("clusterId") == "0202-bi-tableau-cluster"}
    assert any("JDBC" in a for a in agents)
    assert any("ODBC" in a for a in agents)
    # Near-miss: the same Tableau driver on a SQL warehouse is correct usage.
    assert any(r["request_params"].get("warehouseId") == "wh-bi-001" for r in rows["access_audit"])


def test_token_logins_include_a_non_bi_near_miss(rows):
    agents = {r["user_agent"] for r in rows["access_audit"] if r["action_name"] == "tokenLogin"}
    assert any("JDBC" in a for a in agents) and any("ODBC" in a for a in agents)
    assert any(a.startswith("databricks-sdk-py") for a in agents)


def test_new_all_purpose_clusters_have_no_billing_run_as(rows):
    # Assumption: real billing leaves run_as empty for classic all-purpose compute.
    for cid in ("0707-autoterm-61-cluster", "0808-streaming-autoterm-off", "0909-memory-heavy-cluster"):
        usage = [r for r in rows["billing_usage"] if r["usage_metadata"]["cluster_id"] == cid]
        assert usage and all(r["identity_metadata"]["run_as"] is None for r in usage), cid


def test_shared_cluster_has_three_users_and_single_user_near_miss(rows):
    users = defaultdict(set)
    for r in rows["access_audit"]:
        if r["action_name"] == "attachNotebook":
            users[r["request_params"]["clusterId"]].add(r["user_identity"]["email"])
    assert len(users["0101-shared-ds-cluster"]) == 3
    assert len(users["0404-zombie-autoterm-off"]) == 1


def test_scheduled_job_on_all_purpose_cluster(rows):
    current = _current_clusters(rows)
    runs = [r for r in rows["lakeflow_job_run_timeline"] if r["job_id"] == "1007"]
    assert runs and {r["trigger_type"] for r in runs} == {"CRON"}
    assert {c for r in runs for c in r["compute_ids"]} == {"0303-kpi-allpurpose-cluster"}
    assert current["0303-kpi-allpurpose-cluster"]["cluster_source"] == "UI"
    # Like real billing.usage, all-purpose usage carries no job_id: it is billed to the cluster.
    usage = [r for r in rows["billing_usage"]
             if r["usage_metadata"]["cluster_id"] == "0303-kpi-allpurpose-cluster"]
    assert usage and {r["sku_name"] for r in usage} == {"PREMIUM_ALL_PURPOSE_COMPUTE"}
    assert all(r["usage_metadata"]["job_id"] is None for r in usage)
    assert all(r["usage_metadata"]["job_id"] is None for r in rows["billing_usage"]
               if r["sku_name"] == "PREMIUM_ALL_PURPOSE_COMPUTE")


def test_manual_run_on_all_purpose_is_a_near_miss(rows):
    runs = [r for r in rows["lakeflow_job_run_timeline"] if r["job_id"] == "1008"]
    assert runs and {r["trigger_type"] for r in runs} == {"ONETIME"}
    assert {c for r in runs for c in r["compute_ids"]} == {"0101-shared-ds-cluster"}
    job = [r for r in rows["lakeflow_jobs"] if r["job_id"] == "1008"]
    assert job[0]["trigger_type"] is None


# --- F3 shared-warehouse catalog allocation --------------------------------------------------

_PART = r"(?:`([^`]+)`|([A-Za-z_]\w*))"
_THREE_PART = re.compile(rf"(?<![\w.`]){_PART}\s*\.\s*{_PART}\s*\.\s*{_PART}(?![\w`])")


def _catalogs(text):
    """Catalogs named in three-part identifiers, lower-cased; backticks allowed per part."""
    return {(m.group(1) or m.group(2)).lower() for m in _THREE_PART.finditer(text)}


def test_catalog_parser():
    assert _catalogs("SELECT * FROM `sales`.`gold`.`orders`") == {"sales"}
    assert _catalogs("SELECT * FROM Finance.Gold.Invoices") == {"finance"}
    assert _catalogs("SELECT COUNT(*) FROM gold.orders") == set()
    assert _catalogs("SELECT o.amount FROM sales.gold.orders o JOIN marketing.gold.campaigns c "
                     "ON o.campaign_id = c.id") == {"sales", "marketing"}


def _catalog_ms(rows, warehouse):
    exec_ms = defaultdict(int)
    for r in rows["query_history"]:
        if r["compute"]["warehouse_id"] != warehouse:
            continue
        for catalog in _catalogs(r["statement_text"]) or {"UNATTRIBUTED"}:
            exec_ms[catalog] += r["execution_duration_ms"]
    return exec_ms


def test_shared_warehouse_spans_catalogs(rows):
    exec_ms = _catalog_ms(rows, "wh-bi-001")
    assert set(exec_ms) == {"sales", "finance", "marketing", "UNATTRIBUTED"}
    assert max(exec_ms, key=exec_ms.get) == "sales"
    # Both warehouses have billed usage to allocate.
    billed = {r["usage_metadata"]["warehouse_id"] for r in rows["billing_usage"]}
    assert {"wh-bi-001", "wh-fin-002"} <= billed


def test_single_catalog_warehouse_near_miss(rows):
    assert set(_catalog_ms(rows, "wh-fin-002")) == {"finance"}


def test_f3_parsing_cases_present(rows):
    texts = [r["statement_text"] for r in rows["query_history"] if r["compute"]["warehouse_id"] == "wh-bi-001"]
    assert any("`sales`.`gold`" in t for t in texts)          # backticked
    assert any("Finance.Gold." in t for t in texts)            # mixed case
    assert any(t == "SELECT COUNT(*) FROM gold.orders" for t in texts)  # two-part: unattributed


def test_query_history_edge_cases(rows):
    statuses = {r["execution_status"] for r in rows["query_history"]}
    assert {"FINISHED", "FAILED", "CANCELED"} <= statuses
    assert any(r["from_result_cache"] for r in rows["query_history"])
    assert any("sales." in r["statement_text"] and "marketing." in r["statement_text"]
               for r in rows["query_history"])
    drivers = {r["client_driver"] for r in rows["query_history"]}
    assert {"Databricks JDBC Driver", "Databricks ODBC Driver", None} == drivers


# --- F6 retries -----------------------------------------------------------------------------

def _task_runs(rows, job_id):
    """job_run_id -> task_key -> set of task run ids."""
    out = defaultdict(lambda: defaultdict(set))
    for r in rows["lakeflow_job_task_run_timeline"]:
        if r["job_id"] == job_id:
            out[r["job_run_id"]][r["task_key"]].add(r["run_id"])
    return out


def test_task_retries_within_one_job_run(rows):
    runs = _task_runs(rows, "1005")
    retried = [jr for jr, tasks in runs.items() if any(len(ids) > 1 for ids in tasks.values())]
    assert retried, "finance_reconcile must have task retries"
    # Grouping by the task run id (the upstream bug) can never see a retry.
    per_task_run = defaultdict(set)
    for r in rows["lakeflow_job_task_run_timeline"]:
        per_task_run[r["run_id"]].add(r["task_key"])
    assert all(len(keys) == 1 for keys in per_task_run.values())


def test_no_retries_and_slices_are_not_retries(rows):
    # Near-miss: healthy job, three tasks, one attempt each.
    assert all(len(ids) == 1 for tasks in _task_runs(rows, "1001").values() for ids in tasks.values())
    # Near-miss: ml_feature_training's long task is sliced into several rows with one run_id.
    sliced = defaultdict(int)
    for r in rows["lakeflow_job_task_run_timeline"]:
        if r["job_id"] == "1002" and r["task_key"] == "transform":
            sliced[r["run_id"]] += 1
    assert max(sliced.values()) > 1
    assert all(len(ids) == 1 for tasks in _task_runs(rows, "1002").values() for ids in tasks.values())


def _spans(rows, table, key):
    spans = {}
    for r in rows[table]:
        k = r[key]
        s0, s1 = spans.get(k, (r["period_start_time"], r["period_end_time"]))
        spans[k] = (min(s0, r["period_start_time"]), max(s1, r["period_end_time"]))
    return spans


def test_task_order_retries_and_run_end(rows):
    runs = _spans(rows, "lakeflow_job_run_timeline", "run_id")
    tasks = _spans(rows, "lakeflow_job_task_run_timeline", "run_id")
    by_job_run = defaultdict(list)
    for r in rows["lakeflow_job_task_run_timeline"]:
        by_job_run[r["job_run_id"]].append(r)
    for job_run_id, task_rows in by_job_run.items():
        ids = sorted({r["run_id"] for r in task_rows}, key=lambda i: tasks[i])
        # Each task run (including a retry attempt) starts no earlier than the previous one ends.
        for prev, nxt in zip(ids, ids[1:]):
            assert tasks[nxt][0] >= tasks[prev][1], job_run_id
        start, end = runs[job_run_id]
        assert tasks[ids[0]][0] >= start
        assert max(tasks[i][1] for i in ids) == end, job_run_id   # run ends with its last task


def test_failed_run_blocks_downstream_task(rows):
    failed = {r["run_id"] for r in rows["lakeflow_job_run_timeline"] if r["result_state"] == "FAILED"}
    assert failed
    for r in rows["lakeflow_job_task_run_timeline"]:
        if r["job_run_id"] in failed and r["task_key"] == "load":
            assert r["result_state"] == "BLOCKED" and r["termination_code"] == "SKIPPED"
            assert r["period_start_time"] == r["period_end_time"]


def test_hour_slices_are_clock_aligned():
    s = _hour_slices(datetime(2026, 1, 1, 16, 47), datetime(2026, 1, 1, 18, 10))
    assert s == [(datetime(2026, 1, 1, 16, 47), datetime(2026, 1, 1, 17)),
                 (datetime(2026, 1, 1, 17), datetime(2026, 1, 1, 18)),
                 (datetime(2026, 1, 1, 18), datetime(2026, 1, 1, 18, 10))]
    assert _hour_slices(datetime(2026, 1, 1, 9, 5), datetime(2026, 1, 1, 9, 30)) == \
        [(datetime(2026, 1, 1, 9, 5), datetime(2026, 1, 1, 9, 30))]


def test_only_final_slice_carries_result_state(rows):
    by_run = defaultdict(list)
    for r in rows["lakeflow_job_run_timeline"]:
        by_run[r["run_id"]].append(r)
    assert any(len(v) > 1 for v in by_run.values())
    for slices in by_run.values():
        slices.sort(key=lambda r: r["period_start_time"])
        assert slices[-1]["result_state"] is not None
        assert all(r["result_state"] is None for r in slices[:-1])


# --- determinism ----------------------------------------------------------------------------

def _canon(v):
    if isinstance(v, dict):
        return "{" + ",".join(f"{k!r}:{_canon(v[k])}" for k in sorted(v)) + "}"
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(_canon(x) for x in v) + "]"
    return repr(v)


def _digest(rows):
    h = hashlib.sha256()
    for r in rows:
        h.update(_canon(r).encode() + b"\n")
    return h.hexdigest()


# sha256 of origin/development's generator output (days=7, seed=42, today=2026-10-01) for the
# two tables the upstream cost and utilisation numbers come from. The F12 scenarios (their own
# RNG) and job 1007 / cluster 0303 (deliberately billed to the cluster) are excluded.
ORIGINAL_BILLING_DIGEST = "72410b1f0e88c5bccd240fbe5b5c8183e3e4438fa4bcadfb814ac9e29dc8be5e"
ORIGINAL_NODE_DIGEST = "c016f425acd9befc368ac103678fbcf57edc7d5064e081e963c0b6c4fe19988b"


def test_original_workloads_billing_and_nodes_unchanged(rows):
    ids = {w.get(k) for w in WORKLOADS for k in ("cluster", "warehouse", "pipeline", "notebook")} - {None}
    job_ids = {w["job_id"] for w in WORKLOADS if w["kind"] == "JOB"}

    def original(u):
        m = u["usage_metadata"]
        src = [m["cluster_id"], m["warehouse_id"], m["dlt_pipeline_id"], m["notebook_id"]]
        return any(x in ids for x in src) if any(src) else m["job_id"] in job_ids | {None}

    billing = [u for u in rows["billing_usage"] if original(u)]
    nodes = [n for n in rows["compute_node_timeline"] if n["cluster_id"] in ids]
    assert _digest(billing) == ORIGINAL_BILLING_DIGEST
    assert _digest(nodes) == ORIGINAL_NODE_DIGEST


def test_same_seed_gives_same_data(rows):
    assert build_synthetic_rows(days=7, today=TODAY) == rows


def test_data_is_relative_to_today(rows):
    later = build_synthetic_rows(days=7, today=date(2026, 10, 8))
    assert max(r["usage_date"] for r in rows["billing_usage"]) == date(2026, 9, 30)
    assert max(r["usage_date"] for r in later["billing_usage"]) == date(2026, 10, 7)
    assert build_synthetic_rows(days=7, seed=7, today=TODAY) != rows
