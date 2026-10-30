"""The F3/F4/F5/F10 planned pages, the Connection page and the Build progress page."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from cockpit_ui.core.registry import discover, enabled_features, group_features, load_toggles

APP_DIR = Path(__file__).resolve().parents[1]
SANDBOX_OUTPUT = APP_DIR.parent / "sandbox" / "local_output"
# Built at runtime so secret scanners do not flag a fake token as a real one.
FAKE_TOKEN = "dapi" + "0123456789abcdef" * 2 + "-2"

FEATURES = {f.id: f for f in discover().features}

PLANNED = {
    # id: (plan id, group, required table)
    "catalog_allocation": ("F3", "Attribution", "catalog_allocation"),
    "allpurpose_misuse": ("F4", "Waste", "allpurpose_misuse"),
    "idle_compute": ("F5", "Waste", "idle_compute"),
    "monday_alert": ("F10", "Operations", "alert_top_flags"),
}

# Rows shaped like the interface each page documents, used to prove render() works once the engine ships.
SAMPLE_ROWS = {
    "catalog_allocation": [
        {"usage_date": "2026-10-26", "warehouse_id": "wh1", "warehouse_name": "shared_bi", "catalog_name": "sales",
         "query_count": 120, "allocated_cost": 61.5, "allocation_pct": 60.0},
        {"usage_date": "2026-10-26", "warehouse_id": "wh1", "warehouse_name": "shared_bi", "catalog_name": "finance",
         "query_count": 80, "allocated_cost": 41.0, "allocation_pct": 40.0},
    ],
    "allpurpose_misuse": [
        {"cluster_id": "c1", "cluster_name": "team-shared", "owner": "ana@x.com", "misuse_type": "JDBC/ODBC",
         "evidence": "420 BI queries", "estimated_monthly_cost": 900.0, "estimated_monthly_savings": 410.0,
         "suggested_action": "Move BI traffic to a SQL warehouse"},
        {"cluster_id": "c2", "cluster_name": "etl-dev", "owner": "raj@x.com", "misuse_type": "Scheduled job",
         "evidence": "Nightly job on all-purpose", "estimated_monthly_cost": 600.0, "estimated_monthly_savings": 330.0,
         "suggested_action": "Run on job compute"},
    ],
    "idle_compute": [
        {"cluster_id": "c3", "cluster_name": "forgotten", "owner": "lee@x.com", "idle_reason": "auto-termination off",
         "autotermination_minutes": 0, "avg_cpu_pct": 3.1, "idle_hours": 140, "idle_cost": 280.0,
         "suggested_action": "Set auto-termination to 30 minutes"},
    ],
    "alert_top_flags": [
        {"alert_rank": rank, "resource_id": f"r{rank}", "resource_name": f"resource {rank}", "flag_type": "idle",
         "owner": "ops@x.com", "priority_score": 90 - rank, "estimated_monthly_savings": 100.0 * rank,
         "suggested_action": "Fix it"}
        for rank in (1, 2, 3)
    ],
}

FEATURE_SCRIPT = """
import os, sys
from dataclasses import replace
from pathlib import Path
sys.path.insert(0, {app_dir!r})
os.environ["DATABRICKS_TOKEN"] = {token!r}
from cockpit_ui.core.context import AppContext
from cockpit_ui.core.datasource import LocalCsvSource
from cockpit_ui.core.registry import discover
from cockpit_ui.core.settings import Settings
from cockpit_ui.core.shell import render_feature

class Source(LocalCsvSource):
    def describe(self):
        info = super().describe()
        if {leaky!r}:
            info["access token"] = {token!r}
            info["host"] = "https://example.cloud.databricks.com?t=" + {token!r}
        return info

feature = next(f for f in discover().features if f.id == {feature_id!r})
if {force_live!r}:
    feature = replace(feature, status="live")
settings = Settings(local_dir=Path({data_dir!r}))
render_feature(feature, AppContext(source=Source(settings.local_dir), settings=settings))
"""


def _run(feature_id: str, data_dir: Path, *, force_live: bool = False, leaky: bool = False) -> AppTest:
    script = FEATURE_SCRIPT.format(app_dir=str(APP_DIR), feature_id=feature_id, data_dir=str(data_dir),
                                   force_live=force_live, leaky=leaky, token=FAKE_TOKEN)
    return AppTest.from_string(script, default_timeout=30).run()


def _all_text(at: AppTest) -> str:
    parts = [m.value for m in at.markdown] + [c.value for c in at.caption]
    parts += [str(e.label) for e in at.expander]
    parts += [frame.value.to_csv(index=False) for frame in at.dataframe]
    return "\n".join(parts)


def _assert_clean(at: AppTest) -> None:
    assert not at.exception, [e.value for e in at.exception]
    assert not at.error, [e.value for e in at.error]


# ---------------------------------------------------------------- discovery and navigation

@pytest.mark.parametrize("feature_id", sorted(PLANNED))
def test_planned_feature_metadata(feature_id):
    plan_id, group, table = PLANNED[feature_id]
    feature = FEATURES[feature_id]
    assert feature.status == "planned"
    assert feature.plan_id == plan_id
    assert feature.group == group
    assert feature.requires == (table,)
    assert len(feature.highlights) >= 2


@pytest.mark.parametrize("feature_id", ["connection", "roadmap"])
def test_platform_pages_are_live_and_need_no_tables(feature_id):
    feature = FEATURES[feature_id]
    assert feature.status == "live"
    assert feature.group == "Platform"
    assert feature.requires == ()


def test_features_yaml_orders_navigation():
    features = enabled_features(discover().features, load_toggles(APP_DIR / "features.yaml"))
    ids = [f.id for f in features]
    assert ids[0] == "overview"
    expected = ["backlog", "utilization", "idle_compute", "allpurpose_misuse", "tagging", "catalog_allocation",
                "reliability", "monday_alert", "health", "connection", "roadmap"]
    assert [i for i in ids if i in expected] == expected
    groups = group_features(features)
    for feature_id, group in [("idle_compute", "Waste"), ("allpurpose_misuse", "Waste"),
                              ("catalog_allocation", "Attribution"), ("monday_alert", "Operations"),
                              ("connection", "Platform"), ("roadmap", "Platform")]:
        assert feature_id in [f.id for f in groups[group]]
    assert [f.id for f in groups["Platform"]][-2:] == ["connection", "roadmap"]


# ---------------------------------------------------------------- planned pages

@pytest.mark.parametrize("feature_id", sorted(PLANNED))
@pytest.mark.parametrize("where", ["fixtures", "empty"])
def test_planned_page_shows_panel(feature_id, where, fixtures_dir: Path, tmp_path: Path):
    at = _run(feature_id, fixtures_dir if where == "fixtures" else tmp_path)
    _assert_clean(at)
    assert at.title[0].value == FEATURES[feature_id].title
    assert not at.info  # no table gating for planned pages
    text = _all_text(at)
    assert "In development" in text
    assert PLANNED[feature_id][2] in text
    assert FEATURES[feature_id].highlights[0] in text


@pytest.mark.parametrize("feature_id", sorted(PLANNED))
def test_planned_page_renders_once_engine_ships(feature_id, tmp_path: Path):
    table = PLANNED[feature_id][2]
    pd.DataFrame(SAMPLE_ROWS[table]).to_csv(tmp_path / f"{table}.csv", index=False)
    at = _run(feature_id, tmp_path, force_live=True)
    _assert_clean(at)
    assert "In development" not in _all_text(at)
    assert at.dataframe, "the actionable list should be shown"
    assert any(b.label == "Download CSV" for b in at.get("download_button"))


@pytest.mark.parametrize("feature_id", sorted(PLANNED))
def test_planned_page_handles_empty_engine_table(feature_id, tmp_path: Path):
    table = PLANNED[feature_id][2]
    pd.DataFrame(columns=list(SAMPLE_ROWS[table][0])).to_csv(tmp_path / f"{table}.csv", index=False)
    at = _run(feature_id, tmp_path, force_live=True)
    _assert_clean(at)


# ---------------------------------------------------------------- connection

def _data_dirs(fixtures_dir: Path, tmp_path: Path) -> dict[str, Path]:
    dirs = {"fixtures": fixtures_dir, "empty": tmp_path, "missing": tmp_path / "does_not_exist"}
    if SANDBOX_OUTPUT.is_dir():
        dirs["sandbox"] = SANDBOX_OUTPUT
    return dirs


@pytest.mark.parametrize("where", ["fixtures", "sandbox", "empty", "missing"])
def test_connection_page_renders(where, fixtures_dir: Path, tmp_path: Path):
    dirs = _data_dirs(fixtures_dir, tmp_path)
    if where not in dirs:
        pytest.skip("sandbox/local_output not generated")
    at = _run("connection", dirs[where])
    _assert_clean(at)
    text = _all_text(at)
    assert "Gold-table coverage" in text
    coverage = next(frame.value for frame in at.dataframe if "table" in frame.value.columns)
    assert set(coverage["table"]) >= set(PLANNED_TABLES)
    if where in ("empty", "missing"):
        assert "Fail" in text or "Missing" in coverage["status"].tolist()
        assert "sandbox/run_local.py" in text  # actionable hint
    if where == "fixtures":
        assert (coverage.loc[coverage["table"] == "accelerator_health", "status"] == "OK").all()
        assert (coverage.loc[coverage["table"] == "catalog_allocation", "status"] == "Planned").all()


PLANNED_TABLES = ("catalog_allocation", "allpurpose_misuse", "idle_compute", "alert_top_flags")


def test_connection_page_never_shows_secrets(fixtures_dir: Path):
    at = _run("connection", fixtures_dir, leaky=True)
    _assert_clean(at)
    text = _all_text(at)
    assert FAKE_TOKEN not in text
    assert "dapi" not in text
    assert "[hidden]" in text and "[redacted]" in text
    env = next(frame.value for frame in at.dataframe if "variable" in frame.value.columns)
    assert env.loc[env["variable"] == "DATABRICKS_TOKEN", "state"].item() == "set"


def test_redaction_helpers():
    from cockpit_ui.features.connection.page import redact, safe_description

    assert FAKE_TOKEN not in redact(f"Bearer {FAKE_TOKEN}")
    assert "eyJ" not in redact("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc")
    assert redact("/Users/me/finops-cost-cockpit/sandbox/local_output") == "/Users/me/finops-cost-cockpit/sandbox/local_output"
    rows = dict(safe_description({"client secret": "abc", "warehouse id": "1234abcd", "directory": "/tmp/x"}))
    assert rows == {"client secret": "[hidden]", "warehouse id": "1234abcd", "directory": "/tmp/x"}


# ---------------------------------------------------------------- build progress

@pytest.mark.parametrize("where", ["fixtures", "sandbox", "empty"])
def test_roadmap_page_renders(where, fixtures_dir: Path, tmp_path: Path):
    dirs = _data_dirs(fixtures_dir, tmp_path)
    if where not in dirs:
        pytest.skip("sandbox/local_output not generated")
    at = _run("roadmap", dirs[where])
    _assert_clean(at)
    text = _all_text(at)
    for n in range(1, 13):
        assert f">F{n}<" in text
    export = next(frame.value for frame in at.dataframe if "plan_id" in frame.value.columns)
    stages = dict(zip(export["plan_id"], export["stage"]))
    assert stages["F3"] == "Planned"
    assert stages["F12"] == "Pipeline only"
    assert stages["F9"] == "Live"  # Connection and Build progress need no tables
    if where == "empty":
        assert stages["F6"] == "Waiting for data"
    else:
        assert stages["F6"] == "Live"


def test_roadmap_stage_derivation():
    from cockpit_ui.core.feature import Feature
    from cockpit_ui.features.roadmap.page import assess

    def make(fid, status, plan, requires=()):
        return Feature(id=fid, title=fid, render=lambda ctx: None, status=status, plan_id=plan, requires=requires)

    live = make("a", "live", "F6", ("t1",))
    preview = make("b", "preview", "F7", ("t2",))
    planned = make("c", "planned", "F3", ("t3",))
    features = [live, preview, planned]
    on = {"a", "b", "c"}
    assert assess("F6", "", "", "page", features, on, {"t1"}).stage == "Live"
    assert assess("F6", "", "", "page", features, on, set()).stage == "Waiting for data"
    assert assess("F6", "", "", "page", features, {"b", "c"}, {"t1"}).stage == "Not started"  # page switched off
    assert assess("F7", "", "", "page", features, on, {"t2"}).stage == "Preview"
    assert assess("F3", "", "", "page", features, on, {"t3"}).stage == "Planned"
    assert assess("F12", "", "", "pipeline", features, on, set()).stage == "Pipeline only"
    assert assess("F1", "", "", "page", features, on, set()).stage == "Not started"


def test_monday_alert_trusts_engine_order_and_flags_contract_breach(tmp_path: Path):
    rows = [dict(SAMPLE_ROWS["alert_top_flags"][0], alert_rank=rank, resource_name=f"resource {rank}",
                 priority_score=10 * rank)  # higher score on lower-ranked rows: the page must not re-sort by score
            for rank in (4, 2, 1, 3)]
    pd.DataFrame(rows).to_csv(tmp_path / "alert_top_flags.csv", index=False)
    at = _run("monday_alert", tmp_path, force_live=True)
    _assert_clean(at)
    assert any("R5" in w.value for w in at.warning)
    shown = next(frame.value for frame in at.dataframe if "alert_rank" in frame.value.columns)
    assert shown["alert_rank"].tolist() == [1, 2, 3]


def test_monday_alert_keeps_row_order_without_alert_rank(tmp_path: Path):
    rows = [{k: v for k, v in row.items() if k != "alert_rank"} for row in SAMPLE_ROWS["alert_top_flags"]]
    rows.reverse()  # lowest priority first: still shown as the engine returned it
    pd.DataFrame(rows).to_csv(tmp_path / "alert_top_flags.csv", index=False)
    at = _run("monday_alert", tmp_path, force_live=True)
    _assert_clean(at)
    assert not at.warning
    shown = next(frame.value for frame in at.dataframe if "resource_id" in frame.value.columns)
    assert shown["resource_id"].tolist() == ["r3", "r2", "r1"]
