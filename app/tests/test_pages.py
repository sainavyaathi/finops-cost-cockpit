"""Render every page headlessly against the fixture CSVs (no credentials needed)."""

from __future__ import annotations

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from cockpit_ui.core.registry import discover

APP_DIR = Path(__file__).resolve().parents[1]

SINGLE_FEATURE_SCRIPT = """
import sys
from pathlib import Path
sys.path.insert(0, {app_dir!r})
from cockpit_ui.core.context import AppContext
from cockpit_ui.core.datasource import LocalCsvSource
from cockpit_ui.core.registry import discover
from cockpit_ui.core.settings import Settings
from cockpit_ui.core.shell import render_feature

feature = next(f for f in discover().features if f.id == {feature_id!r})
settings = Settings(local_dir=Path({data_dir!r}))
render_feature(feature, AppContext(source=LocalCsvSource(settings.local_dir), settings=settings))
"""


def _run_feature(feature_id: str, data_dir: Path) -> AppTest:
    script = SINGLE_FEATURE_SCRIPT.format(app_dir=str(APP_DIR), feature_id=feature_id, data_dir=str(data_dir))
    return AppTest.from_string(script, default_timeout=30).run()


@pytest.mark.parametrize("feature", discover().features, ids=lambda f: f.id)
def test_each_feature_renders_without_errors(feature, fixtures_dir: Path):
    at = _run_feature(feature.id, fixtures_dir)
    assert not at.exception
    assert not at.error, [e.value for e in at.error]
    assert at.title[0].value == feature.title


@pytest.mark.parametrize("feature", discover().features, ids=lambda f: f.id)
def test_each_feature_degrades_when_tables_are_missing(feature, tmp_path: Path):
    at = _run_feature(feature.id, tmp_path)
    assert not at.exception
    assert not at.error
    if feature.is_planned:  # planned pages show their roadmap panel instead of table gating
        assert not at.info
    elif feature.requires:
        assert any("needs tables" in info.value for info in at.info)


def test_full_app_opens_on_first_enabled_page(local_env):
    at = AppTest.from_file(str(APP_DIR / "streamlit_app.py"), default_timeout=30).run()
    assert not at.exception
    assert not at.error
    assert at.title[0].value == "Cost overview"


def test_disabling_a_feature_in_yaml_removes_its_page(local_env, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    toggles = tmp_path / "features.yaml"
    toggles.write_text("features:\n  overview: {enabled: false}\n", encoding="utf-8")
    monkeypatch.setenv("COCKPIT_FEATURES_FILE", str(toggles))

    at = AppTest.from_file(str(APP_DIR / "streamlit_app.py"), default_timeout=30).run()
    assert not at.exception
    assert at.title[0].value == "What to fix first"


# ---------------------------------------------------------------- overview week-over-week helper

def test_week_over_week_compares_last_seven_days_with_prior_seven():
    import pandas as pd

    from cockpit_ui.features.overview.page import week_over_week

    days = pd.date_range("2026-10-01", periods=21, freq="D")
    trend = pd.DataFrame({"usage_date": days.strftime("%Y-%m-%d"), "estimated_cost": [1.0] * 14 + [2.0] * 7})
    assert week_over_week(trend) == (14.0, 7.0)
    assert week_over_week(trend.head(10)) is None  # under 14 days: no baseline
    assert week_over_week(trend.iloc[0:0]) is None


# ---------------------------------------------------------------- data-source errors in the shell

SOURCE_ERROR_SCRIPT = """
import sys
from pathlib import Path
sys.path.insert(0, {app_dir!r})
from cockpit_ui.core.context import AppContext
from cockpit_ui.core.datasource import DataSourceError, LocalCsvSource
from cockpit_ui.core.feature import Feature
from cockpit_ui.core.settings import Settings
from cockpit_ui.core.shell import render_feature

ERROR = DataSourceError("Permission denied on gold.x.", category="permission",
                        hint="Grant SELECT to the app identity.", detail="PERMISSION_DENIED: nope")

class Failing(LocalCsvSource):
    def available_tables(self):
        if {fail_listing!r}:
            raise ERROR
        return super().available_tables()

def render(ctx):
    raise ERROR

settings = Settings(local_dir=Path({data_dir!r}))
feature = Feature(id="probe", title="Probe", render=render, requires=("vw_executive_summary",))
render_feature(feature, AppContext(source=Failing(settings.local_dir), settings=settings))
"""


@pytest.mark.parametrize("fail_listing", [True, False], ids=["listing", "render"])
def test_data_source_errors_show_message_hint_and_detail(fail_listing: bool, fixtures_dir: Path):
    script = SOURCE_ERROR_SCRIPT.format(app_dir=str(APP_DIR), data_dir=str(fixtures_dir), fail_listing=fail_listing)
    at = AppTest.from_string(script, default_timeout=30).run()
    assert not at.exception
    assert [e.value for e in at.error] == ["Permission denied on gold.x."]
    assert any("Grant SELECT" in c.value for c in at.caption)
    assert any("PERMISSION_DENIED" in c.value for c in at.code)


# ---------------------------------------------------------------- pages without tables survive a down source

DOWN_SOURCE_SCRIPT = """
import shutil, sys
from pathlib import Path
sys.path.insert(0, {app_dir!r})
from cockpit_ui.core.context import AppContext
from cockpit_ui.core.datasource import DataSourceError, LocalCsvSource
from cockpit_ui.core.registry import discover
from cockpit_ui.core.settings import Settings
from cockpit_ui.core.shell import render_feature

def boom(*args, **kwargs):
    raise DataSourceError("Source is down ({category}).", category={category!r}, hint="Try later.", detail="driver says no")

class Down(LocalCsvSource):
    available_tables = boom
    table = boom
    version = boom

feature = next(f for f in discover().features if f.id == {feature_id!r})
settings = Settings(local_dir=Path({data_dir!r}))
render_feature(feature, AppContext(source=Down(settings.local_dir), settings=settings))
"""


@pytest.mark.parametrize("category", ["auth", "connection"])
@pytest.mark.parametrize("feature_id", ["connection", "roadmap"])
def test_table_free_pages_render_when_the_source_is_down(feature_id: str, category: str, tmp_path: Path,
                                                         fixtures_dir: Path):
    import shutil

    data_dir = tmp_path / "data"  # unique per test: cached versions are keyed by directory
    shutil.copytree(fixtures_dir, data_dir)
    script = DOWN_SOURCE_SCRIPT.format(app_dir=str(APP_DIR), data_dir=str(data_dir), feature_id=feature_id,
                                       category=category)
    at = AppTest.from_string(script, default_timeout=30).run()
    assert not at.exception, [e.value for e in at.exception]
    assert not at.error, [e.value for e in at.error]
    assert at.title[0].value == next(f.title for f in discover().features if f.id == feature_id)
    if feature_id == "connection":
        text = "\n".join(m.value for m in at.markdown)
        assert "Read tables" in text and "Source is down" in text  # the failure is the diagnosis
        assert "Gold-table coverage" in text


def test_gated_page_shows_source_error_when_down(tmp_path: Path, fixtures_dir: Path):
    import shutil

    data_dir = tmp_path / "data"
    shutil.copytree(fixtures_dir, data_dir)
    script = DOWN_SOURCE_SCRIPT.format(app_dir=str(APP_DIR), data_dir=str(data_dir), feature_id="overview",
                                       category="auth")
    at = AppTest.from_string(script, default_timeout=30).run()
    assert not at.exception
    assert [e.value for e in at.error] == ["Source is down (auth)."]


# ---------------------------------------------------------------- page crashes: short message, traceback only in debug

CRASH_SCRIPT = """
import os, sys
from pathlib import Path
sys.path.insert(0, {app_dir!r})
if {debug!r}:
    os.environ["COCKPIT_DEBUG"] = "1"
else:
    os.environ.pop("COCKPIT_DEBUG", None)
from cockpit_ui.core.context import AppContext
from cockpit_ui.core.datasource import LocalCsvSource
from cockpit_ui.core.feature import Feature
from cockpit_ui.core.settings import Settings
from cockpit_ui.core.shell import render_feature

def render(ctx):
    raise RuntimeError("boom\\nSELECT secret FROM driver internals")

settings = Settings(local_dir=Path({data_dir!r}))
feature = Feature(id="crash", title="Crash", render=render, requires=("vw_executive_summary",))
render_feature(feature, AppContext(source=LocalCsvSource(settings.local_dir), settings=settings))
"""


@pytest.mark.parametrize("debug", [False, True], ids=["prod", "debug"])
def test_page_crash_hides_traceback_unless_debug(debug: bool, fixtures_dir: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("COCKPIT_DEBUG", raising=False)
    script = CRASH_SCRIPT.format(app_dir=str(APP_DIR), data_dir=str(fixtures_dir), debug=debug)
    at = AppTest.from_string(script, default_timeout=30).run()
    errors = [e.value for e in at.error]
    assert len(errors) == 1 and "RuntimeError: boom" in errors[0]
    assert "driver internals" not in errors[0]
    assert bool(at.exception) is debug  # st.exception (the traceback) only with COCKPIT_DEBUG


# ---------------------------------------------------------------- refresh is scoped to this source

def test_refresh_button_reruns_without_flushing_the_global_cache(local_env, monkeypatch: pytest.MonkeyPatch):
    import streamlit as st

    def forbidden() -> None:
        raise AssertionError("Refresh must not clear every user's cache")

    monkeypatch.setattr(st.cache_data, "clear", forbidden)
    at = AppTest.from_file(str(APP_DIR / "streamlit_app.py"), default_timeout=30).run()
    refresh = next(b for b in at.button if b.label == "Refresh data")
    at = refresh.click().run()
    assert not at.exception, [e.value for e in at.exception]
    assert at.title[0].value == "Cost overview"
