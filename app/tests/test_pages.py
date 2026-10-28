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
