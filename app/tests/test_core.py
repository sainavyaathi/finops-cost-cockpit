from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

from cockpit_ui.core.datasource import LocalCsvSource, build_source
from cockpit_ui.core.feature import Feature
from cockpit_ui.core.registry import discover, enabled_features, load_toggles, missing_tables
from cockpit_ui.core.settings import Settings

EXPECTED_FEATURES = {"overview", "backlog", "utilization", "reliability", "tagging", "health"}


def _noop(ctx) -> None:
    return None


# ---------------------------------------------------------------- registry

def test_discover_finds_every_feature_package():
    discovery = discover()
    assert discovery.errors == {}
    assert {f.id for f in discovery.features} == EXPECTED_FEATURES


def test_toggles_disable_and_reorder(tmp_path: Path):
    features = [Feature(id="a", title="A", render=_noop, order=1), Feature(id="b", title="B", render=_noop, order=2)]
    path = tmp_path / "features.yaml"
    path.write_text("features:\n  a: {enabled: true, order: 5}\n  b: {enabled: false}\n", encoding="utf-8")
    assert [f.id for f in enabled_features(features, load_toggles(path))] == ["a"]

    path.write_text("features:\n  a: {order: 9}\n", encoding="utf-8")
    assert [f.id for f in enabled_features(features, load_toggles(path))] == ["b", "a"]


def test_missing_toggle_file_enables_everything(tmp_path: Path):
    features = [Feature(id="a", title="A", render=_noop)]
    assert enabled_features(features, load_toggles(tmp_path / "nope.yaml")) == features


def test_broken_and_duplicate_plugins_are_isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    pkg = tmp_path / "fake_features"
    for name, body in {
        "good": "FEATURE = Feature(id='good', title='Good', render=lambda ctx: None)",
        "twin": "FEATURE = Feature(id='good', title='Twin', render=lambda ctx: None)",
        "broken": "raise RuntimeError('boom')",
        "empty": "",
    }.items():
        (pkg / name).mkdir(parents=True)
        header = "from cockpit_ui.core import Feature\n" if "Feature" in body else ""
        (pkg / name / "__init__.py").write_text(header + body, encoding="utf-8")
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))

    discovery = discover("fake_features")
    assert [f.id for f in discovery.features] == ["good"]
    assert set(discovery.errors) == {"fake_features.twin", "fake_features.broken", "fake_features.empty"}
    assert "Duplicate" in discovery.errors["fake_features.twin"]
    assert "boom" in discovery.errors["fake_features.broken"]
    sys.modules.pop("fake_features", None)


def test_missing_tables_is_case_insensitive():
    feature = Feature(id="x", title="X", render=_noop, requires=("Daily_Cost", "vw_a"))
    assert missing_tables(feature, {"daily_cost"}) == ["vw_a"]


def test_feature_id_must_be_url_safe():
    with pytest.raises(ValueError):
        Feature(id="Cost Overview", title="X", render=_noop)


def test_feature_requirements_are_produced_by_the_engine():
    from databricks_finops.config import OUTPUT_TABLES, OUTPUT_VIEWS

    produced = {name.lower() for name in [*OUTPUT_TABLES, *OUTPUT_VIEWS]}
    for feature in discover().features:
        assert set(feature.requires) <= produced, f"{feature.id} requires tables the engine never writes"


# ---------------------------------------------------------------- settings

def test_settings_defaults_to_local():
    settings = Settings.from_env({})
    assert settings.source == "local"
    assert isinstance(build_source(settings), LocalCsvSource)


def test_settings_rejects_bad_source_and_missing_warehouse():
    with pytest.raises(ValueError):
        Settings.from_env({"COCKPIT_SOURCE": "excel"})
    with pytest.raises(ValueError):
        Settings.from_env({"COCKPIT_SOURCE": "databricks"})
    assert Settings.from_env({"COCKPIT_SOURCE": "databricks", "DATABRICKS_WAREHOUSE_ID": "abc"}).warehouse_id == "abc"


# ---------------------------------------------------------------- data source

def test_local_source_reads_fixtures(fixtures_dir: Path):
    source = LocalCsvSource(fixtures_dir)
    assert "vw_executive_summary" in source.available_tables()
    assert len(source.table("vw_executive_summary")) == 1
    assert source.version() != "0"


def test_local_source_rejects_unsafe_names(fixtures_dir: Path):
    with pytest.raises(ValueError):
        LocalCsvSource(fixtures_dir).table("../secrets")


def test_local_source_handles_missing_directory(tmp_path: Path):
    source = LocalCsvSource(tmp_path / "missing")
    assert source.available_tables() == set()
    assert source.version() == "0"


def test_app_yaml_targets_databricks():
    import yaml

    app_yaml = yaml.safe_load((Path(__file__).resolve().parents[1] / "app.yaml").read_text(encoding="utf-8"))
    env = {item["name"]: item for item in app_yaml["env"]}
    assert env["COCKPIT_SOURCE"]["value"] == "databricks"
    assert env["DATABRICKS_WAREHOUSE_ID"]["valueFrom"] == "sql-warehouse"
    assert textwrap.dedent(" ".join(app_yaml["command"])) == "streamlit run streamlit_app.py"
