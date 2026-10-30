"""Pure helpers of the design system, the Feature contract, and the shell's planned-feature path."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from cockpit_ui.core import theme
from cockpit_ui.core.components import Kpi, as_kpi, delta_text, money, number, relative_time, status_badge, status_chip
from cockpit_ui.core.feature import Feature
from cockpit_ui.core.registry import group_features
from cockpit_ui.core.shell import describe_source

APP_DIR = Path(__file__).resolve().parents[1]


def _noop(ctx) -> None:
    return None


# ---------------------------------------------------------------- formatting

@pytest.mark.parametrize(("current", "previous", "expected"), [
    (110, 100, "+10.0%"),
    (90, 100, "-10.0%"),
    (100, 100, "0.0%"),
    (100.0004, 100, "0.0%"),
    (-50, -100, "+50.0%"),
    (5, 0, None),
    (None, 100, None),
    (100, float("nan"), None),
])
def test_delta_text(current, previous, expected):
    assert delta_text(current, previous) == expected


def test_money_and_number_formatting():
    assert money(1234.5) == "$1,234"
    assert money(1234.5, 2) == "$1,234.50"
    assert money(-42) == "-$42"
    assert money(15300, compact=True) == "$15.3k"
    assert money(0.67, 2, compact=True) == "$0.67"
    assert money(None) == money(float("nan")) == "-"
    assert number(2_500_000, compact=True) == "2.5M"
    assert number(12) == "12"


def test_relative_time():
    now = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
    assert relative_time(now - timedelta(seconds=10), now) == "just now"
    assert relative_time(now + timedelta(hours=3), now) == "just now"  # clock skew is not "in the future"
    assert relative_time(now - timedelta(minutes=14), now) == "14m ago"
    assert relative_time(now - timedelta(hours=3, minutes=59), now) == "3h ago"
    assert relative_time(datetime(2026, 10, 27, 12, 0), now) == "2d ago"  # naive means UTC


# ---------------------------------------------------------------- chips and KPIs

def test_status_chip_escapes_and_validates_tone():
    assert "&lt;b&gt;" in status_chip("<b>", "good")
    assert "ck-chip--bad" in status_chip("x", "bad")
    with pytest.raises(ValueError):
        status_chip("x", "purple")


def test_status_badge_covers_every_status():
    assert "Planned" in status_badge("planned")
    assert "ck-chip--good" in status_badge("live")
    with pytest.raises(ValueError):
        status_badge("retired")


def test_kpi_accepts_tuples_and_validates_tone():
    assert as_kpi(("Cost", "$1")) == Kpi("Cost", "$1")
    assert as_kpi(("Cost", "$1", "+5%")).delta == "+5%"
    with pytest.raises(ValueError):
        Kpi("Cost", "$1", tone="loud")


def test_theme_css_and_templates():
    theme.register_plotly_templates()
    import plotly.io as pio

    assert {theme.TEMPLATE_LIGHT, theme.TEMPLATE_DARK} <= set(pio.templates)
    assert theme.LIGHT.accent in theme.css("light")
    assert theme.DARK.ink in theme.css("dark")
    assert theme.tone_color("bad", "light") == theme.LIGHT.bad


# ---------------------------------------------------------------- Feature contract

def test_feature_defaults_are_backwards_compatible():
    feature = Feature(id="a", title="A", render=_noop, plan_refs=("F1", "F8"))
    assert (feature.status, feature.group, feature.plan_id) == ("live", "Insights", "")
    assert feature.plan_label == "F1 · F8"
    assert Feature(id="b", title="B", render=_noop, plan_id="F3", plan_refs=("F3",)).plan_label == "F3"


def test_feature_rejects_unknown_status_and_empty_group():
    with pytest.raises(ValueError):
        Feature(id="a", title="A", render=_noop, status="beta")
    with pytest.raises(ValueError):
        Feature(id="a", title="A", render=_noop, group=" ")


def test_group_features_orders_groups_by_first_member():
    features = [
        Feature(id="health", title="Health", render=_noop, order=90, group="Platform"),
        Feature(id="overview", title="Overview", render=_noop, order=10, group="Overview"),
        Feature(id="idle", title="Idle", render=_noop, order=30, group="Waste"),
        Feature(id="backlog", title="Backlog", render=_noop, order=20, group="Waste"),
    ]
    grouped = group_features(features)
    assert list(grouped) == ["Overview", "Waste", "Platform"]
    assert [f.id for f in grouped["Waste"]] == ["backlog", "idle"]


# ---------------------------------------------------------------- shell

def test_describe_source_falls_back_to_label():
    class Bare:
        label = "Databricks (workspace.finops)"

    class Broken:
        label = "Local CSV (x)"

        def describe(self):
            raise RuntimeError("no")

    assert describe_source(Bare())["kind"] == "databricks"
    assert describe_source(Broken()) == {"kind": "local"}


PLANNED_SCRIPT = """
import sys
from pathlib import Path
sys.path.insert(0, {app_dir!r})
from cockpit_ui.core.context import AppContext
from cockpit_ui.core.datasource import LocalCsvSource
from cockpit_ui.core.feature import Feature
from cockpit_ui.core.settings import Settings
from cockpit_ui.core.shell import render_feature

def explode(ctx):
    raise AssertionError("planned features must not render")

feature = Feature(id="catalog_split", title="Catalog allocation", render=explode, status="planned",
                  plan_id="F3", owner="Navya", requires=("cockpit_catalog_allocation",),
                  highlights=("Warehouse cost split by catalog",))
settings = Settings(local_dir=Path({data_dir!r}))
render_feature(feature, AppContext(source=LocalCsvSource(settings.local_dir), settings=settings))
"""


def test_planned_feature_shows_panel_without_table_gating(tmp_path: Path):
    script = PLANNED_SCRIPT.format(app_dir=str(APP_DIR), data_dir=str(tmp_path))
    at = AppTest.from_string(script, default_timeout=30).run()
    assert not at.exception
    assert not at.error
    assert at.title[0].value == "Catalog allocation"
    assert not any("needs tables" in info.value for info in at.info)
    markdown = " ".join(m.value for m in at.markdown)
    assert "cockpit_catalog_allocation" in markdown
    assert "Warehouse cost split by catalog" in markdown


def test_header_shows_pipeline_freshness(fixtures_dir: Path):
    script = PLANNED_SCRIPT.replace('status="planned",', "").replace(
        'raise AssertionError("planned features must not render")', "return None"
    ).replace('requires=("cockpit_catalog_allocation",)', 'requires=("accelerator_run_log",)')
    at = AppTest.from_string(script.format(app_dir=str(APP_DIR), data_dir=str(fixtures_dir)), default_timeout=30).run()
    assert not at.exception
    assert any("Data as of" in m.value for m in at.markdown)
