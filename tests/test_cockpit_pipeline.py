from __future__ import annotations

import pytest

from cockpit import pipeline
from cockpit.pipeline import Step, discover_steps, order_steps, run_cockpit_pipeline
from databricks_finops.config import AppConfig
from databricks_finops.run_logging import StepLogger

CONFIG = AppConfig(catalog="finops", schema="cockpit", lookback_days=30, currency_code="USD",
                   display_currency="USD", fallback_dbu_price=0.55, bundle_root=".")


def _step(name: str, *deps: str, required: bool = True, calls: list | None = None) -> Step:
    def run(ctx):
        if calls is not None:
            calls.append(name)
        if name.startswith("bad"):
            raise RuntimeError("boom")
        return f"{name} ok"

    return Step(name=name, run=run, depends_on=deps, required=required)


def test_steps_package_starts_empty():
    assert discover_steps() == []


def test_order_steps_respects_dependencies():
    steps = [_step("c", "b"), _step("a"), _step("b", "a")]
    assert [s.name for s in order_steps(steps)] == ["a", "b", "c"]


def test_order_steps_rejects_unknown_dependency_and_cycles():
    with pytest.raises(ValueError, match="unknown"):
        order_steps([_step("a", "missing")])
    with pytest.raises(ValueError, match="cycle"):
        order_steps([_step("a", "b"), _step("b", "a")])


def test_cockpit_steps_run_after_upstream_and_share_the_run_log(monkeypatch: pytest.MonkeyPatch):
    calls: list[str] = []

    def fake_upstream(spark, config, run_id, logger):
        calls.append("upstream")
        return "preflight"

    monkeypatch.setattr(pipeline, "run_pipeline", fake_upstream)
    logger = StepLogger("run-1")
    run_cockpit_pipeline(object(), CONFIG, "run-1", logger,
                         steps=[_step("second", "first", calls=calls), _step("first", calls=calls)])

    assert calls == ["upstream", "first", "second"]
    assert [(r.task_name, r.result) for r in logger.records] == [("first", "SUCCESS"), ("second", "SUCCESS")]


def test_optional_step_failure_degrades_but_required_failure_raises(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pipeline, "run_pipeline", lambda *args: "preflight")

    logger = StepLogger("run-1")
    run_cockpit_pipeline(object(), CONFIG, "run-1", logger, steps=[_step("bad_optional", required=False)])
    assert logger.records[-1].result == "DEGRADED"

    with pytest.raises(RuntimeError):
        run_cockpit_pipeline(object(), CONFIG, "run-1", StepLogger("run-1"), steps=[_step("bad_required")])


def test_step_context_names_tables_in_the_configured_schema():
    ctx = pipeline.StepContext(spark=None, config=CONFIG, run_id="r", preflight=None)
    assert ctx.table("waste_flags") == "`finops`.`cockpit`.`waste_flags`"
