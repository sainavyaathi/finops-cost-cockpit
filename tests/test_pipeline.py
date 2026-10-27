from __future__ import annotations

import pytest

from databricks_finops import main as pipeline
from databricks_finops.config import AppConfig
from databricks_finops.preflight import PreflightResult, TableCapability
from databricks_finops.run_logging import StepLogger


TEST_CONFIG = AppConfig(
    catalog="finops",
    schema="accelerator",
    lookback_days=30,
    currency_code="USD",
    display_currency="USD",
    fallback_dbu_price=0.55,
    bundle_root=".",
)


class MockRow:
    def __getitem__(self, index: int) -> int:
        return 0


class MockResult:
    def collect(self) -> list[MockRow]:
        return [MockRow()]


class MockSpark:
    def __init__(self) -> None:
        self.sql_calls: list[str] = []

    def sql(self, query: str) -> MockResult:
        self.sql_calls.append(query)
        return MockResult()


def preflight_result() -> PreflightResult:
    return PreflightResult(
        capabilities=[
            TableCapability(
                "system.billing.usage",
                "billing_usage_available",
                True,
                True,
                "available",
                "all_outputs",
            ),
        ],
        columns_by_table={},
    )


def patch_successful_pipeline(monkeypatch: pytest.MonkeyPatch, order: list[str]) -> None:
    monkeypatch.setattr(
        pipeline,
        "create_namespace",
        lambda spark, catalog, schema: order.append("create_catalog_schema") or "namespace",
    )
    monkeypatch.setattr(
        pipeline,
        "run_preflight",
        lambda spark: order.append("preflight") or preflight_result(),
    )
    monkeypatch.setattr(
        pipeline,
        "create_daily_cost",
        lambda spark, config, run_id, preflight: order.append("daily_cost") or "daily",
    )
    monkeypatch.setattr(
        pipeline,
        "create_workload_cost_summary",
        lambda spark, config, run_id: order.append("workload") or "workload",
    )
    monkeypatch.setattr(
        pipeline,
        "create_compute_utilization_summary",
        lambda spark, config, run_id, preflight: order.append("utilization") or "utilization",
    )
    monkeypatch.setattr(
        pipeline,
        "create_job_reliability_summary",
        lambda spark, config, run_id, preflight: order.append("reliability") or "reliability",
    )
    monkeypatch.setattr(
        pipeline,
        "create_tagging_quality_summary",
        lambda spark, config, run_id: order.append("tagging") or "tagging",
    )
    monkeypatch.setattr(
        pipeline,
        "create_optimization_candidates",
        lambda spark, config, run_id: order.append("optimization") or "optimization",
    )
    monkeypatch.setattr(
        pipeline,
        "create_accelerator_summary",
        lambda spark, config, run_id: order.append("summary") or "summary",
    )
    monkeypatch.setattr(
        pipeline,
        "write_health",
        lambda spark, catalog, schema, run_id, preflight, logger=None: order.append("health"),
    )
    monkeypatch.setattr(
        pipeline,
        "create_views",
        lambda spark, config: order.append("views") or "views",
    )
    monkeypatch.setattr(
        pipeline,
        "_comment_tables",
        lambda spark, config: order.append("comments") or "comments",
    )


def test_run_pipeline_orders_namespace_preflight_and_table_steps(monkeypatch: pytest.MonkeyPatch):
    order: list[str] = []
    patch_successful_pipeline(monkeypatch, order)

    result = pipeline.run_pipeline(MockSpark(), TEST_CONFIG, "run-1", StepLogger("run-1"))

    assert result == preflight_result()
    assert order[0] == "create_catalog_schema"
    assert order.index("preflight") < order.index("daily_cost")
    assert order.index("daily_cost") < order.index("workload")


def test_run_pipeline_writes_health_before_reraising_required_failure(
    monkeypatch: pytest.MonkeyPatch,
):
    order: list[str] = []
    patch_successful_pipeline(monkeypatch, order)

    def fail_daily_cost(spark, config, run_id, preflight):
        order.append("daily_cost")
        raise RuntimeError("required build failed")

    monkeypatch.setattr(pipeline, "create_daily_cost", fail_daily_cost)

    with pytest.raises(RuntimeError):
        pipeline.run_pipeline(MockSpark(), TEST_CONFIG, "run-1", StepLogger("run-1"))

    assert order[-1] == "health"
    assert order.index("health") > order.index("daily_cost")


def test_run_pipeline_degrades_optional_comment_failure(monkeypatch: pytest.MonkeyPatch):
    order: list[str] = []
    patch_successful_pipeline(monkeypatch, order)

    def fail_comments(spark, config):
        order.append("comments")
        raise RuntimeError("comment permissions")

    monkeypatch.setattr(pipeline, "_comment_tables", fail_comments)
    logger = StepLogger("run-1")

    pipeline.run_pipeline(MockSpark(), TEST_CONFIG, "run-1", logger)

    assert order[-1] == "comments"
    assert logger.records[-1].task_name == "apply_table_comments"
    assert logger.records[-1].result == "DEGRADED"
