"""Plug-in step runner that extends the upstream FinOps pipeline.

Add a feature step by creating ``src/cockpit/steps/<name>.py`` with::

    from cockpit.pipeline import Step, StepContext

    def build(ctx: StepContext) -> str:
        ctx.spark.sql(ctx.sql("waste_flags", target=ctx.table("waste_flags")))
        return "waste_flags built"

    STEP = Step(name="build_waste_flags", run=build, output_table="waste_flags",
                depends_on=("build_zombie_clusters",))

SQL templates live in ``src/cockpit/sql/<name>.sql`` and use ``{placeholders}`` like the upstream engine.
"""

from __future__ import annotations

import importlib
import pkgutil
from dataclasses import dataclass
from importlib import resources
from typing import Any, Callable

from databricks_finops.config import AppConfig, config_from_args, parse_args
from databricks_finops.main import _run_id, run_pipeline
from databricks_finops.preflight import PreflightResult
from databricks_finops.run_logging import StepLogger, write_run_log
from databricks_finops.spark_utils import qname

STEPS_PACKAGE = "cockpit.steps"


@dataclass(frozen=True)
class StepContext:
    spark: Any
    config: AppConfig
    run_id: str
    preflight: PreflightResult

    def table(self, name: str) -> str:
        return qname(self.config.catalog, self.config.schema, name)

    @staticmethod
    def sql(name: str, **params: str) -> str:
        relative = name if name.endswith(".sql") else f"{name}.sql"
        return resources.files("cockpit").joinpath("sql", relative).read_text(encoding="utf-8").format_map(params)


@dataclass(frozen=True)
class Step:
    name: str
    run: Callable[[StepContext], str | None]
    output_table: str | None = None
    depends_on: tuple[str, ...] = ()
    required: bool = True


def discover_steps(package: str = STEPS_PACKAGE) -> list[Step]:
    root = importlib.import_module(package)
    steps: dict[str, Step] = {}
    for info in sorted(pkgutil.iter_modules(root.__path__), key=lambda m: m.name):
        if info.name.startswith("_"):
            continue
        step = getattr(importlib.import_module(f"{package}.{info.name}"), "STEP", None)
        if not isinstance(step, Step):
            raise TypeError(f"{package}.{info.name} must define STEP = Step(...)")
        if step.name in steps:
            raise ValueError(f"Duplicate step name {step.name!r}")
        steps[step.name] = step
    return order_steps(list(steps.values()))


def order_steps(steps: list[Step]) -> list[Step]:
    """Topological order by ``depends_on`` (stable by name); rejects unknown deps and cycles."""

    by_name = {s.name: s for s in steps}
    for step in steps:
        unknown = set(step.depends_on) - set(by_name)
        if unknown:
            raise ValueError(f"Step {step.name!r} depends on unknown steps: {sorted(unknown)}")

    ordered: list[Step] = []
    state: dict[str, str] = {}

    def visit(step: Step) -> None:
        if state.get(step.name) == "done":
            return
        if state.get(step.name) == "visiting":
            raise ValueError(f"Dependency cycle at step {step.name!r}")
        state[step.name] = "visiting"
        for dep in sorted(step.depends_on):
            visit(by_name[dep])
        state[step.name] = "done"
        ordered.append(step)

    for step in sorted(steps, key=lambda s: s.name):
        visit(step)
    return ordered


def run_cockpit_pipeline(
    spark: Any,
    config: AppConfig,
    run_id: str,
    logger: StepLogger,
    steps: list[Step] | None = None,
) -> PreflightResult:
    """Run the upstream pipeline, then every cockpit step, all logged to the same run log."""

    preflight = run_pipeline(spark, config, run_id, logger)
    ctx = StepContext(spark=spark, config=config, run_id=run_id, preflight=preflight)
    for step in discover_steps() if steps is None else order_steps(steps):
        logger.run(
            step.name,
            lambda step=step: step.run(ctx),
            spark=spark if step.output_table else None,
            count_table=ctx.table(step.output_table) if step.output_table else None,
            required=step.required,
        )
    return preflight


def main(argv: list[str] | None = None) -> None:
    from pyspark.sql import SparkSession

    config = config_from_args(parse_args(argv))
    run_id = _run_id()
    logger = StepLogger(run_id)
    spark = SparkSession.builder.getOrCreate()
    try:
        run_cockpit_pipeline(spark, config, run_id, logger)
    finally:
        write_run_log(spark, config.catalog, config.schema, logger)


if __name__ == "__main__":
    main()
