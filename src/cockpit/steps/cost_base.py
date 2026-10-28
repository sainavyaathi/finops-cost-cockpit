"""F1 cost base: publish the engine's daily_cost fact table as cockpit.cost_base (rule R4).

The upstream engine keeps writing daily_cost (rule R3); cost_base is the name the cockpit's own steps
and pages build on. It is a view, so it always matches the latest run and stores no second copy.
"""

from cockpit.pipeline import Step, StepContext


def build(ctx: StepContext) -> str:
    ctx.spark.sql(ctx.sql("cost_base", target_view=ctx.table("cost_base"),
                          daily_cost_table=ctx.table("daily_cost")))
    return "cost_base view published over daily_cost"


STEP = Step(name="build_cost_base", run=build, output_table="cost_base")
