from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="overview",
    title="Cost overview",
    icon=":material/dashboard:",
    owner="Navya",
    plan_refs=("F1", "F8"),
    plan_id="F8",
    group="Overview",
    status="live",
    order=10,
    requires=("vw_executive_summary", "vw_cost_trend_daily", "workload_cost_summary"),
    description="Where the money goes, how it moved this week, and which teams own it.",
    render=render,
)
