from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="overview",
    title="Cost overview",
    icon=":material/dashboard:",
    owner="Navya",
    plan_refs=("F1", "F8"),
    order=10,
    requires=("vw_executive_summary", "vw_cost_trend_daily", "workload_cost_summary"),
    description="Where is the money going, and who owns it?",
    render=render,
)
