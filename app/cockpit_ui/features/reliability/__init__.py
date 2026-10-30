from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="reliability",
    title="Job reliability",
    icon=":material/sync_problem:",
    owner="Nikhil",
    plan_refs=("F6",),
    plan_id="F6",
    group="Operations",
    status="live",
    order=40,
    requires=("job_reliability_summary",),
    description="Which jobs fail most often and what their failed runs cost.",
    render=render,
)
