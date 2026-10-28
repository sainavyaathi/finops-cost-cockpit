from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="reliability",
    title="Job reliability",
    icon=":material/sync_problem:",
    owner="Nikhil",
    plan_refs=("F6",),
    order=40,
    requires=("job_reliability_summary",),
    description="Failure rate, retries and the cost of failed job runs.",
    render=render,
)
