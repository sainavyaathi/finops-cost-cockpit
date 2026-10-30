from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="utilization",
    title="Compute utilization",
    icon=":material/memory:",
    owner="Nikhil",
    plan_refs=("F5",),
    plan_id="F5",
    group="Waste",
    status="live",
    order=30,
    requires=("vw_compute_sizing_signals",),
    description="Which clusters and jobs can run on smaller compute, with memory checked before CPU.",
    render=render,
)
