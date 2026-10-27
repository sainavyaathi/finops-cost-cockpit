from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="utilization",
    title="Compute utilization",
    icon=":material/memory:",
    owner="Nikhil",
    plan_refs=("F5",),
    order=30,
    requires=("vw_compute_sizing_signals",),
    description="Low CPU alone is not waste: memory is checked before suggesting smaller compute.",
    render=render,
)
