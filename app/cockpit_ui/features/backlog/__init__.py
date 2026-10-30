from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="backlog",
    title="What to fix first",
    icon=":material/priority_high:",
    owner="Nikhil",
    plan_refs=("F7",),
    plan_id="F7",
    group="Waste",
    status="preview",
    order=20,
    requires=("optimization_candidates",),
    description="The ranked list of compute to fix, highest priority score first.",
    render=render,
)
