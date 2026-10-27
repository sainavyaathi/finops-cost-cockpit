from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="backlog",
    title="What to fix first",
    icon=":material/priority_high:",
    owner="Nikhil",
    plan_refs=("F7",),
    order=20,
    requires=("optimization_candidates",),
    description="Ranked optimization backlog. Priority = 45% cost + 25% waste + 15% reliability + 10% tagging + 5% frequency.",
    render=render,
)
