from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="idle_compute",
    title="Idle and zombie compute",
    icon=":material/bedtime:",
    owner="Nikhil",
    plan_refs=("F5",),
    plan_id="F5",
    order=24,
    group="Waste",
    status="planned",
    requires=("idle_compute",),
    description="Clusters that stay up with nothing to do. Fix auto-termination first, then right-size.",
    highlights=(
        "Clusters with auto-termination off or set above 60 minutes",
        "Low utilisation while running",
        "Cost of idle hours per cluster, with the owner",
    ),
    render=render,
)
