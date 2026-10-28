from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="health",
    title="Pipeline health",
    icon=":material/health_and_safety:",
    owner="Navya & Nikhil",
    plan_refs=("F11",),
    order=90,
    requires=("accelerator_health", "accelerator_run_log"),
    description="Data coverage checks and step results from the latest pipeline run.",
    render=render,
)
