from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="health",
    title="Pipeline health",
    icon=":material/health_and_safety:",
    owner="Navya & Nikhil",
    plan_refs=("F11",),
    plan_id="F11",
    group="Platform",
    status="live",
    order=90,
    requires=("accelerator_health", "accelerator_run_log"),
    description="Whether the latest pipeline run read every source and can be trusted.",
    render=render,
)
