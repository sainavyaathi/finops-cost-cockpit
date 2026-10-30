from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="connection",
    title="Connection",
    icon=":material/cable:",
    owner="Navya & Nikhil",
    plan_refs=("F9",),
    plan_id="F9",
    order=92,
    group="Platform",
    status="live",
    requires=(),
    description="Is the cockpit reading the right data, as the right identity? Fix anything red, top to bottom.",
    render=render,
)
