from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="roadmap",
    title="Build progress",
    icon=":material/checklist:",
    owner="Navya & Nikhil",
    plan_refs=("F9",),
    plan_id="F9",
    order=94,
    group="Platform",
    status="live",
    requires=(),
    description="The twelve POC features and how far each has got. Pages and tables appear here as they ship.",
    render=render,
)
