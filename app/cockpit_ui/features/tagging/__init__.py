from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="tagging",
    title="Tagging & attribution",
    icon=":material/sell:",
    owner="Navya",
    plan_refs=("F2",),
    plan_id="F2",
    group="Attribution",
    status="live",
    order=50,
    requires=("vw_tagging_quality", "vw_attribution_quality"),
    description="How much spend has a clear owner, and which teams leave the rest untagged.",
    render=render,
)
