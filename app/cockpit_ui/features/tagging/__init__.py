from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="tagging",
    title="Tagging & attribution",
    icon=":material/sell:",
    owner="Navya",
    plan_refs=("F2",),
    order=50,
    requires=("vw_tagging_quality", "vw_attribution_quality"),
    description="How much spend can be attributed to a team, project and owner.",
    render=render,
)
