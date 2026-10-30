from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="monday_alert",
    title="Monday alert preview",
    icon=":material/notifications:",
    owner="Navya",
    plan_refs=("F10",),
    plan_id="F10",
    order=42,
    group="Operations",
    status="planned",
    requires=("alert_top_flags",),
    description="The three flags the Monday SQL alert will send, exactly as recipients will see them.",
    highlights=(
        "Top 3 waste flags by priority_score (R5)",
        "Each flag with its owner, estimated savings and suggested action",
        "Same rows the Databricks SQL alert reads every Monday",
    ),
    render=render,
)
