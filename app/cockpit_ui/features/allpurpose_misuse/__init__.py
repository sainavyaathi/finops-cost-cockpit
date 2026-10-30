from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="allpurpose_misuse",
    title="All-purpose compute misuse",
    icon=":material/dns:",
    owner="Nikhil",
    plan_refs=("F4",),
    plan_id="F4",
    order=26,
    group="Waste",
    status="planned",
    requires=("allpurpose_misuse",),
    description="Interactive clusters doing work that cheaper job compute or SQL warehouses should do.",
    highlights=(
        "JDBC/ODBC traffic, scheduled jobs and many shared users on all-purpose clusters",
        "Estimated monthly savings from moving each workload to job compute or a SQL warehouse",
        "One row per cluster, with the evidence and the owner to contact",
    ),
    render=render,
)
