from cockpit_ui.core import Feature

from .page import render

FEATURE = Feature(
    id="catalog_allocation",
    title="Shared-warehouse catalog allocation",
    icon=":material/account_tree:",
    owner="Navya",
    plan_refs=("F3",),
    plan_id="F3",
    order=32,
    group="Attribution",
    status="planned",
    requires=("catalog_allocation",),
    description="Which catalogs drive the cost of each shared SQL warehouse, so it can be charged back.",
    highlights=(
        "Shared SQL warehouse cost split by catalog, from system.query.history",
        "Catalog split beats tags for shared warehouses (R2)",
        "Allocation sums to 100% per warehouse and day",
    ),
    render=render,
)
