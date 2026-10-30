"""F3 page. Reads ``catalog_allocation``; every number is computed by the engine.

Columns the page uses when present (the engine owns the final schema): ``usage_date``,
``warehouse_id``, ``warehouse_name``, ``catalog_name``, ``query_count``, ``allocated_cost``,
``allocation_pct``.
"""

from __future__ import annotations

import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import bar, empty_state, kpi_row, money, number, section, table


def render(ctx: AppContext) -> None:
    alloc = ctx.table("catalog_allocation")
    if alloc.empty:
        empty_state("No shared-warehouse usage", "The engine found no shared SQL warehouse cost to allocate in this window.",
                    icon=":material/account_tree:")
        return

    cols = set(alloc.columns)
    kpis = []
    if "allocated_cost" in cols:
        kpis.append(("Allocated warehouse cost", money(alloc["allocated_cost"].sum())))
    if "warehouse_id" in cols:
        kpis.append(("Shared warehouses", number(alloc["warehouse_id"].nunique())))
    if "catalog_name" in cols:
        kpis.append(("Catalogs charged", number(alloc["catalog_name"].nunique())))
    if "query_count" in cols:
        kpis.append(("Queries", number(alloc["query_count"].sum())))
    kpi_row(kpis)

    if {"catalog_name", "allocated_cost"} <= cols:
        left, right = st.columns([3, 2], gap="large")
        with left:
            by_catalog = alloc.groupby("catalog_name", as_index=False)["allocated_cost"].sum()
            bar(by_catalog.nlargest(15, "allocated_cost"), "catalog_name", "allocated_cost", money=True,
                title="Warehouse cost by catalog")
        warehouse = next((c for c in ("warehouse_name", "warehouse_id") if c in cols), None)
        if warehouse:
            with right:
                by_wh = alloc.groupby([warehouse, "catalog_name"], as_index=False)["allocated_cost"].sum()
                bar(by_wh, warehouse, "allocated_cost", color="catalog_name", money=True,
                    title="Split per warehouse")

    section("Allocation detail", "One row per warehouse, catalog and day. Use it for chargeback.")
    sort_by = [c for c in ("usage_date", "allocated_cost") if c in cols]
    detail = alloc.sort_values(sort_by, ascending=False) if sort_by else alloc
    table(detail, money_columns=[c for c in ("allocated_cost", "warehouse_cost") if c in cols],
          percent_columns=[c for c in ("allocation_pct",) if c in cols], download_name="catalog_allocation")
