"""F4 page. Reads ``allpurpose_misuse``; misuse rules and savings come from the engine.

Columns the page uses when present: ``cluster_id``, ``cluster_name``, ``owner``, ``misuse_type``,
``evidence``, ``estimated_monthly_cost``, ``estimated_monthly_savings``, ``suggested_action``.
"""

from __future__ import annotations

import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import bar, empty_state, kpi_row, money, number, section, table

MONEY = ("estimated_monthly_cost", "estimated_monthly_savings")


def render(ctx: AppContext) -> None:
    misuse = ctx.table("allpurpose_misuse")
    if misuse.empty:
        empty_state("No misuse found", "No all-purpose cluster ran JDBC/ODBC traffic, scheduled jobs or shared-user work.",
                    icon=":material/dns:")
        return

    cols = set(misuse.columns)
    kpis = []
    if "estimated_monthly_savings" in cols:
        kpis.append(("Estimated monthly savings", money(misuse["estimated_monthly_savings"].sum())))
    if "cluster_id" in cols:
        kpis.append(("Clusters flagged", number(misuse["cluster_id"].nunique())))
    if "estimated_monthly_cost" in cols:
        kpis.append(("Monthly cost in scope", money(misuse["estimated_monthly_cost"].sum())))
    kpi_row(kpis)

    value = next((c for c in ("estimated_monthly_savings", "estimated_monthly_cost") if c in cols), None)
    if "misuse_type" in cols and value:
        by_type = misuse.groupby("misuse_type", as_index=False)[value].sum()
        bar(by_type, "misuse_type", value, money=True, height=240, title="By misuse type")

    section("Clusters to move", "Highest savings first. Contact the owner with the suggested action.")
    if value:
        misuse = misuse.sort_values(value, ascending=False)
    preferred = [c for c in ("cluster_name", "cluster_id", "owner", "misuse_type", "evidence", "suggested_action", *MONEY)
                 if c in cols]
    view = misuse[preferred] if preferred else misuse
    table(view, money_columns=[c for c in MONEY if c in cols], download_name="allpurpose_misuse")
    st.caption("Savings assume the workload moves to job compute (scheduled work) or a SQL warehouse (BI traffic).")
