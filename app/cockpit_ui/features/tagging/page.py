from __future__ import annotations

import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import bar, donut, kpi_row, money, table


def render(ctx: AppContext) -> None:
    tagging = ctx.table("vw_tagging_quality")
    tagging["team"] = tagging["team"].fillna("(no team tag)")
    attribution = ctx.table("vw_attribution_quality")

    total = tagging["estimated_cost"].sum()
    incomplete = tagging.loc[tagging["missing_required_tag_count"] > 0, "estimated_cost"].sum()
    kpi_row([
        ("Total cost", money(total)),
        ("Cost missing a required tag", money(incomplete)),
        ("Share missing tags", f"{incomplete / total:.0%}" if total else "-"),
    ])

    left, right = st.columns([3, 2])
    with left:
        by_team = tagging.groupby(["team", "worst_tagging_quality"], as_index=False)["estimated_cost"].sum()
        bar(by_team, "team", "estimated_cost", color="worst_tagging_quality", title="Cost by team and tag quality")
    with right:
        by_quality = attribution.groupby("attribution_quality", as_index=False)["estimated_cost"].sum()
        donut(by_quality, "attribution_quality", "estimated_cost", title="Cost by attribution confidence")

    table(tagging.sort_values("estimated_cost", ascending=False), money_columns=["estimated_cost"],
          download_name="tagging_quality")
