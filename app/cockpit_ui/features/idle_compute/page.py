"""F5 page. Reads ``idle_compute``; idle rules, thresholds and idle cost come from the engine.

Columns the page uses when present: ``cluster_id``, ``cluster_name``, ``owner``, ``idle_reason``,
``autotermination_minutes``, ``avg_cpu_pct``, ``idle_hours``, ``idle_cost``, ``suggested_action``.
"""

from __future__ import annotations

import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import bar, empty_state, kpi_row, money, number, section, table


def render(ctx: AppContext) -> None:
    idle = ctx.table("idle_compute")
    if idle.empty:
        empty_state("No idle compute", "Every cluster terminates on time and stays busy while it runs.",
                    icon=":material/bedtime:")
        return

    cols = set(idle.columns)
    kpis = []
    if "idle_cost" in cols:
        kpis.append(("Cost of idle hours", money(idle["idle_cost"].sum())))
    if "idle_hours" in cols:
        kpis.append(("Idle hours", number(idle["idle_hours"].sum())))
    if "cluster_id" in cols:
        kpis.append(("Clusters flagged", number(idle["cluster_id"].nunique())))
    kpi_row(kpis)

    label = next((c for c in ("cluster_name", "cluster_id") if c in cols), None)
    if label and "idle_cost" in cols:
        bar(idle.nlargest(10, "idle_cost"), label, "idle_cost", money=True, title="Top clusters by idle cost")

    section("Clusters to fix", "Set auto-termination to 60 minutes or less, then review sizing.")
    if "idle_cost" in cols:
        idle = idle.sort_values("idle_cost", ascending=False)
    preferred = [c for c in ("cluster_name", "cluster_id", "owner", "idle_reason", "autotermination_minutes",
                             "avg_cpu_pct", "idle_hours", "idle_cost", "suggested_action") if c in cols]
    table(idle[preferred] if preferred else idle, money_columns=[c for c in ("idle_cost",) if c in cols],
          percent_columns=[c for c in ("avg_cpu_pct",) if c in cols], download_name="idle_compute")
    if "autotermination_minutes" in cols:
        st.caption("Auto-termination of 0 or blank means it is turned off.")
