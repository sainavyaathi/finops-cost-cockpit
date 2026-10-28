from __future__ import annotations

import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import bar, kpi_row, money, table

COLUMNS = ["priority_rank", "workload", "issue_type", "suggested_action", "priority_score",
           "estimated_monthly_cost", "confidence", "owner", "team", "evidence"]


def render(ctx: AppContext) -> None:
    backlog = ctx.table("optimization_candidates").sort_values("priority_score", ascending=False)
    backlog["workload"] = backlog["workload_type"] + ": " + backlog["job_name"].fillna(backlog["workload_id"]).astype(str)

    left, right = st.columns(2)
    issues = left.multiselect("Issue type", sorted(backlog["issue_type"].dropna().unique()))
    confidence = right.multiselect("Confidence", ["HIGH", "MEDIUM", "LOW"])
    if issues:
        backlog = backlog[backlog["issue_type"].isin(issues)]
    if confidence:
        backlog = backlog[backlog["confidence"].isin(confidence)]

    kpi_row([
        ("Items", str(len(backlog))),
        ("Monthly cost in scope", money(backlog["estimated_monthly_cost"].sum())),
        ("Top score", f"{backlog['priority_score'].max():.0f}" if len(backlog) else "-"),
    ])
    if backlog.empty:
        st.info("No items match the filters.")
        return
    bar(backlog, "workload", "priority_score", color="issue_type", title="Priority score by workload")
    table(backlog[COLUMNS], money_columns=["estimated_monthly_cost"], download_name="optimization_backlog")
