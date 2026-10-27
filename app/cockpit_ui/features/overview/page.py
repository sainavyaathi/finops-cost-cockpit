from __future__ import annotations

import pandas as pd
import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import bar, donut, kpi_row, line, money, number


def _workload_label(frame: pd.DataFrame) -> pd.Series:
    return frame["workload_type"] + ": " + frame["job_name"].fillna(frame["workload_id"]).astype(str)


def render(ctx: AppContext) -> None:
    summary = ctx.table("vw_executive_summary").iloc[0]
    st.caption(f"Lookback window: {summary['lookback_window']}")
    kpi_row([
        ("Total estimated cost", money(summary["total_estimated_cost"])),
        ("DBUs", number(summary["total_dbus"])),
        ("Workloads", number(summary["workload_count"])),
        ("Untagged cost", money(summary["untagged_cost"])),
        ("Failed-run cost", money(summary["failed_cost"])),
        ("High-priority items", number(summary["high_priority_candidate_count"])),
    ])

    trend = ctx.table("vw_cost_trend_daily")
    trend["usage_date"] = pd.to_datetime(trend["usage_date"])
    line(trend.sort_values("usage_date"), "usage_date", ["estimated_cost", "untagged_cost", "failed_cost"],
         title="Daily cost trend")

    workloads = ctx.table("workload_cost_summary")
    workloads["workload"] = _workload_label(workloads)
    workloads["team"] = workloads["team"].fillna("(no team tag)")

    left, right = st.columns([2, 1])
    with left:
        top = workloads.nlargest(10, "estimated_cost")
        bar(top, "workload", "estimated_cost", color="workload_type", title="Top 10 workloads by cost")
    with right:
        by_team = workloads.groupby("team", as_index=False)["estimated_cost"].sum()
        donut(by_team, "team", "estimated_cost", title="Cost by team")
