from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import Kpi, bar, chart, delta_text, donut, kpi_row, money, number, section, table
from cockpit_ui.core.theme import palette

NO_TEAM = "No team tag"


def week_over_week(trend: pd.DataFrame, value: str = "estimated_cost") -> tuple[float, float] | None:
    """Spend in the last 7 days of ``trend`` and the 7 days before, or ``None`` without 14 days of data.

    TEMPORARY R6 exception: F8 moves week-over-week change into engine SQL (``cockpit.cost_trend`` or
    similar). Delete this helper then and read the engine's column instead.
    """
    if trend.empty:
        return None
    dates = pd.to_datetime(trend["usage_date"]).dt.normalize()
    end = dates.max()
    if (end - dates.min()).days < 13:
        return None
    current = trend.loc[dates > end - pd.Timedelta(days=7), value].sum()
    previous = trend.loc[(dates <= end - pd.Timedelta(days=7)) & (dates > end - pd.Timedelta(days=14)), value].sum()
    return float(current), float(previous)


def _spend_kpi(total: float, trend: pd.DataFrame) -> Kpi:
    wow = week_over_week(trend)
    change = delta_text(*wow) if wow else None
    if not change:
        return Kpi("Spend", money(total), help="Estimated cost over the lookback window.")
    current, previous = wow
    tone = "neutral" if change == "0.0%" else "warn" if current > previous else "good"
    return Kpi("Spend", money(total), delta=f"{change} week over week", tone=tone,
               help=f"Last 7 days {money(current)} vs the 7 days before {money(previous)}.")


def _translucent(hex_color: str, alpha: float) -> str:
    red, green, blue = (int(hex_color.lstrip("#")[i:i + 2], 16) for i in (0, 2, 4))
    return f"rgba({red},{green},{blue},{alpha})"


def _daily_spend(trend: pd.DataFrame) -> go.Figure:
    """Total spend as a soft accent area with untagged spend drawn inside it in the warn colour."""
    p = palette()
    fig = go.Figure()
    for column, name, color, fill in (("estimated_cost", "Total spend", p.accent, p.accent_soft),
                                      ("untagged_cost", "Untagged spend", p.warn, _translucent(p.warn, 0.12))):
        fig.add_scatter(x=trend["usage_date"], y=trend[column], name=name, mode="lines", fill="tozeroy",
                        line=dict(color=color, width=1.6), fillcolor=fill,
                        hovertemplate="$%{y:,.2f}<extra>" + name + "</extra>")
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(tickprefix="$", rangemode="tozero")
    fig.update_layout(hovermode="x unified")
    return fig


def _share(part: float, total: float) -> str | None:
    return f"{part / total:.0%} of spend" if total else None


def render(ctx: AppContext) -> None:
    summary = ctx.table("vw_executive_summary").iloc[0]
    trend = ctx.table("vw_cost_trend_daily")
    trend["usage_date"] = pd.to_datetime(trend["usage_date"])
    trend = trend.sort_values("usage_date")

    total = float(summary["total_estimated_cost"])
    untagged, failed = float(summary["untagged_cost"]), float(summary["failed_cost"])
    high_priority = int(summary["high_priority_candidate_count"])
    kpi_row([
        _spend_kpi(total, trend),
        Kpi("DBUs", number(summary["total_dbus"], compact=True)),
        Kpi("Workloads", number(summary["workload_count"])),
        Kpi("Untagged cost", money(untagged), delta=_share(untagged, total) if untagged > 0 else None,
            tone="warn" if untagged > 0 else "neutral", help="Spend with no team tag; nobody owns it yet."),
        Kpi("Failed-run cost", money(failed), tone="warn" if failed > 0 else "neutral",
            help="Spend on job runs that failed."),
        Kpi("High-priority fixes", number(high_priority), tone="warn" if high_priority > 0 else "neutral"),
    ])

    section("Daily spend", f"Lookback {summary['lookback_window']}. The amber band is spend nobody owns yet.")
    chart(_daily_spend(trend), height=280)

    workloads = ctx.table("workload_cost_summary")
    workloads["workload"] = workloads["job_name"].fillna(workloads["workload_id"]).astype(str)
    workloads["team"] = workloads["team"].fillna(NO_TEAM)

    section("Who spends it")
    left, right = st.columns([3, 2], gap="large")
    with left:
        top = workloads.nlargest(10, "estimated_cost")
        bar(top, "workload", "estimated_cost", money=True, title="Top 10 workloads", height=340)
    with right:
        by_team = workloads.groupby("team", as_index=False)["estimated_cost"].sum()
        donut(by_team.sort_values("estimated_cost", ascending=False), "team", "estimated_cost", money=True,
              title="Spend by team", height=340, colors={NO_TEAM: palette().border})

    section("All workloads", "Sorted by spend. Owner and attribution show who to talk to.")
    columns = ["workload", "workload_type", "team", "owner", "project", "attribution_quality",
               "active_days", "estimated_cost"]
    table(workloads.sort_values("estimated_cost", ascending=False)[columns].fillna({"owner": "", "project": ""}),
          money_columns=["estimated_cost"], download_name="workload_costs")
