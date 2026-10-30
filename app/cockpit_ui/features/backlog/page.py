from __future__ import annotations

import pandas as pd
import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import Kpi, bar, empty_state, kpi_row, money, number, section, table
from cockpit_ui.core.theme import palette

CONFIDENCE_ORDER = ["HIGH", "MEDIUM", "LOW"]
HEALTHY_ACTION = "LOOKS_HEALTHY"
TOP_N = 15

COLUMNS = {
    "priority_rank": "Rank",
    "workload": "Workload",
    "workload_type": "Type",
    "team": "Team",
    "owner": "Owner",
    "issue": "Issue",
    "action": "Suggested action",
    "priority_score": "Priority score",
    "estimated_monthly_cost": "Monthly cost",
    "confidence": "Confidence",
    "evidence": "Evidence",
}


def _readable(codes: pd.Series) -> pd.Series:
    return codes.fillna("").str.replace("_", " ").str.capitalize()


def _confidence_colors() -> dict[str, str]:
    """Strong accent for HIGH, fading greys below it: the eye lands on what is safe to act on."""
    p = palette()
    return {"HIGH": p.accent, "MEDIUM": p.muted, "LOW": p.faint, "UNKNOWN": p.border}


def render(ctx: AppContext) -> None:
    backlog = ctx.table("optimization_candidates").sort_values("priority_score", ascending=False)
    backlog["workload"] = backlog["job_name"].fillna(backlog["workload_id"]).astype(str)
    backlog["issue"] = _readable(backlog["issue_type"])
    backlog["action"] = _readable(backlog["suggested_action"])

    st.caption("Preview: ranked by the engine's priority score from `optimization_candidates` until the unified "
               "`waste_flags` backlog (F7) ships.")

    issues_col, confidence_col, healthy_col = st.columns([3, 2, 1.4], vertical_alignment="bottom")
    issues = issues_col.multiselect("Issue", sorted(backlog["issue"].unique()), placeholder="All issues")
    present = [c for c in CONFIDENCE_ORDER if c in set(backlog["confidence"])]
    confidence = confidence_col.multiselect("Confidence", present, placeholder="Any confidence")
    hide_healthy = healthy_col.toggle("Hide healthy", value=True, help="Drop items whose suggested action is 'Looks healthy'.")
    if issues:
        backlog = backlog[backlog["issue"].isin(issues)]
    if confidence:
        backlog = backlog[backlog["confidence"].isin(confidence)]
    if hide_healthy:
        backlog = backlog[backlog["suggested_action"] != HEALTHY_ACTION]

    high = int((backlog["confidence"] == "HIGH").sum())
    kpi_row([
        Kpi("Open items", number(len(backlog))),
        Kpi("Monthly cost in scope", money(backlog["estimated_monthly_cost"].sum()),
            help="Current monthly cost of the listed workloads, not a savings estimate."),
        Kpi("High confidence", number(high), tone="accent" if high else "neutral"),
        Kpi("Top score", number(backlog["priority_score"].max(), 1) if len(backlog) else "-"),
    ])
    if backlog.empty:
        empty_state("Nothing matches these filters", "Clear a filter or turn off 'Hide healthy'.",
                    icon=":material/filter_alt_off:")
        return

    top = backlog.head(TOP_N).assign(confidence=lambda f: f["confidence"].fillna("UNKNOWN"))
    section("Priority", f"Top {min(TOP_N, len(backlog))} by priority score, coloured by confidence.")
    bar(top, "workload", "priority_score", color="confidence", colors=_confidence_colors(),
        height=max(220, 34 * len(top) + 60))

    section("Backlog", "Evidence holds the engine's reasoning for each item.")
    view = backlog[list(COLUMNS)].fillna({"team": "", "owner": ""}).rename(columns=COLUMNS)
    view["Priority score"] = view["Priority score"].round(1)
    table(view, money_columns=["Monthly cost"], download_name="optimization_backlog")
