from __future__ import annotations

import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import bar, kpi_row, money, number, table


def render(ctx: AppContext) -> None:
    jobs = ctx.table("job_reliability_summary").sort_values("failure_rate_pct", ascending=False)

    kpi_row([
        ("Jobs", number(len(jobs))),
        ("Runs", number(jobs["run_count"].sum())),
        ("Failed runs", number(jobs["failed_count"].sum())),
        ("Failed-run cost", money(jobs["estimated_failed_cost"].sum(), 2)),
    ])

    left, right = st.columns(2)
    with left:
        bar(jobs, "job_name", "failure_rate_pct", color="reliability_category", title="Failure rate (%)")
    with right:
        bar(jobs, "job_name", "estimated_failed_cost", title="Cost of failed runs ($)")

    table(
        jobs[["job_name", "run_count", "failed_count", "retry_count", "failure_rate_pct", "avg_duration_minutes",
              "estimated_failed_cost", "last_run_result_state", "reliability_category"]],
        money_columns=["estimated_failed_cost"], percent_columns=["failure_rate_pct"],
        download_name="job_reliability",
    )
