from __future__ import annotations

from html import escape

import pandas as pd
import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import Kpi, bar, empty_state, kpi_row, money, number, section, status_chip, table
from cockpit_ui.core.theme import tone_colors

# retry_count is not headlined: the upstream engine groups retries by task run id, so it is always 0
# until the F6 fix lands (see CLAUDE.md, known issues).
CATEGORIES = {
    "FAILURE_HEAVY": ("Failure-heavy", "bad"),
    "RETRY_HEAVY": ("Retry-heavy", "warn"),
    "REVIEW_REQUIRED": ("Review required", "warn"),
    "INSUFFICIENT_DATA": ("Not enough data", "neutral"),
    "HEALTHY": ("Healthy", "neutral"),
}


def _category(code: object) -> tuple[str, str]:
    return CATEGORIES.get(str(code), (str(code).replace("_", " ").capitalize(), "neutral"))


def _attention_list(jobs: pd.DataFrame) -> None:
    rows = []
    for job in jobs.itertuples():
        label, tone = _category(job.reliability_category)
        rows.append(
            '<div style="display:flex;justify-content:space-between;align-items:baseline;gap:12px;'
            'padding:9px 0;border-bottom:1px solid var(--ck-border)">'
            f'<div style="min-width:0"><div style="font-weight:600;color:var(--ck-ink)">{escape(str(job.job_name))}</div>'
            f'<div style="font-size:0.8rem;color:var(--ck-muted)">{number(job.failed_count)} of {number(job.run_count)} runs '
            f'failed · {money(job.estimated_failed_cost, 2)} wasted</div></div>'
            f'<div style="display:flex;gap:6px;flex:none">{status_chip(f"{job.failure_rate_pct:.0f}% failed", tone)}</div>'
            "</div>"
        )
    st.markdown('<div class="ck-label">Needs attention</div>' + "".join(rows), unsafe_allow_html=True)


def render(ctx: AppContext) -> None:
    jobs = ctx.table("job_reliability_summary")
    jobs["status"] = jobs["reliability_category"].map(lambda c: _category(c)[0])
    jobs = jobs.sort_values(["estimated_failed_cost", "failure_rate_pct"], ascending=False)

    flagged = jobs[jobs["reliability_category"].map(lambda c: _category(c)[1] in ("bad", "warn"))]
    heavy = int((jobs["reliability_category"] == "FAILURE_HEAVY").sum())
    failed_cost = jobs["estimated_failed_cost"].sum()
    kpi_row([
        Kpi("Jobs", number(len(jobs))),
        Kpi("Runs", number(jobs["run_count"].sum())),
        Kpi("Failed runs", number(jobs["failed_count"].sum())),
        Kpi("Failure-heavy jobs", number(heavy), tone="bad" if heavy else "good"),
        Kpi("Failed-run cost", money(failed_cost, 2), tone="warn" if failed_cost > 0 else "neutral",
            help="Estimated spend on runs that ended FAILED, TIMED_OUT or ERROR."),
    ])

    costly = jobs[jobs["estimated_failed_cost"] > 0]
    left, right = st.columns([3, 2], gap="large")
    with left:
        if costly.empty:
            empty_state("No failed-run cost", "Every job run in the window succeeded.", icon=":material/check_circle:")
        else:
            colors = tone_colors({label: tone for label, tone in CATEGORIES.values()})
            bar(costly, "job_name", "estimated_failed_cost", color="status", colors=colors, money=True,
                title="Cost of failed runs", height=max(220, 44 * len(costly) + 80))
    with right:
        if flagged.empty:
            empty_state("No failure-heavy jobs", "No job crosses the engine's failure-rate threshold.",
                        icon=":material/check_circle:")
        else:
            _attention_list(flagged)

    section("All jobs", "Sorted by failed-run cost.")
    columns = {"job_name": "Job", "status": "Status", "run_count": "Runs", "failed_count": "Failed",
               "failure_rate_pct": "Failure rate", "estimated_failed_cost": "Failed-run cost",
               "avg_duration_minutes": "Avg minutes", "last_run_result_state": "Last result",
               "last_run_start_time": "Last run"}
    view = jobs[list(columns)].rename(columns=columns)
    view["Avg minutes"] = view["Avg minutes"].round(1)
    table(view, money_columns=["Failed-run cost"], percent_columns=["Failure rate"], download_name="job_reliability")
