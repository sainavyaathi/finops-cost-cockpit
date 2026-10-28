from __future__ import annotations

import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import kpi_row, table

STATUS_RANK = {"FAIL": 0, "WARN": 1, "PASS": 2}


def render(ctx: AppContext) -> None:
    health = ctx.table("accelerator_health")
    run_id = str(health["run_id"].astype(str).max())
    health = health[health["run_id"].astype(str) == run_id]
    run_log = ctx.table("accelerator_run_log")
    run_log = run_log[run_log["run_id"].astype(str) == run_id]

    counts = health["status"].value_counts()
    kpi_row([
        ("Latest run", run_id),
        ("Checks passed", str(int(counts.get("PASS", 0)))),
        ("Warnings", str(int(counts.get("WARN", 0)))),
        ("Failures", str(int(counts.get("FAIL", 0)))),
    ])

    st.subheader("Health checks")
    severity_first = health.assign(_rank=health["status"].map(STATUS_RANK).fillna(len(STATUS_RANK)))
    table(severity_first.sort_values(["_rank", "check_name"])[
        ["check_name", "status", "severity", "message", "affected_output"]])

    st.subheader("Pipeline steps")
    table(run_log[["task_name", "result", "duration_seconds", "rows_written", "message"]])
