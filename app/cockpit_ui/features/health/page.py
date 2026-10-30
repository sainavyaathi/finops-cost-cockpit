from __future__ import annotations

from html import escape

import pandas as pd
import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import Kpi, kpi_row, number, section, status_chip, table

STATUS = {"FAIL": (0, "Fail", "bad"), "WARN": (1, "Warn", "warn"), "PASS": (2, "Pass", "good")}
SUCCESS_RESULTS = {"SUCCESS", "SUCCEEDED", "PASS"}


def _status(code: object) -> tuple[int, str, str]:
    return STATUS.get(str(code).upper(), (len(STATUS), str(code).capitalize(), "neutral"))


def _check_row(check) -> str:
    _, label, tone = _status(check.status)
    name = str(check.check_name).replace("_", " ").capitalize()
    affected = "" if pd.isna(check.affected_output) else str(check.affected_output)
    affected_html = "".join(status_chip(part.strip(), mono=True) for part in affected.split(",") if part.strip())
    severity = "" if pd.isna(check.severity) or str(check.severity).upper() == "INFO" else f" · {str(check.severity).lower()} severity"
    return (
        '<div style="display:grid;grid-template-columns:64px minmax(0,1fr) auto;gap:14px;align-items:start;'
        'padding:10px 2px;border-bottom:1px solid var(--ck-border)">'
        f"<div>{status_chip(label, tone)}</div>"
        f'<div style="min-width:0"><div style="font-weight:600;color:var(--ck-ink)">{escape(name)}'
        f'<span style="font-weight:400;color:var(--ck-faint);font-size:0.8rem">{escape(severity)}</span></div>'
        f'<div style="font-size:0.82rem;color:var(--ck-muted);overflow-wrap:anywhere">{escape(str(check.message))}</div></div>'
        f'<div style="display:flex;flex-wrap:wrap;gap:4px;justify-content:flex-end;max-width:340px">{affected_html}</div>'
        "</div>"
    )


def render(ctx: AppContext) -> None:
    health = ctx.table("accelerator_health")
    run_id = str(health["run_id"].astype(str).max())
    health = health[health["run_id"].astype(str) == run_id]
    run_log = ctx.table("accelerator_run_log")
    run_log = run_log[run_log["run_id"].astype(str) == run_id]

    counts = health["status"].astype(str).str.upper().value_counts()
    warns, fails = int(counts.get("WARN", 0)), int(counts.get("FAIL", 0))
    steps_ok = int(run_log["result"].astype(str).str.upper().isin(SUCCESS_RESULTS).sum())
    kpi_row([
        Kpi("Checks passed", f"{int(counts.get('PASS', 0))} / {len(health)}", tone="good" if not (warns or fails) else "neutral"),
        Kpi("Warnings", number(warns), tone="warn" if warns else "neutral"),
        Kpi("Failures", number(fails), tone="bad" if fails else "good"),
        Kpi("Steps succeeded", f"{steps_ok} / {len(run_log)}", tone="good" if steps_ok == len(run_log) else "bad"),
    ])

    section("Checks", f"Run {run_id}. Failures and warnings first; chips name the outputs each check affects.")
    ordered = health.assign(_rank=health["status"].map(lambda s: _status(s)[0])).sort_values(["_rank", "check_name"])
    st.markdown("".join(_check_row(check) for check in ordered.itertuples()), unsafe_allow_html=True)

    section("Pipeline steps", "In run order.")
    steps = run_log.sort_values("started_at") if "started_at" in run_log else run_log
    view = steps[["task_name", "result", "duration_seconds", "rows_written", "message"]].rename(
        columns={"task_name": "Step", "duration_seconds": "Seconds"})
    view["Seconds"] = view["Seconds"].round(2)
    table(view)
