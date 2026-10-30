"""F10 page. Reads ``alert_top_flags``, which the engine already limits to the top 3 (R5).

The page trusts the engine: rows are shown in ``alert_rank`` order when that column exists, otherwise in
the order the table returns them. It never re-ranks. More than 3 rows breaks the R5 contract, so the page
says so and shows the first 3.

Columns the page uses when present: ``alert_rank``, ``resource_name``, ``resource_id``, ``flag_type``,
``owner``, ``priority_score``, ``estimated_monthly_savings``, ``suggested_action``.
"""

from __future__ import annotations

from html import escape

import pandas as pd
import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import empty_state, kpi_row, money, number, section, status_chip, table

ALERT_SIZE = 3  # R5


def _value(row: pd.Series, column: str) -> str:
    value = row.get(column)
    return "" if value is None or pd.isna(value) else str(value)


def _flag_card(position: int, row: pd.Series) -> str:
    title = _value(row, "resource_name") or _value(row, "resource_id") or f"Flag {position}"
    chips = status_chip(f"#{position}", "accent")
    if _value(row, "flag_type"):
        chips += status_chip(_value(row, "flag_type"))
    facts = []
    if _value(row, "estimated_monthly_savings"):
        facts.append(f"Saves {money(row['estimated_monthly_savings'])}/month")
    if _value(row, "priority_score"):
        facts.append(f"Priority {number(row['priority_score'])}")
    if _value(row, "owner"):
        facts.append(f"Owner {_value(row, 'owner')}")
    meta = "".join(f"<span>{escape(f)}</span>" for f in facts)
    action = _value(row, "suggested_action")
    return (
        '<div class="ck-kpi" style="display:flex;flex-direction:column;gap:6px">'
        f'<div style="display:flex;gap:6px">{chips}</div>'
        f'<div style="font-weight:600;color:var(--ck-ink)">{escape(title)}</div>'
        + (f'<div style="color:var(--ck-muted);font-size:0.85rem">{escape(action)}</div>' if action else "")
        + (f'<div class="ck-meta">{meta}</div>' if meta else "")
        + "</div>"
    )


def render(ctx: AppContext) -> None:
    flags = ctx.table("alert_top_flags")
    if flags.empty:
        empty_state("Nothing to alert", "No open waste flags, so Monday's alert will not fire.",
                    icon=":material/notifications_off:")
        return

    if "alert_rank" in flags.columns:
        flags = flags.sort_values("alert_rank", kind="stable")
    if len(flags) > ALERT_SIZE:
        st.warning(f"`alert_top_flags` returned {len(flags)} rows; the engine contract (R5) is {ALERT_SIZE}. "
                   f"Showing the first {ALERT_SIZE} in engine order. Fix the F10 step.", icon=":material/rule:")
    top = flags.head(ALERT_SIZE).reset_index(drop=True)

    if "estimated_monthly_savings" in top.columns:
        kpi_row([("Savings in this alert", money(top["estimated_monthly_savings"].sum())),
                 ("Flags", number(len(top)))])

    section("This Monday's alert", "Act on these three first; the rest stay in the waste backlog.")
    cards = "".join(_flag_card(i + 1, row) for i, row in top.iterrows())
    st.markdown(f'<div class="ck-kpis" style="grid-template-columns:repeat(auto-fit,minmax(260px,1fr))">{cards}</div>',
                unsafe_allow_html=True)

    section("Alert rows")
    table(top, money_columns=[c for c in ("estimated_monthly_savings",) if c in top.columns],
          download_name="monday_alert_top_flags")
