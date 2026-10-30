"""Build progress for F1-F12, derived from what actually exists.

Static: the plan's feature ids, titles and owners (``PLAN``, from CLAUDE.md / plan v1.1). Derived:
which pages exist and their status (``registry.discover()``), whether they are switched on
(``features.yaml``), and which gold tables the current source has (contract vs ``available_tables``).
Nothing is marked done by hand.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from html import escape

import pandas as pd
import streamlit as st

from cockpit_ui.core import AppContext, Feature
from cockpit_ui.core.components import Kpi, kpi_row, section, status_chip, table
from cockpit_ui.core.contract import CONTRACT
from cockpit_ui.core.registry import discover, load_toggles

# (plan id, feature, owner, where it lives). "page" features are expected to get a cockpit page.
PLAN: tuple[tuple[str, str, str, str], ...] = (
    ("F1", "Cost base", "Navya", "page"),
    ("F2", "Tag attribution and quality", "Navya", "page"),
    ("F3", "Shared-warehouse catalog allocation", "Navya", "page"),
    ("F4", "All-purpose compute misuse", "Nikhil", "page"),
    ("F5", "Idle and zombie compute", "Nikhil", "page"),
    ("F6", "Job failures and failed-run cost", "Nikhil", "page"),
    ("F7", "Unified waste backlog", "Nikhil", "page"),
    ("F8", "Spend trend and week-over-week change", "Navya", "page"),
    ("F9", "Streamlit cockpit", "Navya", "page"),
    ("F10", "Monday SQL alert, top 3 flags", "Navya", "page"),
    ("F11", "Health and coverage checks", "Navya & Nikhil", "page"),
    ("F12", "Synthetic sandbox", "Nikhil", "pipeline"),
)

# Stage -> chip tone. Order is also the summary order.
STAGES = {
    "Live": "good",
    "Preview": "warn",
    "Waiting for data": "warn",
    "Planned": "neutral",
    "Pipeline only": "neutral",
    "Not started": "neutral",
}
_PLAN_ID = re.compile(r"F\d+")


def plan_ids(feature: Feature) -> set[str]:
    return set(_PLAN_ID.findall(" ".join((feature.plan_id, *feature.plan_refs))))


@dataclass
class Progress:
    plan_id: str
    title: str
    owner: str
    stage: str
    pages: list[tuple[Feature, bool, bool]]  # feature, enabled, has its tables
    tables_ready: int
    tables_total: int


def assess(plan_id: str, title: str, owner: str, where: str, features: list[Feature], enabled: set[str],
           available: set[str]) -> Progress:
    pages = []
    for feature in sorted((f for f in features if plan_id in plan_ids(f)), key=lambda f: (f.order, f.title)):
        has_data = all(name.lower() in available for name in feature.requires)
        pages.append((feature, feature.id in enabled, has_data))
    tables = [t for t in CONTRACT.values() if t.feature == plan_id]
    ready = sum(1 for t in tables if t.name in available)

    shipped = [(f, has_data) for f, on, has_data in pages if on and not f.is_planned]
    if any(f.status == "live" and has_data for f, has_data in shipped):
        stage = "Live"
    elif any(has_data for _, has_data in shipped):
        stage = "Preview"
    elif shipped:
        stage = "Waiting for data"
    elif any(f.is_planned for f, _, _ in pages):
        stage = "Planned"
    elif where == "pipeline":
        stage = "Pipeline only"
    else:
        stage = "Not started"
    return Progress(plan_id, title, owner, stage, pages, ready, len(tables))


def _page_links(progress: Progress) -> str:
    links = []
    for feature, on, _ in progress.pages:
        tone = {"live": "good", "preview": "warn", "planned": "neutral"}[feature.status]
        dot = status_chip(feature.status.capitalize(), tone)
        name = escape(feature.title)
        anchor = (f'<a href="{escape(feature.id)}" target="_self" style="color:var(--ck-accent);text-decoration:none">{name}</a>'
                  if on else f'<span style="color:var(--ck-faint)">{name} (off)</span>')
        links.append(f'<div style="display:flex;gap:8px;align-items:center">{anchor}{dot}</div>')
    return "".join(links) or '<span style="color:var(--ck-faint)">No page</span>'


def _row(p: Progress) -> str:
    cell = "padding:9px 10px;border-top:1px solid var(--ck-border);vertical-align:top"
    tables = f"{p.tables_ready}/{p.tables_total}" if p.tables_total else "-"
    return (
        "<tr>"
        f'<td style="{cell}">{status_chip(p.plan_id, "accent", mono=True)}</td>'
        f'<td style="{cell};font-weight:500;color:var(--ck-ink)">{escape(p.title)}</td>'
        f'<td style="{cell};color:var(--ck-muted)">{escape(p.owner)}</td>'
        f'<td style="{cell}">{status_chip(p.stage, STAGES[p.stage])}</td>'
        f'<td style="{cell}">{_page_links(p)}</td>'
        f'<td style="{cell};color:var(--ck-muted);font-variant-numeric:tabular-nums;text-align:right">{tables}</td>'
        "</tr>"
    )


def _progress_bar(counts: dict[str, int], total: int) -> str:
    colours = {"Live": "var(--ck-good)", "Preview": "var(--ck-warn)", "Waiting for data": "var(--ck-warn)",
               "Planned": "var(--ck-faint)", "Pipeline only": "var(--ck-border)", "Not started": "var(--ck-border)"}
    segments = "".join(
        f'<div title="{escape(stage)}: {n}" style="flex:{n};background:{colours[stage]};'
        f'opacity:{0.55 if stage == "Waiting for data" else 1}"></div>'
        for stage, n in counts.items() if n
    )
    return (f'<div style="display:flex;gap:2px;height:8px;border-radius:4px;overflow:hidden;margin:4px 0 6px">'
            f'{segments}</div>')


def render(ctx: AppContext) -> None:
    discovery = discover()
    toggles = load_toggles(ctx.settings.features_file)
    # Apply features.yaml order so pages list in navigation order.
    discovery.features = [replace(f, order=int((toggles.get(f.id) or {}).get("order", f.order)))
                          for f in discovery.features]
    enabled = {f.id for f in discovery.features if (toggles.get(f.id) or {}).get("enabled", True)}
    try:
        available = ctx.available_tables()
    except Exception:  # noqa: BLE001 - progress still shows pages without data
        available = set()

    rows = [assess(pid, title, owner, where, discovery.features, enabled, available) for pid, title, owner, where in PLAN]
    counts = {stage: sum(1 for r in rows if r.stage == stage) for stage in STAGES}
    pages = [f for f in discovery.features if f.id in enabled]
    gold = list(CONTRACT.values())

    kpi_row([
        Kpi("Features live", f"{counts['Live']}/{len(rows)}", tone="good"),
        Kpi("In preview", str(counts["Preview"] + counts["Waiting for data"]),
            help="Preview pages, plus shipped pages whose gold tables are not in this source yet."),
        Kpi("Planned", str(counts["Planned"] + counts["Not started"])),
        Kpi("Pages in the app", str(len(pages)),
            help=f"{sum(1 for f in pages if not f.is_planned)} rendering, {sum(1 for f in pages if f.is_planned)} planned"),
        Kpi("Gold tables present", f"{sum(1 for t in gold if t.name in available)}/{len(gold)}",
            help="Contract tables found in the current data source, including planned cockpit tables."),
    ])
    _html(_progress_bar(counts, len(rows)))
    legend = "".join(status_chip(f"{stage} {n}", STAGES[stage]) for stage, n in counts.items() if n)
    _html(f'<div style="display:flex;gap:6px;flex-wrap:wrap">{legend}</div>')

    section("Features", "Stage comes from the pages that exist and the tables this source has. Click a page to open it.")
    header = "".join(
        f'<th style="text-align:{align};padding:6px 10px;font-weight:500;color:var(--ck-faint);font-size:0.7rem;'
        f'text-transform:uppercase;letter-spacing:0.06em">{label}</th>'
        for label, align in (("Plan", "left"), ("Feature", "left"), ("Owner", "left"), ("Stage", "left"),
                             ("Pages", "left"), ("Tables", "right"))
    )
    _html(
        '<div class="ck-kpi" style="padding:4px 6px;overflow-x:auto">'
        f'<table style="width:100%;border-collapse:collapse;font-size:0.85rem;border:none">'
        f'<thead><tr>{header}</tr></thead><tbody>{"".join(_row(r) for r in rows)}</tbody></table></div>'
    )

    export = [{
        "plan_id": r.plan_id, "feature": r.title, "owner": r.owner, "stage": r.stage,
        "pages": ", ".join(f"{f.title} ({f.status}{'' if on else ', off'})" for f, on, _ in r.pages),
        "tables_present": r.tables_ready, "tables_in_contract": r.tables_total,
    } for r in rows]
    with st.expander("As a table"):
        table(pd.DataFrame(export), download_name="cockpit_build_progress")

    if discovery.errors:
        section("Pages that failed to load")
        for module_name in discovery.errors:
            st.warning(f"`{module_name}` could not be imported; see the sidebar for details.", icon=":material/extension_off:")


def _html(markup: str) -> None:
    st.markdown(markup, unsafe_allow_html=True)
