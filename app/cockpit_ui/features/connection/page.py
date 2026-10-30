"""Connection diagnostics: where the app reads from, whether it can, and which gold tables it sees.

Deliberate R6 exception: diagnostics have to talk to the data source directly, so this page calls
``ctx.describe()`` and ``ctx.source.check()`` instead of reading a gold table. It contains no business
logic and reads no rows. ``describe()`` and ``check()`` return non-secret facts and should never raise;
values are still screened for anything token-like before they are shown, so a misbehaving plugin
source cannot leak a credential onto the page.

``check()`` runs one statement per contract table, so results are cached for ``CHECK_TTL`` seconds per
source (and per user in on-behalf-of-user mode). "Run checks again" bypasses the cache for this session.
"""

from __future__ import annotations

import os
import re
from html import escape
from pathlib import PurePath

import pandas as pd
import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import Kpi, kpi_row, section, status_chip, table
from cockpit_ui.core.contract import CONTRACT
from cockpit_ui.core.registry import discover, enabled_features, load_toggles, missing_tables

DOCS = "docs/connecting_workspaces.md"
CHECK_TTL = 60
_NONCE = "connection_check_nonce"

_SECRET_KEY = re.compile(r"token|secret|password|passwd|credential|private", re.I)
_SECRET_VALUE = re.compile(
    r"dapi[0-9a-f]{8,}(-\d+)?"          # Databricks personal access token
    r"|eyJ[\w-]{8,}\.[\w-]{8,}\.[\w-]*"  # JWT / OAuth access token
    r"|\b[0-9a-f]{32,}\b",               # long hex secrets
    re.I,
)

# Variables an admin sets. Only "set / not set" is shown; values never are.
ENV_VARS = (
    "COCKPIT_SOURCE", "COCKPIT_LOCAL_DIR", "COCKPIT_CATALOG", "COCKPIT_SCHEMA", "COCKPIT_TABLE_MAP", "COCKPIT_AUTH",
    "DATABRICKS_HOST", "DATABRICKS_WAREHOUSE_ID", "DATABRICKS_HTTP_PATH", "DATABRICKS_CONFIG_PROFILE",
    "DATABRICKS_TOKEN", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET",
)


@st.cache_data(show_spinner="Checking the data source...", ttl=CHECK_TTL, max_entries=64)
def _cached_checks(cache_key: str, nonce: int, _source) -> list[tuple[str, bool, str]]:
    try:
        return [(str(n), bool(ok), redact(d)) for n, ok, d in _source.check()]
    except Exception as exc:  # noqa: BLE001 - check() should never raise; show it if it does
        return [("Connect", False, redact(f"{type(exc).__name__}: {exc}"))]


def _checks(ctx: AppContext) -> list[tuple[str, bool, str]]:
    try:
        key = ctx.source.cache_key()
    except Exception as exc:  # noqa: BLE001 - e.g. no forwarded user token; that is the diagnosis
        return [("Connect", False, redact(f"{type(exc).__name__}: {exc}"))]
    return _cached_checks(key, st.session_state.get(_NONCE, 0), ctx.source)


def _describe(ctx: AppContext) -> dict[str, str]:
    try:
        return {str(k): str(v) for k, v in ctx.describe().items()}
    except Exception as exc:  # noqa: BLE001 - describe() should never raise; show it if it does
        return {"kind": str(getattr(ctx.source, "kind", "unknown")), "describe failed": type(exc).__name__}


def redact(value: object) -> str:
    return _SECRET_VALUE.sub("[redacted]", str(value))


def safe_description(info: dict[str, str]) -> list[tuple[str, str]]:
    rows = []
    for key, value in info.items():
        shown = "[hidden]" if _SECRET_KEY.search(str(key)) else redact(value)
        rows.append((str(key), shown))
    return rows


def hint(name: str, ok: bool, detail: str, kind: str) -> str:
    """What to do about a failed check, in one sentence."""
    if ok:
        return ""
    lowered = name.lower()
    if lowered == "directory":
        return ("Run `python sandbox/run_local.py` to create the CSVs, or point COCKPIT_LOCAL_DIR at the folder "
                "that holds them.")
    if lowered == "connect":
        return (f"Check DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID (or DATABRICKS_HTTP_PATH) and exactly one auth "
                f"method: token, CLI profile or OAuth client. See {DOCS}, sections 2 to 4.")
    if lowered.startswith("schema"):
        return (f"Grant USE CATALOG, USE SCHEMA and SELECT on this schema to the identity the app runs as "
                f"({DOCS}, section 3).")
    if lowered == "read tables":
        return f"The app could not list the gold tables just now. Fix the first failed check, then press Refresh data ({DOCS})."
    if lowered == "list tables":
        return "The app connected but cannot list tables. Check COCKPIT_CATALOG / COCKPIT_SCHEMA and the USE SCHEMA grant."
    if lowered.startswith("table"):
        if "lacks column" in detail:
            return ("The table exists but has an older shape. Re-run the pipeline, or map a table with the expected "
                    "columns in app/tables.yaml.")
        if kind == "local":
            return "Run the sandbox pipeline (`python sandbox/run_local.py`), then press Refresh data."
        return ("Run the cockpit pipeline job, or map the name to an existing table in app/tables.yaml "
                f"(COCKPIT_TABLE_MAP, {DOCS} section 5).")
    return f"See {DOCS}."


def _html(markup: str) -> None:
    st.markdown(markup, unsafe_allow_html=True)


def _kv_panel(rows: list[tuple[str, str]]) -> str:
    cells = "".join(
        f'<div style="color:var(--ck-muted)">{escape(k.capitalize())}</div>'
        f'<div style="font-family:var(--ck-mono);font-size:0.8rem;color:var(--ck-ink);overflow-wrap:anywhere">{escape(v)}</div>'
        for k, v in rows
    )
    return ('<div class="ck-kpi" style="display:grid;grid-template-columns:max-content 1fr;gap:7px 18px;'
            f'font-size:0.85rem;align-items:baseline">{cells}</div>')


def _check_list(rows: list[tuple[str, bool, str, str]]) -> str:
    items = []
    for name, ok, detail, advice in rows:
        chip = status_chip("OK", "good") if ok else status_chip("Fail", "bad")
        advice_html = (f'<div style="color:var(--ck-ink);font-size:0.82rem;margin-top:3px">{escape(advice)}</div>'
                       if advice else "")
        items.append(
            '<div style="display:grid;grid-template-columns:52px 160px 1fr;gap:12px;padding:9px 0;'
            'border-top:1px solid var(--ck-border);align-items:baseline">'
            f'<div>{chip}</div><div style="font-weight:500">{escape(name)}</div>'
            f'<div><div style="color:var(--ck-muted);font-size:0.82rem;overflow-wrap:anywhere">{escape(detail)}</div>'
            f'{advice_html}</div></div>'
        )
    return f'<div style="font-size:0.85rem">{"".join(items)}</div>'


def _verdict(problems: int, readable: int, produced: int) -> str:
    if problems == 0:
        chip, text = status_chip("Ready", "good"), f"Connected. All {produced} engine tables are readable."
    else:
        noun = "problem" if problems == 1 else "problems"
        chip = status_chip(f"{problems} {noun}", "bad")
        text = f"{readable} of {produced} engine tables are readable. Fix the failures below, top to bottom."
    return f'<div style="display:flex;gap:10px;align-items:center;margin:2px 0 10px">{chip}<span>{escape(text)}</span></div>'


def render(ctx: AppContext) -> None:
    info = _describe(ctx)
    kind = str(info.get("kind", getattr(ctx.source, "kind", "unknown")))

    # The Source panel needs no round trip, so it is drawn before the checks run; the verdict and KPIs
    # are filled into the reserved slot above it once they are known.
    summary_slot = st.container()
    left, right = st.columns([2, 3], gap="large")
    with left:
        section("Source", "What the app is configured to read. Secrets are never shown.")
        _html(_kv_panel(safe_description(info)))

    checks = _checks(ctx)
    try:
        available = ctx.available_tables()
    except Exception as exc:  # noqa: BLE001 - shown as a failed check, never raised
        available = set()
        message = getattr(exc, "message", None) or type(exc).__name__
        checks = [*checks, ("Read tables", False, redact(message))]

    source_checks = [(n, ok, d, hint(n, ok, d, kind)) for n, ok, d in checks if not n.startswith("Table ")]
    table_checks = {n.removeprefix("Table "): (ok, d) for n, ok, d in checks if n.startswith("Table ")}

    produced = [t for t in CONTRACT.values() if not t.planned]
    planned = [t for t in CONTRACT.values() if t.planned]

    def is_readable(name: str) -> bool:
        return name in available and table_checks.get(name, (True, ""))[0]

    readable = sum(1 for t in produced if is_readable(t.name))
    planned_present = sum(1 for t in planned if t.name in available)

    discovery = discover()
    features = enabled_features(discovery.features, load_toggles(ctx.settings.features_file))
    live_pages = [f for f in features if not f.is_planned]
    blocked = [f for f in live_pages if missing_tables(f, available)]
    used_by: dict[str, list[str]] = {}
    for feature in features:
        for name in feature.requires:
            used_by.setdefault(name.lower(), []).append(feature.title)

    failed_source = sum(1 for _, ok, _, _ in source_checks if not ok)
    failed_tables = len(produced) - readable
    passed = sum(1 for _, ok, _, _ in source_checks if ok)
    with summary_slot:
        _html(_verdict(failed_source + failed_tables, readable, len(produced)))
        kpi_row([
            Kpi("Source", kind.capitalize()),
            Kpi("Connection checks", f"{passed}/{len(source_checks)}", tone="good" if not failed_source else "bad"),
            Kpi("Engine tables readable", f"{readable}/{len(produced)}", tone="good" if not failed_tables else "warn"),
            Kpi("Planned tables present", f"{planned_present}/{len(planned)}",
                help="Cockpit tables from the POC plan; they appear as each feature ships."),
            Kpi("Pages waiting for data", str(len(blocked)), tone="warn" if blocked else "good",
                help=", ".join(f.title for f in blocked) or None),
        ])

    with right:
        section("Checks", f"Run against the source; results are reused for up to {CHECK_TTL} seconds.")
        _html(_check_list(source_checks) if source_checks else "")
        if st.button("Run checks again", icon=":material/refresh:", type="tertiary"):
            st.session_state[_NONCE] = st.session_state.get(_NONCE, 0) + 1
            st.rerun()

    section("Gold-table coverage", "Every table in the contract, whether this source has it, and which pages read it.")
    rows = []
    for t in CONTRACT.values():
        present = t.name in available
        detail = table_checks.get(t.name, (True, ""))[1]
        if t.planned and not present:
            state = "Planned"
        elif is_readable(t.name):
            state = "OK"
        elif present:
            state = "Problem"
        else:
            state = "Missing"
        advice = "" if state in ("OK", "Planned") else hint(f"Table {t.name}", False, detail, kind)
        if state == "OK" and kind == "local":
            detail = PurePath(detail).name  # the directory is already in the Source panel
        rows.append({
            "table": t.name,
            "feature": t.feature,
            "status": state,
            "available": "yes" if present else "no",
            "planned": "yes" if t.planned else "no",
            "used_by": ", ".join(used_by.get(t.name, [])),
            "location_or_problem": " ".join(part for part in (detail, advice) if part),
        })
    coverage = pd.DataFrame(rows)
    rank = {"Problem": 0, "Missing": 1, "OK": 2, "Planned": 3}
    coverage = coverage.sort_values(["status", "table"], key=lambda s: s.map(rank) if s.name == "status" else s)
    table(coverage, download_name="cockpit_table_coverage")

    with st.expander("Environment variables (set or not set; values are never shown)"):
        env = pd.DataFrame({"variable": ENV_VARS, "state": ["set" if os.environ.get(v) else "not set" for v in ENV_VARS]})
        table(env)
        st.caption(f"Full reference: {DOCS} and app/.env.example.")
