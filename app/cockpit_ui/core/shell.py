"""App shell: discovers features, builds grouped navigation, and isolates each page's failures."""

from __future__ import annotations

import os
from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

from . import context as _context
from . import theme
from .components import not_available, page_header, planned_feature, relative_time, status_chip
from .context import AppContext
from .datasource import DataSource, DataSourceError, build_source
from .feature import Feature
from .registry import Discovery, discover, enabled_features, group_features, load_toggles, missing_tables
from .settings import Settings, SettingsError

RUN_LOG_TABLE = "accelerator_run_log"
DEBUG_ENV = "COCKPIT_DEBUG"


def debug_enabled() -> bool:
    """Full tracebacks only when ``COCKPIT_DEBUG`` is truthy (local development)."""
    return os.environ.get(DEBUG_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def _short(exc: BaseException, limit: int = 160) -> str:
    """``TypeName: first line of the message``, truncated; never a traceback."""
    first = next((line.strip() for line in str(exc).splitlines() if line.strip()), "")
    if len(first) > limit:
        first = first[: limit - 1] + "…"
    return f"{type(exc).__name__}: {first}" if first else type(exc).__name__


@st.cache_resource(show_spinner=False)
def _source_for(settings: Settings) -> DataSource:
    return build_source(settings)


# ------------------------------------------------------------------ data-source facts (best effort)

def describe_source(source: DataSource) -> dict[str, str]:
    """``source.describe()`` as strings, with ``kind`` from ``source.kind``.

    Every registered source declares ``kind``; the label is only sniffed for third-party objects that
    predate the protocol.
    """
    info: dict[str, str] = {}
    describe = getattr(source, "describe", None)
    if callable(describe):
        try:
            info = {str(k): str(v) for k, v in describe().items()}
        except Exception:  # noqa: BLE001 - diagnostics must never break the shell
            info = {}
    kind = getattr(source, "kind", None) or info.get("kind")
    if not kind:
        kind = "databricks" if str(getattr(source, "label", "")).lower().startswith("databricks") else "local"
    info["kind"] = str(kind)
    return info


def data_freshness(ctx: AppContext, available: set[str]) -> str | None:
    """When the latest pipeline run finished, from ``accelerator_run_log``; ``None`` if unknown."""
    if RUN_LOG_TABLE not in available:
        return None
    try:
        log = ctx.table(RUN_LOG_TABLE)
        column = next(c for c in ("ended_at", "started_at") if c in log.columns)
        moment = pd.to_datetime(log[column], errors="coerce", utc=True).max()
    except Exception:  # noqa: BLE001 - freshness is decoration, not a requirement
        return None
    if pd.isna(moment):
        return None
    return f"{moment:%d %b %H:%M} UTC ({relative_time(moment.to_pydatetime())})"


# ------------------------------------------------------------------ page rendering

def show_source_error(exc: DataSourceError) -> None:
    """A data-source problem as the user should see it: message, what to do, driver detail on demand."""
    st.error(exc.message, icon=":material/cloud_off:")
    if exc.hint:
        st.caption(exc.hint)
    if exc.detail:
        with st.expander("Details"):
            st.code(exc.detail, language=None)


def _show_failure(text: str, exc: BaseException, icon: str) -> None:
    st.error(f"{text} ({_short(exc)})", icon=icon)
    if debug_enabled():
        with st.expander("Traceback (COCKPIT_DEBUG)"):
            st.exception(exc)
    else:
        st.caption(f"Set {DEBUG_ENV}=1 to see the full traceback.")


def _render_guarded(feature: Feature, ctx: AppContext) -> None:
    try:
        feature.render(ctx)
    except DataSourceError as exc:
        show_source_error(exc)
    except Exception as exc:  # noqa: BLE001 - one broken page must not break the app
        _show_failure(f"The {feature.title} page failed.", exc, ":material/error:")


def render_feature(feature: Feature, ctx: AppContext) -> None:
    """Render one feature with the shared header, table gating and error isolation."""

    if feature.is_planned:
        page_header(feature)
        planned_feature(feature, feature.requires, feature.highlights)
        return

    if not feature.requires:
        # Pages that read no gold table (Connection, Build progress) skip gating and the freshness lookup,
        # so they still render, degraded, when the source is down: that is when diagnostics matter most.
        page_header(feature)
        _render_guarded(feature, ctx)
        return

    try:
        available = ctx.available_tables()
    except DataSourceError as exc:
        page_header(feature)
        show_source_error(exc)
        return
    except Exception as exc:  # noqa: BLE001 - data source unreachable
        page_header(feature)
        _show_failure("Could not reach the data source.", exc, ":material/cloud_off:")
        return

    page_header(feature, freshness=data_freshness(ctx, available))
    missing = missing_tables(feature, available)
    if missing:
        not_available(missing)
        return
    _render_guarded(feature, ctx)


def _page(feature: Feature, ctx: AppContext) -> st.Page:
    def page() -> None:
        render_feature(feature, ctx)

    page.__name__ = f"page_{feature.id}"
    return st.Page(page, title=feature.title, icon=feature.icon, url_path=feature.id)


# ------------------------------------------------------------------ sidebar

def _wordmark(p: theme.Palette, *, with_text: bool = True) -> str:
    mark = (
        f'<rect x="0" y="6" width="20" height="20" rx="5" fill="{p.accent}"/>'
        '<rect x="5" y="16" width="2.4" height="5" rx="1" fill="#fff"/>'
        '<rect x="8.8" y="13" width="2.4" height="8" rx="1" fill="#fff"/>'
        '<rect x="12.6" y="10" width="2.4" height="11" rx="1" fill="#fff"/>'
    )
    if not with_text:
        return f'<svg xmlns="http://www.w3.org/2000/svg" width="20" height="32" viewBox="0 0 20 32">{mark}</svg>'
    text = (
        f'<text x="30" y="21" fill="{p.ink}" font-size="14" font-weight="600" letter-spacing="-0.1" '
        f'font-family="Inter, -apple-system, Segoe UI, Roboto, sans-serif">FinOps Cost Cockpit</text>'
    )
    return f'<svg xmlns="http://www.w3.org/2000/svg" width="200" height="32" viewBox="0 0 200 32">{mark}{text}</svg>'


def _source_panel(ctx: AppContext) -> str:
    info = describe_source(ctx.source)
    kind = info["kind"]
    if kind == "databricks":
        chip = status_chip("Databricks", "accent")
        target = f"{info['catalog']}.{info['schema']}" if "catalog" in info and "schema" in info else ctx.source.label
        auth = info.get("auth") or info.get("auth mode")
    elif kind == "local":
        chip = status_chip("Local sandbox")
        target = Path(info["directory"]).name if "directory" in info else ctx.source.label
        auth = None
    else:
        chip, target, auth = status_chip(kind.title()), ctx.source.label, None

    try:
        available = ctx.available_tables()
        reach = f"{len(available)} tables"
        freshness = data_freshness(ctx, available)
    except Exception:  # noqa: BLE001 - shown as a status, never raised
        reach, freshness = None, None
        chip += status_chip("Unreachable", "bad")

    lines = [f'<div style="display:flex;gap:6px;flex-wrap:wrap">{chip}</div>', f"<code>{escape(target)}</code>"]
    facts = [part for part in (reach, f"auth {auth}" if auth else None) if part]
    if facts:
        lines.append(f'<div class="ck-meta">{"".join(f"<span>{escape(f)}</span>" for f in facts)}</div>')
    if freshness:
        lines.append(f"<div>Last pipeline run {escape(freshness)}</div>")
    return f'<div class="ck-label">Data source</div><div class="ck-source">{"".join(lines)}</div>'


def refresh_source(ctx: AppContext) -> None:
    """Drop only this source's cached version and table list, never other users' cached rows.

    Cached tables are keyed by ``(cache_key, version, name)``. Forgetting the cached version makes the
    next run ask the source again; a new pipeline run yields a new version, so tables are re-read, while
    unchanged data keeps hitting the cache. ``cache_key`` includes the signed-in user in on-behalf-of-user
    mode, so one user's refresh never evicts another user's entries (a global ``st.cache_data.clear()``
    would flush everyone).
    """
    try:
        key = ctx.source.cache_key()
        version = _context._cached_version(key, ctx.source)
    except Exception:  # noqa: BLE001 - nothing cached for an unreachable source
        return
    _context._cached_available.clear(key, version, ctx.source)
    _context._cached_version.clear(key, ctx.source)


def _sidebar(ctx: AppContext, discovery: Discovery) -> None:
    p = theme.palette()
    st.logo(_wordmark(p), icon_image=_wordmark(p, with_text=False), size="large")
    with st.sidebar:
        st.markdown(_source_panel(ctx), unsafe_allow_html=True)
        if st.button("Refresh data", icon=":material/refresh:", width="stretch",
                     help="Check the source for a newer pipeline run now instead of within a minute."):
            refresh_source(ctx)
            st.rerun()
        for module_name, error in discovery.errors.items():
            st.warning(f"Feature `{module_name}` was skipped.", icon=":material/extension_off:")
            with st.expander("Why"):
                st.code(error)


# ------------------------------------------------------------------ entry point

def run(settings: Settings | None = None) -> None:
    st.set_page_config(page_title="FinOps Cost Cockpit", page_icon=":material/savings:", layout="wide",
                       initial_sidebar_state="expanded")
    theme.apply_theme()
    try:
        settings = settings or Settings.from_env()
        ctx = AppContext(source=_source_for(settings), settings=settings)
    except (SettingsError, DataSourceError) as exc:
        st.error(str(exc), icon=":material/settings_alert:")
        st.stop()

    discovery = discover()
    features = enabled_features(discovery.features, load_toggles(settings.features_file))
    _sidebar(ctx, discovery)

    if not features:
        st.warning("No features are enabled. Check app/features.yaml.")
        return
    # Streamlit makes the first page (Overview) the default and serves it at "/".
    sections = {group: [_page(f, ctx) for f in members] for group, members in group_features(features).items()}
    # One group needs no section heading.
    st.navigation(sections if len(sections) > 1 else next(iter(sections.values()))).run()
