"""App shell: discovers features, builds navigation, and isolates each page's failures."""

from __future__ import annotations

import streamlit as st

from .components import not_available, page_header
from .context import AppContext
from .datasource import DataSource, build_source
from .feature import Feature
from .registry import discover, enabled_features, load_toggles, missing_tables
from .settings import Settings


@st.cache_resource(show_spinner=False)
def _source_for(settings: Settings) -> DataSource:
    return build_source(settings)


def render_feature(feature: Feature, ctx: AppContext) -> None:
    """Render one feature with the shared header, table gating and error isolation."""

    page_header(feature)
    try:
        missing = missing_tables(feature, ctx.available_tables())
    except Exception as exc:  # noqa: BLE001 - data source unreachable
        st.error(f"Could not reach the data source ({ctx.source.label}): {exc}", icon=":material/cloud_off:")
        return
    if missing:
        not_available(missing)
        return
    try:
        feature.render(ctx)
    except Exception as exc:  # noqa: BLE001 - one broken page must not break the app
        st.error(f"The **{feature.title}** page failed: {type(exc).__name__}: {exc}", icon=":material/error:")
        with st.expander("Details"):
            st.exception(exc)


def _page(feature: Feature, ctx: AppContext) -> st.Page:
    def page() -> None:
        render_feature(feature, ctx)

    page.__name__ = f"page_{feature.id}"
    return st.Page(page, title=feature.title, icon=feature.icon, url_path=feature.id)


def run(settings: Settings | None = None) -> None:
    st.set_page_config(page_title="FinOps Cost Cockpit", page_icon=":material/savings:", layout="wide")
    settings = settings or Settings.from_env()
    ctx = AppContext(source=_source_for(settings), settings=settings)

    discovery = discover()
    features = enabled_features(discovery.features, load_toggles(settings.features_file))

    with st.sidebar:
        st.caption(f"Data source: **{ctx.source.label}**")
        if st.button("Refresh data", icon=":material/refresh:", width="stretch"):
            st.cache_data.clear()
            st.rerun()
        for module_name, error in discovery.errors.items():
            st.warning(f"Feature `{module_name}` was skipped.", icon=":material/extension_off:")
            with st.expander("Why"):
                st.code(error)

    if not features:
        st.warning("No features are enabled. Check app/features.yaml.")
        return
    st.navigation([_page(feature, ctx) for feature in features]).run()
