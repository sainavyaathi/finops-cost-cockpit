from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import streamlit as st

from .datasource import DataSource
from .settings import Settings


@st.cache_data(show_spinner=False, ttl=3600)
def _cached_table(source_label: str, version: str, name: str, _source: DataSource) -> pd.DataFrame:
    # source_label and version are the cache key; _source is not hashed.
    return _source.table(name)


@st.cache_data(show_spinner=False, ttl=60)
def _cached_version(source_label: str, _source: DataSource) -> str:
    return _source.version()


@st.cache_data(show_spinner=False, ttl=60)
def _cached_available(source_label: str, version: str, _source: DataSource) -> frozenset[str]:
    return frozenset(_source.available_tables())


@dataclass
class AppContext:
    """Everything a feature page gets. Pages read data only through ``table()``."""

    source: DataSource
    settings: Settings

    @property
    def version(self) -> str:
        return _cached_version(self.source.label, self.source)

    def table(self, name: str) -> pd.DataFrame:
        # Return a copy so one page's in-place edits never leak into another page via the cache.
        return _cached_table(self.source.label, self.version, name, self.source).copy()

    def available_tables(self) -> set[str]:
        return set(_cached_available(self.source.label, self.version, self.source))
