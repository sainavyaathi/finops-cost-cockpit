from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import streamlit as st

from .datasource import DataSource
from .settings import Settings

# Every cached function is keyed by ``source.cache_key()``. In on-behalf-of-user mode that key contains
# a hash of the signed-in user, so one user's rows are never served to another. ``_source`` is not hashed.


@st.cache_data(show_spinner=False, ttl=3600, max_entries=512)
def _cached_table(cache_key: str, version: str, name: str, _source: DataSource) -> pd.DataFrame:
    return _source.table(name)


@st.cache_data(show_spinner=False, ttl=60, max_entries=256)
def _cached_version(cache_key: str, _source: DataSource) -> str:
    return _source.version()


@st.cache_data(show_spinner=False, ttl=60, max_entries=256)
def _cached_available(cache_key: str, version: str, _source: DataSource) -> frozenset[str]:
    return frozenset(_source.available_tables())


@dataclass
class AppContext:
    """Everything a feature page gets. Pages read data only through ``table()``."""

    source: DataSource
    settings: Settings

    @property
    def version(self) -> str:
        return _cached_version(self.source.cache_key(), self.source)

    def table(self, name: str) -> pd.DataFrame:
        """Rows of a logical gold table. Raises ``DataSourceError`` with a user-facing message."""
        key = self.source.cache_key()
        # Return a copy so one page's in-place edits never leak into another page via the cache.
        return _cached_table(key, self.version, name.lower(), self.source).copy()

    def available_tables(self) -> set[str]:
        return set(_cached_available(self.source.cache_key(), self.version, self.source))

    def has(self, name: str) -> bool:
        return name.lower() in self.available_tables()

    def describe(self) -> dict[str, str]:
        """Non-secret description of the data source, for the sidebar or a Connection page."""
        return self.source.describe()
