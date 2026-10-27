"""Where the app reads gold tables from.

Pages only ever ask for a table by name (``ctx.table("vw_executive_summary")``), so the same page
works on a laptop against CSVs and in Databricks Apps against a SQL warehouse. Business logic
belongs in the engine (SQL); pages only filter and present.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Protocol

import pandas as pd

from .settings import Settings

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_identifier(name: str) -> str:
    if not _IDENTIFIER.match(name):
        raise ValueError(f"Invalid table name {name!r}.")
    return name


class DataSource(Protocol):
    label: str

    def table(self, name: str) -> pd.DataFrame: ...

    def available_tables(self) -> set[str]: ...

    def version(self) -> str:
        """Changes whenever the underlying data changes; used to invalidate the query cache."""
        ...


class LocalCsvSource:
    """Reads ``<dir>/<table>.csv`` files written by ``sandbox/run_local.py``."""

    def __init__(self, directory: Path) -> None:
        self.directory = Path(directory)
        self.label = f"Local CSV ({self.directory})"

    def table(self, name: str) -> pd.DataFrame:
        path = self.directory / f"{_check_identifier(name)}.csv"
        if not path.exists():
            raise FileNotFoundError(f"{path} not found. Run .\\sandbox\\run_local.ps1 first.")
        return pd.read_csv(path)

    def available_tables(self) -> set[str]:
        if not self.directory.exists():
            return set()
        return {p.stem.lower() for p in self.directory.glob("*.csv")}

    def version(self) -> str:
        files = list(self.directory.glob("*.csv")) if self.directory.exists() else []
        return str(max((p.stat().st_mtime_ns for p in files), default=0))


class DatabricksSqlSource:
    """Reads ``<catalog>.<schema>.<table>`` through a SQL warehouse.

    Auth comes from ``databricks.sdk.core.Config``: the app service principal inside Databricks Apps,
    or DATABRICKS_HOST + DATABRICKS_TOKEN (or a CLI profile) on a laptop.
    """

    def __init__(self, catalog: str, schema: str, warehouse_id: str) -> None:
        self.catalog = _check_identifier(catalog)
        self.schema = _check_identifier(schema)
        self.warehouse_id = warehouse_id
        self.label = f"Databricks ({catalog}.{schema})"
        self._connection = None

    def _connect(self):
        from databricks import sql
        from databricks.sdk.core import Config

        cfg = Config()
        return sql.connect(
            server_hostname=cfg.host.removeprefix("https://").rstrip("/"),
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            credentials_provider=lambda: cfg.authenticate,
        )

    def _query(self, statement: str) -> pd.DataFrame:
        for attempt in range(2):
            try:
                if self._connection is None:
                    self._connection = self._connect()
                with self._connection.cursor() as cursor:
                    cursor.execute(statement)
                    return cursor.fetchall_arrow().to_pandas()
            except Exception:
                self._connection = None  # stale connection: reconnect once, then give up
                if attempt == 1:
                    raise
        raise AssertionError("unreachable")

    def table(self, name: str) -> pd.DataFrame:
        return self._query(f"SELECT * FROM `{self.catalog}`.`{self.schema}`.`{_check_identifier(name)}`")

    def available_tables(self) -> set[str]:
        frame = self._query(f"SHOW TABLES IN `{self.catalog}`.`{self.schema}`")
        return {str(name).lower() for name in frame["tableName"]}

    def version(self) -> str:
        try:
            frame = self._query(
                f"SELECT MAX(run_id) AS run_id FROM `{self.catalog}`.`{self.schema}`.`accelerator_run_log`"
            )
            return str(frame["run_id"].iloc[0])
        except Exception:  # noqa: BLE001 - no run log yet; fall back to a constant key
            return "no-run-log"


def build_source(settings: Settings) -> DataSource:
    if settings.source == "databricks":
        assert settings.warehouse_id is not None
        return DatabricksSqlSource(settings.catalog, settings.schema, settings.warehouse_id)
    return LocalCsvSource(settings.local_dir)
