"""Runtime settings, read from environment variables only (12-factor). See app/.env.example."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .auth import running_in_databricks_apps

APP_DIR = Path(__file__).resolve().parents[2]
REPO_ROOT = APP_DIR.parent
DEFAULT_TABLE_MAP = APP_DIR / "tables.yaml"

AUTH_MODES = ("auto", "app", "user", "sdk")
# Catalog, schema and table names. They are backtick-quoted in SQL, so hyphens are safe; backticks,
# dots, quotes, spaces and anything else are rejected outright.
SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,254}$")
_WAREHOUSE_ID = re.compile(r"^[A-Za-z0-9]+$")
_HTTP_PATH = re.compile(r"^/[A-Za-z0-9_/.\-]+$")


class SettingsError(ValueError):
    """Configuration is invalid. The message lists every problem and how to fix it."""


@dataclass(frozen=True)
class Settings:
    """Runtime settings.

    COCKPIT_SOURCE          local (CSV export of a sandbox run), databricks (SQL warehouse), or any kind
                            registered with ``datasource.register_source``
    COCKPIT_LOCAL_DIR       folder of <table>.csv files, default sandbox/local_output
    COCKPIT_CATALOG         default workspace
    COCKPIT_SCHEMA          default cockpit (rule R4)
    COCKPIT_TABLE_MAP       YAML logical -> physical table map, default app/tables.yaml if it exists
    COCKPIT_AUTH            auto | app | user | sdk (databricks only), default auto; user only inside Databricks Apps
    COCKPIT_MAX_ROWS        optional row cap applied with LIMIT
    COCKPIT_QUERY_TIMEOUT   statement timeout in seconds, default 300, 0 = warehouse default
    COCKPIT_FEATURES_FILE   default app/features.yaml
    DATABRICKS_WAREHOUSE_ID SQL warehouse id; injected by Databricks Apps via app.yaml
    DATABRICKS_HTTP_PATH    alternative to the warehouse id (e.g. a cluster's JDBC/ODBC path)
    DATABRICKS_HOST         workspace URL; databricks-sdk reads it (and the other auth variables) itself
    """

    source: str = "local"
    local_dir: Path = REPO_ROOT / "sandbox" / "local_output"
    catalog: str = "workspace"
    schema: str = "cockpit"
    table_map_file: Path | None = None
    auth: str = "auto"
    max_rows: int | None = None
    query_timeout: int = 300
    warehouse_id: str | None = None
    http_path: str | None = None
    host: str | None = None
    features_file: Path = APP_DIR / "features.yaml"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env
        defaults = cls()
        problems: list[str] = []

        def text(key: str) -> str | None:
            value = (env.get(key) or "").strip()
            return value or None

        def integer(key: str, default: int | None, minimum: int) -> int | None:
            raw = text(key)
            if raw is None:
                return default
            try:
                value = int(raw)
            except ValueError:
                problems.append(f"{key} must be an integer, got {raw!r}.")
                return default
            if value < minimum:
                problems.append(f"{key} must be >= {minimum}, got {value}.")
                return default
            return value

        table_map: Path | None = None
        explicit_map = text("COCKPIT_TABLE_MAP")
        if explicit_map:
            table_map = Path(explicit_map)
            if not table_map.is_file():
                problems.append(f"COCKPIT_TABLE_MAP points to {table_map}, which does not exist.")
        elif DEFAULT_TABLE_MAP.is_file():
            table_map = DEFAULT_TABLE_MAP

        settings = cls(
            source=(text("COCKPIT_SOURCE") or defaults.source).lower(),
            local_dir=Path(text("COCKPIT_LOCAL_DIR") or defaults.local_dir),
            catalog=text("COCKPIT_CATALOG") or defaults.catalog,
            schema=text("COCKPIT_SCHEMA") or defaults.schema,
            table_map_file=table_map,
            auth=(text("COCKPIT_AUTH") or defaults.auth).lower(),
            max_rows=integer("COCKPIT_MAX_ROWS", defaults.max_rows, 1),
            query_timeout=integer("COCKPIT_QUERY_TIMEOUT", defaults.query_timeout, 0) or 0,
            warehouse_id=text("DATABRICKS_WAREHOUSE_ID"),
            http_path=text("DATABRICKS_HTTP_PATH"),
            host=text("DATABRICKS_HOST"),
            features_file=Path(text("COCKPIT_FEATURES_FILE") or defaults.features_file),
        )
        if settings.auth == "user" and not running_in_databricks_apps(env):
            problems.append(
                "COCKPIT_AUTH=user only works inside Databricks Apps (its proxy sets the forwarded user "
                "headers). Use COCKPIT_AUTH=sdk on a laptop or in CI."
            )
        settings.validate(problems)
        return settings

    def validate(self, problems: list[str] | None = None) -> None:
        """Raise ``SettingsError`` listing every problem (plus any passed in)."""

        from .datasource import registered_sources  # late import: datasource imports settings

        problems = list(problems or [])
        kinds = registered_sources()
        if self.source not in kinds:
            problems.append(f"COCKPIT_SOURCE must be one of {', '.join(sorted(kinds))}, got {self.source!r}.")
        for key, value in (("COCKPIT_CATALOG", self.catalog), ("COCKPIT_SCHEMA", self.schema)):
            if not SQL_IDENTIFIER.match(value):
                problems.append(f"{key} must be a plain identifier (letters, digits, _ or -), got {value!r}.")
        if self.auth not in AUTH_MODES:
            problems.append(f"COCKPIT_AUTH must be one of {', '.join(AUTH_MODES)}, got {self.auth!r}.")
        if self.source == "databricks":
            if not (self.warehouse_id or self.http_path):
                problems.append(
                    "COCKPIT_SOURCE=databricks needs DATABRICKS_WAREHOUSE_ID (or DATABRICKS_HTTP_PATH). "
                    "In Databricks Apps it comes from the sql-warehouse resource in app.yaml."
                )
            if self.warehouse_id and not _WAREHOUSE_ID.match(self.warehouse_id):
                problems.append(f"DATABRICKS_WAREHOUSE_ID should be letters and digits only, got {self.warehouse_id!r}.")
            if self.http_path and not _HTTP_PATH.match(self.http_path):
                problems.append(
                    f"DATABRICKS_HTTP_PATH should look like /sql/1.0/warehouses/<id>, got {self.http_path!r}."
                )
        if problems:
            raise SettingsError("Invalid cockpit settings:\n- " + "\n- ".join(problems))
