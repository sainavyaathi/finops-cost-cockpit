"""Where the app reads gold tables from.

Pages only ever ask for a table by its logical name (``ctx.table("vw_executive_summary")``). A
``TableResolver`` maps that to a physical table (default ``<catalog>.<schema>.<name>``, overridable per
table with a YAML map), and a ``DataSource`` reads it. The same page therefore works on a laptop
against CSVs, in Databricks Apps against a SQL warehouse, or against an enterprise's own schemas.

Adding a backend: write a class that satisfies ``DataSource`` and call ``register_source(kind, factory)``
(in this module, or in a module listed in ``COCKPIT_SOURCE_PLUGINS``).
"""

from __future__ import annotations

import importlib
import os
import re
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Protocol

import pandas as pd
import yaml

from . import auth, contract
from .settings import SQL_IDENTIFIER, Settings

CheckResult = tuple[str, bool, str]


# ---------------------------------------------------------------- errors

class DataSourceError(RuntimeError):
    """A data problem with a human-readable message, safe to show in the UI.

    ``category`` is one of ``CATEGORIES``; ``table`` is the logical table involved, if any; ``hint`` says
    what to do; ``detail`` is the underlying driver message (never contains credentials).
    """

    CATEGORIES = ("config", "auth", "permission", "missing_table", "warehouse", "timeout", "connection", "unknown")

    def __init__(
        self,
        message: str,
        *,
        category: str = "unknown",
        table: str | None = None,
        hint: str | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.category = category if category in self.CATEGORIES else "unknown"
        self.table = table
        self.hint = hint
        self.detail = detail

    @property
    def retryable(self) -> bool:
        """Only dropped or stale connections are worth one reconnect."""
        return self.category == "connection"


# Databricks error classes ("[TABLE_OR_VIEW_NOT_FOUND] ...") and SQLSTATEs ("SQLSTATE: 42P01").
_ERROR_CLASS = re.compile(r"\[([A-Z][A-Z0-9_]{2,})(?:\.[A-Z0-9_]+)*\]")
_SQLSTATE = re.compile(r"\bSQLSTATE:?\s*([0-9A-Z]{5})\b")
_CLASS_CATEGORY = {
    "TABLE_OR_VIEW_NOT_FOUND": "missing_table",
    "SCHEMA_NOT_FOUND": "missing_table",
    "CATALOG_NOT_FOUND": "missing_table",
    "PERMISSION_DENIED": "permission",
    "INSUFFICIENT_PERMISSIONS": "permission",
    "UNAUTHORIZED_ACCESS": "permission",
}
_SQLSTATE_CATEGORY = {"42P01": "missing_table", "3F000": "missing_table", "42501": "permission", "HYT00": "timeout"}
_SQLSTATE_CLASS_CATEGORY = {"08": "connection", "28": "auth"}
_HTTP_CATEGORY = {401: "auth", 403: "auth", 404: "warehouse", 429: "connection", 502: "connection",
                  503: "connection", 504: "connection"}
# Driver exception types that mean "the transport failed", not "the query failed".
_CONNECTION_TYPES = {"RequestError", "OperationalError", "InvalidServerResponseError", "MaxRetryDurationError",
                     "NonRecoverableNetworkError", "SessionAlreadyClosedError", "CursorAlreadyClosedError"}
# Last resort: word-bounded phrases.
_PHRASES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("missing_table", re.compile(r"\b(table or view|schema|catalog)\b[^.]{0,120}\bcannot be found\b", re.I)),
    ("permission", re.compile(r"\bpermission denied\b|\bdoes not have\b[^.]{0,80}\b(privileges?|permissions?)\b", re.I)),
    ("timeout", re.compile(r"\b(timed out|statement_timeout)\b", re.I)),
    ("warehouse", re.compile(r"\bwarehouse\b[^.]{0,80}\b(stopped|not running|is starting|does not exist|deleted)\b", re.I)),
    ("auth", re.compile(r"\b(invalid access token|invalid_token|token (is |has )?expired)\b", re.I)),
)


def _http_status(exc: BaseException) -> int | None:
    context = getattr(exc, "context", None)
    candidates = [
        context.get("http-code") if isinstance(context, dict) else None,
        getattr(exc, "status_code", None),
        getattr(getattr(exc, "response", None), "status_code", None),
    ]
    for value in candidates:
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _category(exc: BaseException, text: str) -> str:
    for error_class in _ERROR_CLASS.findall(text):
        if error_class in _CLASS_CATEGORY:
            return _CLASS_CATEGORY[error_class]
        if "TIMEOUT" in error_class:
            return "timeout"
    state = _SQLSTATE.search(text)
    if state:
        code = state.group(1)
        category = _SQLSTATE_CATEGORY.get(code) or _SQLSTATE_CLASS_CATEGORY.get(code[:2])
        if category:
            return category
    status = _http_status(exc)
    if status in _HTTP_CATEGORY:
        return _HTTP_CATEGORY[status]
    if isinstance(exc, ConnectionError) or {t.__name__ for t in type(exc).__mro__} & _CONNECTION_TYPES:
        return "connection"
    return next((category for category, pattern in _PHRASES if pattern.search(text)), "unknown")


def classify_error(exc: BaseException, *, table: str | None = None, where: str | None = None) -> DataSourceError:
    """Map a driver exception to a ``DataSourceError`` with a message a user can act on.

    Uses the error class, SQLSTATE, HTTP status and exception type first; message phrases are a
    word-bounded fallback.
    """

    if isinstance(exc, DataSourceError):
        return exc
    detail = f"{type(exc).__name__}: {exc}"[:2000]
    category = _category(exc, detail)
    target = where or table or "the requested data"
    messages = {
        "missing_table": (f"{target} does not exist or is not visible to this identity.",
                          "Run the pipeline into this schema, or map the table in tables.yaml."),
        "permission": (f"Permission denied on {target}.",
                       "Grant USE CATALOG, USE SCHEMA and SELECT on the schema to the app identity."),
        "timeout": ("The query timed out.", "Raise COCKPIT_QUERY_TIMEOUT or set COCKPIT_MAX_ROWS."),
        "warehouse": ("The SQL warehouse is stopped, starting or does not exist.",
                      "Start the warehouse, or check DATABRICKS_WAREHOUSE_ID."),
        "auth": ("Databricks rejected the credentials, or the identity cannot use the warehouse.",
                 "Check the auth settings and that the identity has CAN USE on the warehouse."),
        "connection": ("Could not connect to Databricks.", "Check DATABRICKS_HOST and network access."),
        "unknown": (f"Reading {target} failed.", None),
    }
    message, hint = messages[category]
    return DataSourceError(message, category=category, table=table, hint=hint, detail=detail)


class InvalidIdentifier(DataSourceError, ValueError):
    """A table, schema or catalog name failed validation (possible SQL injection)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, category="config")


# ---------------------------------------------------------------- table resolution

def check_logical_name(name: str) -> str:
    """Logical names are plain identifiers (they double as CSV file names)."""
    if not isinstance(name, str) or not contract.is_valid_logical_name(name):
        raise InvalidIdentifier(f"Invalid table name {name!r}.")
    return name.lower()


def _check_part(part: str, what: str) -> str:
    if not SQL_IDENTIFIER.match(part):
        raise InvalidIdentifier(f"Invalid {what} {part!r}: use letters, digits, '_' or '-' only.")
    return part


@dataclass(frozen=True)
class TableRef:
    catalog: str
    schema: str
    table: str

    @property
    def sql(self) -> str:
        # Every part passed SQL_IDENTIFIER, which excludes backticks, so quoting cannot be escaped.
        return f"`{self.catalog}`.`{self.schema}`.`{self.table}`"

    @property
    def dotted(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


class TableResolver:
    """Logical table name -> physical ``TableRef``.

    ``mapping`` values may be ``table``, ``schema.table`` or ``catalog.schema.table``; missing parts come
    from the default catalog and schema.
    """

    def __init__(self, catalog: str, schema: str, mapping: Mapping[str, str] | None = None) -> None:
        self.catalog = _check_part(catalog, "catalog")
        self.schema = _check_part(schema, "schema")
        self._refs: dict[str, TableRef] = {}
        for logical, physical in (mapping or {}).items():
            self._refs[check_logical_name(logical)] = self._parse(logical, physical)

    def _parse(self, logical: str, physical: Any) -> TableRef:
        if not isinstance(physical, str) or not physical.strip():
            raise DataSourceError(f"Table map entry {logical!r} must be a non-empty string.", category="config")
        parts = physical.strip().split(".")
        if len(parts) > 3:
            raise DataSourceError(
                f"Table map entry {logical!r} -> {physical!r}: use table, schema.table or catalog.schema.table.",
                category="config",
            )
        parts = [_check_part(p, "identifier") for p in parts]
        defaults = [self.catalog, self.schema]
        catalog, schema, table = [*defaults[: 3 - len(parts)], *parts]
        return TableRef(catalog, schema, table)

    @property
    def mapped(self) -> Mapping[str, TableRef]:
        return dict(self._refs)

    def resolve(self, name: str) -> TableRef:
        logical = check_logical_name(name)
        return self._refs.get(logical) or TableRef(self.catalog, self.schema, logical)

    def fingerprint(self) -> str:
        body = ";".join(f"{k}={v.dotted}" for k, v in sorted(self._refs.items()))
        return auth.fingerprint(f"{self.catalog}.{self.schema}|{body}", 12)


def load_table_map(path: Path | None) -> dict[str, str]:
    """Read ``tables:`` from a YAML file. ``None`` or an empty file means no overrides."""

    if path is None:
        return {}
    try:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise DataSourceError(f"Cannot read table map {path}: {exc}", category="config") from exc
    if not isinstance(data, dict) or not isinstance(data.get("tables") or {}, dict):
        raise DataSourceError(f"Table map {path} must contain a 'tables:' mapping.", category="config")
    return {str(k): v for k, v in (data.get("tables") or {}).items()}


def resolver_for(settings: Settings) -> TableResolver:
    return TableResolver(settings.catalog, settings.schema, load_table_map(settings.table_map_file))


# ---------------------------------------------------------------- protocol

class DataSource(Protocol):
    kind: str
    label: str

    def table(self, name: str) -> pd.DataFrame:
        """Rows of the logical table ``name``. Raises ``DataSourceError``."""
        ...

    def available_tables(self) -> set[str]:
        """Logical names that exist. Unreadable schemas are skipped, not fatal."""
        ...

    def version(self) -> str:
        """Changes whenever the underlying data changes; used to invalidate the query cache."""
        ...

    def cache_key(self) -> str:
        """Non-secret key that separates cached data per source, config and (if OBO) per user."""
        ...

    def describe(self) -> dict[str, str]:
        """Non-secret facts for diagnostics."""
        ...

    def check(self) -> list[CheckResult]:
        """Connection checks as (name, ok, detail). Never raises."""
        ...


def _contract_checks(available: set[str], probe: Callable[[str], list[str]], where: Callable[[str], str]) -> list[CheckResult]:
    results: list[CheckResult] = []
    for table in contract.CONTRACT.values():
        label = f"Table {table.name}"
        if table.name not in available:
            if table.planned:
                results.append((label, True, f"planned ({table.feature}); not produced yet"))
            else:
                results.append((label, False, f"not found at {where(table.name)}"))
            continue
        try:
            missing = contract.missing_columns(table.name, probe(table.name))
        except DataSourceError as exc:
            results.append((label, False, exc.message))
            continue
        if missing:
            results.append((label, False, f"{where(table.name)} lacks column(s): {', '.join(missing)}"))
        else:
            results.append((label, True, where(table.name)))
    return results


# ---------------------------------------------------------------- local CSV

class LocalCsvSource:
    """Reads ``<dir>/<table>.csv`` files written by ``sandbox/run_local.py``.

    With a table map, only the table part of the mapped name is used (catalog and schema mean nothing
    for files).
    """

    kind = "local"

    def __init__(self, directory: Path, *, resolver: TableResolver | None = None, max_rows: int | None = None) -> None:
        self.directory = Path(directory)
        self.resolver = resolver
        self.max_rows = max_rows
        self.label = f"Local CSV ({self.directory})"

    def _path(self, name: str) -> Path:
        stem = self.resolver.resolve(name).table if self.resolver else check_logical_name(name)
        return self.directory / f"{stem}.csv"

    def table(self, name: str) -> pd.DataFrame:
        path = self._path(name)
        if not path.exists():
            raise DataSourceError(
                f"{path} not found.", category="missing_table", table=name.lower(),
                hint="Run the sandbox pipeline first (sandbox/run_local.py).",
            )
        return pd.read_csv(path, nrows=self.max_rows)

    def available_tables(self) -> set[str]:
        if not self.directory.exists():
            return set()
        names = {p.stem.lower() for p in self.directory.glob("*.csv")}
        if self.resolver:
            mapped = self.resolver.mapped
            targets = {ref.table.lower() for ref in mapped.values()}
            names = {n for n in names if n not in mapped and n not in targets}
            names |= {logical for logical in mapped if self._path(logical).exists()}
        return names

    def version(self) -> str:
        files = list(self.directory.glob("*.csv")) if self.directory.exists() else []
        return str(max((p.stat().st_mtime_ns for p in files), default=0))

    def cache_key(self) -> str:
        mapping = self.resolver.fingerprint() if self.resolver else "-"
        return f"local|{self.directory}|{mapping}|{self.max_rows}"

    def describe(self) -> dict[str, str]:
        return {"kind": self.kind, "label": self.label, "directory": str(self.directory),
                "max rows": str(self.max_rows or "unlimited")}

    def check(self) -> list[CheckResult]:
        if not self.directory.is_dir():
            return [("Directory", False, f"{self.directory} does not exist")]
        available = self.available_tables()
        results: list[CheckResult] = [("Directory", True, str(self.directory)),
                                      ("List tables", True, f"{len(available)} CSV file(s)")]
        probe = lambda name: list(pd.read_csv(self._path(name), nrows=0).columns)  # noqa: E731
        return results + _contract_checks(available, probe, lambda name: str(self._path(name)))


# ---------------------------------------------------------------- Databricks SQL warehouse

Connect = Callable[..., Any]


def _default_connect(**kwargs: Any) -> Any:
    from databricks import sql  # lazy: local mode and tests need no connector or credentials

    return sql.connect(**kwargs)


def _close_quietly(connection: Any) -> None:
    try:
        connection.close()
    except Exception:  # noqa: BLE001
        pass


class _TTLCache:
    """Small thread-safe dict with per-entry expiry and a size cap (oldest evicted first)."""

    def __init__(self, ttl_seconds: float, max_entries: int, clock: Callable[[], float] = time.monotonic) -> None:
        self._data: OrderedDict[Any, tuple[float, Any]] = OrderedDict()
        self._lock = threading.Lock()
        self._ttl = ttl_seconds
        self._max = max_entries
        self._clock = clock

    def get(self, key: Any) -> Any | None:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            if self._clock() - entry[0] >= self._ttl:
                del self._data[key]
                return None
            return entry[1]

    def put(self, key: Any, value: Any) -> None:
        with self._lock:
            self._data.pop(key, None)
            self._data[key] = (self._clock(), value)
            while len(self._data) > self._max:
                self._data.popitem(last=False)

    def __len__(self) -> int:
        return len(self._data)


class _Pool:
    """Idle connections per identity. Thread-safe; Streamlit runs every session in its own thread.

    Idle connections are closed after ``max_idle_seconds``; at most ``max_identities`` identities keep
    idle connections (least recently used evicted), each with at most ``max_idle`` connections.
    """

    def __init__(self, max_idle: int = 4, max_idle_seconds: float = 600.0, max_identities: int = 64,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self._idle: OrderedDict[str, list[tuple[str, Any, float]]] = OrderedDict()
        self._lock = threading.Lock()
        self._max_idle = max_idle
        self._max_idle_seconds = max_idle_seconds
        self._max_identities = max_identities
        self._clock = clock

    def _sweep(self, now: float) -> list[Any]:
        expired: list[Any] = []
        for key in list(self._idle):
            keep = [e for e in self._idle[key] if now - e[2] < self._max_idle_seconds]
            expired += [e[1] for e in self._idle[key] if now - e[2] >= self._max_idle_seconds]
            if keep:
                self._idle[key] = keep
            else:
                del self._idle[key]
        return expired

    def acquire(self, key: str, token_fp: str) -> Any | None:
        with self._lock:
            to_close = self._sweep(self._clock())
            entries = self._idle.pop(key, [])
            to_close += [c for fp, c, _ in entries if fp != token_fp]  # the user's token rotated
            fresh = [e for e in entries if e[0] == token_fp]
            connection = fresh.pop()[1] if fresh else None
            if fresh:
                self._idle[key] = fresh
        for c in to_close:
            _close_quietly(c)
        return connection

    def release(self, key: str, token_fp: str, connection: Any) -> None:
        to_close: list[Any] = []
        with self._lock:
            now = self._clock()
            to_close += self._sweep(now)
            entries = self._idle.pop(key, [])
            if len(entries) < self._max_idle:
                entries.append((token_fp, connection, now))
            else:
                to_close.append(connection)
            self._idle[key] = entries  # most recently used last
            while len(self._idle) > self._max_identities:
                _, evicted = self._idle.popitem(last=False)
                to_close += [c for _, c, _ in evicted]
        for c in to_close:
            _close_quietly(c)

    def idle_count(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._idle.values())


@dataclass(frozen=True)
class _Identity:
    key: str  # non-secret: "app:service" or "user:<hash of the verified user name>"
    token_fp: str  # non-secret fingerprint, used only to detect token rotation
    user: str | None  # verified via current_user(), never taken from a header
    connect_kwargs: dict[str, Any] = field(repr=False, compare=False)  # may hold the user's token


@dataclass(frozen=True)
class _Listing:
    tables: frozenset[str]
    errors: tuple[tuple[str, str], ...]  # (catalog.schema, message) for schemas that could not be listed


_SESSION_EXPIRED = (
    "Your Databricks session token expired or was rejected. Reload the page to sign in again.",
    "Databricks Apps hands the app a fresh token when the page loads. If reloading does not help, check "
    "that you have CAN USE on the warehouse and that user authorization (scope 'sql') is enabled.",
)


class DatabricksSqlSource:
    """Reads tables through a Databricks SQL warehouse (see ``auth`` for the identity modes).

    One instance is shared by every Streamlit session (``st.cache_resource``), so it holds no
    per-request state: listings and resolved users live in caches keyed by a verified identity.
    """

    kind = "databricks"
    LISTING_TTL_SECONDS = 60.0
    USER_TTL_SECONDS = 900.0

    def __init__(
        self,
        catalog: str,
        schema: str,
        warehouse_id: str | None = None,
        *,
        http_path: str | None = None,
        auth_mode: str = "auto",
        host: str | None = None,
        resolver: TableResolver | None = None,
        max_rows: int | None = None,
        query_timeout: int = 300,
        table_map_file: Path | None = None,
        connect: Connect | None = None,
        headers: auth.HeaderProvider | None = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        if not (warehouse_id or http_path):
            raise DataSourceError("Databricks source needs a warehouse id or an HTTP path.", category="config")
        self.resolver = resolver or TableResolver(catalog, schema)
        self.catalog, self.schema = self.resolver.catalog, self.resolver.schema
        self.warehouse_id = warehouse_id
        self.http_path = http_path or f"/sql/1.0/warehouses/{warehouse_id}"
        self.auth_mode = auth.resolve_auth_mode(auth_mode, env)
        if self.auth_mode == "user" and headers is None and not auth.running_in_databricks_apps(env):
            # Outside Apps nothing strips client-supplied X-Forwarded-* headers. Injected header
            # providers (tests) are the only escape hatch.
            raise DataSourceError(
                "COCKPIT_AUTH=user only works inside Databricks Apps.", category="config",
                hint="Use COCKPIT_AUTH=sdk on a laptop or in CI, or COCKPIT_AUTH=app for the service principal.",
            )
        self.host = auth.normalize_host(host) if host else None
        self.max_rows = max_rows
        self.query_timeout = query_timeout
        self.table_map_file = table_map_file
        self.label = f"Databricks ({self.catalog}.{self.schema})"
        self._connect = connect or _default_connect
        self._headers = headers or auth.streamlit_headers
        self._pool = _Pool()
        self._sdk_config: Any = None
        self._sdk_lock = threading.Lock()
        self._listings = _TTLCache(self.LISTING_TTL_SECONDS, max_entries=256)
        self._users = _TTLCache(self.USER_TTL_SECONDS, max_entries=1024)  # token fingerprint -> user

    def __repr__(self) -> str:
        return f"DatabricksSqlSource({self.label}, http_path={self.http_path!r}, auth={self.auth_mode!r})"

    # -- identity ------------------------------------------------------------

    def _base_kwargs(self, host: str) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "server_hostname": host,
            "http_path": self.http_path,
            "user_agent_entry": "finops-cost-cockpit",
        }
        if self.query_timeout > 0:
            kwargs["session_configuration"] = {"STATEMENT_TIMEOUT": str(self.query_timeout)}
        return kwargs

    def _config(self) -> Any:
        with self._sdk_lock:
            if self._sdk_config is None:
                try:
                    self._sdk_config = auth.sdk_config()
                except Exception as exc:  # noqa: BLE001 - SDK raises ValueError with a debug summary
                    raise DataSourceError(
                        "Could not resolve Databricks credentials.", category="auth",
                        hint="Set DATABRICKS_HOST plus a token, profile or OAuth client (see docs/connecting_workspaces.md).",
                        detail=str(exc),
                    ) from exc
                if not self.host and self._sdk_config.host:
                    self.host = auth.normalize_host(self._sdk_config.host)
            return self._sdk_config

    def _forwarded(self) -> auth.ForwardedUser:
        try:
            return auth.forwarded_user(self._headers())
        except auth.MissingUserToken as exc:
            raise DataSourceError(str(exc), category="auth") from exc

    def _identity(self) -> _Identity:
        if self.auth_mode != "user":
            cfg = self._config()
            kwargs = {**self._base_kwargs(self.host or ""), "credentials_provider": lambda: cfg.authenticate}
            return _Identity(f"{self.auth_mode}:service", "", None, kwargs)

        forwarded = self._forwarded()
        host = self.host or auth.normalize_host(os.environ.get("DATABRICKS_HOST", ""))
        if not host:
            raise DataSourceError("COCKPIT_AUTH=user needs DATABRICKS_HOST.", category="config")
        kwargs = {**self._base_kwargs(host), "access_token": forwarded.token}
        fp = forwarded.token_fingerprint
        user = self._users.get(fp)
        if user is None:
            user = self._whoami(fp, kwargs)
            self._users.put(fp, user)
        return _Identity(f"user:{auth.identity_key(user)}", fp, user, kwargs)

    def _whoami(self, token_fp: str, kwargs: dict[str, Any]) -> str:
        """Resolve the identity behind a token with ``current_user()``; headers are never trusted."""
        for attempt in (1, 2):
            connection = None
            try:
                connection = self._connect(**kwargs)
                with connection.cursor() as cursor:
                    cursor.execute("SELECT current_user() AS user")
                    frame = cursor.fetchall_arrow().to_pandas()
                user = str(frame.iloc[0, 0]).strip()
                if not user:
                    raise DataSourceError("Databricks returned no user for the forwarded token.", category="auth")
                self._pool.release(f"user:{auth.identity_key(user)}", token_fp, connection)
                return user
            except Exception as exc:  # noqa: BLE001 - classified below
                if connection is not None:
                    _close_quietly(connection)
                error = self._classify(exc, where="the signed-in user")
                if attempt == 2 or not error.retryable:
                    raise error from exc
        raise AssertionError("unreachable")

    def _classify(self, exc: BaseException, *, table: str | None = None, where: str | None = None) -> DataSourceError:
        if isinstance(exc, DataSourceError):
            return exc
        error = classify_error(exc, table=table, where=where)
        if self.auth_mode == "user" and error.category == "auth":
            message, hint = _SESSION_EXPIRED
            return DataSourceError(message, category="auth", table=table, hint=hint, detail=error.detail)
        return error

    @contextmanager
    def _connection(self, identity: _Identity) -> Iterator[Any]:
        connection = self._pool.acquire(identity.key, identity.token_fp)
        if connection is None:
            connection = self._connect(**identity.connect_kwargs)
        healthy = False
        try:
            yield connection
            healthy = True
        finally:
            if healthy:
                self._pool.release(identity.key, identity.token_fp, connection)
            else:
                _close_quietly(connection)

    def _query(
        self, statement: str, *, table: str | None = None, where: str | None = None, identity: _Identity | None = None
    ) -> pd.DataFrame:
        for attempt in (1, 2):
            try:
                current = identity or self._identity()
                with self._connection(current) as connection, connection.cursor() as cursor:
                    cursor.execute(statement)
                    return cursor.fetchall_arrow().to_pandas()
            except Exception as exc:  # noqa: BLE001 - classified below
                error = self._classify(exc, table=table, where=where)
                if attempt == 2 or not error.retryable:
                    raise error from exc
                # stale or dropped connection: it was closed above; reconnect once
        raise AssertionError("unreachable")

    # -- DataSource ------------------------------------------------------------

    def select_statement(self, name: str, *, limit: int | None = None) -> str:
        ref = self.resolver.resolve(name)
        limit = self.max_rows if limit is None else limit
        return f"SELECT * FROM {ref.sql}" + (f" LIMIT {int(limit)}" if limit is not None else "")

    def table(self, name: str) -> pd.DataFrame:
        ref = self.resolver.resolve(name)
        return self._query(self.select_statement(name), table=name.lower(), where=ref.dotted)

    def _columns(self, name: str, identity: _Identity) -> list[str]:
        ref = self.resolver.resolve(name)
        frame = self._query(self.select_statement(name, limit=0), table=name.lower(), where=ref.dotted, identity=identity)
        return list(frame.columns)

    def _listing(self, identity: _Identity) -> _Listing:
        """Tables per mapped schema for this identity, cached per identity (never shared across users)."""
        cache_key = (identity.key, self.resolver.fingerprint())
        cached = self._listings.get(cache_key)
        if cached is not None:
            return cached
        mapped = self.resolver.mapped
        schemas = {(self.catalog, self.schema)} | {(r.catalog, r.schema) for r in mapped.values()}
        found_by_schema: dict[tuple[str, str], frozenset[str]] = {}
        errors: list[tuple[str, str]] = []
        for catalog, schema in sorted(schemas):
            try:
                frame = self._query(f"SHOW TABLES IN `{catalog}`.`{schema}`", where=f"schema {catalog}.{schema}",
                                    identity=identity)
            except DataSourceError as exc:
                if exc.category in {"auth", "warehouse", "connection", "config", "timeout"}:
                    raise  # the whole source is down, not just one schema
                errors.append((f"{catalog}.{schema}", exc.message))
                continue
            found_by_schema[(catalog, schema)] = frozenset(str(n).lower() for n in frame["tableName"])
        default = found_by_schema.get((self.catalog, self.schema), frozenset())
        targets = {ref.table.lower() for ref in mapped.values() if (ref.catalog, ref.schema) == (self.catalog, self.schema)}
        found = {name for name in default if name not in mapped and name not in targets}
        for logical, ref in mapped.items():
            if ref.table.lower() in found_by_schema.get((ref.catalog, ref.schema), frozenset()):
                found.add(logical)
        listing = _Listing(frozenset(found), tuple(errors))
        self._listings.put(cache_key, listing)
        return listing

    def available_tables(self) -> set[str]:
        return set(self._listing(self._identity()).tables)

    def version(self) -> str:
        ref = self.resolver.resolve("accelerator_run_log")
        try:
            frame = self._query(f"SELECT MAX(run_id) AS run_id FROM {ref.sql}", table="accelerator_run_log",
                                where=ref.dotted)
        except DataSourceError as exc:
            if exc.category == "missing_table":
                return "no-run-log"  # no run log yet; a constant key is correct
            raise  # transient: must not be cached as a constant version
        return str(frame["run_id"].iloc[0])

    def cache_key(self) -> str:
        who = self._identity().key if self.auth_mode == "user" else self.auth_mode
        return f"databricks|{self.http_path}|{self.resolver.fingerprint()}|{self.max_rows}|{who}"

    def describe(self) -> dict[str, str]:
        info = {
            "kind": self.kind,
            "label": self.label,
            "host": self.host or os.environ.get("DATABRICKS_HOST") or "(resolved by databricks-sdk)",
            "warehouse id": self.warehouse_id or "-",
            "http path": self.http_path,
            "catalog": self.catalog,
            "schema": self.schema,
            "auth mode": self.auth_mode,
            "table map": str(self.table_map_file) if self.table_map_file else "none",
            "mapped tables": str(len(self.resolver.mapped)),
            "max rows": str(self.max_rows or "unlimited"),
            "query timeout": f"{self.query_timeout}s" if self.query_timeout else "warehouse default",
        }
        if self.auth_mode == "user":
            # Only a user already verified for this token is shown; describe() never queries.
            try:
                info["user"] = self._users.get(self._forwarded().token_fingerprint) or "(not verified yet)"
            except DataSourceError:
                info["user"] = "(no forwarded user token)"
        return info

    def check(self) -> list[CheckResult]:
        try:
            identity = self._identity()
            self._query("SELECT 1 AS ok", identity=identity)
        except DataSourceError as exc:
            return [("Connect", False, f"{exc.message} {exc.hint or ''}".strip())]
        results: list[CheckResult] = [("Connect", True, f"{self.host or 'workspace'} {self.http_path}")]
        try:
            listing = self._listing(identity)
        except DataSourceError as exc:
            return results + [("List tables", False, exc.message)]
        results += [(f"Schema {schema}", False, message) for schema, message in listing.errors]
        results.append(("List tables", not listing.errors, f"{len(listing.tables)} table(s) found"))
        return results + _contract_checks(
            set(listing.tables), lambda n: self._columns(n, identity), lambda n: self.resolver.resolve(n).dotted
        )


# ---------------------------------------------------------------- registry

SourceFactory = Callable[[Settings], DataSource]
_SOURCES: dict[str, SourceFactory] = {}
_plugins_loaded = False


def register_source(kind: str, factory: SourceFactory, *, replace: bool = False) -> None:
    """Make ``COCKPIT_SOURCE=<kind>`` build a source with ``factory(settings)``."""
    kind = kind.lower()
    if not contract.is_valid_logical_name(kind):
        raise ValueError(f"Invalid source kind {kind!r}.")
    if kind in _SOURCES and not replace:
        raise ValueError(f"Source kind {kind!r} is already registered.")
    _SOURCES[kind] = factory


def _load_plugins() -> None:
    """Import modules named in COCKPIT_SOURCE_PLUGINS (comma-separated); they call register_source."""
    global _plugins_loaded
    if _plugins_loaded:
        return
    _plugins_loaded = True
    for module in filter(None, (m.strip() for m in os.environ.get("COCKPIT_SOURCE_PLUGINS", "").split(","))):
        importlib.import_module(module)


def registered_sources() -> frozenset[str]:
    _load_plugins()
    return frozenset(_SOURCES)


def build_source(settings: Settings) -> DataSource:
    _load_plugins()
    factory = _SOURCES.get(settings.source)
    if factory is None:
        raise DataSourceError(
            f"Unknown COCKPIT_SOURCE {settings.source!r}; registered: {', '.join(sorted(_SOURCES))}.",
            category="config",
        )
    return factory(settings)


def _local(settings: Settings) -> DataSource:
    return LocalCsvSource(settings.local_dir, resolver=resolver_for(settings), max_rows=settings.max_rows)


def _databricks(settings: Settings) -> DataSource:
    return DatabricksSqlSource(
        settings.catalog,
        settings.schema,
        settings.warehouse_id,
        http_path=settings.http_path,
        auth_mode=settings.auth,
        host=settings.host,
        resolver=resolver_for(settings),
        max_rows=settings.max_rows,
        query_timeout=settings.query_timeout,
        table_map_file=settings.table_map_file,
    )


register_source("local", _local)
register_source("databricks", _databricks)
