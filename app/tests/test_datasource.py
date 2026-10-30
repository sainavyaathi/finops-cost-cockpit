"""Data-access layer tests. No network: the SQL connector is replaced by fakes."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import pandas as pd
import pytest

from cockpit_ui.core import auth, contract
from cockpit_ui.core import datasource as ds
from cockpit_ui.core.datasource import (
    DatabricksSqlSource,
    DataSourceError,
    InvalidIdentifier,
    LocalCsvSource,
    TableResolver,
    build_source,
    classify_error,
    load_table_map,
    register_source,
)
from cockpit_ui.core.settings import Settings, SettingsError

TOKEN_A = "dapi-secret-token-for-alice"
TOKEN_A2 = "dapi-rotated-token-for-alice"
TOKEN_B = "dapi-secret-token-for-bob"
USERS = {TOKEN_A: "alice@corp.com", TOKEN_A2: "alice@corp.com", TOKEN_B: "bob@corp.com"}
IN_APPS = {"DATABRICKS_APP_NAME": "finops-cost-cockpit"}


# ---------------------------------------------------------------- fakes

class _Result:
    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame

    def to_pandas(self) -> pd.DataFrame:
        return self._frame


class FakeConnection:
    def __init__(self, server: "FakeServer", kwargs: dict[str, Any]) -> None:
        self.server, self.kwargs, self.closed = server, kwargs, False

    @property
    def user(self) -> str:
        return USERS.get(self.kwargs.get("access_token", ""), "service-principal")

    def cursor(self) -> "FakeCursor":
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = True


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self._frame: pd.DataFrame | None = None

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, statement: str) -> None:
        self.connection.server.statements.append(statement)
        self._frame = self.connection.server.respond(statement, self.connection.user)

    def fetchall_arrow(self) -> _Result:
        assert self._frame is not None
        return _Result(self._frame)


class FakeServer:
    """Answers SQL by statement prefix; ``failures`` raise once each, in order."""

    def __init__(self, schemas: dict[str, list[str]] | None = None) -> None:
        self.schemas = schemas or {}
        self.statements: list[str] = []
        self.connections: list[FakeConnection] = []
        self.failures: list[Exception] = []
        self.denied: dict[str, set[str]] = {}  # user -> schemas that user cannot list

    def connect(self, **kwargs: Any) -> FakeConnection:
        connection = FakeConnection(self, kwargs)
        self.connections.append(connection)
        return connection

    def respond(self, statement: str, user: str) -> pd.DataFrame:
        if statement.startswith("SELECT current_user()"):
            return pd.DataFrame({"user": [user]})
        if self.failures:
            raise self.failures.pop(0)
        if statement.startswith("SHOW TABLES IN "):
            schema = statement.removeprefix("SHOW TABLES IN ").replace("`", "")
            if schema in self.denied.get(user, set()) or schema in self.denied.get("*", set()):
                raise RuntimeError(f"[INSUFFICIENT_PERMISSIONS] User does not have USE SCHEMA on {schema}")
            if schema not in self.schemas:
                raise RuntimeError(f"[SCHEMA_NOT_FOUND] The schema `{schema}` cannot be found. SQLSTATE: 42704")
            return pd.DataFrame({"database": schema, "tableName": self.schemas[schema]})
        if statement.startswith("SELECT MAX(run_id)"):
            return pd.DataFrame({"run_id": [7]})
        return pd.DataFrame({"a": [1, 2], "who": [user, user]})


class FakeConfig:
    host = "https://adb-123.azuredatabricks.net/"

    def authenticate(self) -> dict[str, str]:
        return {"Authorization": "Bearer sp-token"}


class HttpError(Exception):
    def __init__(self, message: str, code: int) -> None:
        super().__init__(message)
        self.context = {"http-code": code}


@pytest.fixture
def fake_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(auth, "sdk_config", lambda **_: FakeConfig())


def make_source(server: FakeServer, *, headers: Callable[[], dict[str, str]] | None = None, **kwargs: Any) -> DatabricksSqlSource:
    kwargs.setdefault("auth_mode", "sdk")
    if kwargs["auth_mode"] == "user":
        kwargs.setdefault("host", "h")
    return DatabricksSqlSource(
        kwargs.pop("catalog", "workspace"), kwargs.pop("schema", "cockpit"), "abc123",
        connect=server.connect, headers=headers or (lambda: {}), **kwargs,
    )


def obo_headers(email: str, token: str) -> Callable[[], dict[str, str]]:
    return lambda: {"X-Forwarded-Email": email, "X-Forwarded-Access-Token": token}


class Switch:
    """Mutable header provider: simulates different requests hitting one shared source."""

    def __init__(self, email: str, token: str) -> None:
        self.set(email, token)

    def set(self, email: str, token: str) -> None:
        self.headers = obo_headers(email, token)()

    def __call__(self) -> dict[str, str]:
        return self.headers


# ---------------------------------------------------------------- contract

def test_contract_covers_every_table_pages_require():
    from cockpit_ui.core.registry import discover

    for feature in discover().features:
        for name in feature.requires:
            assert contract.get(name) is not None, f"{feature.id}: {name} missing from contract"


def test_contract_matches_engine_outputs_and_marks_planned_tables():
    from databricks_finops.config import OUTPUT_TABLES, OUTPUT_VIEWS

    produced = {t.name for t in contract.produced_tables()}
    assert produced == {name.lower() for name in [*OUTPUT_TABLES, *OUTPUT_VIEWS]}
    planned = {t.name for t in contract.CONTRACT.values() if t.planned}
    assert planned == {"cost_base", "waste_flags", "catalog_allocation", "allpurpose_misuse", "idle_compute", "alert_top_flags"}


def test_required_columns_match_fixtures(fixtures_dir: Path):
    for path in fixtures_dir.glob("*.csv"):
        assert contract.check_frame(path.stem, pd.read_csv(path)) == []


def test_required_columns_match_sandbox_output():
    output = Path(__file__).resolve().parents[2] / "sandbox" / "local_output"
    if not output.is_dir():
        pytest.skip("no sandbox output; run sandbox/run_local.py")
    for path in output.glob("*.csv"):
        if contract.get(path.stem):
            assert contract.check_frame(path.stem, pd.read_csv(path, nrows=5)) == []


def test_planned_waste_tables_declare_columns():
    assert "estimated_monthly_savings" in contract.CONTRACT["allpurpose_misuse"].required_columns
    assert "idle_cost" in contract.CONTRACT["idle_compute"].required_columns


def test_check_frame_reports_problems_without_raising():
    assert contract.check_frame("vw_cost_trend_daily", pd.DataFrame({"usage_date": []}))[0].startswith(
        "'vw_cost_trend_daily' is missing required column(s): estimated_cost"
    )
    assert "not in the gold-table contract" in contract.check_frame("nope", pd.DataFrame())[0]


# ---------------------------------------------------------------- resolution

def test_default_resolution_uses_catalog_and_schema():
    ref = TableResolver("workspace", "cockpit").resolve("VW_Executive_Summary")
    assert ref.dotted == "workspace.cockpit.vw_executive_summary"
    assert ref.sql == "`workspace`.`cockpit`.`vw_executive_summary`"


def test_table_map_accepts_one_two_and_three_part_names():
    resolver = TableResolver("workspace", "cockpit", {
        "daily_cost": "daily_cost_v2",
        "vw_failed_jobs": "ops.failed_jobs",
        "accelerator_run_log": "finance-prod.finops_gold.run_log",
    })
    assert resolver.resolve("daily_cost").dotted == "workspace.cockpit.daily_cost_v2"
    assert resolver.resolve("vw_failed_jobs").dotted == "workspace.ops.failed_jobs"
    assert resolver.resolve("accelerator_run_log").sql == "`finance-prod`.`finops_gold`.`run_log`"


@pytest.mark.parametrize("bad", [
    "x; DROP TABLE y", "a.b.c.d", "tab`le", "cat.`sch`.t", "-t", "", "a..b", "t name", "t'",
])
def test_table_map_rejects_unsafe_physical_names(bad: str):
    with pytest.raises(DataSourceError):
        TableResolver("workspace", "cockpit", {"daily_cost": bad})


@pytest.mark.parametrize("bad", ["../secrets", "a.b", "x`;--", "1abc", "", "vw-exec"])
def test_unsafe_logical_names_are_rejected(bad: str):
    with pytest.raises(InvalidIdentifier):
        TableResolver("workspace", "cockpit").resolve(bad)
    with pytest.raises(ValueError):  # InvalidIdentifier is also a ValueError
        TableResolver("workspace", "cockpit", {bad: "t"})


def test_bad_catalog_is_rejected():
    with pytest.raises(InvalidIdentifier):
        TableResolver("work`space", "cockpit")


def test_load_table_map(tmp_path: Path):
    path = tmp_path / "tables.yaml"
    path.write_text("tables:\n  daily_cost: fin.gold.daily\n", encoding="utf-8")
    assert load_table_map(path) == {"daily_cost": "fin.gold.daily"}
    path.write_text("# only comments\n", encoding="utf-8")
    assert load_table_map(path) == {}
    path.write_text("- not a mapping\n", encoding="utf-8")
    with pytest.raises(DataSourceError):
        load_table_map(path)
    assert load_table_map(None) == {}


def test_shipped_tables_yaml_is_valid():
    assert load_table_map(Path(__file__).resolve().parents[1] / "tables.yaml") == {}


# ---------------------------------------------------------------- databricks source

def test_select_applies_limit_only_when_configured(fake_sdk):
    server = FakeServer()
    make_source(server, max_rows=500).table("daily_cost")
    make_source(server).table("daily_cost")
    assert server.statements == [
        "SELECT * FROM `workspace`.`cockpit`.`daily_cost` LIMIT 500",
        "SELECT * FROM `workspace`.`cockpit`.`daily_cost`",
    ]


def test_connection_is_reused_and_configured(fake_sdk):
    server = FakeServer()
    source = make_source(server, query_timeout=120)
    source.table("daily_cost")
    source.table("vw_failed_jobs")
    assert len(server.connections) == 1
    kwargs = server.connections[0].kwargs
    assert kwargs["server_hostname"] == "adb-123.azuredatabricks.net"
    assert kwargs["http_path"] == "/sql/1.0/warehouses/abc123"
    assert kwargs["session_configuration"] == {"STATEMENT_TIMEOUT": "120"}
    assert kwargs["credentials_provider"]()() == {"Authorization": "Bearer sp-token"}


def test_real_connector_accepts_our_kwargs(monkeypatch: pytest.MonkeyPatch):
    """Drive databricks-sql-connector 4.1.3's real ``sql.connect`` with the kwargs we build.

    Only ``Session.open`` (the network call) is replaced, so kwarg handling, auth-provider selection,
    session configuration and the user agent all go through the installed connector.
    """
    import uuid

    from databricks.sql import session as session_module
    from databricks.sql.backend.types import BackendType, SessionId

    seen: list[dict[str, Any]] = []

    def fake_open(self) -> None:
        seen.append({"auth": type(self.auth_provider).__name__, "conf": self.session_configuration,
                     "ua": self.useragent_header, "host": self.host, "path": self.http_path})
        self._session_id = SessionId(BackendType.THRIFT, uuid.uuid4().bytes, uuid.uuid4().bytes)
        self.protocol_version = 0
        self.is_open = True

    monkeypatch.setattr(session_module.Session, "open", fake_open)
    monkeypatch.setattr(session_module.Session, "close", lambda self: setattr(self, "is_open", False))
    monkeypatch.setattr(auth, "sdk_config", lambda **_: FakeConfig())

    sp = DatabricksSqlSource("workspace", "cockpit", "abc123", auth_mode="sdk", query_timeout=90)
    connections = [sp._connect(**sp._identity().connect_kwargs)]
    obo = DatabricksSqlSource("workspace", "cockpit", "abc123", auth_mode="user", host="adb-1.net",
                              headers=obo_headers("a@b.c", TOKEN_A))
    identity_kwargs = {**obo._base_kwargs("adb-1.net"), "access_token": TOKEN_A}
    connections.append(obo._connect(**identity_kwargs))
    for connection in connections:
        connection.close()  # Session.close is patched: no network on close or garbage collection

    assert seen[0] == {"auth": "ExternalAuthProvider", "conf": {"STATEMENT_TIMEOUT": "90"},
                       "ua": "PyDatabricksSqlConnector/4.1.3 (finops-cost-cockpit)",
                       "host": "adb-123.azuredatabricks.net", "path": "/sql/1.0/warehouses/abc123"}
    assert seen[1]["auth"] == "AccessTokenAuthProvider"


def test_reconnects_once_on_connection_error(fake_sdk):
    server = FakeServer()
    server.failures = [ConnectionResetError("Connection reset by peer")]
    frame = make_source(server).table("daily_cost")
    assert len(frame) == 2
    assert len(server.connections) == 2 and server.connections[0].closed


def test_unknown_errors_are_not_retried(fake_sdk):
    server = FakeServer()
    server.failures = [RuntimeError("something odd happened in this session")]
    with pytest.raises(DataSourceError) as info:
        make_source(server).table("daily_cost")
    assert info.value.category == "unknown"
    assert len(server.statements) == 1


def test_permission_error_is_typed_and_not_retried(fake_sdk):
    server = FakeServer()
    server.failures = [RuntimeError("[INSUFFICIENT_PERMISSIONS] User does not have SELECT on table")]
    with pytest.raises(DataSourceError) as info:
        make_source(server).table("daily_cost")
    assert info.value.category == "permission"
    assert info.value.table == "daily_cost"
    assert "workspace.cockpit.daily_cost" in info.value.message
    assert len(server.statements) == 1


@pytest.mark.parametrize("exc, category", [
    (RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] The table or view `x` cannot be found."), "missing_table"),
    (RuntimeError("Failed. SQLSTATE: 42P01"), "missing_table"),
    (RuntimeError("[PERMISSION_DENIED.SELECT] no"), "permission"),
    (RuntimeError("The warehouse is stopped."), "warehouse"),
    (RuntimeError("Query has been timed out"), "timeout"),
    (HttpError("Forbidden", 403), "auth"),
    (HttpError("Unauthorized", 401), "auth"),
    (HttpError("Service unavailable", 503), "connection"),
    (ConnectionResetError("reset"), "connection"),
    # Substrings that used to misfire: a table named session_403, the word "connection" in data.
    (RuntimeError("Reading table session_403_connection failed"), "unknown"),
    (RuntimeError("bad credential_store column"), "unknown"),
])
def test_classify_error(exc: Exception, category: str):
    assert classify_error(exc).category == category


def test_available_tables_spans_mapped_schemas_and_skips_unreadable(fake_sdk):
    server = FakeServer({
        "workspace.cockpit": ["daily_cost", "vw_failed_jobs", "accelerator_run_log"],
        "fin.gold": ["exec_summary"],
    })
    server.denied = {"*": {"locked.away"}}
    resolver = TableResolver("workspace", "cockpit", {
        "vw_executive_summary": "fin.gold.exec_summary",
        "vw_failed_jobs": "fin.gold.failed_jobs",  # mapped elsewhere and missing there
        "job_reliability_summary": "locked.away.jobs",
    })
    source = make_source(server, resolver=resolver)
    assert source.available_tables() == {"daily_cost", "accelerator_run_log", "vw_executive_summary"}
    checks = {name: (ok, detail) for name, ok, detail in source.check()}
    assert checks["Connect"][0]
    assert checks["Schema locked.away"][0] is False
    assert checks["Table vw_failed_jobs"] == (False, "not found at fin.gold.failed_jobs")
    assert checks["Table waste_flags"][0] is True  # planned


def test_schema_listing_is_cached(fake_sdk):
    server = FakeServer({"workspace.cockpit": ["daily_cost"]})
    source = make_source(server)
    source.available_tables()
    source.available_tables()
    assert sum(s.startswith("SHOW TABLES") for s in server.statements) == 1


def test_version_falls_back_only_when_run_log_missing(fake_sdk):
    server = FakeServer()
    assert make_source(server).version() == "7"
    server.failures = [RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] accelerator_run_log")]
    assert make_source(server).version() == "no-run-log"


@pytest.mark.parametrize("failure", [
    RuntimeError("The warehouse is stopped."),
    RuntimeError("[INSUFFICIENT_PERMISSIONS] no SELECT"),
    HttpError("Unauthorized", 401),
])
def test_version_does_not_swallow_transient_errors(fake_sdk, failure: Exception):
    server = FakeServer()
    server.failures = [failure]
    with pytest.raises(DataSourceError):
        make_source(server).version()


# ---------------------------------------------------------------- on-behalf-of-user

def test_obo_cache_key_is_bound_to_the_token_not_the_header():
    server = FakeServer()
    alice = make_source(server, auth_mode="user", headers=obo_headers("alice@corp.com", TOKEN_A))
    bob = make_source(server, auth_mode="user", headers=obo_headers("bob@corp.com", TOKEN_B))
    spoof = make_source(server, auth_mode="user", headers=obo_headers("alice@corp.com", TOKEN_B))
    rotated = make_source(server, auth_mode="user", headers=obo_headers("whatever", TOKEN_A2))
    assert alice.cache_key() != bob.cache_key()
    assert spoof.cache_key() == bob.cache_key()  # Bob's token with Alice's email header is still Bob
    assert rotated.cache_key() == alice.cache_key()  # same verified user keeps the cache after rotation
    for key in (alice.cache_key(), bob.cache_key()):
        assert not any(t in key for t in USERS) and "alice" not in key.lower()


def test_obo_shared_source_serves_each_request_as_its_token_owner():
    server = FakeServer()
    request = Switch("alice@corp.com", TOKEN_A)
    source = make_source(server, auth_mode="user", headers=request)
    assert source.table("daily_cost")["who"].iloc[0] == "alice@corp.com"
    request.set("alice@corp.com", TOKEN_B)  # spoofed header
    assert source.table("daily_cost")["who"].iloc[0] == "bob@corp.com"


def test_obo_resolves_each_token_once():
    server = FakeServer()
    source = make_source(server, auth_mode="user", headers=obo_headers("alice@corp.com", TOKEN_A))
    for _ in range(3):
        source.cache_key()
        source.table("daily_cost")
    assert sum(s.startswith("SELECT current_user()") for s in server.statements) == 1
    assert len(server.connections) == 1  # the whoami connection is pooled and reused


def test_obo_uses_each_users_token_and_separate_connections():
    server = FakeServer()
    request = Switch("alice@corp.com", TOKEN_A)
    source = make_source(server, auth_mode="user", host="https://adb-1.net/", headers=request)
    source.table("daily_cost")
    request.set("bob@corp.com", TOKEN_B)
    source.table("daily_cost")
    assert [c.kwargs["access_token"] for c in server.connections] == [TOKEN_A, TOKEN_B]
    assert server.connections[0].kwargs["server_hostname"] == "adb-1.net"
    assert "credentials_provider" not in server.connections[0].kwargs


def test_obo_reconnects_when_token_rotates():
    server = FakeServer()
    request = Switch("alice@corp.com", TOKEN_A)
    source = make_source(server, auth_mode="user", headers=request)
    source.table("daily_cost")
    request.set("alice@corp.com", TOKEN_A2)
    source.table("daily_cost")
    assert server.connections[0].closed
    assert server.connections[1].kwargs["access_token"] == TOKEN_A2


def test_obo_listing_errors_are_per_user():
    server = FakeServer({"workspace.cockpit": ["daily_cost"], "fin.gold": ["exec"]})
    server.denied = {"bob@corp.com": {"fin.gold"}}
    request = Switch("alice@corp.com", TOKEN_A)
    resolver = TableResolver("workspace", "cockpit", {"vw_executive_summary": "fin.gold.exec"})
    source = make_source(server, auth_mode="user", headers=request, resolver=resolver)
    alice_tables = source.available_tables()
    request.set("bob@corp.com", TOKEN_B)
    bob_checks = {name: ok for name, ok, _ in source.check()}
    request.set("alice@corp.com", TOKEN_A)
    alice_checks = {name: ok for name, ok, _ in source.check()}
    assert "vw_executive_summary" in alice_tables
    assert bob_checks["Schema fin.gold"] is False
    assert "Schema fin.gold" not in alice_checks


def test_obo_missing_header_is_a_clear_auth_error():
    source = make_source(FakeServer(), auth_mode="user")
    with pytest.raises(DataSourceError) as info:
        source.table("daily_cost")
    assert info.value.category == "auth"
    assert "X-Forwarded-Access-Token" in info.value.message


def test_obo_rejected_token_says_reload():
    server = FakeServer()
    source = make_source(server, auth_mode="user", headers=obo_headers("a@corp.com", TOKEN_A))
    source.cache_key()
    server.failures = [HttpError("Invalid access token", 401)]
    with pytest.raises(DataSourceError) as info:
        source.table("daily_cost")
    assert info.value.category == "auth" and "Reload the page" in info.value.message


def test_obo_refused_outside_databricks_apps(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    with pytest.raises(DataSourceError, match="only works inside Databricks Apps"):
        DatabricksSqlSource("w", "s", "abc", auth_mode="user", host="h", env={})
    with pytest.raises(SettingsError, match="only works inside Databricks Apps"):
        Settings.from_env({"COCKPIT_SOURCE": "databricks", "DATABRICKS_WAREHOUSE_ID": "abc", "COCKPIT_AUTH": "user"})
    assert Settings.from_env({"COCKPIT_SOURCE": "databricks", "DATABRICKS_WAREHOUSE_ID": "abc",
                              "COCKPIT_AUTH": "user", **IN_APPS}).auth == "user"


def test_auto_mode_detects_databricks_apps():
    assert auth.resolve_auth_mode("auto", IN_APPS) == "app"
    assert auth.resolve_auth_mode("auto", {}) == "sdk"
    assert auth.resolve_auth_mode("user", {}) == "user"


# ---------------------------------------------------------------- secrets never leak

def test_reprs_contain_no_token():
    server = FakeServer()
    source = make_source(server, auth_mode="user", headers=obo_headers("alice@corp.com", TOKEN_A))
    identity = source._identity()
    assert identity.connect_kwargs["access_token"] == TOKEN_A
    for text in (repr(source), repr(identity), str(identity), repr(auth.forwarded_user(obo_headers("a", TOKEN_A)()))):
        assert TOKEN_A not in text


def test_describe_has_no_secrets(fake_sdk, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATABRICKS_TOKEN", TOKEN_A)
    server = FakeServer()
    obo = make_source(server, auth_mode="user", headers=obo_headers("spoofed@corp.com", TOKEN_B))
    assert obo.describe()["user"] == "(not verified yet)"
    obo.table("daily_cost")
    sp = make_source(server)
    sp.table("daily_cost")
    for info in (obo.describe(), sp.describe()):
        assert {"kind", "label", "host", "warehouse id", "catalog", "schema", "auth mode", "table map"} <= set(info)
        assert all(isinstance(v, str) for v in info.values())
        text = repr(info)
        assert TOKEN_A not in text and TOKEN_B not in text and "sp-token" not in text
    assert obo.describe()["user"] == "bob@corp.com"  # verified from the token, not the header
    assert sp.describe()["host"] == "adb-123.azuredatabricks.net"


# ---------------------------------------------------------------- pool and caches

class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_pool_closes_idle_connections_and_caps_identities():
    clock = Clock()
    pool = ds._Pool(max_idle=2, max_idle_seconds=60, max_identities=2, clock=clock)
    server = FakeServer()
    a, b, c = (server.connect() for _ in range(3))
    pool.release("a", "", a)
    pool.release("b", "", b)
    pool.release("c", "", c)  # third identity evicts the least recently used ("a")
    assert a.closed and not b.closed
    clock.now = 61
    assert pool.acquire("b", "") is None  # expired while idle
    assert b.closed and c.closed and pool.idle_count() == 0


def test_ttl_cache_is_bounded_and_expires():
    clock = Clock()
    cache = ds._TTLCache(ttl_seconds=10, max_entries=2, clock=clock)
    for key in "abc":
        cache.put(key, key.upper())
    assert len(cache) == 2 and cache.get("a") is None and cache.get("c") == "C"
    clock.now = 11
    assert cache.get("c") is None


# ---------------------------------------------------------------- local source

def test_local_source_describe_and_check(fixtures_dir: Path):
    source = LocalCsvSource(fixtures_dir)
    assert source.describe()["kind"] == "local"
    checks = {name: ok for name, ok, _ in source.check()}
    assert checks["Directory"] and checks["Table vw_executive_summary"]
    assert checks["Table waste_flags"]  # planned: missing is fine
    assert checks["Table daily_cost"] is False  # produced by the engine but absent from fixtures


def test_local_source_honours_map_and_max_rows(tmp_path: Path):
    (tmp_path / "exec_v2.csv").write_text("a\n1\n2\n3\n", encoding="utf-8")
    source = LocalCsvSource(tmp_path, resolver=TableResolver("w", "s", {"vw_executive_summary": "x.y.exec_v2"}), max_rows=2)
    assert source.available_tables() == {"vw_executive_summary"}
    assert len(source.table("vw_executive_summary")) == 2
    with pytest.raises(DataSourceError) as info:
        source.table("daily_cost")
    assert info.value.category == "missing_table"


# ---------------------------------------------------------------- settings and registry

def test_settings_defaults_and_parsing():
    defaults = Settings.from_env({})
    assert (defaults.source, defaults.catalog, defaults.schema, defaults.auth) == ("local", "workspace", "cockpit", "auto")
    parsed = Settings.from_env({
        "COCKPIT_SOURCE": "Databricks", "DATABRICKS_WAREHOUSE_ID": "abc123", "COCKPIT_MAX_ROWS": "1000",
        "COCKPIT_QUERY_TIMEOUT": "0", "COCKPIT_AUTH": "USER", "COCKPIT_SCHEMA": "finops-gold", **IN_APPS,
    })
    assert (parsed.source, parsed.max_rows, parsed.query_timeout, parsed.auth) == ("databricks", 1000, 0, "user")


@pytest.mark.parametrize("env, expected", [
    ({"COCKPIT_SOURCE": "excel"}, "COCKPIT_SOURCE must be one of"),
    ({"COCKPIT_SOURCE": "databricks"}, "needs DATABRICKS_WAREHOUSE_ID"),
    ({"COCKPIT_CATALOG": "a.b"}, "COCKPIT_CATALOG must be a plain identifier"),
    ({"COCKPIT_SCHEMA": "x`; DROP"}, "COCKPIT_SCHEMA must be a plain identifier"),
    ({"COCKPIT_AUTH": "magic"}, "COCKPIT_AUTH must be one of"),
    ({"COCKPIT_MAX_ROWS": "lots"}, "COCKPIT_MAX_ROWS must be an integer"),
    ({"COCKPIT_MAX_ROWS": "0"}, "COCKPIT_MAX_ROWS must be >= 1"),
    ({"COCKPIT_TABLE_MAP": "/nope/tables.yaml"}, "COCKPIT_TABLE_MAP points to"),
    ({"COCKPIT_SOURCE": "databricks", "DATABRICKS_WAREHOUSE_ID": "abc; x"}, "DATABRICKS_WAREHOUSE_ID"),
])
def test_settings_validation_messages(env: dict[str, str], expected: str):
    with pytest.raises(SettingsError, match=expected.replace("`", ".")):
        Settings.from_env(env)


def test_settings_collects_every_problem():
    with pytest.raises(SettingsError) as info:
        Settings.from_env({"COCKPIT_SOURCE": "excel", "COCKPIT_AUTH": "magic"})
    assert "COCKPIT_SOURCE" in str(info.value) and "COCKPIT_AUTH" in str(info.value)


def test_http_path_replaces_warehouse_id():
    settings = Settings.from_env({"COCKPIT_SOURCE": "databricks", "DATABRICKS_HTTP_PATH": "/sql/protocolv1/o/1/0123-abc"})
    source = build_source(settings)
    assert isinstance(source, DatabricksSqlSource)
    assert source.http_path == "/sql/protocolv1/o/1/0123-abc"


def test_build_source_applies_table_map(tmp_path: Path):
    path = tmp_path / "tables.yaml"
    path.write_text("tables:\n  vw_executive_summary: fin.gold.exec\n", encoding="utf-8")
    settings = Settings.from_env({"COCKPIT_SOURCE": "databricks", "DATABRICKS_WAREHOUSE_ID": "abc", "COCKPIT_TABLE_MAP": str(path)})
    source = build_source(settings)
    assert source.select_statement("vw_executive_summary") == "SELECT * FROM `fin`.`gold`.`exec`"
    assert source.describe()["table map"] == str(path)


def test_source_registry_accepts_new_backends(monkeypatch: pytest.MonkeyPatch, fixtures_dir: Path):
    monkeypatch.setattr(ds, "_SOURCES", dict(ds._SOURCES))
    register_source("fake", lambda settings: LocalCsvSource(fixtures_dir))
    assert "fake" in ds.registered_sources()
    settings = Settings.from_env({"COCKPIT_SOURCE": "fake"})
    assert isinstance(build_source(settings), LocalCsvSource)
    with pytest.raises(ValueError):
        register_source("fake", lambda settings: LocalCsvSource(fixtures_dir))
    with pytest.raises(ValueError):
        register_source("bad kind", lambda settings: LocalCsvSource(fixtures_dir))


def test_databricks_connector_is_imported_lazily():
    import subprocess
    import sys

    code = (
        "import sys; sys.path.insert(0, %r);"
        "from cockpit_ui.core.datasource import build_source; from cockpit_ui.core.settings import Settings;"
        "build_source(Settings.from_env({})).available_tables();"
        "assert 'databricks.sql' not in sys.modules and 'databricks.sdk' not in sys.modules"
    ) % str(Path(__file__).resolve().parents[1])
    subprocess.run([sys.executable, "-c", code], check=True)
