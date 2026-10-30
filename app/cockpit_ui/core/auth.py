"""Who the app talks to Databricks as.

Modes (``COCKPIT_AUTH``):

- ``app``:  the Databricks Apps service principal. Apps injects DATABRICKS_HOST, DATABRICKS_CLIENT_ID
            and DATABRICKS_CLIENT_SECRET; ``databricks.sdk.core.Config()`` picks them up.
- ``user``: on-behalf-of-user. Databricks Apps forwards the signed-in user's token in the
            ``X-Forwarded-Access-Token`` header (needs user authorization scopes on the app, e.g. ``sql``).
            Only allowed inside Databricks Apps, whose proxy strips client-supplied X-Forwarded-* headers.
- ``sdk``:  laptop / CI. Whatever ``Config()`` resolves: DATABRICKS_HOST + DATABRICKS_TOKEN,
            DATABRICKS_CONFIG_PROFILE, OAuth M2M client id/secret, Azure or GCP credentials.
- ``auto``: ``app`` inside Databricks Apps (DATABRICKS_APP_NAME is set), otherwise ``sdk``.

Tokens are never logged, displayed, or used in cache keys. Streamlit is imported lazily so the data
layer stays testable without a running app.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

TOKEN_HEADER = "x-forwarded-access-token"
_USER_HEADERS = ("x-forwarded-email", "x-forwarded-preferred-username", "x-forwarded-user")

HeaderProvider = Callable[[], Mapping[str, str]]


class MissingUserToken(RuntimeError):
    """COCKPIT_AUTH=user but the request carries no forwarded user token."""


def running_in_databricks_apps(env: Mapping[str, str] | None = None) -> bool:
    env = os.environ if env is None else env
    return bool(env.get("DATABRICKS_APP_NAME"))


def resolve_auth_mode(mode: str, env: Mapping[str, str] | None = None) -> str:
    """Turn ``auto`` into ``app`` or ``sdk``; other modes pass through."""
    if mode != "auto":
        return mode
    return "app" if running_in_databricks_apps(env) else "sdk"


def normalize_host(host: str) -> str:
    """``https://adb-1.azuredatabricks.net/`` -> ``adb-1.azuredatabricks.net``."""
    return host.strip().removeprefix("https://").removeprefix("http://").rstrip("/")


def fingerprint(value: str, length: int = 16) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


@dataclass(frozen=True)
class ForwardedUser:
    """The credential of the current request (on-behalf-of-user mode).

    ``header_name`` comes from X-Forwarded-Email / -User and is display-only: it is NOT trusted for
    cache keys. The data source resolves the real identity from the token itself (``current_user()``)
    and keys caches on that, so a request cannot borrow another user's cache by spoofing a header.
    """

    header_name: str | None
    token: str = field(repr=False)

    @property
    def token_fingerprint(self) -> str:
        """Non-secret SHA-256 prefix of the token; used to look up the identity resolved for it."""
        return fingerprint(self.token, 32)


def identity_key(user: str) -> str:
    """Non-secret cache key for a verified Databricks identity."""
    return fingerprint(f"user:{user.strip().lower()}")


def forwarded_user(headers: Mapping[str, str]) -> ForwardedUser:
    lowered = {str(k).lower(): str(v) for k, v in headers.items()}
    token = lowered.get(TOKEN_HEADER, "").strip()
    if not token:
        raise MissingUserToken(
            "COCKPIT_AUTH=user, but the request has no X-Forwarded-Access-Token header. Enable user "
            "authorization for the app (scope 'sql') in Databricks Apps, or use COCKPIT_AUTH=app."
        )
    name = next((lowered[h].strip() for h in _USER_HEADERS if lowered.get(h, "").strip()), None)
    return ForwardedUser(header_name=name, token=token)


def streamlit_headers() -> Mapping[str, str]:
    """Headers of the current Streamlit request; empty outside a script run.

    Databricks Apps sets X-Forwarded-Access-Token when the page (websocket session) opens; Streamlit
    does not see a fresh one until the user reloads. A long-lived tab therefore ends up with an expired
    token, which the data source reports as "reload the page".
    """
    try:
        import streamlit as st

        return dict(st.context.headers)
    except Exception:  # noqa: BLE001 - no script run context (tests, bare python)
        return {}


def sdk_config(**overrides: Any):
    """``databricks.sdk.core.Config`` resolved from the environment (imported lazily)."""
    from databricks.sdk.core import Config

    return Config(**overrides)
