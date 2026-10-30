from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

APP_DIR = Path(__file__).resolve().parents[2]
REPO_ROOT = APP_DIR.parent


@dataclass(frozen=True)
class Settings:
    """Runtime settings, read from environment variables only.

    COCKPIT_SOURCE        local (CSV export of a sandbox run) or databricks (SQL warehouse)
    COCKPIT_LOCAL_DIR     folder of <table>.csv files, default sandbox/local_output
    COCKPIT_CATALOG       default workspace
    COCKPIT_SCHEMA        default cockpit
    DATABRICKS_WAREHOUSE_ID  required for databricks; injected by Databricks Apps via app.yaml
    COCKPIT_FEATURES_FILE default app/features.yaml
    """

    source: str = "local"
    local_dir: Path = REPO_ROOT / "sandbox" / "local_output"
    catalog: str = "workspace"
    schema: str = "cockpit"
    warehouse_id: str | None = None
    features_file: Path = APP_DIR / "features.yaml"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env
        defaults = cls()
        settings = cls(
            source=env.get("COCKPIT_SOURCE", defaults.source).strip().lower(),
            local_dir=Path(env.get("COCKPIT_LOCAL_DIR", defaults.local_dir)),
            catalog=env.get("COCKPIT_CATALOG", defaults.catalog),
            schema=env.get("COCKPIT_SCHEMA", defaults.schema),
            warehouse_id=env.get("DATABRICKS_WAREHOUSE_ID") or None,
            features_file=Path(env.get("COCKPIT_FEATURES_FILE", defaults.features_file)),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.source not in {"local", "databricks"}:
            raise ValueError(f"COCKPIT_SOURCE must be 'local' or 'databricks', got {self.source!r}.")
        if self.source == "databricks" and not self.warehouse_id:
            raise ValueError("COCKPIT_SOURCE=databricks needs DATABRICKS_WAREHOUSE_ID.")
