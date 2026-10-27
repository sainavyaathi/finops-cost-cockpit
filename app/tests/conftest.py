from __future__ import annotations

import sys
from pathlib import Path

import pytest

APP_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = APP_DIR.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"

for path in (APP_DIR, REPO_ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES


@pytest.fixture
def local_env(monkeypatch: pytest.MonkeyPatch, fixtures_dir: Path) -> None:
    monkeypatch.setenv("COCKPIT_SOURCE", "local")
    monkeypatch.setenv("COCKPIT_LOCAL_DIR", str(fixtures_dir))
    monkeypatch.delenv("COCKPIT_FEATURES_FILE", raising=False)
