from __future__ import annotations

import importlib
import pkgutil
import traceback
from dataclasses import dataclass, field, replace
from pathlib import Path

import yaml

from .feature import Feature

FEATURES_PACKAGE = "cockpit_ui.features"


@dataclass
class Discovery:
    features: list[Feature] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)  # module name -> error text


def discover(package: str = FEATURES_PACKAGE) -> Discovery:
    """Import every sub-package of ``package`` and collect its ``FEATURE``.

    A broken feature is reported in ``errors`` instead of taking the whole app down.
    """

    result = Discovery()
    seen: dict[str, str] = {}
    root = importlib.import_module(package)
    for module_info in sorted(pkgutil.iter_modules(root.__path__), key=lambda m: m.name):
        if not module_info.ispkg or module_info.name.startswith("_"):
            continue
        module_name = f"{package}.{module_info.name}"
        try:
            module = importlib.import_module(module_name)
        except Exception:  # noqa: BLE001 - isolate one bad plugin from the rest
            result.errors[module_name] = traceback.format_exc(limit=3)
            continue
        feature = getattr(module, "FEATURE", None)
        if not isinstance(feature, Feature):
            result.errors[module_name] = "Package must define FEATURE = Feature(...)."
            continue
        if feature.id in seen:
            result.errors[module_name] = f"Duplicate feature id {feature.id!r} (already used by {seen[feature.id]})."
            continue
        seen[feature.id] = module_name
        result.features.append(feature)
    return result


def load_toggles(path: Path) -> dict[str, dict]:
    """Read ``features.yaml``. A missing file means every discovered feature is enabled."""

    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return data.get("features") or {}


def enabled_features(features: list[Feature], toggles: dict[str, dict]) -> list[Feature]:
    selected = []
    for feature in features:
        toggle = toggles.get(feature.id) or {}
        if not toggle.get("enabled", True):
            continue
        if "order" in toggle:
            feature = replace(feature, order=int(toggle["order"]))
        selected.append(feature)
    return sorted(selected, key=lambda f: (f.order, f.title))


def missing_tables(feature: Feature, available: set[str]) -> list[str]:
    return [table for table in feature.requires if table.lower() not in available]
