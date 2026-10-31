"""Thresholds for cockpit steps, read from a per-step section of ``config/cockpit_thresholds.yml``.

The upstream ``ThresholdConfig`` is frozen to the engine's own keys and lives in the vendored
package (rule R3), and ``config/thresholds.yml`` is the engine's file (its sections are pinned by
``tests/test_main.py``). Cockpit steps therefore keep their thresholds in a separate file, one
section per step, for example ``allpurpose_misuse:``.

Reading reuses the upstream YAML helpers (read-only use of the vendored package), so a missing or
malformed file behaves the same way: code defaults are used and a warning is returned.
"""

from __future__ import annotations

import re
from pathlib import Path

from databricks_finops.config import _numeric_section, _read_yaml_file

THRESHOLDS_FILE = Path("config") / "cockpit_thresholds.yml"


def load_step_thresholds(
    bundle_root: str,
    section: str,
    defaults: dict[str, float],
) -> tuple[dict[str, float], list[str]]:
    """Return ``(values, warnings)``: ``defaults`` overridden by numeric keys found in ``section``."""

    data, warning = _read_yaml_file(Path(bundle_root) / THRESHOLDS_FILE)
    warnings = [warning] if warning else []
    values, section_warnings = _numeric_section(data, section, defaults)
    return values, warnings + section_warnings


SQL_IDENTIFIER = re.compile(r"^[A-Z][A-Z0-9_]*$")


def load_step_lists(
    bundle_root: str,
    section: str,
    defaults: dict[str, tuple[str, ...]],
) -> tuple[dict[str, tuple[str, ...]], list[str]]:
    """Return ``(values, warnings)`` for list keys in ``section``.

    A value may be a YAML list or a comma-separated string (the upstream fallback parser, used when
    PyYAML is missing, only understands scalars). Every item must be an UPPERCASE identifier
    (``^[A-Z][A-Z0-9_]*$``) because the values are written into SQL. Anything else, including an
    empty list, falls back to the default with a warning. A missing file is reported by
    ``load_step_thresholds``, so it is not reported again here.
    """

    data, _ = _read_yaml_file(Path(bundle_root) / THRESHOLDS_FILE)
    raw_section = data.get(section, {})
    values = dict(defaults)
    warnings: list[str] = []
    if not isinstance(raw_section, dict):
        return values, warnings
    for key, default in defaults.items():
        if key not in raw_section:
            continue
        raw = raw_section[key]
        items = raw.split(",") if isinstance(raw, str) else raw
        if not isinstance(items, list):
            warnings.append(f"{section}.{key} must be a list or comma-separated string; default used.")
            continue
        cleaned = tuple(str(item).strip() for item in items if str(item).strip())
        bad = [item for item in cleaned if not SQL_IDENTIFIER.match(item)]
        if bad or not cleaned:
            reason = f"invalid values {bad}" if bad else "empty"
            warnings.append(f"{section}.{key} is {reason} (UPPERCASE identifiers only); default {list(default)} used.")
            continue
        values[key] = cleaned
    return values, warnings


def enforce_bounds(
    values: dict[str, float],
    defaults: dict[str, float],
    section: str,
    bounds: dict[str, tuple[float | None, float | None, bool]],
) -> list[str]:
    """Reset out-of-range values to their defaults in place and return warnings.

    ``bounds[key] = (low, high, low_exclusive)``; ``None`` means unbounded. Use
    ``(0, None, True)`` for divisors, which must be strictly positive.
    """

    warnings = []
    for key, (low, high, low_exclusive) in bounds.items():
        value = values.get(key)
        if value is None:
            continue
        too_low = low is not None and (value <= low if low_exclusive else value < low)
        too_high = high is not None and value > high
        if too_low or too_high:
            sign = ">" if low_exclusive else ">="
            rule = " and ".join(part for part in (
                f"{sign} {low:g}" if low is not None else "", f"<= {high:g}" if high is not None else "") if part)
            warnings.append(f"{section}.{key}={value:g} must be {rule}; default {defaults[key]:g} used.")
            values[key] = defaults[key]
    return warnings
