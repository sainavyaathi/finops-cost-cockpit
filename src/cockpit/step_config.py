"""Thresholds for cockpit steps, read from a per-step section of ``config/cockpit_thresholds.yml``.

The upstream ``ThresholdConfig`` is frozen to the engine's own keys and lives in the vendored
package (rule R3), and ``config/thresholds.yml`` is the engine's file (its sections are pinned by
``tests/test_main.py``). Cockpit steps therefore keep their thresholds in a separate file, one
section per step, for example ``allpurpose_misuse:``.

Reading reuses the upstream YAML helpers (read-only use of the vendored package), so a missing or
malformed file behaves the same way: code defaults are used and a warning is returned.
"""

from __future__ import annotations

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
