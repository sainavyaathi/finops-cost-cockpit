"""FinOps Cost Cockpit entry point.

Local:            .\\app\\run_app.ps1
Databricks Apps:  started from app.yaml
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cockpit_ui.core.shell import run  # noqa: E402

run()
