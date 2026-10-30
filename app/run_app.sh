#!/usr/bin/env bash
# Starts the cockpit on macOS/Linux. The theme in app/.streamlit/config.toml applies from any directory.
# Usage (from anywhere):
#   app/run_app.sh               # synthetic data from sandbox/local_output (run sandbox/run_local.py first)
#   app/run_app.sh --databricks  # live tables; needs DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export COCKPIT_SOURCE="local"
if [[ "${1:-}" == "--databricks" ]]; then
  export COCKPIT_SOURCE="databricks"
  shift
fi

exec "$root/.venv/bin/python" -m streamlit run "$root/app/streamlit_app.py" "$@"
