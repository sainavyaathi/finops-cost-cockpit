# Starts the cockpit on your laptop.
# Usage (from the repo root):
#   .\app\run_app.ps1              # synthetic data from sandbox/local_output (run .\sandbox\run_local.ps1 first)
#   .\app\run_app.ps1 -Databricks  # live tables; needs DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID
param([switch]$Databricks)

$root = Split-Path -Parent $PSScriptRoot
$env:COCKPIT_SOURCE = if ($Databricks) { "databricks" } else { "local" }

& "$root\.venv\Scripts\python.exe" -m streamlit run "$root\app\streamlit_app.py"
