# FinOps Cost Cockpit

A 4-week POC built by **Navya** (GitHub `sainavyaathi`) and **Nikhil** (GitHub `nikhil187`). It turns Databricks system tables into a cost cockpit that answers three questions: where the money goes, who owns it, and where compute is wasted. The full plan is in `docs/plan/FinOps_Cost_Cockpit_POC_Plan.pdf` (v1.1).

**Read `docs/project_history.md` first.** It explains how the project started, every decision so far and the reasons behind it, and what exists in the Databricks workspace.

- Private repo: https://github.com/sainavyaathi/finops-cost-cockpit
- Work happens on branches named `<name>/<feature>`, for example `navya/f3-catalog-allocation`.
- Changes reach `main` only through a pull request that the other person reviews.
- CI runs all tests on every pull request.

## Architecture (three layers, one-way)

```
Engine (Spark + SQL on Databricks) --writes--> gold tables (the contract) <--reads-- Streamlit app
```

- `src/databricks_finops/`: the upstream engine, copied from github.com/rashad-ahmed-imtiaz/databricks-finops-accelerator.
  - **Do not modify it.** It has no licence, which is why our repo stays private.
  - It writes 20 tables and views (`OUTPUT_TABLES` and `OUTPUT_VIEWS` in `config.py`).
- `src/cockpit/`: our pipeline extensions.
  - New features are plug-in steps: `src/cockpit/steps/<name>.py` exports `STEP = Step(...)`, with its SQL in `src/cockpit/sql/`.
  - `run_cockpit_pipeline()` runs the upstream pipeline first, then the steps in `depends_on` order.
- `app/`: the Streamlit cockpit. Each page is a feature module in `app/cockpit_ui/features/<id>/`.
  - `__init__.py` exports `FEATURE = Feature(id, title, owner, plan_refs, order, requires, render)`.
  - `page.py` holds `render(ctx)`.
  - Pages are auto-discovered. To hide or reorder one, edit `app/features.yaml`.
  - `app/cockpit_ui/core/` is shared plumbing; adding a page never changes it.
- Pages read data only through `ctx.table("<gold table>")`.
  - The data source is `COCKPIT_SOURCE=local`, which reads `sandbox/local_output/*.csv`, or `databricks`, which reads through a SQL warehouse with `DATABRICKS_WAREHOUSE_ID`.
  - Business logic belongs in engine SQL, not in pages (rule R6).
- `sandbox/`: a synthetic system-table generator (`finops_sandbox.py`) plus `RedirectingSpark`, which rewrites `system.*` table names to the synthetic tables. The whole pipeline therefore runs with no real account.

## Commands (Windows PowerShell, from the repo root)

```powershell
.\sandbox\run_local.ps1                                   # synthetic data -> pipeline -> CSVs in sandbox\local_output
.\app\run_app.ps1                                         # cockpit at http://localhost:8501 (local CSVs)
$env:PYTHONPATH="src"; .\.venv\Scripts\python.exe -m pytest -q tests app/tests   # 60 tests
```

The tools are portable installs under `%LOCALAPPDATA%\Programs`: python311-portable, jdk17, hadoop (winutils) and mingit. The `.venv` is not committed. Recreate it with portable Python, then `pip install -r app/requirements.txt pyspark==3.5.3 delta-spark==3.2.1 pytest pyyaml`.

## Features and owners (from the plan)

| ID | Feature | Owner | Status |
|---|---|---|---|
| F1 | Cost base | Navya | works (engine) |
| F2 | Tag attribution and quality | Navya | works; page `tagging` |
| F3 | Shared-warehouse catalog allocation (`system.query.history`) | Navya | build new |
| F4 | All-purpose compute misuse (JDBC/ODBC, scheduled jobs, shared users) | Nikhil | extend |
| F5 | Idle/zombie compute (auto-termination off or over 60 minutes, plus low utilisation) | Nikhil | extend; page `utilization` |
| F6 | Job failures and failed-run cost | Nikhil | works; page `reliability`. Known bug below |
| F7 | Unified waste backlog to `cockpit.waste_flags` | Nikhil | extend; page `backlog` |
| F8 | Spend trend and week-over-week change | Navya | extend; page `overview` |
| F9 | Streamlit cockpit (this app, deployed as a Databricks App) | Navya | scaffold done |
| F10 | Monday SQL alert, top 3 flags | Navya | build new |
| F11 | Health and coverage checks | Both | works; page `health` |
| F12 | Synthetic sandbox | Nikhil | extend: add audit, clusters and query_history tables |

Design rules:
- R1: count savings once per resource.
- R2: for shared warehouses, catalog split beats tags.
- R3: keep the upstream engine.
- R4: outputs go to the cockpit schema.
- R5: the alert shows the top 3 by `priority_score`.
- R6: pages read gold tables only.

## Known issues and gotchas

- **Upstream retry bug (F6):** `reliability.py` `_retry_cte` groups `job_task_run_timeline` by `run_id`, which is the *task* run id, so `retry_count` is always 0. The fix is to group and join on `job_run_id`.
- Locally, the message "Catalog creation failed" in `create_catalog_schema` is expected, because local Spark only has `spark_catalog`.
- Databricks Free Edition: the catalog is `workspace`, synthetic sources are in `workspace.finops_synthetic`, and outputs are in `workspace.finops_accelerator`. `system.compute.clusters` is empty there.
- Never commit tokens. Databricks auth comes from env vars or the Databricks Apps service principal.
