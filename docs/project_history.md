# Project history: how the FinOps Cost Cockpit came to be

A record of decisions and their reasons, so anyone (or any AI assistant) joining later knows why the repo looks the way it does.

## 1. Starting point: study an existing accelerator (October 2026)

Navya wanted to build a FinOps project and found **github.com/rashad-ahmed-imtiaz/databricks-finops-accelerator**. It is a Databricks Asset Bundle job: Python orchestrates SQL templates that read Databricks system tables and write cost tables. Her starting constraints:

- **Databricks Free Edition only**, which has almost no real usage data and an empty `system.compute.clusters`.
- **No company data.**

To understand the repo without real data:

- **Synthetic system tables.** `sandbox/finops_sandbox.py` generates fake versions of the six system tables the accelerator reads:
  - `billing.usage` and `billing.list_prices`
  - `compute.node_timeline`
  - `lakeflow.jobs`, `job_run_timeline` and `job_task_run_timeline`

  There are 12 designed workloads, each built to trigger one outcome: a healthy job, an oversized job, a memory-bound job, a flaky job, a shared cluster, untagged spend, fallback pricing, and so on.
- **`RedirectingSpark`.** It rewrites `system.*` table names to the synthetic tables, so the upstream code runs **unmodified**.
- **Runs.**
  - **Locally:** PySpark plus Delta. The result was all 12 steps succeeding, about $4,549 of synthetic spend and 12 workloads. The top backlog items were the shared cluster (score 72) and `ml_feature_training` (score 60).
  - **Databricks Free Edition (synthetic data):** outputs went to `workspace.finops_accelerator`.
  - **Databricks Free Edition (real account):** outputs went to `workspace.finops_accelerator_real`. Real spend was about $0.67.
- **Upstream bug found.** The `retry_count` is always 0. `reliability.py` joins task runs on `run_id`, which is the task run id, instead of `job_run_id`. This was left unfixed on purpose and assigned to F6.

A Lakeview dashboard was built for the first mentor demo. The repo itself does not include one, so this was our addition. It was later dropped (see section 4).

## 2. From "their accelerator" to "our cockpit"

The team (Navya and Nikhil) had their own design, "FinOps & System Tables Cost Cockpit". The work to reconcile the two went like this:

- **Gap analysis** between that design and the accelerator.
- **Merging.** Common and colliding features were merged into **12 unique features (F1–F12)**.
- **Design rules R1–R5** to resolve the collisions:
  - R1: count savings once per resource.
  - R2: for shared warehouses, catalog split beats tags.
  - R3: keep the upstream engine.
  - R4: outputs go to the cockpit schema.
  - R5: the alert shows the top 3 by priority.
- **A 4-week plan** split by area:
  - **Navya:** cost, attribution and reporting (F1, F2, F3, F8, F9, F10).
  - **Nikhil:** waste detection (F4, F5, F6, F7, F12).
  - **Shared:** F11.

All of this is in `docs/plan/FinOps_Cost_Cockpit_POC_Plan.pdf`. Version 1.0 was produced first as a formal monochrome document for mentor review.

## 3. Clean repository (27 October 2026)

- An unrelated, empty "multi-agent business chatbot" skeleton was deleted.
- The accelerator code, the sandbox and the plan were moved into this repo. The first commit is the "base code".
- **The repo is private.** The upstream repo has no licence, so its code stays the author's. Treat `src/databricks_finops` as read-only.
- Nikhil (`nikhil187`) was invited with write access.

## 4. Decision: Streamlit instead of Lakeview

Navya decided the cockpit should be a **Streamlit application** rather than a native AI/BI (Lakeview) dashboard. The goal is code where features can be injected, updated or removed easily. The resulting design (PR #1, plan v1.1):

- **Three layers with one-way dependencies.** The engine writes gold tables, and the app only reads them (rule R6).
- **App pages are plug-in feature modules** in `app/cockpit_ui/features/<id>/`.
  - They are auto-discovered, and `app/features.yaml` turns them on or off.
  - A page with missing tables shows a notice, and errors in one page are isolated from the rest.
- **Pipeline features are plug-in steps** in `src/cockpit/steps/`. They run after the untouched upstream pipeline.
- **One codebase, two runtimes:**
  - locally on synthetic CSVs (`COCKPIT_SOURCE=local`)
  - on Databricks Apps through a SQL warehouse (`COCKPIT_SOURCE=databricks`)
- **CI and CODEOWNERS.** GitHub Actions runs every test on each pull request, and CODEOWNERS routes each feature folder to its owner for review.
- **Branch model: feature → `development` → `testing` → `main` (28 October 2026).** Feature branches merge into `development`. Promotions to `testing` and then `main` are PRs, so `main` stays demo-ready while both of us integrate in parallel.

## Where things are on Databricks (Navya's Free Edition workspace)

- **Workspace folder:** `/Users/sainavyaathi@gmail.com/databricks-finops-accelerator`. It holds the first uploaded copy, plus the notebook `sandbox/run_on_databricks`.
- **Schemas:** `workspace.finops_synthetic` (fake sources), `workspace.finops_accelerator` (outputs from synthetic data) and `workspace.finops_accelerator_real` (outputs from real data).
- **Old Lakeview dashboard:** "FinOps Accelerator Dashboard". It has been superseded by the Streamlit app.

## Next steps (as of 27 October 2026)

1. Review and merge PR #1.
2. Deploy the app to Databricks Apps and grant the app's service principal SELECT on the output schema.
3. Start Week 1 of the plan:
   - F1: rename the outputs into the cockpit schema.
   - F12: add the extra synthetic tables.
   - Agree the `cockpit.waste_flags` interface.
