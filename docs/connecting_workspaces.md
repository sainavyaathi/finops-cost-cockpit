# Connecting the cockpit to data

The app reads gold tables by logical name (`ctx.table("vw_executive_summary")`). Where those names
point, and as whom the app reads them, is set with environment variables. Nothing here needs a code
change. The full variable list is in `app/.env.example`. The logical table names are in
`app/cockpit_ui/core/contract.py`.

| Variable | Default | Purpose |
|---|---|---|
| `COCKPIT_SOURCE` | `local` | `local` (CSV), `databricks` (SQL warehouse), or a registered plugin kind |
| `COCKPIT_LOCAL_DIR` | `sandbox/local_output` | CSV folder for `local` |
| `COCKPIT_CATALOG` / `COCKPIT_SCHEMA` | `workspace` / `cockpit` | default location of every table (rule R4) |
| `COCKPIT_TABLE_MAP` | `app/tables.yaml` if present | per-table overrides |
| `COCKPIT_AUTH` | `auto` | `auto`, `app`, `user`, `sdk` |
| `DATABRICKS_WAREHOUSE_ID` | none | warehouse to query (or `DATABRICKS_HTTP_PATH`) |
| `COCKPIT_QUERY_TIMEOUT` | `300` | statement timeout in seconds, `0` = warehouse default |
| `COCKPIT_MAX_ROWS` | unlimited | row cap per table read, applied with `LIMIT` |
| `COCKPIT_SOURCE_PLUGINS` | none | modules to import that register extra backends |

Invalid settings stop the app at startup with a list of every problem.

## 1. Local sandbox (no account)

```bash
python sandbox/run_local.py          # writes sandbox/local_output/*.csv
streamlit run app/streamlit_app.py   # COCKPIT_SOURCE defaults to local
```

On Windows use `.\sandbox\run_local.ps1` and `.\app\run_app.ps1`.

## 2. Laptop to a real workspace

Set `COCKPIT_AUTH=sdk` (or leave `auto`). The app passes authentication to `databricks-sdk`, so any
method the SDK supports works. Pick one:

```bash
export COCKPIT_SOURCE=databricks
export COCKPIT_CATALOG=workspace COCKPIT_SCHEMA=cockpit
export DATABRICKS_WAREHOUSE_ID=<warehouse id>

# a) personal access token
export DATABRICKS_HOST=https://<workspace-host>
export DATABRICKS_TOKEN=...            # type it in the shell, never put it in a committed file

# b) CLI profile (after `databricks auth login --host https://<workspace-host>`)
export DATABRICKS_CONFIG_PROFILE=<profile>

# c) OAuth machine-to-machine (CI, service principal)
export DATABRICKS_HOST=https://<workspace-host>
export DATABRICKS_CLIENT_ID=<application id> DATABRICKS_CLIENT_SECRET=...

# d) Azure: ARM_TENANT_ID, ARM_CLIENT_ID, ARM_CLIENT_SECRET (or `az login` with only DATABRICKS_HOST)
# e) GCP: DATABRICKS_GOOGLE_SERVICE_ACCOUNT or GOOGLE_CREDENTIALS
```

If several methods are set, the SDK may refuse to choose one. Set `DATABRICKS_AUTH_TYPE` (for
example `pat` or `oauth-m2m`) to pick one explicitly. The identity you use needs the same grants as
the app service principal (section 3).

## 3. Databricks Apps (service principal)

This is what `app/app.yaml` configures: `COCKPIT_SOURCE=databricks`, `COCKPIT_AUTH=app`, and
`DATABRICKS_WAREHOUSE_ID` taken from the app's `sql-warehouse` resource (`resources/cockpit_app.yml`,
permission `CAN_USE`). Apps injects `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID` and
`DATABRICKS_CLIENT_SECRET` for the app's service principal.

Grant the service principal read access only:

```sql
GRANT USE CATALOG ON CATALOG workspace TO `<app service principal application id>`;
GRANT USE SCHEMA  ON SCHEMA  workspace.cockpit TO `<app service principal application id>`;
GRANT SELECT      ON SCHEMA  workspace.cockpit TO `<app service principal application id>`;
```

It also needs `CAN USE` on the warehouse. The bundle's `sql-warehouse` resource grants this when the
app is deployed. `COCKPIT_CATALOG`/`COCKPIT_SCHEMA` in `app.yaml` must match the catalog and schema
the pipeline job writes (bundle variables `catalog` and `schema`).

## 4. On-behalf-of-user

With `COCKPIT_AUTH=user`, each query runs as the signed-in viewer, so Unity Catalog grants and row
filters apply per person.

1. Enable user authorization on the app and add the `sql` scope (app settings in the workspace, or
   `user_api_scopes: [sql]` on the app resource in the bundle).
2. Set `COCKPIT_AUTH=user` in `app.yaml`.
3. Give viewers `USE CATALOG`, `USE SCHEMA` and `SELECT`, plus `CAN USE` on the warehouse.

Databricks Apps forwards the user's token in the `X-Forwarded-Access-Token` header. The app reads
it per request and does not log it, display it, or put it in a cache key.

- **Identity comes from the token, not the headers.** The first time the app sees a token, it runs
  `SELECT current_user()` with that token and remembers the result for 15 minutes, keyed by a
  SHA-256 fingerprint of the token. Cached data is keyed by a hash of that verified user. The
  `X-Forwarded-Email` header is never used for caching, so a request cannot get another user's
  cached rows by sending their email.
- **Only inside Databricks Apps.** The Apps proxy sets the forwarded headers itself. Anywhere else, a
  client could send its own. The app therefore refuses to start with `COCKPIT_AUTH=user` unless
  `DATABRICKS_APP_NAME` is set (Apps sets it). On a laptop or in CI, use `COCKPIT_AUTH=sdk`.
- **Token expiry.** Apps hands Streamlit the token when the page opens, and Streamlit does not see a
  new one until the page is reloaded. In a tab left open for a long time, queries start failing
  with "Your Databricks session token expired or was rejected. Reload the page to sign in again."
  Reloading fixes it.
- **Missing header.** If the header is missing, for example because user authorization is off,
  pages show an auth error that names the setting to fix.

## 5. Mapping to existing enterprise schemas

If the gold tables live elsewhere or under other names, map them in `app/tables.yaml`, or in the
file `COCKPIT_TABLE_MAP` points to:

```yaml
tables:
  vw_executive_summary: finance_prod.finops_gold.exec_summary   # catalog.schema.table
  job_reliability_summary: observability.job_reliability         # schema.table (default catalog)
  accelerator_run_log: run_log                                   # table (default catalog and schema)
```

Unmapped names resolve to `<COCKPIT_CATALOG>.<COCKPIT_SCHEMA>.<name>`. Each part may contain only
letters, digits, `_` or `-`, and is backtick-quoted in SQL. Any other name is rejected at startup.
Grant `USE SCHEMA` and `SELECT` on every schema you map to. If one mapped schema cannot be read,
only the pages that need its tables are hidden, and the problem is listed in the source's `check()`
output.

## 6. Adding a backend

A backend is a class that satisfies `DataSource` in `app/cockpit_ui/core/datasource.py`:
`kind`, `label`, `table(name)`, `available_tables()`, `version()`, `cache_key()`, `describe()` and
`check()`. Use `TableResolver` to turn logical names into physical ones, and raise `DataSourceError`
with a clear message. Then register it:

```python
# my_company/cockpit_snowflake.py
from cockpit_ui.core.datasource import register_source, resolver_for

class SnowflakeSource: ...

register_source("snowflake", lambda settings: SnowflakeSource(resolver_for(settings)))
```

Then run with `COCKPIT_SOURCE_PLUGINS=my_company.cockpit_snowflake COCKPIT_SOURCE=snowflake`. The
backend reads its own connection variables (for example `SNOWFLAKE_ACCOUNT`) from the environment.
No other file changes. To ship a backend inside this repo, define the class in `datasource.py` and add
the `register_source` line next to the existing ones.
