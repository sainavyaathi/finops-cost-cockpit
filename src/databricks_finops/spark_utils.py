from __future__ import annotations

import json
from importlib import resources
from datetime import datetime, timezone
from typing import Any


def qident(identifier: str) -> str:
    return f"`{identifier}`"


def qname(catalog: str, schema: str, object_name: str | None = None) -> str:
    if object_name is None:
        return f"{qident(catalog)}.{qident(schema)}"
    return f"{qident(catalog)}.{qident(schema)}.{qident(object_name)}"


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def log_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, default=str))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def timestamp_literal(value: datetime) -> str:
    return f"TIMESTAMP {sql_string(value.strftime('%Y-%m-%d %H:%M:%S'))}"


def scalar_value(spark: Any, sql: str, default: Any = None) -> Any:
    try:
        rows = spark.sql(sql).collect()
        if not rows:
            return default
        row = rows[0]
        return row[0]
    except Exception:
        return default


def table_count(spark: Any, table_name: str) -> int | None:
    value = scalar_value(spark, f"SELECT COUNT(*) AS row_count FROM {table_name}")
    if value is None:
        return None
    return int(value)


def create_namespace(spark: Any, catalog: str, schema: str) -> str:
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {qident(catalog)}")
        catalog_message = "Catalog checked or created."
    except Exception as exc:
        catalog_message = (
            "Catalog creation failed, likely due to permissions. "
            f"Continuing with schema creation. Error: {exc}"
        )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qname(catalog, schema)}")
    return catalog_message


def first_existing(columns: set[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def comment_on_table(spark: Any, table_name: str, comment: str) -> None:
    spark.sql(f"COMMENT ON TABLE {table_name} IS {sql_string(comment)}")


def load_sql(name: str, **kwargs: str) -> str:
    """Load a SQL template from the packaged sql directory and substitute kwargs."""

    relative_name = name if name.endswith(".sql") else f"{name}.sql"
    parts = relative_name.replace("\\", "/").split("/")
    sql_path = resources.files("databricks_finops").joinpath("sql", *parts)
    template = sql_path.read_text(encoding="utf-8")
    return template.format_map(kwargs)
