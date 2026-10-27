from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .spark_utils import comment_on_table, qname, sql_string, table_count, timestamp_literal, utc_now


@dataclass
class StepLog:
    run_id: str
    task_name: str
    status: str
    result: str
    started_at: Any
    ended_at: Any
    duration_seconds: float
    rows_written: int | None
    message: str


class StepLogger:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.records: list[StepLog] = []

    def run(
        self,
        task_name: str,
        operation: Callable[[], str | None],
        *,
        spark: Any | None = None,
        count_table: str | None = None,
        required: bool = True,
    ) -> str | None:
        started_at = utc_now()
        rows_written: int | None = None
        try:
            message = operation() or "completed"
            if spark is not None and count_table is not None:
                rows_written = table_count(spark, count_table)
            status = "SUCCESS"
            result = "SUCCESS"
            return message
        except Exception as exc:
            status = "FAILED"
            message = f"{type(exc).__name__}: {exc}"
            if required:
                result = "FAILED"
                raise
            result = "DEGRADED"
            return message
        finally:
            ended_at = utc_now()
            self.records.append(
                StepLog(
                    run_id=self.run_id,
                    task_name=task_name,
                    status=status,
                    result=result,
                    started_at=started_at,
                    ended_at=ended_at,
                    duration_seconds=round((ended_at - started_at).total_seconds(), 6),
                    rows_written=rows_written,
                    message=message[:4000],
                )
            )


RUN_LOG_COLUMNS = [
    "run_id",
    "task_name",
    "status",
    "result",
    "started_at",
    "ended_at",
    "duration_seconds",
    "rows_written",
    "message",
]


def _run_log_table_sql(target: str, create_or_replace: str) -> str:
    return f"""
    {create_or_replace} {target} (
        run_id STRING,
        task_name STRING,
        status STRING,
        result STRING,
        started_at TIMESTAMP,
        ended_at TIMESTAMP,
        duration_seconds DOUBLE,
        rows_written BIGINT,
        message STRING
    )
    USING DELTA
    """


def _ensure_run_log_table(spark: Any, catalog: str, schema: str, target: str) -> None:
    spark.sql(_run_log_table_sql(target, "CREATE TABLE IF NOT EXISTS"))
    try:
        existing_columns = set(spark.table(f"{catalog}.{schema}.accelerator_run_log").columns)
    except Exception:
        existing_columns = set()

    if not set(RUN_LOG_COLUMNS).issubset(existing_columns):
        spark.sql(_run_log_table_sql(target, "CREATE OR REPLACE TABLE"))


def write_run_log(spark: Any, catalog: str, schema: str, logger: StepLogger) -> None:
    target = qname(catalog, schema, "accelerator_run_log")
    _ensure_run_log_table(spark, catalog, schema, target)
    try:
        comment_on_table(spark, target, "Step-level execution log for accelerator runs.")
    except Exception:
        pass

    if not logger.records:
        return

    rows = ",\n        ".join(
        "("
        f"{sql_string(record.run_id)}, "
        f"{sql_string(record.task_name)}, "
        f"{sql_string(record.status)}, "
        f"{sql_string(record.result)}, "
        f"{timestamp_literal(record.started_at)}, "
        f"{timestamp_literal(record.ended_at)}, "
        f"{record.duration_seconds}, "
        f"{'NULL' if record.rows_written is None else record.rows_written}, "
        f"{sql_string(record.message)}"
        ")"
        for record in logger.records
    )
    spark.sql(
        f"""
        INSERT INTO {target} (
            run_id,
            task_name,
            status,
            result,
            started_at,
            ended_at,
            duration_seconds,
            rows_written,
            message
        )
        SELECT
            run_id,
            task_name,
            status,
            result,
            started_at,
            ended_at,
            duration_seconds,
            rows_written,
            message
        FROM VALUES
            {rows}
        AS run_log(
            run_id,
            task_name,
            status,
            result,
            started_at,
            ended_at,
            duration_seconds,
            rows_written,
            message
        )
        """
    )
