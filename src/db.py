"""Acceso a PostgreSQL: COPY, consultas, metricas y manifiestos."""

from __future__ import annotations

import io
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import psycopg2
from psycopg2 import extras

from src import config


def connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=config.WAREHOUSE_HOST,
        port=config.WAREHOUSE_PORT,
        dbname=config.WAREHOUSE_DB,
        user=config.WAREHOUSE_USER,
        password=config.WAREHOUSE_PASSWORD,
        connect_timeout=10,
        options="-c timezone=UTC",
    )


@contextmanager
def connection() -> Iterator[psycopg2.extensions.connection]:
    conn = connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query_df(sql: str, params: tuple | None = None) -> pd.DataFrame:
    with connection() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def execute(sql: str, params: tuple | None = None) -> None:
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)


def execute_file(path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)


def copy_dataframe(
    conn: psycopg2.extensions.connection,
    table: str,
    dataframe: pd.DataFrame,
    columns: list[str],
) -> None:
    buffer = io.StringIO()
    dataframe[columns].to_csv(
        buffer,
        index=False,
        header=False,
        na_rep="",
        lineterminator="\n",
    )
    buffer.seek(0)
    with conn.cursor() as cursor:
        cursor.copy_expert(
            f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV, NULL '')",
            buffer,
        )


def truncate(conn: psycopg2.extensions.connection, table: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {table}")


def table_count(table: str) -> int:
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            return int(cursor.fetchone()[0])


def upsert_manifest(
    conn: psycopg2.extensions.connection,
    *,
    batch_id: str,
    source: str,
    layer: str,
    object_path: str,
    row_count: int | None,
    checksum: str | None,
    status: str,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ops.batch_manifest
                (batch_id, source, layer, object_path, row_count, checksum, status, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (batch_id, source, layer) DO UPDATE SET
                object_path = EXCLUDED.object_path,
                row_count = EXCLUDED.row_count,
                checksum = EXCLUDED.checksum,
                status = EXCLUDED.status,
                updated_at = NOW()
            """,
            (batch_id, source, layer, object_path, row_count, checksum, status),
        )


METRICS_COLUMNS = [
    "batch_id",
    "source",
    "layer",
    "dag_run_id",
    "logical_date",
    "rows_read",
    "rows_valid",
    "rows_rejected",
    "rows_duplicated",
    "rows_late",
    "rows_published",
    "rows_conflicting",
    "started_at",
    "finished_at",
    "status",
    "error_message",
]


def upsert_metrics(
    conn: psycopg2.extensions.connection,
    *,
    batch_id: str,
    source: str,
    layer: str,
    dag_run_id: str | None = None,
    logical_date: str | None = None,
    rows_read: int = 0,
    rows_valid: int = 0,
    rows_rejected: int = 0,
    rows_duplicated: int = 0,
    rows_late: int = 0,
    rows_published: int = 0,
    rows_conflicting: int = 0,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    status: str = "running",
    error_message: str | None = None,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ops.pipeline_run_metrics (
                batch_id, source, layer, dag_run_id, logical_date,
                rows_read, rows_valid, rows_rejected, rows_duplicated,
                rows_late, rows_published, rows_conflicting,
                started_at, finished_at, status, error_message
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (batch_id, source, layer) DO UPDATE SET
                dag_run_id = EXCLUDED.dag_run_id,
                logical_date = EXCLUDED.logical_date,
                rows_read = EXCLUDED.rows_read,
                rows_valid = EXCLUDED.rows_valid,
                rows_rejected = EXCLUDED.rows_rejected,
                rows_duplicated = EXCLUDED.rows_duplicated,
                rows_late = EXCLUDED.rows_late,
                rows_published = EXCLUDED.rows_published,
                rows_conflicting = EXCLUDED.rows_conflicting,
                started_at = COALESCE(EXCLUDED.started_at, ops.pipeline_run_metrics.started_at),
                finished_at = EXCLUDED.finished_at,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message
            """,
            (
                batch_id,
                source,
                layer,
                dag_run_id,
                logical_date,
                rows_read,
                rows_valid,
                rows_rejected,
                rows_duplicated,
                rows_late,
                rows_published,
                rows_conflicting,
                started_at,
                finished_at,
                status,
                error_message,
            ),
        )


def get_metrics(
    batch_id: str, source: str, layer: str | None = None
) -> dict[str, Any] | None:
    sql = """
        SELECT * FROM ops.pipeline_run_metrics
        WHERE batch_id = %s AND source = %s
    """
    params: list[Any] = [batch_id, source]
    if layer:
        sql += " AND layer = %s"
        params.append(layer)
    sql += " ORDER BY finished_at DESC NULLS LAST LIMIT 1"
    df = query_df(sql, tuple(params))
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def add_quality_result(
    conn: psycopg2.extensions.connection,
    *,
    batch_id: str,
    check_name: str,
    status: str,
    observed_value: str,
    threshold: str,
    details: str,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ops.data_quality_results
                (batch_id, check_name, status, observed_value, threshold, details, checked_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (batch_id, check_name) DO UPDATE SET
                status = EXCLUDED.status,
                observed_value = EXCLUDED.observed_value,
                threshold = EXCLUDED.threshold,
                details = EXCLUDED.details,
                checked_at = NOW()
            """,
            (batch_id, check_name, status, observed_value, threshold, details),
        )


def add_alert(
    conn: psycopg2.extensions.connection,
    *,
    alert_key: str,
    severity: str,
    category: str,
    message: str,
    batch_id: str | None,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ops.alerts
                (alert_key, severity, category, message, batch_id, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (alert_key) DO NOTHING
            """,
            (alert_key, severity, category, message, batch_id),
        )


def clear_quarantine(
    conn: psycopg2.extensions.connection, *, batch_id: str, source: str
) -> None:
    # Un batch pertenece a una sola fuente. Borrar por batch (no por source)
    # permite limpiar tambien filas legacy o reetiquetadas sin dejarlas huerfanas.
    # `source` se conserva en la firma para compatibilidad con los llamadores.
    _ = source
    with conn.cursor() as cursor:
        cursor.execute(
            "DELETE FROM ops.quarantine_records WHERE batch_id = %s",
            (batch_id,),
        )


def insert_quarantine(conn: psycopg2.extensions.connection, dataframe: pd.DataFrame) -> int:
    if dataframe.empty:
        return 0
    columns = [
        "quarantine_key",
        "source",
        "source_record_id",
        "reason_code",
        "reason_detail",
        "raw_payload",
        "batch_id",
        "processed_at",
        "record_hash",
    ]
    rows = [
        tuple(row)
        for row in dataframe[columns].itertuples(index=False, name=None)
    ]
    with conn.cursor() as cursor:
        extras.execute_values(
            cursor,
            """
            INSERT INTO ops.quarantine_records (
                quarantine_key, source, source_record_id, reason_code,
                reason_detail, raw_payload, batch_id, processed_at, record_hash
            ) VALUES %s
            ON CONFLICT (quarantine_key) DO NOTHING
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s::jsonb, %s, %s::timestamptz, %s)",
            page_size=500,
        )
        return int(cursor.rowcount)


def execute_values(
    conn: psycopg2.extensions.connection,
    sql: str,
    rows: list[tuple],
) -> int:
    if not rows:
        return 0
    with conn.cursor() as cursor:
        extras.execute_values(cursor, sql, rows, page_size=1000)
        return int(cursor.rowcount)
