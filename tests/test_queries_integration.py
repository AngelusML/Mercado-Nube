from __future__ import annotations

import os
from pathlib import Path

import pytest
from psycopg2 import connect

from src import config

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION_TESTS") != "1",
        reason="Define RUN_INTEGRATION_TESTS=1 y levanta docker compose",
    ),
]

QUERY_DIR = Path(__file__).resolve().parents[1] / "sql" / "queries"


def _execute_script(cursor, script: str) -> None:
    # psycopg2 usa el protocolo simple cuando no hay parametros; permite
    # ejecutar varios SELECT separados por ';' en un mismo archivo.
    cursor.execute(script)


def test_database_has_no_duplicate_event_or_current_product_keys() -> None:
    with connect(
        host=config.WAREHOUSE_HOST,
        port=config.WAREHOUSE_PORT,
        dbname=config.WAREHOUSE_DB,
        user=config.WAREHOUSE_USER,
        password=config.WAREHOUSE_PASSWORD,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT source, event_id
                    FROM analytics.fact_order_event
                    GROUP BY source, event_id
                    HAVING COUNT(*) > 1
                ) d
                """
            )
            assert cursor.fetchone()[0] == 0

            cursor.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT sku
                    FROM analytics.dim_product
                    WHERE is_current = true AND sku <> '__UNKNOWN__'
                    GROUP BY sku
                    HAVING COUNT(*) > 1
                ) d
                """
            )
            assert cursor.fetchone()[0] == 0


def test_all_seven_business_queries_execute() -> None:
    files = sorted(QUERY_DIR.glob("*.sql"))
    assert len(files) == 7

    with connect(
        host=config.WAREHOUSE_HOST,
        port=config.WAREHOUSE_PORT,
        dbname=config.WAREHOUSE_DB,
        user=config.WAREHOUSE_USER,
        password=config.WAREHOUSE_PASSWORD,
    ) as conn:
        with conn.cursor() as cursor:
            for path in files:
                _execute_script(cursor, path.read_text(encoding="utf-8"))
