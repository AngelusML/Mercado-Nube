"""DAG 2: lote diario de eventos + version de catalogo + quality gate.

Ejemplos manuales:
  docker exec mn_airflow airflow dags trigger 02_daily_events \
    --conf '{"ingest_date":"2026-10-22"}'
  docker exec mn_airflow airflow dags trigger 02_daily_events \
    --conf '{"ingest_date":"2026-10-23"}'

Demostracion de recuperacion:
  --conf '{"ingest_date":"2026-10-22","fail_point":"after_raw"}'
  --conf '{"ingest_date":"2026-10-22","fail_point":"after_clean"}'
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from src import config
from src.tasks import (
    clean_catalog_task,
    clean_events_task,
    extract_catalog_task,
    extract_events_task,
    publish_catalog_task,
    publish_events_task,
    quality_gate_daily_task,
)

with DAG(
    dag_id="02_daily_events",
    description="Eventos del dia + catalogo versionado + SCD2 + quality gate",
    start_date=datetime(2026, 10, 20, tzinfo=timezone.utc),
    schedule=os.getenv("DAILY_SCHEDULE") or None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "mercado-nube",
        "retries": config.PIPELINE_RETRIES,
        "retry_delay": timedelta(seconds=config.PIPELINE_RETRY_DELAY_SECONDS),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=config.PIPELINE_MAX_RETRY_DELAY_MINUTES),
        "execution_timeout": timedelta(hours=1),
    },
    params={"ingest_date": "2026-10-22", "catalog_version": 0},
    tags=["mercado-nube", "diario", "eventos", "local"],
    doc_md="""
### Lote diario
- `ingest_date=2026-10-22` usa catalogo v2.
- `ingest_date=2026-10-23` usa catalogo v3.
- Si `catalog_version=0`, se resuelve por fecha.
- El archivo local es `data/incoming/events/ingest_date=YYYY-MM-DD/events.jsonl`.

**Tardios:** `ingested_at - event_time_utc > 24h` => `is_late=true`; se
recalcula el ciclo completo de las ordenes afectadas usando todo el historial.

**Idempotencia:** `batch_id=events_YYYY-MM-DD`; llaves S3 deterministas;
`UNIQUE(source,event_id)` + upsert de lifecycle + metricas/manifiestos upsert.
    """,
) as dag:
    extract_events = PythonOperator(
        task_id="extract_events",
        python_callable=extract_events_task,
        op_kwargs={"ingest_date": "{{ params.ingest_date }}"},
    )
    clean_events = PythonOperator(
        task_id="clean_events",
        python_callable=clean_events_task,
        op_kwargs={"ingest_date": "{{ params.ingest_date }}"},
    )
    publish_events = PythonOperator(
        task_id="publish_events",
        python_callable=publish_events_task,
        op_kwargs={"ingest_date": "{{ params.ingest_date }}"},
    )
    extract_catalog = PythonOperator(
        task_id="extract_catalog",
        python_callable=extract_catalog_task,
        op_kwargs={
            "ingest_date": "{{ params.ingest_date }}",
            "version": "{{ params.catalog_version }}",
        },
    )
    clean_catalog = PythonOperator(
        task_id="clean_catalog",
        python_callable=clean_catalog_task,
        op_kwargs={
            "ingest_date": "{{ params.ingest_date }}",
            "version": "{{ params.catalog_version }}",
        },
    )
    publish_catalog = PythonOperator(
        task_id="publish_catalog",
        python_callable=publish_catalog_task,
        op_kwargs={
            "ingest_date": "{{ params.ingest_date }}",
            "version": "{{ params.catalog_version }}",
        },
    )
    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate_daily_task,
        op_kwargs={
            "ingest_date": "{{ params.ingest_date }}",
            "version": "{{ params.catalog_version }}",
        },
    )

    extract_events >> clean_events >> publish_events
    extract_catalog >> clean_catalog >> publish_catalog
    [publish_events, publish_catalog] >> quality_gate
