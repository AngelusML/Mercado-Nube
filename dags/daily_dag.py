"""DAG 2: lote diario de eventos + version de catalogo + validacion + quality gate.

Fecha del lote (ingest_date):
  - Disparo manual: --conf '{"ingest_date":"2026-10-22"}'
  - Corrida programada o backfill: se usa la fecha logica de Airflow ({{ ds }}).

Ejemplos manuales:
  docker exec mn_airflow airflow dags trigger 02_daily_events \
    --conf '{"ingest_date":"2026-10-22"}'

Intervalo historico sin editar codigo:
  .\\scripts\\backfill.ps1 -Start 2026-10-20 -End 2026-10-26
  (o, con DAILY_SCHEDULE=@daily en .env:
   docker exec mn_airflow airflow dags backfill 02_daily_events -s 2026-10-20 -e 2026-10-26)

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
    validate_catalog_task,
    validate_events_task,
)

# Si no llega ingest_date por --conf, se usa la fecha logica de la corrida.
INGEST_DATE = "{{ params.ingest_date or ds }}"
CATALOG_VERSION = "{{ params.catalog_version }}"

with DAG(
    dag_id="02_daily_events",
    description="Eventos del dia + catalogo versionado + validacion + SCD2 + quality gate",
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
    # ingest_date vacio = usar la fecha logica de Airflow (ds).
    params={"ingest_date": "", "catalog_version": 0},
    tags=["mercado-nube", "diario", "eventos", "local"],
    doc_md="""
### Lote diario
- `ingest_date` vacio => se usa la fecha logica (`ds`); asi funcionan el
  schedule diario y el backfill por intervalo.
- Catalogo por fecha: antes del 2026-10-22 => v1; 2026-10-22 => v2; desde 2026-10-23 => v3.
- El archivo local es `data/incoming/events/ingest_date=YYYY-MM-DD/events.jsonl`.

**Flujo:** extract -> clean -> **validate** (umbrales sobre cleaned; si falla,
NO se publica) -> publish -> quality_gate (reconciliacion final).

**Tardios:** `ingested_at - event_time_utc > 24h` => `is_late=true`; se
recalcula el ciclo completo de las ordenes afectadas usando todo el historial.

**Idempotencia:** `batch_id=events_YYYY-MM-DD`; llaves S3 deterministas;
`UNIQUE(source,event_id)` + upsert de lifecycle + metricas/manifiestos upsert.
    """,
) as dag:
    extract_events = PythonOperator(
        task_id="extract_events",
        python_callable=extract_events_task,
        op_kwargs={"ingest_date": INGEST_DATE},
    )
    clean_events = PythonOperator(
        task_id="clean_events",
        python_callable=clean_events_task,
        op_kwargs={"ingest_date": INGEST_DATE},
    )
    validate_events = PythonOperator(
        task_id="validate_events",
        python_callable=validate_events_task,
        op_kwargs={"ingest_date": INGEST_DATE},
    )
    publish_events = PythonOperator(
        task_id="publish_events",
        python_callable=publish_events_task,
        op_kwargs={"ingest_date": INGEST_DATE},
    )
    extract_catalog = PythonOperator(
        task_id="extract_catalog",
        python_callable=extract_catalog_task,
        op_kwargs={"ingest_date": INGEST_DATE, "version": CATALOG_VERSION},
    )
    clean_catalog = PythonOperator(
        task_id="clean_catalog",
        python_callable=clean_catalog_task,
        op_kwargs={"ingest_date": INGEST_DATE, "version": CATALOG_VERSION},
    )
    validate_catalog = PythonOperator(
        task_id="validate_catalog",
        python_callable=validate_catalog_task,
        op_kwargs={"ingest_date": INGEST_DATE, "version": CATALOG_VERSION},
    )
    publish_catalog = PythonOperator(
        task_id="publish_catalog",
        python_callable=publish_catalog_task,
        op_kwargs={"ingest_date": INGEST_DATE, "version": CATALOG_VERSION},
    )
    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate_daily_task,
        op_kwargs={"ingest_date": INGEST_DATE, "version": CATALOG_VERSION},
    )

    extract_events >> clean_events >> validate_events >> publish_events
    extract_catalog >> clean_catalog >> validate_catalog >> publish_catalog
    [publish_events, publish_catalog] >> quality_gate
