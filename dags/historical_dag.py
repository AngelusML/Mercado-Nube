"""DAG 1: carga historica unica + catalogo v1 + validacion + quality gate.

Ejecucion manual:
  docker exec mn_airflow airflow dags trigger 01_historical_load
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from src import config
from src.tasks import (
    clean_catalog_task,
    clean_historical_task,
    extract_catalog_task,
    extract_historical_task,
    publish_historical_dimensions_task,
    publish_historical_facts_task,
    quality_gate_historical_task,
    validate_catalog_task,
    validate_historical_task,
)

with DAG(
    dag_id="01_historical_load",
    description="Historico Online Retail II + catalogo v1 + validacion + SCD2 + quality gate",
    start_date=datetime(2026, 10, 20, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    dagrun_timeout=timedelta(hours=3),
    default_args={
        "owner": "mercado-nube",
        "retries": config.PIPELINE_RETRIES,
        "retry_delay": timedelta(seconds=config.PIPELINE_RETRY_DELAY_SECONDS),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=config.PIPELINE_MAX_RETRY_DELAY_MINUTES),
        "execution_timeout": timedelta(hours=2),
    },
    params={"ingest_date": "2026-10-21"},
    tags=["mercado-nube", "historico", "local"],
    doc_md="""
### Carga historica
1. Extrae el CSV historico a `raw/`.
2. Extrae el catalogo v1 desde el API paginado (rate limit + 500 transitorios).
3. Limpia, normaliza, deduplica y manda rechazos a cuarentena.
3b. **Valida** la capa cleaned (umbrales de rechazo/duplicados y cuadre de
    conteos). Si falla, el lote NO se publica.
4. Publica SCD2 de productos y clientes historicos.
5. Publica hechos con conversion USD.
6. Ejecuta el quality gate (reconciliacion, tasas, USD, cobertura).

**Idempotencia:** mismo `ingest_date` => mismo `batch_id`; se sobreescriben
las mismas llaves S3 y los hechos se recargan por `DELETE` + insercion
transaccional.
    """,
) as dag:
    extract_historical = PythonOperator(
        task_id="extract_historical",
        python_callable=extract_historical_task,
        op_kwargs={"ingest_date": "{{ params.ingest_date }}"},
    )
    extract_catalog = PythonOperator(
        task_id="extract_catalog_v1",
        python_callable=extract_catalog_task,
        op_kwargs={"ingest_date": "{{ params.ingest_date }}", "version": 1},
    )
    clean_historical = PythonOperator(
        task_id="clean_historical",
        python_callable=clean_historical_task,
        op_kwargs={"ingest_date": "{{ params.ingest_date }}"},
    )
    clean_catalog = PythonOperator(
        task_id="clean_catalog_v1",
        python_callable=clean_catalog_task,
        op_kwargs={"ingest_date": "{{ params.ingest_date }}", "version": 1},
    )
    validate_historical = PythonOperator(
        task_id="validate_historical",
        python_callable=validate_historical_task,
        op_kwargs={"ingest_date": "{{ params.ingest_date }}"},
    )
    validate_catalog = PythonOperator(
        task_id="validate_catalog_v1",
        python_callable=validate_catalog_task,
        op_kwargs={"ingest_date": "{{ params.ingest_date }}", "version": 1},
    )
    publish_dimensions = PythonOperator(
        task_id="publish_historical_dimensions",
        python_callable=publish_historical_dimensions_task,
        op_kwargs={"ingest_date": "{{ params.ingest_date }}"},
    )
    publish_facts = PythonOperator(
        task_id="publish_historical_facts",
        python_callable=publish_historical_facts_task,
        op_kwargs={"ingest_date": "{{ params.ingest_date }}"},
    )
    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate_historical_task,
        op_kwargs={"ingest_date": "{{ params.ingest_date }}"},
    )

    extract_historical >> clean_historical >> validate_historical
    extract_catalog >> clean_catalog >> validate_catalog
    [validate_historical, validate_catalog] >> publish_dimensions
    publish_dimensions >> publish_facts
    [publish_facts, publish_dimensions] >> quality_gate
