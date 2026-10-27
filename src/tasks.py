"""Funciones invocadas por Airflow. Delgadas: delegan en src.pipelines."""

from __future__ import annotations

import logging
from typing import Any

from src import config, pipelines, quality_gate

logger = logging.getLogger(__name__)

# Fecha desde la cual rige cada version del catalogo (vigencia del API).
# Antes de 2026-10-22 rige v1 (la carga inicial), el 22 rige v2 y desde el 23, v3.
# Asi cualquier fecha del intervalo (incluido un backfill) usa la version que
# correspondia ese dia y nunca publica una version futura antes que una pasada.
CATALOG_VERSION_START_DATES = [
    ("2026-10-23", 3),
    ("2026-10-22", 2),
    ("0000-01-01", 1),
]


def resolve_catalog_version(ingest_date: str, version: Any = 0) -> int:
    try:
        requested = int(version)
    except (TypeError, ValueError):
        requested = 0
    if requested > 0:
        return requested
    for start_date, resolved in CATALOG_VERSION_START_DATES:
        if ingest_date >= start_date:
            return resolved
    return 1


def _fail_point(context: dict[str, Any]) -> str | None:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    if isinstance(conf, dict):
        value = conf.get("fail_point")
        return str(value) if value else None
    return None


def extract_historical_task(ingest_date: str, **context: Any) -> dict[str, Any]:
    return pipelines.extract_historical(ingest_date)


def clean_historical_task(ingest_date: str, **context: Any) -> dict[str, Any]:
    return pipelines.clean_historical(ingest_date)


def extract_catalog_task(
    ingest_date: str, version: Any = 1, **context: Any
) -> dict[str, Any]:
    resolved = resolve_catalog_version(ingest_date, version)
    return pipelines.extract_catalog(ingest_date, resolved)


def clean_catalog_task(
    ingest_date: str, version: Any = 1, **context: Any
) -> dict[str, Any]:
    resolved = resolve_catalog_version(ingest_date, version)
    return pipelines.clean_catalog(ingest_date, resolved)


def publish_catalog_task(
    ingest_date: str, version: Any = 1, **context: Any
) -> dict[str, Any]:
    resolved = resolve_catalog_version(ingest_date, version)
    return pipelines.publish_catalog(ingest_date, resolved)


def publish_historical_dimensions_task(
    ingest_date: str, **context: Any
) -> dict[str, Any]:
    return pipelines.publish_historical_dimensions(ingest_date)


def publish_historical_facts_task(
    ingest_date: str, **context: Any
) -> dict[str, Any]:
    return pipelines.publish_historical_facts(ingest_date)


def extract_events_task(ingest_date: str, **context: Any) -> dict[str, Any]:
    return pipelines.extract_events(
        ingest_date, fail_point=_fail_point(context)
    )


def clean_events_task(ingest_date: str, **context: Any) -> dict[str, Any]:
    return pipelines.clean_events(
        ingest_date, fail_point=_fail_point(context)
    )


def validate_historical_task(ingest_date: str, **context: Any) -> dict[str, Any]:
    return quality_gate.validate_cleaned_batch(
        source=config.HISTORICAL_SOURCE,
        batch_id=config.historical_batch_id(ingest_date),
    )


def validate_events_task(ingest_date: str, **context: Any) -> dict[str, Any]:
    return quality_gate.validate_cleaned_batch(
        source=config.EVENTS_SOURCE,
        batch_id=config.events_batch_id(ingest_date),
    )


def validate_catalog_task(
    ingest_date: str, version: Any = 1, **context: Any
) -> dict[str, Any]:
    resolved = resolve_catalog_version(ingest_date, version)
    return quality_gate.validate_cleaned_batch(
        source=config.CATALOG_SOURCE,
        batch_id=config.catalog_batch_id(ingest_date, resolved),
    )


def publish_events_task(ingest_date: str, **context: Any) -> dict[str, Any]:
    return pipelines.publish_events(ingest_date)


def quality_gate_historical_task(
    ingest_date: str, **context: Any
) -> dict[str, Any]:
    return quality_gate.run_pipeline_quality(
        ingest_date=ingest_date,
        specs=[
            {
                "source": config.HISTORICAL_SOURCE,
                "batch_id": config.historical_batch_id(ingest_date),
            },
            {
                "source": config.CATALOG_SOURCE,
                "batch_id": config.catalog_batch_id(ingest_date, 1),
            },
        ],
    )


def quality_gate_daily_task(
    ingest_date: str, version: Any = 0, **context: Any
) -> dict[str, Any]:
    resolved = resolve_catalog_version(ingest_date, version)
    return quality_gate.run_pipeline_quality(
        ingest_date=ingest_date,
        specs=[
            {
                "source": config.EVENTS_SOURCE,
                "batch_id": config.events_batch_id(ingest_date),
            },
            {
                "source": config.CATALOG_SOURCE,
                "batch_id": config.catalog_batch_id(ingest_date, resolved),
            },
        ],
    )
