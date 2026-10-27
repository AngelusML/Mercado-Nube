"""Funciones invocadas por Airflow. Delgadas: delegan en src.pipelines."""

from __future__ import annotations

import logging
from typing import Any

from src import config, pipelines, quality_gate

logger = logging.getLogger(__name__)

CATALOG_VERSION_BY_DATE = {
    "2026-10-22": 2,
    "2026-10-23": 3,
}


def resolve_catalog_version(ingest_date: str, version: Any = 0) -> int:
    try:
        requested = int(version)
    except (TypeError, ValueError):
        requested = 0
    if requested > 0:
        return requested
    return CATALOG_VERSION_BY_DATE.get(ingest_date, 3)


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
