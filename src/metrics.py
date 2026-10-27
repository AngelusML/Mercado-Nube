"""Metricas operativas del pipeline hacia CloudWatch.

- Siempre escribe una linea `METRIC {...}` en el log de la tarea (modo local).
- Solo publica en CloudWatch si CLOUDWATCH_METRICS_ENABLED=true (modo AWS),
  usando el rol de ejecucion de MWAA: no hay access keys en el codigo.
- Es "best effort": si CloudWatch falla, se registra un warning y el
  pipeline NO se detiene por eso.

Las alarmas que usan estas metricas estan en terraform/main.tf:
  RejectionRatePct    -> alarma de aumento de rechazos.
  DuplicateRatePct    -> informativa.
  BatchesPublished    -> alarma de retraso de datos (sin lotes en 24 h).
  QualityGateFailed   -> alarma de fallo de calidad.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

NAMESPACE = os.getenv("CLOUDWATCH_METRICS_NAMESPACE", "MercadoNube/Pipeline")


def _enabled() -> bool:
    return os.getenv("CLOUDWATCH_METRICS_ENABLED", "false").strip().lower() == "true"


def _put(metric_data: list[dict[str, Any]]) -> None:
    for item in metric_data:
        logger.info("METRIC %s", json.dumps(item, ensure_ascii=False, default=str))
    if not _enabled():
        return
    try:
        import boto3

        boto3.client("cloudwatch").put_metric_data(
            Namespace=NAMESPACE, MetricData=metric_data
        )
    except Exception as exc:  # pragma: no cover - depende de AWS
        logger.warning("No se pudieron publicar metricas en CloudWatch: %s", exc)


def emit_batch_metrics(
    *,
    source: str,
    batch_id: str,
    rows_read: int,
    rows_rejected: int,
    rows_duplicated: int,
) -> None:
    """Tasas de rechazo y duplicados de la capa cleaned, por fuente."""
    rejection_pct = 100.0 * rows_rejected / rows_read if rows_read else 0.0
    duplicate_pct = 100.0 * rows_duplicated / rows_read if rows_read else 0.0
    dimensions = [{"Name": "Source", "Value": source}]
    _put(
        [
            {"MetricName": "RowsRead", "Dimensions": dimensions,
             "Value": float(rows_read), "Unit": "Count"},
            {"MetricName": "RejectionRatePct", "Dimensions": dimensions,
             "Value": round(rejection_pct, 4), "Unit": "Percent"},
            {"MetricName": "DuplicateRatePct", "Dimensions": dimensions,
             "Value": round(duplicate_pct, 4), "Unit": "Percent"},
        ]
    )
    logger.info("Metricas del lote %s emitidas", batch_id)


def emit_quality_result(*, ingest_date: str, passed: bool) -> None:
    """Resultado del quality gate final; un lote sano cuenta como publicado."""
    _put(
        [
            {"MetricName": "BatchesPublished", "Value": 1.0 if passed else 0.0,
             "Unit": "Count"},
            {"MetricName": "QualityGateFailed", "Value": 0.0 if passed else 1.0,
             "Unit": "Count"},
        ]
    )
    logger.info("Resultado de calidad del %s emitido (passed=%s)", ingest_date, passed)
