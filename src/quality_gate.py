"""Quality gates.

- validate_cleaned_batch: corre ANTES de publicar y bloquea la publicacion.
- run_pipeline_quality: corre DESPUES de publicar y reconcilia el destino.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pandas as pd

from src import config, db
from src import metrics as cw_metrics

logger = logging.getLogger(__name__)


def _scalar(conn, sql: str, params: tuple = ()) -> int:
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        value = cursor.fetchone()[0]
        return int(value) if value is not None else 0


def _metric(metrics: dict[str, Any], key: str) -> int:
    value = metrics.get(key)
    try:
        if value is None or pd.isna(value):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _add_check(
    conn,
    checks: list[dict[str, Any]],
    *,
    batch_id: str,
    name: str,
    passed: bool,
    observed: str,
    threshold: str,
    details: str = "",
) -> None:
    status = "pass" if passed else "fail"
    checks.append(
        {
            "check": name,
            "status": status,
            "observed": observed,
            "threshold": threshold,
        }
    )
    db.add_quality_result(
        conn,
        batch_id=batch_id,
        check_name=name,
        status=status,
        observed_value=observed,
        threshold=threshold,
        details=details,
    )
    if not passed:
        db.add_alert(
            conn,
            alert_key=f"{batch_id}:{name}",
            severity="critical",
            category="data_quality",
            message=f"{name} fallo: observado={observed}, esperado={threshold}. {details}",
            batch_id=batch_id,
        )


def _run_one(
    *,
    ingest_date: str,
    source: str,
    batch_id: str,
) -> dict[str, Any]:
    metrics = db.get_metrics(batch_id, source, layer="published")
    if metrics is None:
        raise RuntimeError(
            f"No hay metricas published para batch={batch_id} source={source}."
        )

    checks: list[dict[str, Any]] = []
    rows_read = _metric(metrics, "rows_read")
    rows_valid = _metric(metrics, "rows_valid")
    rows_rejected = _metric(metrics, "rows_rejected")
    rows_duplicated = _metric(metrics, "rows_duplicated")
    rows_late = _metric(metrics, "rows_late")
    rows_published = _metric(metrics, "rows_published")
    rows_conflicting = _metric(metrics, "rows_conflicting")

    with db.connection() as conn:
        _add_check(
            conn, checks, batch_id=batch_id, name="non_empty_batch",
            passed=rows_read > 0,
            observed=f"rows_read={rows_read}",
            threshold="rows_read > 0",
        )
        reconciled = rows_rejected + rows_duplicated + rows_published
        _add_check(
            conn, checks, batch_id=batch_id, name="row_accounting",
            passed=rows_read == reconciled,
            observed=(
                f"read={rows_read}; rejected={rows_rejected}; "
                f"duplicated={rows_duplicated}; published={rows_published}; "
                f"sum={reconciled}"
            ),
            threshold="read = rejected + duplicated + published",
            details=f"valid={rows_valid}; conflicting={rows_conflicting}; late={rows_late}",
        )
        rejection_rate = rows_rejected / rows_read if rows_read else 0.0
        _add_check(
            conn, checks, batch_id=batch_id, name="rejection_rate",
            passed=rows_read > 0 and rejection_rate <= config.QUALITY_MAX_REJECTION_RATE,
            observed=f"{rejection_rate:.4f} ({rows_rejected}/{rows_read})",
            threshold=f"<= {config.QUALITY_MAX_REJECTION_RATE:.4f}",
        )
        duplicate_rate = rows_duplicated / rows_read if rows_read else 0.0
        _add_check(
            conn, checks, batch_id=batch_id, name="duplicate_rate",
            passed=rows_read > 0 and duplicate_rate <= config.QUALITY_MAX_DUPLICATE_RATE,
            observed=f"{duplicate_rate:.4f} ({rows_duplicated}/{rows_read})",
            threshold=f"<= {config.QUALITY_MAX_DUPLICATE_RATE:.4f}",
        )
        bad_quarantine = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM ops.quarantine_records
            WHERE batch_id = %s
              AND (raw_payload IS NULL OR jsonb_typeof(raw_payload) = 'null'
                   OR reason_code IS NULL OR reason_code = '')
            """,
            (batch_id,),
        )
        _add_check(
            conn, checks, batch_id=batch_id, name="quarantine_payload",
            passed=bad_quarantine == 0,
            observed=f"invalid_quarantine_rows={bad_quarantine}",
            threshold="0",
        )

        if source == config.HISTORICAL_SOURCE:
            null_usd = _scalar(
                conn,
                """
                SELECT COUNT(*) FROM analytics.fact_order_line
                WHERE batch_id = %s
                  AND (gross_amount_usd IS NULL OR return_amount_usd IS NULL
                       OR net_amount_usd IS NULL)
                """,
                (batch_id,),
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="fact_line_usd_complete",
                passed=null_usd == 0,
                observed=f"null_usd={null_usd}",
                threshold="0",
            )
            unknown_product = _scalar(
                conn,
                "SELECT COUNT(*) FROM analytics.fact_order_line WHERE batch_id = %s AND product_key = 0",
                (batch_id,),
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="fact_line_product_known",
                passed=unknown_product == 0,
                observed=f"unknown_product={unknown_product}",
                threshold="0",
            )
            # country_key=0 / currency_key=0 son miembros desconocidos EXPLICITOS
            # (ZZ / XXX), no fallos de join. Se permiten y se monitorean.
            missing_dimension_refs = _scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM analytics.fact_order_line f
                LEFT JOIN analytics.dim_country c ON c.country_key = f.country_key
                LEFT JOIN analytics.dim_currency cur ON cur.currency_key = f.currency_key
                WHERE f.batch_id = %s
                  AND (c.country_key IS NULL OR cur.currency_key IS NULL)
                """,
                (batch_id,),
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="fact_line_geo_currency_known",
                passed=missing_dimension_refs == 0,
                observed=f"missing_dimension_refs={missing_dimension_refs}",
                threshold="0",
            )
            total_lines = _scalar(
                conn,
                "SELECT COUNT(*) FROM analytics.fact_order_line WHERE batch_id = %s",
                (batch_id,),
            )
            unknown_geo = _scalar(
                conn,
                """
                SELECT COUNT(*) FROM analytics.fact_order_line
                WHERE batch_id = %s AND (country_key = 0 OR currency_key = 0)
                """,
                (batch_id,),
            )
            unknown_rate = unknown_geo / total_lines if total_lines else 0.0
            _add_check(
                conn, checks, batch_id=batch_id, name="fact_line_explicit_unknown_rate",
                passed=unknown_rate <= config.QUALITY_MAX_UNKNOWN_RATE,
                observed=f"{unknown_rate:.4f} ({unknown_geo}/{total_lines})",
                threshold=f"<= {config.QUALITY_MAX_UNKNOWN_RATE:.4f}; ZZ/XXX son explicitos",
            )

        if source == config.EVENTS_SOURCE:
            null_usd = _scalar(
                conn,
                "SELECT COUNT(*) FROM analytics.fact_order_event WHERE batch_id = %s AND amount_usd IS NULL",
                (batch_id,),
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="fact_event_usd_complete",
                passed=null_usd == 0,
                observed=f"null_usd={null_usd}",
                threshold="0",
            )
            missing_event_refs = _scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM analytics.fact_order_event e
                LEFT JOIN analytics.dim_country c ON c.country_key = e.country_key
                LEFT JOIN analytics.dim_currency cur ON cur.currency_key = e.currency_key
                WHERE e.batch_id = %s
                  AND (c.country_key IS NULL OR cur.currency_key IS NULL)
                """,
                (batch_id,),
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="fact_event_dimension_refs",
                passed=missing_event_refs == 0,
                observed=f"missing_dimension_refs={missing_event_refs}",
                threshold="0",
            )
            total_events_batch = _scalar(
                conn,
                "SELECT COUNT(*) FROM analytics.fact_order_event WHERE batch_id = %s",
                (batch_id,),
            )
            unknown_events = _scalar(
                conn,
                """
                SELECT COUNT(*) FROM analytics.fact_order_event
                WHERE batch_id = %s AND (country_key = 0 OR currency_key = 0)
                """,
                (batch_id,),
            )
            unknown_event_rate = (
                unknown_events / total_events_batch if total_events_batch else 0.0
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="fact_event_explicit_unknown_rate",
                passed=unknown_event_rate <= config.QUALITY_MAX_UNKNOWN_RATE,
                observed=f"{unknown_event_rate:.4f} ({unknown_events}/{total_events_batch})",
                threshold=f"<= {config.QUALITY_MAX_UNKNOWN_RATE:.4f}; ZZ/XXX son explicitos",
            )
            duplicate_ids = _scalar(
                conn,
                """
                SELECT COUNT(*) FROM (
                    SELECT source, event_id FROM analytics.fact_order_event
                    GROUP BY source, event_id HAVING COUNT(*) > 1
                ) duplicated
                """
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="event_id_unique",
                passed=duplicate_ids == 0,
                observed=f"duplicated_event_ids={duplicate_ids}",
                threshold="0",
            )
            missing_lifecycle = _scalar(
                conn,
                """
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT order_id FROM analytics.fact_order_event
                    WHERE source = 'events'
                ) e
                LEFT JOIN analytics.fact_order_lifecycle l
                  ON l.source = 'events' AND l.order_id = e.order_id
                WHERE l.order_id IS NULL
                """
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="lifecycle_coverage",
                passed=missing_lifecycle == 0,
                observed=f"orders_without_lifecycle={missing_lifecycle}",
                threshold="0",
            )
            # Frescura de INGESTA: un evento tardio se acepta y se marca
            # is_late; lo que no debe quedar viejo es el lote ingerido.
            # La latencia del event_time se mide aparte con late_event_ratio.
            max_ingested = pd.read_sql_query(
                "SELECT MAX(ingested_at) AS max_ingested FROM analytics.fact_order_event WHERE batch_id = %s",
                conn,
                params=(batch_id,),
            ).iloc[0]["max_ingested"]
            batch_end = config.ingest_timestamp(ingest_date)
            if pd.isna(max_ingested):
                age_hours = None
                freshness_ok = False
            else:
                max_ingested_ts = pd.Timestamp(max_ingested).to_pydatetime()
                age_hours = (batch_end - max_ingested_ts).total_seconds() / 3600
                freshness_ok = age_hours <= config.QUALITY_FRESHNESS_HOURS
            _add_check(
                conn, checks, batch_id=batch_id, name="event_freshness",
                passed=freshness_ok,
                observed=(
                    f"ingest_age_hours={age_hours:.2f}"
                    if age_hours is not None else "sin eventos"
                ),
                threshold=(
                    f"ingesta <= {config.QUALITY_FRESHNESS_HOURS}h respecto de "
                    f"{ingest_date}T23:59:59Z; tardios se monitorean aparte"
                ),
            )
            future_events = _scalar(
                conn,
                "SELECT COUNT(*) FROM analytics.fact_order_event WHERE batch_id = %s AND event_time_utc > %s",
                (batch_id, batch_end),
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="event_not_future",
                passed=future_events == 0,
                observed=f"future_events={future_events}",
                threshold="0",
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="late_event_ratio_informative",
                passed=True,
                observed=f"late={rows_late}/{rows_published}",
                threshold="informativo; se reprocesa el ciclo completo de la orden",
                details="Los tardios se insertan con is_late=true y disparan recomputo de lifecycle.",
            )

        if source == config.CATALOG_SOURCE:
            current_dupes = _scalar(
                conn,
                """
                SELECT COUNT(*) FROM (
                    SELECT sku FROM analytics.dim_product
                    WHERE is_current = true AND sku <> '__UNKNOWN__'
                    GROUP BY sku HAVING COUNT(*) > 1
                ) d
                """
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="scd2_one_current_per_sku",
                passed=current_dupes == 0,
                observed=f"skus_with_multiple_current={current_dupes}",
                threshold="0",
            )
            overlaps = _scalar(
                conn,
                """
                SELECT COUNT(*) FROM analytics.dim_product a
                JOIN analytics.dim_product b
                  ON a.sku = b.sku AND a.product_key < b.product_key
                 AND a.valid_from <= b.valid_to
                 AND b.valid_from <= a.valid_to
                WHERE a.sku <> '__UNKNOWN__'
                """
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="scd2_no_overlap",
                passed=overlaps == 0,
                observed=f"overlapping_pairs={overlaps}",
                threshold="0",
            )
            missing_unknown = _scalar(
                conn,
                """
                SELECT COUNT(*) FROM analytics.dim_product
                WHERE product_key = 0 AND sku = '__UNKNOWN__'
                """
            )
            _add_check(
                conn, checks, batch_id=batch_id, name="unknown_product_member",
                passed=missing_unknown == 1,
                observed=f"unknown_product_rows={missing_unknown}",
                threshold="1",
            )

    failed = [item for item in checks if item["status"] == "fail"]
    summary = {
        "batch_id": batch_id,
        "source": source,
        "passed": not failed,
        "checks": checks,
        "failed": [item["check"] for item in failed],
        "metrics": {
            "rows_read": rows_read,
            "rows_valid": rows_valid,
            "rows_rejected": rows_rejected,
            "rows_duplicated": rows_duplicated,
            "rows_late": rows_late,
            "rows_published": rows_published,
            "rows_conflicting": rows_conflicting,
        },
    }
    logger.info(
        "Quality gate %s/%s: %s (%s checks, fallos=%s)",
        source, batch_id, "PASS" if not failed else "FAIL",
        len(checks), summary["failed"],
    )
    return summary


def run_quality_gate(
    *,
    ingest_date: str,
    source: str,
    batch_id: str,
) -> dict[str, Any]:
    return _run_one(ingest_date=ingest_date, source=source, batch_id=batch_id)


def run_pipeline_quality(
    *,
    ingest_date: str,
    specs: list[dict[str, str]],
) -> dict[str, Any]:
    """Ejecuta varios quality gates y falla al final si alguno fallo."""
    results = []
    for spec in specs:
        try:
            results.append(
                run_quality_gate(
                    ingest_date=ingest_date,
                    source=spec["source"],
                    batch_id=spec["batch_id"],
                )
            )
        except Exception as exc:  # el fallo de un gate no oculta los demas
            results.append(
                {
                    "batch_id": spec.get("batch_id"),
                    "source": spec.get("source"),
                    "passed": False,
                    "checks": [],
                    "failed": ["gate_exception"],
                    "error": str(exc),
                }
            )
    failed = [result for result in results if not result.get("passed")]
    summary = {
        "ingest_date": ingest_date,
        "passed": not failed,
        "results": results,
        "failed": [
            {"source": item.get("source"), "batch_id": item.get("batch_id"),
             "checks": item.get("failed")}
            for item in failed
        ],
    }
    cw_metrics.emit_quality_result(ingest_date=ingest_date, passed=not failed)
    if failed:
        raise RuntimeError(
            "Quality gate fallo: " + json.dumps(summary["failed"], ensure_ascii=False)
        )
    return summary


# ---------------------------------------------------------------------------
# Validacion PREVIA a publicar (write-audit-publish).
# Se ejecuta sobre la capa cleaned: si falla, el lote NO llega al warehouse.
# El quality gate final (run_pipeline_quality) sigue corriendo despues de
# publicar para reconciliar lo que quedo en el destino.
# ---------------------------------------------------------------------------
def evaluate_cleaned_batch(
    *,
    rows_read: int,
    rows_rejected: int,
    rows_duplicated: int,
    cleaned_rows: int,
    max_rejection_rate: float,
    max_duplicate_rate: float,
) -> list[dict[str, Any]]:
    """Reglas puras (sin base de datos) para poder probarlas con pytest."""
    rejection_rate = rows_rejected / rows_read if rows_read else 0.0
    duplicate_rate = rows_duplicated / rows_read if rows_read else 0.0
    accounted = rows_rejected + rows_duplicated + cleaned_rows
    return [
        {
            "name": "pre_publish_non_empty_batch",
            "passed": rows_read > 0,
            "observed": f"rows_read={rows_read}",
            "threshold": "rows_read > 0",
        },
        {
            "name": "pre_publish_row_accounting",
            "passed": rows_read == accounted,
            "observed": (
                f"read={rows_read}; rejected={rows_rejected}; "
                f"duplicated={rows_duplicated}; cleaned={cleaned_rows}; sum={accounted}"
            ),
            "threshold": "read = rejected + duplicated + cleaned",
        },
        {
            "name": "pre_publish_rejection_rate",
            "passed": rows_read > 0 and rejection_rate <= max_rejection_rate,
            "observed": f"{rejection_rate:.4f} ({rows_rejected}/{rows_read})",
            "threshold": f"<= {max_rejection_rate:.4f}",
        },
        {
            "name": "pre_publish_duplicate_rate",
            "passed": rows_read > 0 and duplicate_rate <= max_duplicate_rate,
            "observed": f"{duplicate_rate:.4f} ({rows_duplicated}/{rows_read})",
            "threshold": f"<= {max_duplicate_rate:.4f}",
        },
    ]


def validate_cleaned_batch(*, source: str, batch_id: str) -> dict[str, Any]:
    """Bloquea la publicacion si la capa cleaned incumple los umbrales."""
    metrics = db.get_metrics(batch_id, source, layer="cleaned")
    if metrics is None:
        raise RuntimeError(
            f"No hay metricas cleaned para batch={batch_id} source={source}."
        )
    manifest = db.query_df(
        """
        SELECT row_count FROM ops.batch_manifest
        WHERE batch_id = %s AND source = %s AND layer = 'cleaned'
        """,
        (batch_id, source),
    )
    if manifest.empty:
        raise RuntimeError(
            f"No hay manifiesto cleaned para batch={batch_id} source={source}."
        )
    results = evaluate_cleaned_batch(
        rows_read=_metric(metrics, "rows_read"),
        rows_rejected=_metric(metrics, "rows_rejected"),
        rows_duplicated=_metric(metrics, "rows_duplicated"),
        cleaned_rows=int(manifest.iloc[0]["row_count"]),
        max_rejection_rate=config.QUALITY_MAX_REJECTION_RATE,
        max_duplicate_rate=config.QUALITY_MAX_DUPLICATE_RATE,
    )
    checks: list[dict[str, Any]] = []
    with db.connection() as conn:
        for item in results:
            _add_check(
                conn, checks, batch_id=batch_id, name=item["name"],
                passed=item["passed"], observed=item["observed"],
                threshold=item["threshold"],
            )
    failed = [check["check"] for check in checks if check["status"] != "pass"]
    summary = {
        "batch_id": batch_id,
        "source": source,
        "passed": not failed,
        "checks": checks,
        "failed": failed,
    }
    logger.info(
        "Validacion previa %s/%s: %s", source, batch_id,
        "PASS" if not failed else f"FAIL {failed}",
    )
    cw_metrics.emit_batch_metrics(
        source=source,
        batch_id=batch_id,
        rows_read=_metric(metrics, "rows_read"),
        rows_rejected=_metric(metrics, "rows_rejected"),
        rows_duplicated=_metric(metrics, "rows_duplicated"),
    )
    if failed:
        raise RuntimeError(
            "Validacion previa a publicar fallo; el lote NO se publico: "
            + json.dumps(summary["failed"], ensure_ascii=False)
        )
    return summary
