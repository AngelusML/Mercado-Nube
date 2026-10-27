"""Pipeline local: S3 por capas -> cuarentena -> PostgreSQL -> curado.

Cada funcion es idempotente respecto de su batch_id. Las tareas de Airflow
solo orquestan; la logica vive aqui para poder probarla sin levantar Airflow.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src import catalog_client, config, db, quality, storage

logger = logging.getLogger(__name__)

HISTORICAL_CLEAN_COLUMNS = [
    "line_key", "invoice_no", "source", "product_sku", "description",
    "customer_id", "country_code", "currency_code", "date_key", "rate_date",
    "invoice_date_utc", "quantity", "unit_price", "gross_amount",
    "return_amount", "net_amount", "line_type", "is_cancellation",
    "record_hash", "batch_id", "created_at",
]

EVENT_CLEAN_COLUMNS = [
    "event_id", "order_id", "source", "event_type", "event_time_utc",
    "event_time_original", "ingested_at", "is_late", "late_hours",
    "country_code", "currency_code", "amount_original", "record_hash",
    "batch_id", "created_at", "raw_payload",
]

CATALOG_CLEAN_COLUMNS = [
    "sku", "name", "category", "is_service", "is_active", "valid_from",
    "valid_to", "updated_at", "catalog_version", "record_hash", "batch_id",
    "created_at",
]

STAGING_ORDER_LINE_COLUMNS = [
    "line_key", "invoice_no", "source", "product_sku", "customer_id",
    "country_code", "currency_code", "date_key", "rate_date",
    "invoice_date_utc", "quantity", "unit_price", "gross_amount",
    "return_amount", "net_amount", "line_type", "is_cancellation",
    "record_hash", "batch_id", "created_at",
]

STAGING_EVENT_COLUMNS = EVENT_CLEAN_COLUMNS

FAR_FUTURE = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or pd.isna(value):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _upsert_metrics_from_stats(
    conn,
    *,
    batch_id: str,
    source: str,
    stats: dict[str, Any],
    layer: str,
    status: str,
    rows_published: int = 0,
    rows_duplicated: int | None = None,
    rows_rejected: int | None = None,
    rows_conflicting: int | None = None,
) -> None:
    db.upsert_metrics(
        conn,
        batch_id=batch_id,
        source=source,
        layer=layer,
        rows_read=_as_int(stats.get("rows_read")),
        rows_valid=_as_int(stats.get("rows_valid")),
        rows_rejected=(
            _as_int(rows_rejected)
            if rows_rejected is not None
            else _as_int(stats.get("rows_rejected"))
        ),
        rows_duplicated=(
            _as_int(rows_duplicated)
            if rows_duplicated is not None
            else _as_int(stats.get("rows_duplicated"))
        ),
        rows_late=_as_int(stats.get("rows_late")),
        rows_published=rows_published,
        rows_conflicting=(
            _as_int(rows_conflicting)
            if rows_conflicting is not None
            else _as_int(stats.get("rows_conflicting"))
        ),
        finished_at=config.utc_now(),
        status=status,
    )


def _empty_frame(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="object") for column in columns})


# ---------------------------------------------------------------------------
# Historico
# ---------------------------------------------------------------------------
def extract_historical(ingest_date: str, **_: Any) -> dict[str, Any]:
    batch_id = config.historical_batch_id(ingest_date)
    path = config.HISTORICAL_CSV
    if not path.exists():
        raise FileNotFoundError(
            f"No existe {path}. Ejecuta scripts/prepare_historical.py primero."
        )
    with path.open("r", encoding="utf-8") as handle:
        row_count = max(0, sum(1 for line in handle if line.strip()) - 1)
    checksum = _sha256_file(path)
    key = storage.raw_historical_key(ingest_date, batch_id)
    storage.upload_file(path, key, "text/csv")
    metadata_key = key.rsplit("/", 1)[0] + "/source_metadata.json"
    storage.write_json(
        metadata_key,
        {
            "batch_id": batch_id,
            "source": config.HISTORICAL_SOURCE,
            "ingest_date": ingest_date,
            "file": path.name,
            "row_count": row_count,
            "sha256": checksum,
            "note": "Online Retail II, hoja Year 2010-2011; zona origen Europe/London.",
        },
    )
    with db.connection() as conn:
        db.upsert_manifest(
            conn,
            batch_id=batch_id,
            source=config.HISTORICAL_SOURCE,
            layer="raw",
            object_path=key,
            row_count=row_count,
            checksum=checksum,
            status="complete",
        )
    logger.info("Historico raw: %s filas, sha256=%s", row_count, checksum[:12])
    return {
        "batch_id": batch_id,
        "ingest_date": ingest_date,
        "raw_key": key,
        "row_count": row_count,
        "checksum": checksum,
    }


def clean_historical(ingest_date: str, **_: Any) -> dict[str, Any]:
    batch_id = config.historical_batch_id(ingest_date)
    raw_key = storage.raw_historical_key(ingest_date, batch_id)
    raw_bytes = storage.get_bytes(raw_key)
    dataframe = pd.read_csv(
        io.BytesIO(raw_bytes),
        dtype=str,
        keep_default_na=False,
        encoding="utf-8",
    )
    result = quality.validate_historical_frame(
        dataframe, batch_id=batch_id, ingest_date=ingest_date
    )
    cleaned_key = storage.layer_key(
        "cleaned", config.HISTORICAL_SOURCE, ingest_date, batch_id,
        "order_lines.parquet",
    )
    quarantine_key = storage.layer_key(
        "cleaned", config.HISTORICAL_SOURCE, ingest_date, batch_id,
        "quarantine.parquet",
    )
    duplicates_key = storage.layer_key(
        "cleaned", config.HISTORICAL_SOURCE, ingest_date, batch_id,
        "duplicates.parquet",
    )
    cleaned_checksum = storage.put_parquet(cleaned_key, result["valid"])
    quarantine_checksum = storage.put_parquet(quarantine_key, result["quarantine"])
    duplicates_checksum = storage.put_parquet(duplicates_key, result["duplicates"])

    with db.connection() as conn:
        db.clear_quarantine(
            conn, batch_id=batch_id, source=config.HISTORICAL_SOURCE
        )
        inserted_quarantine = db.insert_quarantine(conn, result["quarantine"])
        db.upsert_manifest(
            conn, batch_id=batch_id, source=config.HISTORICAL_SOURCE,
            layer="cleaned", object_path=cleaned_key,
            row_count=len(result["valid"]), checksum=cleaned_checksum,
            status="complete",
        )
        db.upsert_manifest(
            conn, batch_id=batch_id, source=config.HISTORICAL_SOURCE,
            layer="quarantine", object_path=quarantine_key,
            row_count=len(result["quarantine"]), checksum=quarantine_checksum,
            status="complete",
        )
        db.upsert_manifest(
            conn, batch_id=batch_id, source=config.HISTORICAL_SOURCE,
            layer="duplicates", object_path=duplicates_key,
            row_count=len(result["duplicates"]), checksum=duplicates_checksum,
            status="complete",
        )
        _upsert_metrics_from_stats(
            conn, batch_id=batch_id, source=config.HISTORICAL_SOURCE,
            stats=result["stats"], layer="cleaned", status="cleaned",
        )
    stats = result["stats"]
    logger.info(
        "Historico limpio: %s leidas, %s validas, %s rechazadas, %s duplicadas, %s en cuarentena nuevas",
        stats["rows_read"], stats["rows_valid"], stats["rows_rejected"],
        stats["rows_duplicated"], inserted_quarantine,
    )
    return {
        "batch_id": batch_id,
        "ingest_date": ingest_date,
        "cleaned_key": cleaned_key,
        "quarantine_key": quarantine_key,
        "duplicates_key": duplicates_key,
        **stats,
    }


def _catalog_current_map(conn) -> dict[str, dict[str, Any]]:
    current = pd.read_sql_query(
        """
        SELECT product_key, sku, record_hash, valid_from
        FROM analytics.dim_product
        WHERE is_current = true AND sku <> '__UNKNOWN__'
        """,
        conn,
    )
    return {
        str(row.sku): {
            "product_key": int(row.product_key),
            "record_hash": str(row.record_hash),
            "valid_from": pd.Timestamp(row.valid_from).to_pydatetime(),
        }
        for row in current.itertuples(index=False)
    }


def _publish_catalog_scd2(
    conn,
    catalog_df: pd.DataFrame,
    *,
    version: int,
    batch_id: str,
) -> dict[str, int]:
    """Cierra versiones cambiadas e inserta nuevas; compara hash de atributos."""
    if catalog_df.empty:
        return {"inserted": 0, "changed": 0, "unchanged": 0}

    current_map = _catalog_current_map(conn)
    inserts: list[tuple] = []
    inserted = 0
    changed = 0
    unchanged = 0

    for row in catalog_df.itertuples(index=False):
        sku = str(row.sku)
        valid_from = row.valid_from
        if hasattr(valid_from, "to_pydatetime"):
            valid_from = valid_from.to_pydatetime()
        if valid_from.tzinfo is None:
            valid_from = valid_from.replace(tzinfo=timezone.utc)
        updated_at = row.updated_at
        if hasattr(updated_at, "to_pydatetime"):
            updated_at = updated_at.to_pydatetime()
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        record_hash = str(row.record_hash)
        existing = current_map.get(sku)
        if existing is not None and valid_from < existing["valid_from"]:
            # Snapshot anterior al corriente: no reabrir historia.
            # Un rerun viejo no debe convertir v1 en "current" despues de v3.
            unchanged += 1
            continue
        if existing is None:
            inserted += 1
        elif existing["record_hash"] != record_hash:
            changed += 1
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE analytics.dim_product
                    SET valid_to = %s - INTERVAL '1 microsecond',
                        is_current = false,
                        updated_at = %s
                    WHERE product_key = %s AND is_current = true
                    """,
                    (valid_from, updated_at, existing["product_key"]),
                )
        else:
            unchanged += 1
            continue
        inserts.append(
            (
                sku,
                str(row.name),
                str(row.category),
                bool(row.is_service),
                bool(row.is_active),
                valid_from,
                FAR_FUTURE,
                True,
                int(version),
                updated_at,
                record_hash,
            )
        )

    if inserts:
        db.execute_values(
            conn,
            """
            INSERT INTO analytics.dim_product
                (sku, product_name, category, is_service, is_active,
                 valid_from, valid_to, is_current, catalog_version,
                 updated_at, record_hash)
            VALUES %s
            ON CONFLICT (sku, valid_from) DO NOTHING
            """,
            inserts,
        )
    logger.info(
        "SCD2 catalogo v%s: nuevos=%s cambiados=%s sin_cambio=%s",
        version, inserted, changed, unchanged,
    )
    return {"inserted": inserted, "changed": changed, "unchanged": unchanged}

def _catalog_clean_metrics(batch_id: str) -> dict[str, Any]:
    metrics = db.get_metrics(batch_id, config.CATALOG_SOURCE, layer="cleaned")
    if metrics is None:
        raise RuntimeError(
            f"No existen metricas cleaned para {batch_id}. "
            "Ejecuta clean_catalog antes de publicar."
        )
    return metrics


def _publish_catalog_metrics(
    conn,
    *,
    batch_id: str,
    counts: dict[str, int],
) -> None:
    clean = _catalog_clean_metrics(batch_id)
    duplicate_sku = _as_int(clean.get("rows_duplicated"))
    rejected = _as_int(clean.get("rows_rejected"))
    rows_read = _as_int(clean.get("rows_read"))
    rows_valid = _as_int(clean.get("rows_valid"))
    # Para catalogo, "duplicado" significa SKU repetido en la MISMA foto.
    # Los productos sin cambio tambien se procesaron/publicaron como snapshot
    # (no generan nueva version SCD2, pero si son filas aceptadas del lote).
    duplicates_total = duplicate_sku
    published = counts["inserted"] + counts["changed"] + counts["unchanged"]
    db.upsert_metrics(
        conn,
        batch_id=batch_id,
        source=config.CATALOG_SOURCE,
        layer="published",
        rows_read=rows_read,
        rows_valid=rows_valid,
        rows_rejected=rejected,
        rows_duplicated=duplicates_total,
        rows_late=0,
        rows_published=published,
        rows_conflicting=0,
        finished_at=config.utc_now(),
        status="published",
    )


def _write_curated_dim_product(
    conn,
    *,
    ingest_date: str,
    batch_id: str,
) -> str:
    dim_product = pd.read_sql_query(
        """
        SELECT * FROM analytics.dim_product
        WHERE sku <> '__UNKNOWN__'
        ORDER BY sku, valid_from
        """,
        conn,
    )
    key = storage.curated_key("dim_product", ingest_date, batch_id)
    checksum = storage.put_parquet(key, dim_product)
    db.upsert_manifest(
        conn, batch_id=batch_id, source=config.CATALOG_SOURCE,
        layer="curated", object_path=key, row_count=len(dim_product),
        checksum=checksum, status="complete",
    )
    return key


def _write_curated_dim_customer(
    conn,
    *,
    ingest_date: str,
    batch_id: str,
) -> str:
    dim_customer = pd.read_sql_query(
        """
        SELECT * FROM analytics.dim_customer
        WHERE source = 'historical'
        ORDER BY customer_key
        """,
        conn,
    )
    key = storage.curated_key("dim_customer", ingest_date, batch_id)
    checksum = storage.put_parquet(key, dim_customer)
    db.upsert_manifest(
        conn, batch_id=batch_id, source=config.HISTORICAL_SOURCE,
        layer="curated", object_path=key, row_count=len(dim_customer),
        checksum=checksum, status="complete",
    )
    return key


def extract_catalog(ingest_date: str, version: int, **_: Any) -> dict[str, Any]:
    batch_id = config.catalog_batch_id(ingest_date, version)
    items, metadata = catalog_client.fetch_catalog(version)
    payload = {
        "version": version,
        "generated_at": metadata.get("extracted_at"),
        "product_count": len(items),
        "pages_fetched": metadata.get("pages_fetched"),
        "products": items,
    }
    key = storage.raw_catalog_key(ingest_date, batch_id, version)
    storage.write_json(key, payload)
    checksum = quality.sha256_text(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    )
    with db.connection() as conn:
        db.upsert_manifest(
            conn, batch_id=batch_id, source=config.CATALOG_SOURCE,
            layer="raw", object_path=key, row_count=len(items),
            checksum=checksum, status="complete",
        )
    logger.info("Catalogo v%s raw: %s productos", version, len(items))
    return {
        "batch_id": batch_id,
        "ingest_date": ingest_date,
        "version": version,
        "raw_key": key,
        "row_count": len(items),
        "checksum": checksum,
        "pages_fetched": metadata.get("pages_fetched"),
    }


def clean_catalog(ingest_date: str, version: int, **_: Any) -> dict[str, Any]:
    batch_id = config.catalog_batch_id(ingest_date, version)
    raw_key = storage.raw_catalog_key(ingest_date, batch_id, version)
    payload = storage.read_json(raw_key)
    result = quality.validate_catalog_frame(
        payload.get("products", []),
        batch_id=batch_id,
        version=version,
        ingest_date=ingest_date,
    )
    cleaned_key = storage.layer_key(
        "cleaned", config.CATALOG_SOURCE, ingest_date, batch_id,
        f"catalog_v{version}.parquet",
    )
    quarantine_key = storage.layer_key(
        "cleaned", config.CATALOG_SOURCE, ingest_date, batch_id,
        "quarantine.parquet",
    )
    duplicates_key = storage.layer_key(
        "cleaned", config.CATALOG_SOURCE, ingest_date, batch_id,
        "duplicates.parquet",
    )
    cleaned_checksum = storage.put_parquet(cleaned_key, result["valid"])
    quarantine_checksum = storage.put_parquet(quarantine_key, result["quarantine"])
    duplicates_checksum = storage.put_parquet(duplicates_key, result["duplicates"])
    with db.connection() as conn:
        db.clear_quarantine(
            conn, batch_id=batch_id, source=config.CATALOG_SOURCE
        )
        db.insert_quarantine(conn, result["quarantine"])
        db.upsert_manifest(
            conn, batch_id=batch_id, source=config.CATALOG_SOURCE,
            layer="cleaned", object_path=cleaned_key,
            row_count=len(result["valid"]), checksum=cleaned_checksum,
            status="complete",
        )
        db.upsert_manifest(
            conn, batch_id=batch_id, source=config.CATALOG_SOURCE,
            layer="quarantine", object_path=quarantine_key,
            row_count=len(result["quarantine"]), checksum=quarantine_checksum,
            status="complete",
        )
        db.upsert_manifest(
            conn, batch_id=batch_id, source=config.CATALOG_SOURCE,
            layer="duplicates", object_path=duplicates_key,
            row_count=len(result["duplicates"]), checksum=duplicates_checksum,
            status="complete",
        )
        _upsert_metrics_from_stats(
            conn, batch_id=batch_id, source=config.CATALOG_SOURCE,
            stats=result["stats"], layer="cleaned", status="cleaned",
        )
    logger.info(
        "Catalogo v%s limpio: %s validos, %s rechazados, %s duplicados",
        version, result["stats"]["rows_valid"],
        result["stats"]["rows_rejected"], result["stats"]["rows_duplicated"],
    )
    return {
        "batch_id": batch_id,
        "ingest_date": ingest_date,
        "version": version,
        "cleaned_key": cleaned_key,
        **result["stats"],
    }


def publish_catalog(ingest_date: str, version: int, **_: Any) -> dict[str, Any]:
    batch_id = config.catalog_batch_id(ingest_date, version)
    cleaned_key = storage.layer_key(
        "cleaned", config.CATALOG_SOURCE, ingest_date, batch_id,
        f"catalog_v{version}.parquet",
    )
    catalog_df = storage.get_parquet(cleaned_key)
    with db.connection() as conn:
        counts = _publish_catalog_scd2(
            conn, catalog_df, version=version, batch_id=batch_id
        )
        _publish_catalog_metrics(conn, batch_id=batch_id, counts=counts)
        curated_key = _write_curated_dim_product(
            conn, ingest_date=ingest_date, batch_id=batch_id
        )
    logger.info("Catalogo v%s publicado: %s", version, counts)
    return {
        "batch_id": batch_id,
        "ingest_date": ingest_date,
        "version": version,
        "curated_key": curated_key,
        **counts,
    }


def publish_historical_dimensions(ingest_date: str, **_: Any) -> dict[str, Any]:
    historical_batch = config.historical_batch_id(ingest_date)
    catalog_batch = config.catalog_batch_id(ingest_date, 1)
    hist_key = storage.layer_key(
        "cleaned", config.HISTORICAL_SOURCE, ingest_date, historical_batch,
        "order_lines.parquet",
    )
    catalog_key = storage.layer_key(
        "cleaned", config.CATALOG_SOURCE, ingest_date, catalog_batch,
        "catalog_v1.parquet",
    )
    hist_df = storage.get_parquet(hist_key)
    catalog_df = storage.get_parquet(catalog_key)

    with db.connection() as conn:
        counts = _publish_catalog_scd2(
            conn, catalog_df, version=1, batch_id=catalog_batch
        )
        _publish_catalog_metrics(conn, batch_id=catalog_batch, counts=counts)

        customers = hist_df[["customer_id", "country_code"]].dropna(
            subset=["customer_id"]
        ).copy()
        customers["customer_id"] = customers["customer_id"].astype(str)
        customers = customers[customers["customer_id"] != ""]

        def mode_or_unknown(series: pd.Series) -> str:
            mode = series.mode()
            return str(mode.iat[0]) if len(mode) else "ZZ"

        grouped = (
            customers.groupby("customer_id", as_index=False)["country_code"]
            .agg(mode_or_unknown)
        )
        grouped["batch_id"] = historical_batch
        db.truncate(conn, "staging.customers")
        db.copy_dataframe(
            conn, "staging.customers", grouped,
            ["customer_id", "country_code", "batch_id"],
        )
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO analytics.dim_customer
                    (source, source_customer_id, country_key, is_unknown,
                     created_at, updated_at)
                SELECT 'historical', cu.customer_id,
                       COALESCE(c.country_key, 0), false, NOW(), NOW()
                FROM staging.customers cu
                LEFT JOIN analytics.dim_country c ON c.country_code = cu.country_code
                ON CONFLICT (source, source_customer_id) DO UPDATE SET
                    country_key = EXCLUDED.country_key,
                    updated_at = NOW()
                """
            )
            customers_upserted = int(cursor.rowcount)

        product_curated = _write_curated_dim_product(
            conn, ingest_date=ingest_date, batch_id=catalog_batch
        )
        customer_curated = _write_curated_dim_customer(
            conn, ingest_date=ingest_date, batch_id=historical_batch
        )
    logger.info(
        "Dimensiones historicas: catalogo=%s, clientes=%s", counts, customers_upserted
    )
    return {
        "ingest_date": ingest_date,
        "catalog_batch_id": catalog_batch,
        "historical_batch_id": historical_batch,
        "catalog_counts": counts,
        "customers_upserted": customers_upserted,
        "dim_product_key": product_curated,
        "dim_customer_key": customer_curated,
    }

def publish_historical_facts(ingest_date: str, **_: Any) -> dict[str, Any]:
    batch_id = config.historical_batch_id(ingest_date)
    cleaned_key = storage.layer_key(
        "cleaned", config.HISTORICAL_SOURCE, ingest_date, batch_id,
        "order_lines.parquet",
    )
    dataframe = storage.get_parquet(cleaned_key)
    clean_metrics = db.get_metrics(batch_id, config.HISTORICAL_SOURCE, layer="cleaned")
    if clean_metrics is None:
        raise RuntimeError("Faltan metricas cleaned del historico.")

    with db.connection() as conn:
        db.truncate(conn, "staging.order_lines")
        db.copy_dataframe(
            conn, "staging.order_lines", dataframe,
            STAGING_ORDER_LINE_COLUMNS,
        )
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM analytics.fact_order_line WHERE batch_id = %s",
                (batch_id,),
            )
            cursor.execute(
                """
                INSERT INTO analytics.fact_order_line (
                    line_key, invoice_no, source, product_key, customer_key,
                    country_key, currency_key, date_key, invoice_date_utc,
                    quantity, unit_price, gross_amount, return_amount,
                    net_amount, gross_amount_usd, return_amount_usd,
                    net_amount_usd, line_type, is_cancellation, record_hash,
                    batch_id, created_at
                )
                SELECT
                    s.line_key, s.invoice_no, s.source,
                    COALESCE(p.product_key, 0),
                    COALESCE(cu.customer_key, -1),
                    c.country_key, cur.currency_key, s.date_key::integer,
                    s.invoice_date_utc::timestamptz, s.quantity::integer,
                    s.unit_price::numeric, s.gross_amount::numeric,
                    s.return_amount::numeric, s.net_amount::numeric,
                    ROUND(s.gross_amount::numeric * fx.rate, 2),
                    ROUND(s.return_amount::numeric * fx.rate, 2),
                    ROUND(s.net_amount::numeric * fx.rate, 2),
                    s.line_type, s.is_cancellation::boolean, s.record_hash,
                    s.batch_id, s.created_at::timestamptz
                FROM staging.order_lines s
                JOIN analytics.dim_country c
                  ON c.country_code = s.country_code
                JOIN analytics.dim_currency cur
                  ON cur.currency_code = s.currency_code
                LEFT JOIN analytics.dim_product p
                  ON p.sku = s.product_sku
                 AND s.invoice_date_utc::timestamptz
                     BETWEEN p.valid_from AND p.valid_to
                LEFT JOIN analytics.dim_customer cu
                  ON cu.source = 'historical'
                 AND cu.source_customer_id = s.customer_id
                LEFT JOIN LATERAL (
                    SELECT r.rate
                    FROM ops.exchange_rates r
                    WHERE r.from_currency = s.currency_code
                      AND r.to_currency = 'USD'
                      AND r.rate_date <= s.rate_date::date
                    ORDER BY r.rate_date DESC
                    LIMIT 1
                ) fx ON TRUE
                ON CONFLICT (line_key) DO NOTHING
                """
            )
            inserted = int(cursor.rowcount)

        existing_elsewhere = max(0, len(dataframe) - inserted)
        duplicates_total = _as_int(clean_metrics.get("rows_duplicated")) + existing_elsewhere
        rejected = _as_int(clean_metrics.get("rows_rejected"))
        rows_read = _as_int(clean_metrics.get("rows_read"))
        db.upsert_metrics(
            conn,
            batch_id=batch_id,
            source=config.HISTORICAL_SOURCE,
            layer="published",
            rows_read=rows_read,
            rows_valid=_as_int(clean_metrics.get("rows_valid")),
            rows_rejected=rejected,
            rows_duplicated=duplicates_total,
            rows_late=0,
            rows_published=inserted,
            rows_conflicting=0,
            finished_at=config.utc_now(),
            status="published",
        )
        fact_df = pd.read_sql_query(
            "SELECT * FROM analytics.fact_order_line WHERE batch_id = %s ORDER BY line_key",
            conn,
            params=(batch_id,),
        )
        curated_key = storage.curated_key(
            "fact_order_line", ingest_date, batch_id
        )
        checksum = storage.put_parquet(curated_key, fact_df)
        db.upsert_manifest(
            conn, batch_id=batch_id, source=config.HISTORICAL_SOURCE,
            layer="curated", object_path=curated_key,
            row_count=len(fact_df), checksum=checksum, status="complete",
        )
    logger.info(
        "Hechos historicos publicados: %s insertadas, %s duplicadas, %s rechazadas",
        inserted, duplicates_total, rejected,
    )
    return {
        "batch_id": batch_id,
        "ingest_date": ingest_date,
        "rows_read": rows_read,
        "rows_rejected": rejected,
        "rows_duplicated": duplicates_total,
        "rows_published": inserted,
        "rows_conflicting": 0,
        "curated_key": curated_key,
    }


# ---------------------------------------------------------------------------
# Eventos diarios
# ---------------------------------------------------------------------------
def _events_path(ingest_date: str) -> Path:
    return config.EVENTS_DIR / f"ingest_date={ingest_date}" / "events.jsonl"


def extract_events(
    ingest_date: str,
    fail_point: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    batch_id = config.events_batch_id(ingest_date)
    path = _events_path(ingest_date)
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo de eventos {path}")
    with path.open("r", encoding="utf-8") as handle:
        row_count = sum(1 for line in handle if line.strip())
    checksum = _sha256_file(path)
    key = storage.raw_events_key(ingest_date, batch_id)
    storage.upload_file(path, key, "application/x-ndjson")
    metadata_key = key.rsplit("/", 1)[0] + "/source_metadata.json"
    storage.write_json(
        metadata_key,
        {
            "batch_id": batch_id,
            "source": config.EVENTS_SOURCE,
            "ingest_date": ingest_date,
            "file": path.name,
            "row_count": row_count,
            "sha256": checksum,
            "late_rule_hours": 24,
        },
    )
    with db.connection() as conn:
        db.upsert_manifest(
            conn, batch_id=batch_id, source=config.EVENTS_SOURCE,
            layer="raw", object_path=key, row_count=row_count,
            checksum=checksum, status="complete",
        )
    logger.info("Eventos raw %s: %s filas", ingest_date, row_count)
    if fail_point == "after_raw":
        raise RuntimeError(
            "FALLA SIMULADA: el lote quedo en raw y no se proceso. "
            "Reejecuta la tarea para demostrar recuperacion."
        )
    return {
        "batch_id": batch_id,
        "ingest_date": ingest_date,
        "raw_key": key,
        "row_count": row_count,
        "checksum": checksum,
    }


def clean_events(
    ingest_date: str,
    fail_point: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    batch_id = config.events_batch_id(ingest_date)
    raw_key = storage.raw_events_key(ingest_date, batch_id)
    text = storage.read_text(raw_key)
    records: list[dict] = []
    parse_quarantine: list[dict] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            parse_quarantine.append(
                quality.quarantine_record(
                    source=config.EVENTS_SOURCE,
                    batch_id=batch_id,
                    source_record_id=f"line_{line_number}",
                    reason_code="invalid_json",
                    reason_detail=str(exc),
                    raw_payload=line,
                )
            )
            continue
        if not isinstance(item, dict):
            parse_quarantine.append(
                quality.quarantine_record(
                    source=config.EVENTS_SOURCE,
                    batch_id=batch_id,
                    source_record_id=f"line_{line_number}",
                    reason_code="invalid_payload",
                    reason_detail="La linea JSON no es un objeto.",
                    raw_payload=line,
                )
            )
            continue
        records.append(item)

    result = quality.validate_events_frame(
        records, batch_id=batch_id, ingest_date=ingest_date
    )
    quarantine = result["quarantine"]
    if parse_quarantine:
        extra = pd.DataFrame(parse_quarantine, columns=quality.QUARANTINE_COLUMNS)
        quarantine = pd.concat([quarantine, extra], ignore_index=True)
    valid = result["valid"]
    if valid.empty:
        valid = _empty_frame(EVENT_CLEAN_COLUMNS)
    duplicates = result["duplicates"]
    stats = result["stats"]
    stats["rows_read"] = _as_int(stats.get("rows_read")) + len(parse_quarantine)
    stats["rows_rejected"] = _as_int(stats.get("rows_rejected")) + len(parse_quarantine)

    cleaned_key = storage.layer_key(
        "cleaned", config.EVENTS_SOURCE, ingest_date, batch_id,
        "events.parquet",
    )
    quarantine_key = storage.layer_key(
        "cleaned", config.EVENTS_SOURCE, ingest_date, batch_id,
        "quarantine.parquet",
    )
    duplicates_key = storage.layer_key(
        "cleaned", config.EVENTS_SOURCE, ingest_date, batch_id,
        "duplicates.parquet",
    )
    cleaned_checksum = storage.put_parquet(cleaned_key, valid)
    quarantine_checksum = storage.put_parquet(quarantine_key, quarantine)
    duplicates_checksum = storage.put_parquet(duplicates_key, duplicates)
    with db.connection() as conn:
        db.clear_quarantine(
            conn, batch_id=batch_id, source=config.EVENTS_SOURCE
        )
        inserted_quarantine = db.insert_quarantine(conn, quarantine)
        db.upsert_manifest(
            conn, batch_id=batch_id, source=config.EVENTS_SOURCE,
            layer="cleaned", object_path=cleaned_key,
            row_count=len(valid), checksum=cleaned_checksum,
            status="complete",
        )
        db.upsert_manifest(
            conn, batch_id=batch_id, source=config.EVENTS_SOURCE,
            layer="quarantine", object_path=quarantine_key,
            row_count=len(quarantine), checksum=quarantine_checksum,
            status="complete",
        )
        db.upsert_manifest(
            conn, batch_id=batch_id, source=config.EVENTS_SOURCE,
            layer="duplicates", object_path=duplicates_key,
            row_count=len(duplicates), checksum=duplicates_checksum,
            status="complete",
        )
        _upsert_metrics_from_stats(
            conn, batch_id=batch_id, source=config.EVENTS_SOURCE,
            stats=stats, layer="cleaned", status="cleaned",
        )
    logger.info(
        "Eventos limpios %s: %s leidos, %s validos, %s rechazados, %s duplicados, %s tardios",
        ingest_date, stats["rows_read"], stats["rows_valid"],
        stats["rows_rejected"], stats["rows_duplicated"], stats["rows_late"],
    )
    if fail_point == "after_clean":
        raise RuntimeError(
            "FALLA SIMULADA: el lote quedo limpio en S3 y no se publico. "
            "Reejecuta publish_events; es idempotente."
        )
    return {
        "batch_id": batch_id,
        "ingest_date": ingest_date,
        "cleaned_key": cleaned_key,
        "quarantine_key": quarantine_key,
        "duplicates_key": duplicates_key,
        "quarantine_new": inserted_quarantine,
        **stats,
    }

def publish_events(ingest_date: str, **_: Any) -> dict[str, Any]:
    batch_id = config.events_batch_id(ingest_date)
    cleaned_key = storage.layer_key(
        "cleaned", config.EVENTS_SOURCE, ingest_date, batch_id,
        "events.parquet",
    )
    dataframe = storage.get_parquet(cleaned_key)
    clean_metrics = db.get_metrics(batch_id, config.EVENTS_SOURCE, layer="cleaned")
    if clean_metrics is None:
        raise RuntimeError("Faltan metricas cleaned de eventos.")

    batch_ts = config.ingest_timestamp(ingest_date)
    cross_batch_duplicates = 0
    same_batch_existing = 0
    conflicts_existing = 0
    inserted = 0
    order_ids: list[str] = []
    curated_event_key: str | None = None
    curated_lifecycle_key: str | None = None
    curated_event_checksum: str | None = None
    curated_lifecycle_checksum: str | None = None

    if not dataframe.empty:
        order_ids = sorted(
            str(value) for value in dataframe["order_id"].dropna().unique()
        )
        with db.connection() as conn:
            db.truncate(conn, "staging.events")
            db.copy_dataframe(
                conn, "staging.events", dataframe, STAGING_EVENT_COLUMNS
            )
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TEMP TABLE existing_events ON COMMIT DROP AS
                    SELECT DISTINCT f.event_id, f.record_hash, f.batch_id
                    FROM analytics.fact_order_event f
                    JOIN staging.events s
                      ON s.event_id = f.event_id AND s.source = f.source
                    WHERE f.source = 'events'
                    """
                )
                # Duplicado exacto de OTRO lote: se descarta y se cuenta.
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM staging.events s
                    JOIN existing_events e ON e.event_id = s.event_id
                    WHERE e.record_hash = s.record_hash
                      AND e.batch_id <> s.batch_id
                    """
                )
                cross_batch_duplicates = int(cursor.fetchone()[0])
                # Ya publicado por ESTE mismo lote: no-op idempotente.
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM staging.events s
                    JOIN existing_events e ON e.event_id = s.event_id
                    WHERE e.record_hash = s.record_hash
                      AND e.batch_id = s.batch_id
                    """
                )
                same_batch_existing = int(cursor.fetchone()[0])
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM staging.events s
                    JOIN existing_events e ON e.event_id = s.event_id
                    WHERE e.record_hash <> s.record_hash
                    """
                )
                conflicts_existing = int(cursor.fetchone()[0])

                cursor.execute(
                    """
                    INSERT INTO ops.quarantine_records (
                        quarantine_key, source, source_record_id,
                        reason_code, reason_detail, raw_payload,
                        batch_id, processed_at, record_hash
                    )
                    SELECT
                        md5(%s || '|' || s.batch_id || '|' ||
                            s.event_id || '|duplicate_id_conflict|' ||
                            s.record_hash),
                        %s, s.event_id, 'duplicate_id_conflict',
                        'Mismo event_id con contenido distinto; se conserva la version ya publicada.',
                        s.raw_payload::jsonb, s.batch_id, NOW(), s.record_hash
                    FROM staging.events s
                    JOIN existing_events e ON e.event_id = s.event_id
                    WHERE e.record_hash <> s.record_hash
                    ON CONFLICT (quarantine_key) DO NOTHING
                    """,
                    (config.EVENTS_SOURCE, config.EVENTS_SOURCE),
                )

                cursor.execute(
                    """
                    INSERT INTO analytics.fact_order_event (
                        event_id, order_id, source, event_type, event_time_utc,
                        event_time_original, ingested_at, is_late, late_hours,
                        country_key, currency_key, amount_original, amount_usd,
                        record_hash, batch_id, created_at
                    )
                    SELECT
                        s.event_id, s.order_id, s.source, s.event_type,
                        s.event_time_utc::timestamptz, s.event_time_original,
                        s.ingested_at::timestamptz, s.is_late::boolean,
                        s.late_hours::numeric, c.country_key, cur.currency_key,
                        s.amount_original::numeric,
                        ROUND(s.amount_original::numeric * fx.rate, 2),
                        s.record_hash, s.batch_id, s.created_at::timestamptz
                    FROM staging.events s
                    JOIN analytics.dim_country c
                      ON c.country_code = s.country_code
                    JOIN analytics.dim_currency cur
                      ON cur.currency_code = s.currency_code
                    LEFT JOIN LATERAL (
                        SELECT r.rate
                        FROM ops.exchange_rates r
                        WHERE r.from_currency = s.currency_code
                          AND r.to_currency = 'USD'
                          AND r.rate_date <= s.event_time_utc::timestamptz::date
                        ORDER BY r.rate_date DESC
                        LIMIT 1
                    ) fx ON TRUE
                    WHERE NOT EXISTS (
                        SELECT 1 FROM existing_events e
                        WHERE e.event_id = s.event_id
                    )
                    ON CONFLICT (source, event_id) DO NOTHING
                    """
                )
                inserted = int(cursor.rowcount)

                cursor.execute(
                    """
                    WITH affected AS (
                        SELECT DISTINCT order_id FROM staging.events
                    ),
                    agg AS (
                        SELECT
                            e.order_id,
                            MIN(e.event_time_utc) FILTER (WHERE e.event_type = 'created') AS created_at,
                            MIN(e.event_time_utc) FILTER (WHERE e.event_type = 'paid') AS paid_at,
                            MIN(e.event_time_utc) FILTER (WHERE e.event_type = 'cancelled') AS cancelled_at,
                            MIN(e.event_time_utc) FILTER (WHERE e.event_type = 'refunded') AS refunded_at,
                            MIN(e.event_time_utc) AS first_event_at,
                            MAX(e.event_time_utc) AS last_event_at,
                            COUNT(*) AS event_count,
                            COUNT(*) FILTER (WHERE e.is_late) AS late_event_count,
                            (ARRAY_AGG(
                                e.event_type
                                ORDER BY e.event_time_utc DESC,
                                         CASE e.event_type
                                             WHEN 'created' THEN 1
                                             WHEN 'paid' THEN 2
                                             WHEN 'cancelled' THEN 3
                                             WHEN 'refunded' THEN 4
                                             ELSE 5 END DESC,
                                         e.event_id DESC
                            ))[1] AS current_status,
                            (ARRAY_AGG(
                                e.country_key
                                ORDER BY e.event_time_utc DESC, e.event_id DESC
                            ))[1] AS country_key,
                            (ARRAY_AGG(
                                e.currency_key
                                ORDER BY e.event_time_utc DESC, e.event_id DESC
                            ))[1] AS currency_key
                        FROM analytics.fact_order_event e
                        JOIN affected a ON a.order_id = e.order_id
                        WHERE e.source = 'events'
                        GROUP BY e.order_id
                    )
                    INSERT INTO analytics.fact_order_lifecycle (
                        source, order_id, country_key, currency_key,
                        created_at, paid_at, cancelled_at, refunded_at,
                        first_event_at, last_event_at, current_status,
                        time_to_pay_hours, was_cancelled, was_refunded,
                        event_count, late_event_count, batch_id, updated_at
                    )
                    SELECT
                        'events', order_id, country_key, currency_key,
                        created_at, paid_at, cancelled_at, refunded_at,
                        first_event_at, last_event_at, current_status,
                        CASE
                            WHEN paid_at IS NOT NULL AND created_at IS NOT NULL
                             AND paid_at >= created_at
                            THEN ROUND(
                                EXTRACT(EPOCH FROM (paid_at - created_at))::numeric
                                / 3600, 2
                            )
                            ELSE NULL
                        END,
                        cancelled_at IS NOT NULL,
                        refunded_at IS NOT NULL,
                        event_count, late_event_count, %s, %s
                    FROM agg
                    ON CONFLICT (source, order_id) DO UPDATE SET
                        country_key = EXCLUDED.country_key,
                        currency_key = EXCLUDED.currency_key,
                        created_at = EXCLUDED.created_at,
                        paid_at = EXCLUDED.paid_at,
                        cancelled_at = EXCLUDED.cancelled_at,
                        refunded_at = EXCLUDED.refunded_at,
                        first_event_at = EXCLUDED.first_event_at,
                        last_event_at = EXCLUDED.last_event_at,
                        current_status = EXCLUDED.current_status,
                        time_to_pay_hours = EXCLUDED.time_to_pay_hours,
                        was_cancelled = EXCLUDED.was_cancelled,
                        was_refunded = EXCLUDED.was_refunded,
                        event_count = EXCLUDED.event_count,
                        late_event_count = EXCLUDED.late_event_count,
                        batch_id = EXCLUDED.batch_id,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (batch_id, batch_ts),
                )

            fact_event_df = pd.read_sql_query(
                """
                SELECT * FROM analytics.fact_order_event
                WHERE batch_id = %s ORDER BY event_id
                """,
                conn,
                params=(batch_id,),
            )
            lifecycle_df = pd.read_sql_query(
                """
                SELECT * FROM analytics.fact_order_lifecycle
                WHERE source = 'events' AND order_id = ANY(%s)
                ORDER BY order_id
                """,
                conn,
                params=(order_ids,),
            )
            curated_event_key = storage.curated_key(
                "fact_order_event", ingest_date, batch_id
            )
            curated_event_checksum = storage.put_parquet(
                curated_event_key, fact_event_df
            )
            db.upsert_manifest(
                conn, batch_id=batch_id, source=config.EVENTS_SOURCE,
                layer="curated", object_path=curated_event_key,
                row_count=len(fact_event_df), checksum=curated_event_checksum,
                status="complete",
            )
            curated_lifecycle_key = storage.curated_key(
                "fact_order_lifecycle", ingest_date, batch_id
            )
            curated_lifecycle_checksum = storage.put_parquet(
                curated_lifecycle_key, lifecycle_df
            )
            db.upsert_manifest(
                conn, batch_id=batch_id, source=config.EVENTS_SOURCE,
                layer="curated_lifecycle", object_path=curated_lifecycle_key,
                row_count=len(lifecycle_df), checksum=curated_lifecycle_checksum,
                status="complete",
            )

            rejected_total = _as_int(clean_metrics.get("rows_rejected")) + conflicts_existing
            duplicates_total = (
                _as_int(clean_metrics.get("rows_duplicated"))
                + cross_batch_duplicates
            )
            published_total = inserted + same_batch_existing
            conflicting_total = (
                _as_int(clean_metrics.get("rows_conflicting")) + conflicts_existing
            )
            db.upsert_metrics(
                conn,
                batch_id=batch_id,
                source=config.EVENTS_SOURCE,
                layer="published",
                rows_read=_as_int(clean_metrics.get("rows_read")),
                rows_valid=_as_int(clean_metrics.get("rows_valid")),
                rows_rejected=rejected_total,
                rows_duplicated=duplicates_total,
                rows_late=_as_int(clean_metrics.get("rows_late")),
                rows_published=published_total,
                rows_conflicting=conflicting_total,
                finished_at=config.utc_now(),
                status="published",
            )
    else:
        with db.connection() as conn:
            db.upsert_metrics(
                conn,
                batch_id=batch_id,
                source=config.EVENTS_SOURCE,
                layer="published",
                rows_read=_as_int(clean_metrics.get("rows_read")),
                rows_valid=_as_int(clean_metrics.get("rows_valid")),
                rows_rejected=_as_int(clean_metrics.get("rows_rejected")),
                rows_duplicated=_as_int(clean_metrics.get("rows_duplicated")),
                rows_late=_as_int(clean_metrics.get("rows_late")),
                rows_published=0,
                rows_conflicting=_as_int(clean_metrics.get("rows_conflicting")),
                finished_at=config.utc_now(),
                status="published",
            )


    logger.info(
        "Eventos publicados %s: %s nuevos, %s ya publicados del mismo lote, "
        "%s duplicados de otros lotes, %s conflictos",
        ingest_date, inserted, same_batch_existing,
        cross_batch_duplicates, conflicts_existing,
    )
    return {
        "batch_id": batch_id,
        "ingest_date": ingest_date,
        "rows_read": _as_int(clean_metrics.get("rows_read")),
        "rows_rejected": _as_int(clean_metrics.get("rows_rejected")) + conflicts_existing,
        "rows_duplicated": (
            _as_int(clean_metrics.get("rows_duplicated"))
            + cross_batch_duplicates
        ),
        "rows_late": _as_int(clean_metrics.get("rows_late")),
        "rows_published": inserted + same_batch_existing,
        "rows_inserted_this_run": inserted,
        "rows_conflicting": _as_int(clean_metrics.get("rows_conflicting")) + conflicts_existing,
        "curated_event_key": curated_event_key,
        "curated_lifecycle_key": curated_lifecycle_key,
        "affected_orders": len(order_ids),
    }

