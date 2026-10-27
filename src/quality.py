"""Reglas de calidad, normalizacion y cuarentena.

Principio: no se corrige un dato invalido en silencio. Se normaliza lo que
tiene un mapeo explicito y lo demas se registra en cuarentena con un motivo.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src import config

CONFIG_DIR = config.PROJECT_ROOT / "config"
EVENT_TYPES = {"created", "paid", "cancelled", "refunded"}

QUARANTINE_COLUMNS = [
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


def _load_country_aliases() -> dict[str, str]:
    path = CONFIG_DIR / "country_aliases.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        return {
            row["source_name"].strip().lower(): row["country_code"].strip().upper()
            for row in csv.DictReader(handle)
        }


def _load_countries() -> tuple[dict[str, str], dict[str, str]]:
    path = CONFIG_DIR / "countries.csv"
    names: dict[str, str] = {}
    regions: dict[str, str] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            code = row["country_code"].strip().upper()
            names[code] = row["country_name"].strip()
            regions[code] = row["region"].strip()
    return names, regions


def _load_currencies() -> set[str]:
    path = CONFIG_DIR / "currencies.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        return {row["currency_code"].strip().upper() for row in csv.DictReader(handle)}


COUNTRY_ALIASES = _load_country_aliases()
COUNTRY_NAMES, COUNTRY_REGIONS = _load_countries()
VALID_COUNTRY_CODES = set(COUNTRY_NAMES) | set(COUNTRY_ALIASES.values())
VALID_CURRENCIES = _load_currencies()


def normalize_sku(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def normalize_country(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text:
        return None
    upper = text.upper()
    if len(upper) == 2 and upper in VALID_COUNTRY_CODES:
        return upper
    return COUNTRY_ALIASES.get(text.lower())


def normalize_currency(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip().upper()
    return text if text in VALID_CURRENCIES else None


def parse_datetime_utc(value: Any, assume_tz: str = config.LOCAL_TIMEZONE) -> datetime | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    else:
        text = str(value).strip().replace("Z", "+00:00")
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            try:
                parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(assume_tz))
    return parsed.astimezone(timezone.utc)


def _clean_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return None if math.isnan(value) else value
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, dict):
        return {str(key): _clean_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean_json_value(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _clean_json_value(value.item())
        except Exception:
            pass
    return str(value)


def safe_json(value: Any) -> str:
    return json.dumps(
        _clean_json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def quarantine_record(
    *,
    source: str,
    batch_id: str,
    source_record_id: str | None,
    reason_code: str,
    reason_detail: str,
    raw_payload: str,
    record_hash: str | None = None,
    row_index: int | str | None = None,
) -> dict:
    stable_id = source_record_id or "unknown"
    content_hash = record_hash or sha256_text(raw_payload)
    # row_index hace unica la clave incluso si dos filas invalidas son
    # identicas; asi cuarentena conserva una fila por rechazo y sigue siendo
    # idempotente porque el indice del archivo fuente es estable.
    row_suffix = "" if row_index is None else f"|row={row_index}"
    quarantine_key = sha256_text(
        f"{source}|{batch_id}|{stable_id}{row_suffix}|{reason_code}|"
        f"{content_hash}|{raw_payload}"
    )
    return {
        "quarantine_key": quarantine_key,
        "source": source,
        "source_record_id": stable_id,
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "raw_payload": raw_payload,
        "batch_id": batch_id,
        "processed_at": config.utc_now().isoformat(),
        "record_hash": content_hash,
    }


def _mark(
    reason: pd.Series,
    detail: pd.Series,
    mask: pd.Series,
    code: str,
    message: str,
) -> None:
    new = mask.fillna(False) & (reason == "")
    reason.loc[new] = code
    detail.loc[new] = message


def validate_historical_frame(
    dataframe: pd.DataFrame,
    *,
    batch_id: str,
    ingest_date: str,
) -> dict[str, Any]:
    """Valida una copia del historico y devuelve datos limpios + cuarentena."""
    df = dataframe.copy()
    for column in [
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
    ]:
        if column not in df:
            df[column] = ""

    invoice = df["invoice_no"].fillna("").astype(str).str.strip()
    stock = df["stock_code"].fillna("").astype(str).str.strip().str.upper()
    description = df["description"].fillna("").astype(str).str.strip()
    quantity = pd.to_numeric(
        df["quantity"].replace("", None), errors="coerce"
    )
    invoice_date = pd.to_datetime(
        df["invoice_date"].replace("", None), errors="coerce"
    )
    unit_price = pd.to_numeric(
        df["unit_price"].replace("", None), errors="coerce"
    )
    customer = (
        df["customer_id"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    country_raw = df["country"].fillna("").astype(str).str.strip()
    country_code = country_raw.str.lower().map(COUNTRY_ALIASES)
    two_letter = country_raw.str.upper()
    country_code = country_code.fillna(
        two_letter.where(
            (two_letter.str.len() == 2) & two_letter.isin(VALID_COUNTRY_CODES)
        )
    ).infer_objects(copy=False)

    localized = invoice_date.dt.tz_localize(
        config.LOCAL_TIMEZONE,
        ambiguous="NaT",
        nonexistent="shift_forward",
    )
    invoice_utc = localized.dt.tz_convert("UTC")

    reason = pd.Series("", index=df.index, dtype="object")
    detail = pd.Series("", index=df.index, dtype="object")
    _mark(reason, detail, invoice == "", "missing_invoice", "invoice_no vacio")
    _mark(reason, detail, stock == "", "missing_sku", "stock_code vacio")
    _mark(
        reason,
        detail,
        invoice_date.isna() | invoice_utc.isna(),
        "invalid_date",
        "invoice_date vacia o no parseable",
    )
    _mark(
        reason,
        detail,
        quantity.isna() | (quantity == 0),
        "invalid_quantity",
        "quantity vacia o igual a cero",
    )
    _mark(
        reason,
        detail,
        unit_price.isna() | (unit_price <= 0),
        "invalid_price",
        "unit_price vacio o <= 0",
    )
    _mark(
        reason,
        detail,
        country_code.isna(),
        "invalid_country",
        "country sin mapeo ISO-2 explicito",
    )

    invalid_mask = reason != ""
    quarantine_records = []
    for index, row in df.loc[invalid_mask].iterrows():
        raw_payload = safe_json(row.to_dict())
        source_id = str(row.get("invoice_no", "")).strip() or f"row_{index}"
        quarantine_records.append(
            quarantine_record(
                source=config.HISTORICAL_SOURCE,
                batch_id=batch_id,
                source_record_id=source_id,
                reason_code=str(reason.loc[index]),
                reason_detail=str(detail.loc[index]),
                raw_payload=raw_payload,
                row_index=index,
            )
        )
    quarantine = pd.DataFrame(quarantine_records, columns=QUARANTINE_COLUMNS)

    valid = df.loc[~invalid_mask].copy()
    valid["invoice_no"] = invoice.loc[valid.index]
    valid["product_sku"] = stock.loc[valid.index]
    valid["description"] = description.loc[valid.index]
    valid["quantity"] = quantity.loc[valid.index].astype(int)
    valid["unit_price"] = unit_price.loc[valid.index].astype(float)
    valid["invoice_date"] = invoice_date.loc[valid.index]
    valid["invoice_date_utc"] = invoice_utc.loc[valid.index]
    valid["date_key"] = valid["invoice_date"].dt.strftime("%Y%m%d").astype(int)
    valid["rate_date"] = valid["invoice_date"].dt.date.astype(str)
    valid["customer_id"] = (
        customer.loc[valid.index].where(customer.loc[valid.index] != "", None)
    )
    valid["country_code"] = country_code.loc[valid.index]
    valid["currency_code"] = "GBP"
    valid["source"] = "historical"
    valid["batch_id"] = batch_id
    valid["created_at"] = config.ingest_timestamp(ingest_date).isoformat()

    invoice_upper = valid["invoice_no"].str.upper()
    is_return = (valid["quantity"] < 0) | invoice_upper.str.startswith("C")
    valid["line_type"] = np.where(is_return, "return", "sale")
    valid["is_cancellation"] = invoice_upper.str.startswith("C")
    valid["gross_amount"] = np.where(
        is_return, 0.0, valid["quantity"] * valid["unit_price"]
    )
    valid["return_amount"] = np.where(
        is_return, (valid["quantity"] * valid["unit_price"]).abs(), 0.0
    )
    valid["net_amount"] = valid["gross_amount"] - valid["return_amount"]

    hash_frame = pd.DataFrame(
        {
            "invoice_no": valid["invoice_no"],
            "stock_code": valid["product_sku"],
            "description": valid["description"],
            "quantity": valid["quantity"],
            "invoice_date": valid["invoice_date"],
            "unit_price": valid["unit_price"],
            "customer_id": valid["customer_id"],
            "country_code": valid["country_code"],
            "hash_version": "mn-historical-01",
        }
    )
    hashes = pd.util.hash_pandas_object(hash_frame, index=False).astype("uint64")
    valid["record_hash"] = hashes.map(lambda value: f"{value:016x}")
    valid["line_key"] = valid["record_hash"]

    duplicate_mask = valid.duplicated("line_key", keep="first")
    duplicates = valid.loc[
        duplicate_mask,
        ["line_key", "invoice_no", "product_sku", "quantity", "unit_price"],
    ].copy()
    valid_unique = valid.loc[~duplicate_mask].copy()

    columns = [
        "line_key",
        "invoice_no",
        "source",
        "product_sku",
        "description",
        "customer_id",
        "country_code",
        "currency_code",
        "date_key",
        "rate_date",
        "invoice_date_utc",
        "quantity",
        "unit_price",
        "gross_amount",
        "return_amount",
        "net_amount",
        "line_type",
        "is_cancellation",
        "record_hash",
        "batch_id",
        "created_at",
    ]
    valid_unique = valid_unique[columns]

    stats = {
        "rows_read": int(len(df)),
        "rows_valid": int(len(valid)),
        "rows_rejected": int(len(quarantine)),
        "rows_duplicated": int(duplicate_mask.sum()),
        "rows_late": 0,
        "rows_published": 0,
        "rows_conflicting": 0,
        "ingest_date": ingest_date,
    }
    return {
        "valid": valid_unique,
        "quarantine": quarantine,
        "duplicates": duplicates,
        "stats": stats,
    }


def _canonical_event(row: pd.Series) -> dict[str, Any]:
    payload = row["payload"] if isinstance(row["payload"], dict) else {}
    lines = payload.get("lines") if isinstance(payload.get("lines"), list) else []
    canonical_lines = []
    for line in lines:
        if not isinstance(line, dict):
            continue
        canonical_lines.append(
            {
                "sku": normalize_sku(line.get("sku")),
                "quantity": int(float(line.get("quantity", 0))),
                "unit_price": round(float(line.get("unit_price", 0)), 2),
            }
        )
    canonical_lines.sort(key=lambda item: (item["sku"] or "", item["quantity"], item["unit_price"]))
    event_time = row["event_time_utc"]
    return {
        "event_id": row["event_id"],
        "order_id": row["order_id"],
        "event_type": row["event_type"],
        "event_time": event_time.isoformat() if pd.notna(event_time) else None,
        "country": row["country_code"],
        "currency": row["currency_code"],
        "amount": round(float(row["amount_original"]), 2),
        "lines": canonical_lines,
    }


def _validate_lines(payload: Any) -> tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "payload no es un objeto"
    lines = payload.get("lines")
    if not isinstance(lines, list) or not lines:
        return False, "payload.lines vacio"
    for line in lines:
        if not isinstance(line, dict):
            return False, "linea no es un objeto"
        if not normalize_sku(line.get("sku")):
            return False, "linea sin sku"
        quantity = pd.to_numeric(line.get("quantity"), errors="coerce")
        if pd.isna(quantity) or quantity <= 0:
            return False, "linea con quantity <= 0"
        unit_price = pd.to_numeric(line.get("unit_price"), errors="coerce")
        if pd.isna(unit_price) or unit_price <= 0:
            return False, "linea con unit_price <= 0"
    return True, ""


def validate_events_frame(
    records: list[dict],
    *,
    batch_id: str,
    ingest_date: str,
) -> dict[str, Any]:
    """Valida eventos, deduplica y separa conflictos de event_id."""
    df = pd.DataFrame(records)
    if df.empty:
        empty = pd.DataFrame()
        return {
            "valid": empty,
            "quarantine": pd.DataFrame(columns=QUARANTINE_COLUMNS),
            "duplicates": empty,
            "stats": {
                "rows_read": 0,
                "rows_valid": 0,
                "rows_rejected": 0,
                "rows_duplicated": 0,
                "rows_late": 0,
                "rows_published": 0,
                "rows_conflicting": 0,
                "ingest_date": ingest_date,
            },
        }

    for column in ["event_id", "order_id", "event_type", "event_time", "country", "currency"]:
        if column not in df:
            df[column] = ""
        df[column] = df[column].fillna("").astype(str).str.strip()

    if "payload" not in df:
        df["payload"] = None
    df["payload"] = df["payload"].apply(lambda value: value if isinstance(value, dict) else None)
    df["raw_payload"] = [
        json.dumps(record, ensure_ascii=False, sort_keys=True, default=str)
        for record in records
    ]
    df["amount_original"] = df["payload"].apply(
        lambda payload: pd.to_numeric(payload.get("amount"), errors="coerce")
        if isinstance(payload, dict)
        else np.nan
    )
    df["event_time_utc"] = pd.to_datetime(
        df["event_time"].replace("", None), errors="coerce", utc=True
    )
    df["country_code"] = df["country"].apply(normalize_country)
    df["currency_code"] = df["currency"].apply(normalize_currency)

    ingested_at = config.ingest_timestamp(ingest_date)
    df["ingested_at"] = ingested_at
    df["late_hours"] = (
        ingested_at - df["event_time_utc"]
    ).dt.total_seconds() / 3600
    df["is_late"] = df["late_hours"] > 24

    line_checks = df["payload"].apply(_validate_lines)
    df["lines_ok"] = [item[0] for item in line_checks]
    df["lines_detail"] = [item[1] for item in line_checks]

    reason = pd.Series("", index=df.index, dtype="object")
    detail = pd.Series("", index=df.index, dtype="object")
    _mark(reason, detail, df["event_id"] == "", "missing_event_id", "event_id vacio")
    _mark(reason, detail, df["order_id"] == "", "missing_order_id", "order_id vacio")
    _mark(
        reason,
        detail,
        ~df["event_type"].isin(EVENT_TYPES),
        "invalid_event_type",
        "event_type no permitido",
    )
    _mark(
        reason,
        detail,
        df["event_time_utc"].isna()
        | ~df["event_time_utc"].dt.year.between(2000, 2100),
        "invalid_event_time",
        "event_time vacio, no parseable o fuera de rango",
    )
    _mark(
        reason,
        detail,
        df["country_code"].isna(),
        "invalid_country",
        "country sin mapeo ISO-2 explicito",
    )
    _mark(
        reason,
        detail,
        df["currency_code"].isna(),
        "invalid_currency",
        "currency no permitida o vacia",
    )
    _mark(
        reason,
        detail,
        df["payload"].isna(),
        "invalid_payload",
        "payload ausente o no es objeto",
    )
    _mark(
        reason,
        detail,
        df["amount_original"].isna() | (df["amount_original"] <= 0),
        "non_positive_amount",
        "payload.amount vacio o <= 0",
    )
    _mark(
        reason,
        detail,
        ~df["lines_ok"],
        "invalid_lines",
        "payload.lines invalido",
    )

    invalid_mask = reason != ""
    valid = df.loc[~invalid_mask].copy()
    valid["record_hash"] = valid.apply(
        lambda row: sha256_text(
            json.dumps(_canonical_event(row), sort_keys=True, ensure_ascii=False)
        ),
        axis=1,
    )
    valid["source"] = "events"
    valid["batch_id"] = batch_id
    valid["created_at"] = config.ingest_timestamp(ingest_date).isoformat()
    valid["event_time_original"] = valid["event_time"]
    valid["amount_original"] = valid["amount_original"].astype(float).round(2)

    winner = valid.sort_values(
        ["event_time_utc", "record_hash"], na_position="last"
    ).drop_duplicates("event_id", keep="first")
    losers = valid.loc[~valid.index.isin(winner.index)].copy()
    winner_hash = winner.set_index("event_id")["record_hash"]
    losers["winner_hash"] = losers["event_id"].map(winner_hash)
    duplicates = losers.loc[losers["record_hash"] == losers["winner_hash"]].copy()
    conflicts = losers.loc[losers["record_hash"] != losers["winner_hash"]].copy()

    quarantine_records = []
    for index, row in df.loc[invalid_mask].iterrows():
        source_id = str(row.get("event_id", "")).strip() or str(
            row.get("order_id", "")
        ).strip() or f"row_{index}"
        quarantine_records.append(
            quarantine_record(
                source=config.EVENTS_SOURCE,
                batch_id=batch_id,
                source_record_id=source_id,
                reason_code=str(reason.loc[index]),
                reason_detail=str(detail.loc[index]),
                raw_payload=str(row["raw_payload"]),
                row_index=index,
            )
        )
    for index, row in conflicts.iterrows():
        quarantine_records.append(
            quarantine_record(
                source=config.EVENTS_SOURCE,
                batch_id=batch_id,
                source_record_id=str(row["event_id"]),
                reason_code="duplicate_id_conflict",
                reason_detail=(
                    "Mismo event_id con contenido distinto; se conserva la "
                    "version mas antigua y, ante empate, el menor hash."
                ),
                raw_payload=str(row["raw_payload"]),
                record_hash=str(row["record_hash"]),
                row_index=index,
            )
        )
    quarantine = pd.DataFrame(quarantine_records, columns=QUARANTINE_COLUMNS)

    columns = [
        "event_id",
        "order_id",
        "source",
        "event_type",
        "event_time_utc",
        "event_time_original",
        "ingested_at",
        "is_late",
        "late_hours",
        "country_code",
        "currency_code",
        "amount_original",
        "record_hash",
        "batch_id",
        "created_at",
        "raw_payload",
    ]
    winner = winner[columns]

    stats = {
        "rows_read": int(len(df)),
        "rows_valid": int(len(valid)),
        "rows_rejected": int(len(quarantine)),
        "rows_duplicated": int(len(duplicates)),
        "rows_late": int(winner["is_late"].sum()),
        "rows_published": 0,
        "rows_conflicting": int(len(conflicts)),
        "ingest_date": ingest_date,
    }
    return {
        "valid": winner,
        "quarantine": quarantine,
        "duplicates": duplicates,
        "stats": stats,
    }


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes"}:
            return True
        if text in {"false", "0", "no"}:
            return False
    if isinstance(value, (int, np.integer)) and value in (0, 1):
        return bool(value)
    return None


def validate_catalog_frame(
    products: list[dict],
    *,
    batch_id: str,
    version: int,
    ingest_date: str,
) -> dict[str, Any]:
    """Valida productos del API y calcula el hash de atributos para SCD2."""
    df = pd.DataFrame(products)
    if df.empty:
        empty = pd.DataFrame()
        return {
            "valid": empty,
            "quarantine": pd.DataFrame(columns=QUARANTINE_COLUMNS),
            "duplicates": empty,
            "stats": {
                "rows_read": 0,
                "rows_valid": 0,
                "rows_rejected": 0,
                "rows_duplicated": 0,
                "rows_late": 0,
                "rows_published": 0,
                "rows_conflicting": 0,
                "ingest_date": ingest_date,
            },
        }

    for column in ["sku", "name", "category", "valid_from", "valid_to", "updated_at"]:
        if column not in df:
            df[column] = ""
        df[column] = df[column].fillna("").astype(str).str.strip()
    df["sku"] = df["sku"].str.upper()
    df["is_service_bool"] = df["is_service"].apply(_as_bool) if "is_service" in df else None
    df["is_active_bool"] = df["is_active"].apply(_as_bool) if "is_active" in df else None
    df["valid_from_ts"] = pd.to_datetime(df["valid_from"], errors="coerce", utc=True)
    df["valid_to_ts"] = pd.to_datetime(df["valid_to"], errors="coerce", utc=True)
    df["updated_at_ts"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)

    reason = pd.Series("", index=df.index, dtype="object")
    detail = pd.Series("", index=df.index, dtype="object")
    _mark(reason, detail, df["sku"] == "", "missing_sku", "sku vacio")
    _mark(reason, detail, df["name"] == "", "missing_name", "name vacio")
    _mark(reason, detail, df["category"] == "", "missing_category", "category vacia")
    _mark(
        reason,
        detail,
        df["valid_from_ts"].isna() | df["valid_to_ts"].isna() | df["updated_at_ts"].isna(),
        "invalid_date",
        "fechas de vigencia invalidas",
    )
    _mark(
        reason,
        detail,
        df["is_service_bool"].isna() | df["is_active_bool"].isna(),
        "invalid_boolean",
        "is_service o is_active invalido",
    )

    invalid_mask = reason != ""
    quarantine_records = []
    for index, row in df.loc[invalid_mask].iterrows():
        raw_payload = safe_json(row.to_dict())
        source_id = str(row.get("sku", "")).strip() or f"row_{index}"
        quarantine_records.append(
            quarantine_record(
                source=config.CATALOG_SOURCE,
                batch_id=batch_id,
                source_record_id=source_id,
                reason_code=str(reason.loc[index]),
                reason_detail=str(detail.loc[index]),
                raw_payload=raw_payload,
                row_index=index,
            )
        )
    quarantine = pd.DataFrame(quarantine_records, columns=QUARANTINE_COLUMNS)
    invalid_count = len(quarantine)

    valid = df.loc[~invalid_mask].copy()
    valid["is_service"] = valid["is_service_bool"].astype(bool)
    valid["is_active"] = valid["is_active_bool"].astype(bool)
    valid["record_hash"] = valid.apply(
        lambda row: sha256_text(
            "|".join(
                [
                    str(row["sku"]),
                    str(row["name"]),
                    str(row["category"]),
                    str(bool(row["is_service"])).lower(),
                    str(bool(row["is_active"])).lower(),
                ]
            )
        ),
        axis=1,
    )
    valid["catalog_version"] = version
    valid["batch_id"] = batch_id
    valid["created_at"] = config.ingest_timestamp(ingest_date).isoformat()
    valid["valid_from"] = valid["valid_from_ts"]
    valid["valid_to"] = valid["valid_to_ts"]
    valid["updated_at"] = valid["updated_at_ts"]

    duplicate_mask = valid.duplicated("sku", keep="first")
    duplicates = valid.loc[duplicate_mask, ["sku", "name", "category"]].copy()
    valid_unique = valid.loc[~duplicate_mask].copy()
    for index, row in duplicates.iterrows():
        quarantine_records.append(
            quarantine_record(
                source=config.CATALOG_SOURCE,
                batch_id=batch_id,
                source_record_id=str(row["sku"]),
                reason_code="duplicate_sku",
                reason_detail="SKU repetido en la misma version del catalogo",
                raw_payload=safe_json(row.to_dict()),
                row_index=index,
            )
        )
    quarantine = pd.DataFrame(quarantine_records, columns=QUARANTINE_COLUMNS)

    columns = [
        "sku",
        "name",
        "category",
        "is_service",
        "is_active",
        "valid_from",
        "valid_to",
        "updated_at",
        "catalog_version",
        "record_hash",
        "batch_id",
        "created_at",
    ]
    valid_unique = valid_unique[columns]

    stats = {
        "rows_read": int(len(df)),
        "rows_valid": int(len(valid)),
        "rows_rejected": int(invalid_count),
        "rows_duplicated": int(len(duplicates)),
        "rows_late": 0,
        "rows_published": 0,
        "rows_conflicting": 0,
        "ingest_date": ingest_date,
    }
    return {
        "valid": valid_unique,
        "quarantine": quarantine,
        "duplicates": duplicates,
        "stats": stats,
    }



