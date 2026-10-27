"""Configuracion central del proyecto.

Las credenciales SIEMPRE vienen de variables de entorno. Los valores por
defecto son solo para el modo local con Docker Compose.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("DATA_DIR", str(PROJECT_ROOT / "data")))
SQL_DIR = PROJECT_ROOT / "sql"

HISTORICAL_CSV = DATA_DIR / "incoming" / "historical" / "online_retail_2010_2011.csv"
EVENTS_DIR = DATA_DIR / "incoming" / "events"
CATALOG_DIR = DATA_DIR / "catalog"

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://moto:5000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "test")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "test")
S3_BUCKET = os.getenv("S3_BUCKET", "mercado-nube")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "postgres")
WAREHOUSE_PORT = int(os.getenv("WAREHOUSE_PORT", "5432"))
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "mercado_nube")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "mn_app")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "")

CATALOG_BASE_URL = os.getenv("CATALOG_BASE_URL", "http://api:8000").rstrip("/")
REPORTING_CURRENCY = os.getenv("REPORTING_CURRENCY", "USD")
LOCAL_TIMEZONE = "Europe/London"

QUALITY_MAX_REJECTION_RATE = float(os.getenv("QUALITY_MAX_REJECTION_RATE", "0.10"))
QUALITY_MAX_DUPLICATE_RATE = float(os.getenv("QUALITY_MAX_DUPLICATE_RATE", "0.10"))
QUALITY_MAX_UNKNOWN_RATE = float(os.getenv("QUALITY_MAX_UNKNOWN_RATE", "0.05"))
QUALITY_FRESHNESS_HOURS = int(os.getenv("QUALITY_FRESHNESS_HOURS", "24"))

# Politica de reintentos de Airflow (configurable sin editar DAGs).
PIPELINE_RETRIES = int(os.getenv("PIPELINE_RETRIES", "2"))
PIPELINE_RETRY_DELAY_SECONDS = int(os.getenv("PIPELINE_RETRY_DELAY_SECONDS", "30"))
PIPELINE_MAX_RETRY_DELAY_MINUTES = int(os.getenv("PIPELINE_MAX_RETRY_DELAY_MINUTES", "10"))

HISTORICAL_SOURCE = "orders_historical"
EVENTS_SOURCE = "order_events"
CATALOG_SOURCE = "catalog"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def historical_batch_id(ingest_date: str) -> str:
    return f"historical_{ingest_date}"


def events_batch_id(ingest_date: str) -> str:
    return f"events_{ingest_date}"


def catalog_batch_id(ingest_date: str, version: int) -> str:
    return f"catalog_{ingest_date}_v{version}"


def ingest_timestamp(ingest_date: str) -> datetime:
    """Momento de ingesta determinista para detectar eventos tardios."""
    return datetime.fromisoformat(f"{ingest_date}T23:59:59+00:00")
