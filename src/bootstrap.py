"""Inicializa el modo local: bucket S3, DDL y dimensiones semilla."""

from __future__ import annotations

import csv
import time
from pathlib import Path

from src import config, db, storage


def load_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def ensure_bucket_with_retry() -> None:
    last_error: Exception | None = None
    for attempt in range(15):
        try:
            storage.ensure_bucket()
            print(f"Bucket S3 listo: {config.S3_BUCKET}")
            return
        except Exception as exc:  # pragma: no cover - depende de Docker
            last_error = exc
            print(f"Esperando S3 ({attempt + 1}/15): {exc}")
            time.sleep(2)
    raise RuntimeError(f"No se pudo crear el bucket S3: {last_error}")


def seed_countries(conn) -> None:
    rows = load_csv(config.PROJECT_ROOT / "config" / "countries.csv")
    values = [
        (
            row["country_code"].strip().upper(),
            row["country_name"].strip(),
            row["region"].strip(),
            "seed",
        )
        for row in rows
    ]
    db.execute_values(
        conn,
        """
        INSERT INTO analytics.dim_country (country_code, country_name, region, source_name)
        VALUES %s
        ON CONFLICT (country_code) DO UPDATE SET
            country_name = EXCLUDED.country_name,
            region = EXCLUDED.region,
            source_name = EXCLUDED.source_name
        """,
        values,
    )


def seed_currencies(conn) -> None:
    rows = load_csv(config.PROJECT_ROOT / "config" / "currencies.csv")
    values = [
        (
            row["currency_code"].strip().upper(),
            row["currency_name"].strip(),
            row["is_reporting"].strip().lower() == "true",
        )
        for row in rows
    ]
    db.execute_values(
        conn,
        """
        INSERT INTO analytics.dim_currency (currency_code, currency_name, is_reporting)
        VALUES %s
        ON CONFLICT (currency_code) DO UPDATE SET
            currency_name = EXCLUDED.currency_name,
            is_reporting = EXCLUDED.is_reporting
        """,
        values,
    )


def seed_exchange_rates(conn) -> None:
    rows = load_csv(config.PROJECT_ROOT / "config" / "exchange_rates.csv")
    values = [
        (
            row["rate_date"].strip(),
            row["from_currency"].strip().upper(),
            row["to_currency"].strip().upper(),
            float(row["rate"]),
            row["source_note"].strip(),
        )
        for row in rows
    ]
    db.execute_values(
        conn,
        """
        INSERT INTO ops.exchange_rates
            (rate_date, from_currency, to_currency, rate, source_note)
        VALUES %s
        ON CONFLICT (rate_date, from_currency, to_currency) DO UPDATE SET
            rate = EXCLUDED.rate,
            source_note = EXCLUDED.source_note
        """,
        values,
    )


def main() -> None:
    ensure_bucket_with_retry()
    db.execute_file(config.SQL_DIR / "ddl.sql")
    print("DDL aplicado.")
    with db.connection() as conn:
        seed_countries(conn)
        seed_currencies(conn)
        seed_exchange_rates(conn)
    counts = {
        "dim_date": db.table_count("analytics.dim_date"),
        "dim_country": db.table_count("analytics.dim_country"),
        "dim_currency": db.table_count("analytics.dim_currency"),
        "exchange_rates": db.table_count("ops.exchange_rates"),
    }
    print("Semillas:", counts)
    print("Bootstrap completado.")


if __name__ == "__main__":
    main()
