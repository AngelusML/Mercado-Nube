from __future__ import annotations

import pandas as pd

from src import config, quality


def _historical_row(**overrides):
    row = {
        "invoice_no": "1001",
        "stock_code": "SKU-1",
        "description": "Producto de prueba",
        "quantity": "2",
        "invoice_date": "2011-01-01 10:00:00",
        "unit_price": "3.50",
        "customer_id": "12345",
        "country": "United Kingdom",
    }
    row.update(overrides)
    return row


def test_historical_validation_reconciles_and_records_unique_quarantine() -> None:
    base = _historical_row()
    invalid_price = _historical_row(invoice_no="1002", unit_price="0")
    invalid_country = _historical_row(invoice_no="1003", country="Atlantis")
    rows = [base, invalid_price, invalid_price, invalid_country, base]

    result = quality.validate_historical_frame(
        pd.DataFrame(rows), batch_id="historical_test", ingest_date="2026-10-21"
    )
    stats = result["stats"]

    assert stats["rows_read"] == 5
    assert stats["rows_valid"] == 2  # base + su copia exacta, antes de deduplicar
    assert stats["rows_rejected"] == 3  # dos precios invalidos + pais invalido
    assert stats["rows_duplicated"] == 1
    assert len(result["valid"]) == 1
    assert len(result["quarantine"]) == 3
    assert set(result["quarantine"]["source"]) == {config.HISTORICAL_SOURCE}
    assert {
        "invalid_price",
        "invalid_country",
    }.issubset(set(result["quarantine"]["reason_code"]))
    # La cuarentena conserva una fila por rechazo, aunque dos filas sean identicas.
    assert result["quarantine"]["quarantine_key"].is_unique


def test_historical_validation_deduplicates_only_after_validation() -> None:
    valid = _historical_row()
    invalid = _historical_row(invoice_no="2000", quantity="0")
    result = quality.validate_historical_frame(
        pd.DataFrame([valid, valid, invalid]),
        batch_id="historical_test",
        ingest_date="2026-10-21",
    )
    assert result["stats"]["rows_read"] == 3
    assert result["stats"]["rows_rejected"] == 1
    assert result["stats"]["rows_duplicated"] == 1
    assert len(result["valid"]) == 1


def _event_row(**overrides):
    row = {
        "event_id": "evt-1",
        "order_id": "order-1",
        "event_type": "created",
        "event_time": "2026-10-22T01:00:00Z",
        "country": "CO",
        "currency": "COP",
        "payload": {
            "amount": 10.0,
            "lines": [{"sku": "SKU-1", "quantity": 1, "unit_price": 10.0}],
        },
    }
    row.update(overrides)
    return row


def test_event_validation_detects_late_duplicate_and_conflict() -> None:
    base = _event_row()
    late = _event_row(
        event_id="evt-late",
        order_id="order-late",
        event_time="2026-10-20T00:00:00Z",
    )
    duplicate = _event_row()
    conflict = _event_row(
        event_id="evt-1",
        payload={
            "amount": 99.0,
            "lines": [{"sku": "SKU-1", "quantity": 1, "unit_price": 99.0}],
        },
    )
    invalid = _event_row(event_id="", order_id="")

    result = quality.validate_events_frame(
        [base, late, duplicate, conflict, invalid],
        batch_id="events_test",
        ingest_date="2026-10-22",
    )
    stats = result["stats"]

    assert stats["rows_read"] == 5
    assert stats["rows_valid"] == 4
    assert stats["rows_rejected"] == 2  # invalido + conflicto
    assert stats["rows_duplicated"] == 1
    assert stats["rows_conflicting"] == 1
    assert stats["rows_late"] == 1  # solo evt-late supera 24h
    assert len(result["valid"]) == 2
    assert len(result["quarantine"]) == 2
    assert set(result["quarantine"]["source"]) == {config.EVENTS_SOURCE}
    assert "duplicate_id_conflict" in set(result["quarantine"]["reason_code"])


def _catalog_row(**overrides):
    row = {
        "sku": "SKU-1",
        "name": "Producto",
        "category": "General",
        "is_service": False,
        "is_active": True,
        "valid_from": "2026-10-22T00:00:00Z",
        "valid_to": "2100-12-31T23:59:59Z",
        "updated_at": "2026-10-22T00:00:00Z",
    }
    row.update(overrides)
    return row


def test_catalog_validation_handles_duplicate_sku_without_double_accounting() -> None:
    valid = _catalog_row()
    duplicate = _catalog_row(name="Producto duplicado")
    invalid_date = _catalog_row(sku="SKU-2", valid_from="not-a-date")

    result = quality.validate_catalog_frame(
        [valid, duplicate, invalid_date],
        batch_id="catalog_test_v1",
        version=1,
        ingest_date="2026-10-22",
    )
    stats = result["stats"]

    assert stats["rows_read"] == 3
    assert stats["rows_valid"] == 2
    assert stats["rows_rejected"] == 1
    assert stats["rows_duplicated"] == 1
    assert len(result["valid"]) == 1
    assert len(result["quarantine"]) == 2
    assert set(result["quarantine"]["source"]) == {config.CATALOG_SOURCE}
    assert "duplicate_sku" in set(result["quarantine"]["reason_code"])
    # read = rejected + duplicated + filas unicas publicables
    assert stats["rows_rejected"] + stats["rows_duplicated"] + len(result["valid"]) == 3


def test_catalog_valid_to_sentinel_is_within_pandas_range() -> None:
    # El contrato del catalogo usa 2100 como fecha abierta, no 9999.
    result = quality.validate_catalog_frame(
        [_catalog_row()],
        batch_id="catalog_test_v1",
        version=1,
        ingest_date="2026-10-22",
    )
    assert result["stats"]["rows_rejected"] == 0
    assert str(result["valid"].iloc[0]["valid_to"]).startswith("2100-12-31")
