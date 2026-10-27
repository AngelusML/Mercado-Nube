"""Regla de coherencia pais-moneda en eventos."""

from __future__ import annotations

from src import quality


def _event(**overrides):
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


def test_moneda_que_no_corresponde_al_pais_va_a_cuarentena() -> None:
    ok = _event()
    mal = _event(event_id="evt-2", order_id="order-2", country="CO", currency="MXN")

    result = quality.validate_events_frame(
        [ok, mal], batch_id="events_test", ingest_date="2026-10-22"
    )

    assert len(result["valid"]) == 1
    assert list(result["valid"]["event_id"]) == ["evt-1"]
    reasons = set(result["quarantine"]["reason_code"])
    assert reasons == {"country_currency_mismatch"}


def test_las_tres_combinaciones_validas_pasan() -> None:
    rows = [
        _event(event_id="e-co", order_id="o-co", country="CO", currency="COP"),
        _event(event_id="e-mx", order_id="o-mx", country="MX", currency="MXN"),
        _event(event_id="e-pe", order_id="o-pe", country="PE", currency="PEN"),
    ]

    result = quality.validate_events_frame(
        rows, batch_id="events_test", ingest_date="2026-10-22"
    )

    assert len(result["valid"]) == 3
    assert result["quarantine"].empty
