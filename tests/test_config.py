from datetime import datetime, timezone

from src import config


def test_batch_ids_are_deterministic() -> None:
    assert config.historical_batch_id("2026-10-21") == "historical_2026-10-21"
    assert config.events_batch_id("2026-10-22") == "events_2026-10-22"
    assert config.catalog_batch_id("2026-10-23", 3) == "catalog_2026-10-23_v3"


def test_ingest_timestamp_is_end_of_day_utc() -> None:
    value = config.ingest_timestamp("2026-10-24")
    assert value == datetime(2026, 10, 24, 23, 59, 59, tzinfo=timezone.utc)
    assert value.tzinfo == timezone.utc


def test_reporting_and_source_contracts() -> None:
    assert config.REPORTING_CURRENCY == "USD"
    assert config.HISTORICAL_SOURCE == "orders_historical"
    assert config.EVENTS_SOURCE == "order_events"
    assert config.CATALOG_SOURCE == "catalog"
