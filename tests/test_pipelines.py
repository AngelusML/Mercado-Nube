from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from src import pipelines


def test_catalog_scd2_does_not_reopen_older_snapshot(monkeypatch) -> None:
    current_from = datetime(2026, 10, 23, tzinfo=timezone.utc)
    monkeypatch.setattr(
        pipelines,
        "_catalog_current_map",
        lambda conn: {
            "SKU-1": {
                "product_key": 99,
                "record_hash": "hash-v3",
                "valid_from": current_from,
            }
        },
    )

    executed = []
    monkeypatch.setattr(
        pipelines.db,
        "execute_values",
        lambda *args, **kwargs: executed.append((args, kwargs)),
    )

    old_snapshot = pd.DataFrame(
        [
            {
                "sku": "SKU-1",
                "name": "Producto",
                "category": "General",
                "is_service": False,
                "is_active": True,
                "valid_from": pd.Timestamp("2026-10-21T00:00:00Z"),
                "updated_at": pd.Timestamp("2026-10-21T00:00:00Z"),
                "record_hash": "hash-v1",
            }
        ]
    )

    counts = pipelines._publish_catalog_scd2(
        None, old_snapshot, version=1, batch_id="catalog_2026-10-21_v1"
    )

    assert counts == {"inserted": 0, "changed": 0, "unchanged": 1}
    assert executed == []
