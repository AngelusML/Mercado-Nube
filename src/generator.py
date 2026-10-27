"""Generador reproducible de eventos de pedidos (JSONL).

Cumple el requisito de minimo 50.000 eventos sinteticos con semilla fija.
Incluye: eventos fuera de orden, duplicados, eventos tardios, conflictos de
event_id y registros invalidos para alimentar la cuarentena.

Uso:
    python -m src.generator --orders 27000 --seed 42
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

COUNTRIES = [
    ("CO", "COP"),
    ("MX", "MXN"),
    ("PE", "PEN"),
]

CHANNELS = ["web", "mobile", "partner"]
PAYMENT_METHODS = {
    "CO": ["card", "pse", "nequi", "cash"],
    "MX": ["card", "oxxo", "transfer"],
    "PE": ["card", "yape", "transfer"],
}
CURRENCY_RANGES = {
    "COP": (10_000, 300_000),
    "MXN": (100, 3_000),
    "PEN": (20, 800),
}
EVENT_TYPES = ["created", "paid", "cancelled", "refunded"]


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_products(catalog_path: Path) -> list[dict]:
    payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    products = [
        product
        for product in payload["products"]
        if not product.get("is_service") and product.get("is_active", True)
    ]
    if not products:
        raise SystemExit("El catalogo no tiene productos activos.")
    return products


def make_lines(rng: random.Random, products: list[dict], currency: str) -> list[dict]:
    count = rng.choices([1, 2, 3], weights=[0.55, 0.30, 0.15], k=1)[0]
    low, high = CURRENCY_RANGES[currency]
    lines = []
    for _ in range(count):
        product = rng.choice(products)
        quantity = rng.randint(1, 5)
        unit_price = round(rng.uniform(low, high), 2)
        lines.append(
            {
                "sku": product["sku"],
                "quantity": quantity,
                "unit_price": unit_price,
            }
        )
    return lines


def make_event(
    rng: random.Random,
    event_id: str,
    order_id: str,
    event_type: str,
    event_time: datetime,
    country: str,
    currency: str,
    amount: float,
    lines: list[dict],
    reason: str | None = None,
) -> dict:
    payload = {
        "amount": round(amount, 2),
        "channel": rng.choice(CHANNELS),
        "payment_method": rng.choice(PAYMENT_METHODS[country]),
        "lines": lines,
    }
    if reason:
        payload["reason"] = reason
    return {
        "event_id": event_id,
        "order_id": order_id,
        "event_type": event_type,
        "event_time": utc_iso(event_time),
        "country": country,
        "currency": currency,
        "payload": payload,
    }


def build_events(
    rng: random.Random,
    products: list[dict],
    orders: int,
    start: datetime,
    end: datetime,
) -> list[dict]:
    events: list[dict] = []

    for index in range(1, orders + 1):
        order_id = f"SYN-{index:06d}"
        country, currency = rng.choice(COUNTRIES)
        lines = make_lines(rng, products, currency)
        total = round(
            sum(line["quantity"] * line["unit_price"] for line in lines), 2
        )

        # Creamos los pedidos en los primeros 4 dias para que pago/devolucion
        # quepan dentro de la ventana de 7 dias.
        max_created = start + timedelta(days=4) - timedelta(seconds=1)
        created = start + timedelta(
            seconds=rng.randint(0, int((max_created - start).total_seconds()))
        )

        events.append(
            make_event(
                rng,
                f"evt_{order_id}_created",
                order_id,
                "created",
                created,
                country,
                currency,
                total,
                lines,
            )
        )

        roll = rng.random()
        if roll < 0.08:
            cancelled_at = min(
                created + timedelta(minutes=rng.randint(60, 48 * 60)),
                end - timedelta(minutes=1),
            )
            events.append(
                make_event(
                    rng,
                    f"evt_{order_id}_cancelled",
                    order_id,
                    "cancelled",
                    cancelled_at,
                    country,
                    currency,
                    total,
                    lines,
                    reason=rng.choice(["customer_request", "payment_failed", "out_of_stock"]),
                )
            )
        elif roll < 0.93:
            paid_at = min(
                created + timedelta(minutes=rng.randint(30, 36 * 60)),
                end - timedelta(minutes=1),
            )
            events.append(
                make_event(
                    rng,
                    f"evt_{order_id}_paid",
                    order_id,
                    "paid",
                    paid_at,
                    country,
                    currency,
                    total,
                    lines,
                )
            )
            if rng.random() < 0.07:
                refunded_at = min(
                    paid_at + timedelta(hours=rng.randint(2, 72)),
                    end - timedelta(minutes=1),
                )
                refund_amount = round(total * rng.uniform(0.5, 1.0), 2)
                events.append(
                    make_event(
                        rng,
                        f"evt_{order_id}_refunded",
                        order_id,
                        "refunded",
                        refunded_at,
                        country,
                        currency,
                        refund_amount,
                        lines,
                        reason=rng.choice(["damaged", "late_delivery", "customer_request"]),
                    )
                )
        # else: pedido creado sin pago ni cancelacion (abandono)

    return events


def make_invalid_events(rng: random.Random, products: list[dict]) -> list[dict]:
    invalid: list[dict] = []
    base_time = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)

    def base(order_index: int) -> dict:
        order_id = f"BAD-{order_index:04d}"
        country, currency = rng.choice(COUNTRIES)
        lines = make_lines(rng, products, currency)
        return {
            "event_id": f"evt_{order_id}_created",
            "order_id": order_id,
            "event_type": "created",
            "event_time": utc_iso(base_time + timedelta(minutes=order_index)),
            "country": country,
            "currency": currency,
            "payload": {"amount": 100.0, "channel": "web", "payment_method": "card", "lines": lines},
        }

    for i in range(8):
        event = base(i)
        event.pop("event_id")
        invalid.append(event)

    for i in range(8, 16):
        event = base(i)
        event.pop("order_id")
        invalid.append(event)

    for i in range(16, 24):
        event = base(i)
        event["event_time"] = "not-a-date"
        invalid.append(event)

    for i in range(24, 32):
        event = base(i)
        event["country"] = "XX"
        invalid.append(event)

    for i in range(32, 40):
        event = base(i)
        event["payload"]["amount"] = -50.0
        invalid.append(event)

    for i in range(40, 48):
        event = base(i)
        event["event_type"] = "mystery"
        invalid.append(event)

    for i in range(48, 56):
        event = base(i)
        event.pop("currency")
        invalid.append(event)

    for i in range(56, 64):
        event = base(i)
        event.pop("event_type")
        invalid.append(event)

    for i in range(64, 72):
        event = base(i)
        event["payload"]["lines"][0]["quantity"] = 0
        invalid.append(event)

    for i in range(72, 80):
        event = base(i)
        event["payload"]["lines"][0].pop("sku")
        invalid.append(event)

    for event in invalid:
        event["_target_date"] = f"2026-10-{rng.randint(20, 26):02d}"
    return invalid


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=Path, default=Path("data/catalog/catalog_v3.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/incoming/events"))
    parser.add_argument("--orders", type=int, default=27_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-date", default="2026-10-20")
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    if not args.catalog.exists():
        raise SystemExit(
            f"No existe {args.catalog}. Ejecuta scripts/generate_catalog.py primero."
        )

    rng = random.Random(args.seed)
    products = load_products(args.catalog)
    start = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=args.days)
    last_date = (end - timedelta(days=1)).date()

    events = build_events(rng, products, args.orders, start, end)
    invalid = make_invalid_events(rng, products)
    events_by_date: dict[str, list[dict]] = defaultdict(list)

    def target_date(event: dict) -> str:
        event_time = datetime.strptime(event["event_time"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
        ingest = event_time.date()
        if rng.random() < 0.03:
            late = ingest + timedelta(days=2)
            ingest = min(late, last_date)
        return ingest.isoformat()

    for event in events:
        events_by_date[target_date(event)].append(event)

    for event in invalid:
        events_by_date[event.pop("_target_date")].append(event)

    # Duplicados exactos: misma linea, mismo contenido, otro lote o el mismo.
    duplicate_count = max(1, int(len(events) * 0.01))
    for event in rng.sample(events, duplicate_count):
        copy = json.loads(json.dumps(event))
        original_dates = [
            date for date, bucket in events_by_date.items() if event in bucket
        ]
        source_date = original_dates[0] if original_dates else sorted(events_by_date)[0]
        source_dt = datetime.strptime(source_date, "%Y-%m-%d").date()
        if rng.random() < 0.5:
            target = source_date
        else:
            target = min(source_dt + timedelta(days=1), last_date).isoformat()
        events_by_date[target].append(copy)

    # Conflictos: mismo event_id, contenido distinto. Llegan en un lote posterior.
    conflict_count = 0
    for event in rng.sample(events, min(5, len(events))):
        copy = json.loads(json.dumps(event))
        copy["country"] = "MX" if event["country"] != "MX" else "CO"
        copy["payload"]["amount"] = round(float(copy["payload"]["amount"]) * 1.1, 2)
        original_dates = [
            date for date, bucket in events_by_date.items() if event in bucket
        ]
        source_date = original_dates[0] if original_dates else sorted(events_by_date)[0]
        source_dt = datetime.strptime(source_date, "%Y-%m-%d").date()
        target = min(source_dt + timedelta(days=1), last_date).isoformat()
        events_by_date[target].append(copy)
        conflict_count += 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "seed": args.seed,
        "orders": args.orders,
        "date_start": args.start_date,
        "date_end": last_date.isoformat(),
        "event_types": EVENT_TYPES,
        "base_events": len(events),
        "duplicates": duplicate_count,
        "conflicts": conflict_count,
        "invalid": len(invalid),
        "files": [],
    }

    type_counts: Counter[str] = Counter()
    total_written = 0
    for date in sorted(events_by_date):
        bucket = events_by_date[date]
        rng.shuffle(bucket)
        directory = args.output_dir / f"ingest_date={date}"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "events.jsonl"
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            for event in bucket:
                handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True))
                handle.write("\n")
                total_written += 1
                event_type = event.get("event_type", "INVALID")
                type_counts[event_type] += 1
        manifest["files"].append(
            {
                "ingest_date": date,
                "file": str(path),
                "rows": len(bucket),
                "sha256": sha256(path),
            }
        )
        print(f"{date}: {len(bucket):,} eventos -> {path}")

    manifest["total_rows"] = total_written
    manifest["event_type_counts"] = dict(sorted(type_counts.items()))
    manifest_path = args.output_dir / "events_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Total eventos: {total_written:,}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
