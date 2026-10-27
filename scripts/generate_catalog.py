"""Genera un catalogo mock reproducible a partir de los SKU historicos.

Crea tres versiones del catalogo para demostrar SCD2:
  v1: carga historica
  v2: primer lote diario
  v3: segundo lote diario

No usa dependencias externas. Uso:
    python scripts/generate_catalog.py --historical data/incoming/historical/online_retail_2010_2011.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

CATEGORIES = [
    "Christmas",
    "Bags",
    "Kitchen",
    "Home Decor",
    "Toys",
    "Stationery",
    "Textiles",
    "Garden",
    "Jewellery",
    "General",
]

CATEGORY_KEYWORDS = {
    "Christmas": ["CHRISTMAS", "XMAS", "ADVENT", "SANTA", "REINDEER", "SNOWMAN"],
    "Bags": ["BAG", "TOTE", "PURSE", "SHOPPER", "BACKPACK", "LUGGAGE"],
    "Kitchen": [
        "MUG",
        "CUP",
        "TEAPOT",
        "BOWL",
        "PLATE",
        "KITCHEN",
        "JUG",
        "BOTTLE",
        "CUTLERY",
        "CAKESTAND",
        "BAKING",
        "JAR",
    ],
    "Home Decor": [
        "CANDLE",
        "HOLDER",
        "LANTERN",
        "LIGHT",
        "FRAME",
        "CLOCK",
        "VASE",
        "ORNAMENT",
        "DECORATION",
        "HEART",
    ],
    "Toys": ["TOY", "GAME", "PLAYHOUSE", "DOLL", "PUZZLE", "SKIPPING", "BALL", "TRAIN"],
    "Stationery": [
        "CARD",
        "PAPER",
        "NOTEBOOK",
        "PEN",
        "PENCIL",
        "STICKER",
        "ENVELOPE",
        "WRAP",
        "RIBBON",
    ],
    "Textiles": [
        "APRON",
        "TOWEL",
        "CUSHION",
        "COVER",
        "TEXTILE",
        "BLANKET",
        "QUILT",
        "FABRIC",
        "CURTAIN",
    ],
    "Garden": ["GARDEN", "PLANT", "FLOWER", "WATERING", "SEED"],
    "Jewellery": ["NECKLACE", "BRACELET", "EARRING", "RING", "JEWEL", "BEAD"],
}

SERVICE_TOKENS = [
    "POSTAGE",
    "POST",
    "BANK CHARGES",
    "DISCOUNT",
    "MANUAL",
    "AMAZON",
    "DOTCOM",
    "CRUK",
    "COMMISSION",
    "FEE",
    "CARRIAGE",
    "ADJUST",
    "SAMPLES",
    "BROCHURE",
    "GIFT VOUCHER",
]

SERVICE_SKUS = {
    "POST",
    "D",
    "M",
    "C2",
    "DOT",
    "BANK CHARGES",
    "CRUK",
    "AMAZONFEE",
    "S",
    "PADS",
    "GIFT",
    "SP1002",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_service(sku: str, description: str) -> bool:
    if sku in SERVICE_SKUS:
        return True
    upper = description.upper()
    return any(token in upper for token in SERVICE_TOKENS)


def category_for(description: str, is_service_flag: bool) -> str:
    if is_service_flag:
        return "Services"
    upper = description.upper()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(keyword in upper for keyword in keywords):
            return category
    return "General"


def build_products(csv_path: Path) -> list[dict]:
    descriptions: dict[str, Counter] = defaultdict(Counter)
    counts: Counter[str] = Counter()

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            sku = (row.get("stock_code") or "").strip().upper()
            if not sku:
                continue
            counts[sku] += 1
            description = (row.get("description") or "").strip()
            if description:
                descriptions[sku][description] += 1

    products = []
    for sku in sorted(counts):
        description = descriptions[sku].most_common(1)[0][0] if descriptions[sku] else ""
        service = is_service(sku, description)
        name = description or f"Product {sku}"
        products.append(
            {
                "sku": sku,
                "name": name,
                "category": category_for(description, service),
                "is_service": service,
                "is_active": True,
            }
        )
    return products


def make_version(
    base_products: list[dict],
    version: int,
    version_date: str,
    next_date: str,
    rng: random.Random,
) -> list[dict]:
    products = [dict(product) for product in base_products]
    product_categories = [category for category in CATEGORIES]

    if version == 1:
        for product in products:
            product.update(
                {
                    "valid_from": version_date,
                    "valid_to": next_date,
                    "updated_at": version_date,
                }
            )
        return products

    # Cambios controlados y deterministas para demostrar historial SCD2.
    change_count = max(1, int(len(products) * 0.10))
    for index in rng.sample(range(len(products)), change_count):
        product = products[index]
        current = product["category"]
        alternatives = [category for category in product_categories if category != current]
        product["category"] = rng.choice(alternatives)
        product["valid_from"] = version_date
        product["valid_to"] = next_date
        product["updated_at"] = version_date

    if version >= 3:
        name_count = max(1, int(len(products) * 0.05))
        for index in rng.sample(range(len(products)), name_count):
            product = products[index]
            if not product["name"].endswith(" (actualizado)"):
                product["name"] = product["name"] + " (actualizado)"
            product["valid_from"] = version_date
            product["valid_to"] = next_date
            product["updated_at"] = version_date

        inactive_count = max(1, int(len(products) * 0.02))
        for index in rng.sample(range(len(products)), inactive_count):
            product = products[index]
            product["is_active"] = False
            product["valid_from"] = version_date
            product["valid_to"] = next_date
            product["updated_at"] = version_date

    # Los productos que no cambiaron conservan la vigencia de su version anterior.
    for product in products:
        if "valid_from" not in product:
            product["valid_from"] = version_date
            product["valid_to"] = next_date
            product["updated_at"] = version_date

    return products


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--historical",
        type=Path,
        default=Path("data/incoming/historical/online_retail_2010_2011.csv"),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/catalog"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if not args.historical.exists():
        raise SystemExit(
            f"No existe {args.historical}. Ejecuta primero scripts/prepare_historical.py"
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    base_products = build_products(args.historical)
    if not base_products:
        raise SystemExit("El historico no produjo SKU.")

    versions = [
        (1, "2010-01-01T00:00:00Z", "2026-10-22T00:00:00Z"),
        (2, "2026-10-22T00:00:00Z", "2026-10-23T00:00:00Z"),
        (3, "2026-10-23T00:00:00Z", "2100-12-31T23:59:59Z"),
    ]

    manifest = {
        "source_historical": str(args.historical),
        "source_sha256": sha256(args.historical),
        "seed": args.seed,
        "generated_at": "2026-10-21T00:00:00Z",
        "product_count": len(base_products),
        "versions": [],
    }

    for version, version_date, next_date in versions:
        rng = random.Random(args.seed + version)
        products = make_version(
            base_products, version, version_date, next_date, rng
        )
        payload = {
            "version": version,
            "generated_at": "2026-10-21T00:00:00Z",
            "product_count": len(products),
            "products": products,
        }
        path = args.output_dir / f"catalog_v{version}.json"
        path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        manifest["versions"].append(
            {
                "version": version,
                "file": path.name,
                "sha256": sha256(path),
                "product_count": len(products),
            }
        )
        print(f"v{version}: {len(products):,} productos -> {path}")

    (args.output_dir / "catalog_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Manifest: {args.output_dir / 'catalog_manifest.json'}")


if __name__ == "__main__":
    main()

