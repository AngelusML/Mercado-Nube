"""API mock del catalogo de Mercado Nube.

Simula:
- paginacion
- limite de tasa (HTTP 429)
- errores transitorios (HTTP 500)
- versiones del catalogo (para demostrar SCD2)

Los datos se generan con scripts/generate_catalog.py.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("catalog-api")

CATALOG_DIR = Path(os.getenv("CATALOG_DIR", "/data/catalog"))
RATE_LIMIT_MAX_REQUESTS = int(os.getenv("RATE_LIMIT_MAX_REQUESTS", "40"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "10"))
TRANSIENT_ERROR_EVERY = int(os.getenv("TRANSIENT_ERROR_EVERY", "17"))

app = FastAPI(
    title="Mercado Nube - Catalogo API",
    version="1.0.0",
    description="Mock local con paginacion, rate limit y errores transitorios.",
)

catalogs: dict[int, dict[str, Any]] = {}
request_windows: dict[str, deque[float]] = defaultdict(deque)
transient_failed_once: set[tuple[int, int]] = set()


def load_catalogs() -> None:
    global catalogs
    loaded: dict[int, dict[str, Any]] = {}
    for path in sorted(CATALOG_DIR.glob("catalog_v*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            version = int(payload["version"])
            loaded[version] = payload
        except Exception as exc:  # pragma: no cover - defensa operativa
            logger.error("No se pudo cargar %s: %s", path, exc)
    catalogs = loaded
    logger.info("Catalogo(s) cargado(s): %s", sorted(catalogs))


@app.on_event("startup")
def startup() -> None:
    load_catalogs()


def check_rate_limit(request: Request) -> None:
    client = request.client.host if request.client else "unknown"
    now = time.monotonic()
    window = request_windows[client]
    while window and now - window[0] > RATE_LIMIT_WINDOW_SECONDS:
        window.popleft()
    if len(window) >= RATE_LIMIT_MAX_REQUESTS:
        retry_after = max(1, int(RATE_LIMIT_WINDOW_SECONDS - (now - window[0])))
        raise HTTPException(
            status_code=429,
            detail="Limite de tasa simulado. Reintenta con espera incremental.",
            headers={"Retry-After": str(retry_after)},
        )
    window.append(now)


@app.get("/health")
def health() -> JSONResponse:
    if not catalogs:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "detail": "No hay catalogos cargados"},
        )
    return JSONResponse(
        content={
            "status": "ok",
            "versions": sorted(catalogs),
            "product_count": {str(k): v["product_count"] for k, v in catalogs.items()},
        }
    )


@app.get("/v1/catalog/version")
def catalog_version() -> dict[str, Any]:
    if not catalogs:
        raise HTTPException(status_code=503, detail="No hay catalogos cargados")
    return {
        "versions": sorted(catalogs),
        "latest": max(catalogs),
        "product_count": {str(k): v["product_count"] for k, v in catalogs.items()},
    }


@app.get("/v1/products")
def products(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=100),
    version: int | None = Query(None, ge=1),
) -> dict[str, Any]:
    check_rate_limit(request)

    if not catalogs:
        raise HTTPException(status_code=503, detail="No hay catalogos cargados")

    selected_version = version or max(catalogs)
    if selected_version not in catalogs:
        raise HTTPException(
            status_code=404,
            detail=f"Version {selected_version} no existe. Disponibles: {sorted(catalogs)}",
        )

    # Error transitorio deterministico que ocurre UNA vez por pagina/version.
    # El primer intento recibe 500; el reintento del cliente debe recuperarlo.
    if page > 1 and TRANSIENT_ERROR_EVERY > 0:
        transient_key = (selected_version, page)
        if (
            (page * 31 + selected_version * 17) % TRANSIENT_ERROR_EVERY == 0
            and transient_key not in transient_failed_once
        ):
            transient_failed_once.add(transient_key)
            logger.warning("Error transitorio simulado page=%s version=%s", page, selected_version)
            raise HTTPException(status_code=500, detail="Error transitorio simulado")

    payload = catalogs[selected_version]
    all_products = payload["products"]
    total = len(all_products)
    total_pages = (total + page_size - 1) // page_size
    start = (page - 1) * page_size
    end = start + page_size
    items = all_products[start:end]

    logger.info(
        "products version=%s page=%s/%s items=%s",
        selected_version,
        page,
        total_pages,
        len(items),
    )

    return {
        "version": selected_version,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "items": items,
        "next_page": page + 1 if page < total_pages else None,
        "extracted_at": payload.get("generated_at", "2026-10-21T00:00:00Z"),
    }

