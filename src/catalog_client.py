"""Cliente del API mock de catalogo con paginacion y reintentos."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src import config

logger = logging.getLogger(__name__)


class TransientCatalogError(Exception):
    """Error recuperable: 429, 5xx o timeout."""


class CatalogNotFoundError(Exception):
    """Version o recurso inexistente; no se reintenta."""


@retry(
    retry=retry_if_exception_type((TransientCatalogError, requests.RequestException)),
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _get_page(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    response = session.get(url, params=params, timeout=15)
    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            time.sleep(min(float(retry_after), 5.0))
        raise TransientCatalogError(
            f"429 rate limit page={params.get('page')} version={params.get('version')}"
        )
    if response.status_code in {500, 502, 503, 504}:
        raise TransientCatalogError(
            f"{response.status_code} error transitorio page={params.get('page')} "
            f"version={params.get('version')}"
        )
    if response.status_code == 404:
        raise CatalogNotFoundError(response.text)
    response.raise_for_status()
    return response.json()


def fetch_catalog(version: int, page_size: int = 100) -> tuple[list[dict], dict[str, Any]]:
    """Descarga todas las paginas de una version del catalogo."""
    session = requests.Session()
    url = f"{config.CATALOG_BASE_URL}/v1/products"
    items: list[dict] = []
    page = 1
    pages_fetched = 0
    total = 0
    extracted_at = None

    while True:
        payload = _get_page(
            session,
            url,
            {"page": page, "page_size": page_size, "version": version},
        )
        items.extend(payload.get("items", []))
        total = int(payload.get("total", len(items)))
        extracted_at = payload.get("extracted_at")
        pages_fetched += 1
        next_page = payload.get("next_page")
        if not next_page:
            break
        page = int(next_page)
        # Pequena pausa para no saturar; el rate limit se demuestra igual.
        time.sleep(0.05)

    if len(items) != total:
        raise RuntimeError(
            f"Catalogo incompleto: esperados={total} recibidos={len(items)}"
        )

    metadata = {
        "version": version,
        "total": total,
        "pages_fetched": pages_fetched,
        "extracted_at": extracted_at,
        "base_url": config.CATALOG_BASE_URL,
    }
    logger.info("Catalogo v%s descargado: %s productos en %s paginas", version, total, pages_fetched)
    return items, metadata
