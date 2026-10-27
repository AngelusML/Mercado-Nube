"""Pruebas de la validacion previa a publicar y del calendario del catalogo."""

from __future__ import annotations

from src.quality_gate import evaluate_cleaned_batch
from src.tasks import resolve_catalog_version


def _checks(**overrides):
    values = {
        "rows_read": 100,
        "rows_rejected": 2,
        "rows_duplicated": 3,
        "cleaned_rows": 95,
        "max_rejection_rate": 0.10,
        "max_duplicate_rate": 0.05,
    }
    values.update(overrides)
    return {item["name"]: item["passed"] for item in evaluate_cleaned_batch(**values)}


def test_lote_sano_pasa_todas_las_reglas() -> None:
    assert all(_checks().values())


def test_lote_con_muchos_rechazos_no_se_publica() -> None:
    result = _checks(rows_rejected=20, cleaned_rows=77)
    assert result["pre_publish_rejection_rate"] is False


def test_lote_con_muchos_duplicados_no_se_publica() -> None:
    result = _checks(rows_duplicated=10, cleaned_rows=88)
    assert result["pre_publish_duplicate_rate"] is False


def test_conteos_que_no_cuadran_no_se_publican() -> None:
    result = _checks(cleaned_rows=90)
    assert result["pre_publish_row_accounting"] is False


def test_lote_vacio_no_se_publica() -> None:
    result = _checks(rows_read=0, rows_rejected=0, rows_duplicated=0, cleaned_rows=0)
    assert result["pre_publish_non_empty_batch"] is False


def test_catalogo_se_resuelve_por_fecha_de_vigencia() -> None:
    assert resolve_catalog_version("2026-10-20") == 1
    assert resolve_catalog_version("2026-10-21") == 1
    assert resolve_catalog_version("2026-10-22") == 2
    assert resolve_catalog_version("2026-10-23") == 3
    assert resolve_catalog_version("2026-10-26") == 3


def test_version_explicita_tiene_prioridad() -> None:
    assert resolve_catalog_version("2026-10-26", 1) == 1
    assert resolve_catalog_version("2026-10-20", "2") == 2
