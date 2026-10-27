# Calidad de datos

## Principio

La calidad se controla en dos momentos:

1. **Antes de publicar:** validar filas, normalizar, deduplicar y separar cuarentena.
2. **Después de publicar:** reconciliar, verificar integridad referencial, SCD2, frescura y cobertura; si falla, generar alerta y no declarar el lote saludable.

La cuarentena no es un basurero: es una tabla consultable con lote, fuente, motivo, payload y hash. Cada rerun limpia la cuarentena de su `batch_id` y la reescribe de forma determinista.

## Reglas de validación

### Histórico

| Regla | `reason_code` | Acción |
|---|---|---|
| `invoice_no` vacío | `missing_invoice` | Cuarentena |
| `stock_code` vacío | `missing_sku` | Cuarentena |
| Fecha inválida | `invalid_date` | Cuarentena |
| Cantidad vacía o 0 | `invalid_quantity` | Cuarentena |
| Precio vacío o ≤ 0 | `invalid_price` | Cuarentena |
| País sin mapeo ISO-2 | `invalid_country` | Cuarentena |
| Fila válida repetida | — | Duplicado; se publica la primera |

`country` se normaliza con `config/country_aliases.csv`; se aceptan códigos ISO-2 válidos. `customer_id` vacío no invalida la fila: se mapea al miembro desconocido `-1`. La fecha local se interpreta en `Europe/London` y se convierte a UTC.

### Eventos

| Regla | `reason_code` | Acción |
|---|---|---|
| Sin `event_id` | `missing_event_id` | Cuarentena |
| Sin `order_id` | `missing_order_id` | Cuarentena |
| Tipo no permitido | `invalid_event_type` | Cuarentena |
| Timestamp inválido/futuro fuera de 2000–2100 | `invalid_event_time` | Cuarentena |
| País no mapeable | `invalid_country` | Cuarentena |
| Moneda no permitida | `invalid_currency` | Cuarentena |
| Payload ausente/no objeto | `invalid_payload` | Cuarentena |
| Monto ≤ 0 | `non_positive_amount` | Cuarentena |
| Líneas inválidas | `invalid_lines` | Cuarentena |
| Mismo `event_id` + mismo hash | — | Duplicado exacto; no se inserta |
| Mismo `event_id` + hash distinto | `duplicate_id_conflict` | Cuarentena; gana la versión ya publicada |

La deduplicación dentro del lote elige el evento con menor `event_time_utc` y, en empate, el menor `record_hash`. Esto hace el resultado determinista.

### Catálogo

| Regla | `reason_code` | Acción |
|---|---|---|
| SKU vacío | `missing_sku` | Cuarentena |
| Nombre vacío | `missing_name` | Cuarentena |
| Categoría vacía | `missing_category` | Cuarentena |
| Fechas inválidas | `invalid_date` | Cuarentena |
| Booleanos inválidos | `invalid_boolean` | Cuarentena |
| SKU repetido en la misma foto | `duplicate_sku` | Duplicado; se conserva el primero |

La foto usa `valid_from/valid_to` del API. La fecha abierta es `2100-12-31` para no salir del rango de pandas; el SCD2 mantiene `9999-12-31` como “sin fin” interno. El hash de atributos incluye nombre, categoría, `is_service` y `is_active`; un cambio de fecha solamente no crea versión.

## Quality gates

### Histórico

| Check | Regla | Resultado |
|---|---|---|
| `non_empty_batch` | `rows_read > 0` | pass (541,910) |
| `row_accounting` | read = rejected + duplicated + published | pass |
| `rejection_rate` | ≤ 10% | pass 0.46% |
| `duplicate_rate` | ≤ 5% | pass 0.97% |
| `quarantine_payload` | 0 cuarentenas inválidas | pass |
| `fact_line_usd_complete` | 0 montos USD nulos | pass |
| `fact_line_product_known` | 0 productos desconocidos | pass |
| `fact_line_geo_currency_known` | 0 FKs a dimensiones faltantes | pass |
| `fact_line_explicit_unknown_rate` | ZZ/XXX ≤ 5% | pass 0.08% (442/534,130) |

### Eventos

| Check | Regla | Resultado 10-24 |
|---|---|---|
| `non_empty_batch` | read > 0 | pass |
| `row_accounting` | read = rejected + duplicated + published | pass (5,106 = 9 + 84 + 5,013) |
| `rejection_rate` | ≤ 10% | pass 0.18% |
| `duplicate_rate` | ≤ 5% | pass 1.65% |
| `quarantine_payload` | 0 cuarentenas inválidas | pass |
| `event_id_unique` | 0 IDs duplicados | pass |
| `fact_event_usd_complete` | 0 montos USD nulos | pass |
| `fact_event_dimension_refs` | 0 FKs faltantes | pass |
| `fact_event_explicit_unknown_rate` | ZZ/XXX ≤ 5% | pass 0% |
| `lifecycle_coverage` | 0 órdenes sin lifecycle | pass |
| `event_freshness` | frescura de **ingesta** ≤ 24 h | pass 0.00 h |
| `event_not_future` | 0 eventos futuros | pass |
| `late_event_ratio_informative` | informativo | 391/5,013 (7.80%) publicados; 449 filas tardías en la limpieza |

### Catálogo

| Check | Regla | Resultado v3 |
|---|---|---|
| `non_empty_batch` | read > 0 | pass (3,958) |
| `row_accounting` | read = rejected + duplicated + published | pass |
| `rejection_rate` | ≤ 10% | pass 0% |
| `duplicate_rate` | ≤ 5% | pass 0% |
| `quarantine_payload` | 0 cuarentenas inválidas | pass |
| `scd2_one_current_per_sku` | 0 SKUs con múltiples corrientes | pass |
| `scd2_no_overlap` | 0 pares solapados | pass |
| `unknown_product_member` | exactamente 1 miembro desconocido | pass |

## Semántica de métricas (importante para defender)

### Histórico y eventos

- `rows_read`: filas físicas leídas del archivo.
- `rows_rejected`: filas inválidas + conflictos de `event_id` (si aplica).
- `rows_duplicated`: duplicados exactos del archivo + duplicados exactos de otro lote.
- `rows_published`: filas insertadas o ya publicadas por el **mismo** lote.
- Reconciliación: `read = rejected + duplicated + published`.

En un rerun del mismo lote, los eventos ya publicados se cuentan como `rows_published` (no-op idempotente), no como duplicados nuevos. Los duplicados de **otro** lote sí se cuentan como duplicados.

### Catálogo

- `rows_duplicated`: SKUs repetidos dentro de la misma foto (no “productos sin cambio”).
- `rows_published`: todos los productos aceptados del snapshot, incluidos los que no generan nueva versión SCD2.
- Las versiones insertadas/cambiadas/sin cambio se reportan en el XCom de la tarea y en el manifiesto, no en `rows_published`.
- Esto evita un falso “90% duplicado” cuando una foto de catálogo no cambia.

### Unknown members

`country_key=0` y `currency_key=0` son miembros **explícitos** `ZZ`/`XXX`; no son fallos de join. Por eso el gate verifica FKs faltantes por separado y monitorea la tasa de desconocidos, en vez de rechazar todo valor desconocido.

## Cuarentena final

| Lote | Fuente | Filas | Motivos |
|---|---|---:|---|
| `historical_2026-10-21` | `orders_historical` | 2,517 | `invalid_price` |
| `events_2026-10-22` | `order_events` | 12 | líneas, país, moneda, timestamp, tipo, IDs |
| `events_2026-10-23` | `order_events` | 7 | 6 inválidas + 1 conflicto entre lotes |
| `events_2026-10-24` | `order_events` | 9 | líneas, tipo, timestamp, IDs |

No hay cuarentena de catálogo en las fotos actuales.

## Hallazgos corregidos durante la construcción

1. **Desconocidos explícitos:** el gate inicialmente fallaba con 442 filas `ZZ`; se separó “join faltante” de “desconocido explícito” y se agregó una tasa monitoreada.
2. **Duplicados de catálogo:** los productos sin cambio se contaban como duplicados (90%); se corrigió la semántica para medir SKU repetido en la foto.
3. **Etiquetas de fuente:** la cuarentena usaba `historical/events` y las métricas `orders_historical/order_events`; se unificaron para que las queries operativas puedan unir por fuente.
4. **Rechazos idénticos:** dos filas inválidas idénticas colapsaban en una sola clave de cuarentena; se agregó `row_index` a la clave para conservar una fila por rechazo.
5. **Sentinel de fecha:** el API usaba `9999`, fuera de rango de pandas; se cambió a `2100` (el SCD2 conserva `9999` interno).
6. **Idempotencia de eventos:** un rerun contaba todo como duplicado; se separó “ya publicado por el mismo lote” de “duplicado de otro lote”.
7. **Frescura:** el gate medía el `event_time` máximo, lo que castigaba lotes tardíos; ahora mide la frescura de **ingesta** y la latencia se monitorea aparte.
8. **Snapshots fuera de orden:** un rerun del catálogo v1 después de v3 podía reabrir la historia; se agregó una guarda por `valid_from` y una prueba unitaria.

## Cómo inspeccionar

```sql
-- Cuarentena por lote/fuente/motivo
SELECT batch_id, source, reason_code, count(*)
FROM ops.quarantine_records
GROUP BY 1,2,3
ORDER BY 1,2,3;

-- Gates fallidos (debe devolver 0 filas)
SELECT batch_id, check_name, observed_value, threshold
FROM ops.data_quality_results
WHERE status = 'fail'
ORDER BY batch_id, check_name;

-- Reconciliación por lote
SELECT batch_id, layer, rows_read, rows_rejected, rows_duplicated, rows_published
FROM ops.pipeline_run_metrics
ORDER BY batch_id, layer;

-- SCD2
SELECT count(*) FROM (
    SELECT sku FROM analytics.dim_product
    WHERE is_current AND sku <> '__UNKNOWN__'
    GROUP BY sku HAVING count(*) > 1
) d;
```
