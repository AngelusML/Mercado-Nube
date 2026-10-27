# Evidencia de ejecución

Esta página consolida los resultados verificables de la construcción. Los archivos crudos están en `docs/evidence/`.

> **Actualización (corrida final con 7 lotes, validación previa y regla país-moneda):** los archivos crudos de `docs/evidence/` y la tabla de resultados del `README.md` son la referencia vigente: 534,130 líneas, 53,783 eventos publicados de 54,405 leídos, 27,000 órdenes, 1,594 eventos tardíos, 2,602 filas en cuarentena y 216/216 checks de calidad. Las cifras de las secciones siguientes corresponden a la primera corrida (3 lotes) y se conservan como historial.

## 1. Conteos finales

| Métrica | Valor |
|---|---:|
| Filas históricas leídas | 541,910 |
| `analytics.fact_order_line` | **534,130** |
| `analytics.fact_order_event` | **31,589** |
| `analytics.fact_order_lifecycle` | **19,330** |
| Versiones SCD2 de producto | **5,312** |
| Productos corrientes | **3,958** |
| Eventos tardíos | **1,076** (3.41%) |
| Filas en cuarentena | **2,545** |

Reconciliación histórica:

```text
541,910 leídas = 2,517 rechazadas + 5,263 duplicadas + 534,130 publicadas
```

Evidencia cruda: [`docs/evidence/final_counts.txt`](evidence/final_counts.txt).

## 2. Histórico

| Concepto | Valor |
|---|---:|
| Líneas de venta/devolución publicadas | 534,130 |
| Facturas | 23,796 |
| Líneas de devolución | 9,251 |
| Rechazos | 2,517 (`invalid_price`) |
| Duplicados exactos | 5,263 |
| Desconocidos explícitos (ZZ/XXX) | 442 (0.08%) |

Checksum del Parquet curado de hechos:

```text
historical_2026-10-21  curated  74734c354f9bc7de3111f2183d3525c5bcbf51662418d3c0a08ce517f532268e
```

Quality gate: **9/9 checks pass**. La carga se ejecutó con la lógica de `src.pipelines` durante la construcción; el DAG `01_historical_load` está importado y disponible para reproducirla.

En total se registraron **80 checks** (9 históricos + 39 de eventos + 32 de catálogo), **0 fallas**.

## 3. Catálogo SCD2

| Versión | Cambios | Versiones acumuladas | Checksum curado |
|---|---:|---:|---|
| v1 (2026-10-21) | carga inicial 3,958 | 3,958 | `ed78469ef1ac62e58db361494046de40df1734fe3612ca57641e92e8e507217d` |
| v2 (2026-10-22) | 395 cambiados | 4,353 | `cc53bc9284638acee4031c22ef442f74915adc028c27b3c30a3f734702566200` |
| v3 (2026-10-23) | 959 cambiados | 5,312 | `88ce7075938afdf03e9114f6d71e808c3341e87f3ac16b575b42c334a51a823b` |

Distribución final:

```text
catalog_version 1: 2,993 corrientes / 3,958 versiones
catalog_version 2:     6 corrientes /   395 versiones
catalog_version 3:   959 corrientes /   959 versiones
Total: 5,312 versiones, 3,958 corrientes
Duplicados corrientes: 0
Solapamientos: 0
```

Quality gates: **8/8 por cada versión** (32 checks). El rerun de v3 devolvió `inserted=0, changed=0, unchanged=3958` y conservó el checksum `88ce...` y las 5,312 versiones.

## 4. Eventos

| Lote | Leídos | Rechazados | Duplicados | Publicados | Tardíos publicados | % tardío |
|---|---:|---:|---:|---:|---:|---:|
| `events_2026-10-22` | 13,347 | 12 | 63 | 13,272 | 318 | 2.40% |
| `events_2026-10-23` | 13,454 | 7 | 143 | 13,304 | 367 | 2.76% |
| `events_2026-10-24` | 5,106 | 9 | 84 | 5,013 | 391 | 7.80% |
| **Total** | **31,907** | **28** | **290** | **31,589** | **1,076** | **3.41%** |

- Conflicto entre lotes: **1** (`evt_SYN-002785_paid`), conservado en cuarentena; gana la primera versión publicada.
- Órdenes con al menos un tardío: **1,068**.
- Eventos en esas órdenes: 1,502; promedio 1.41 eventos/orden.
- Checksums de hechos: `80a434...` (10-22), `dd7172...` (10-23), `be58f...` (10-24).
- Lifecycle final: 19,330 órdenes; checksum del lote 10-24 `9d3bdd...`.

> Diferencia de semántica: `rows_late` en métricas de limpieza cuenta filas válidas antes de resolver duplicados/conflictos (por ejemplo 449 en 10-24); en hechos quedan 391 eventos tardíos publicados. Las dos cifras son correctas y están explicadas en [`calidad.md`](calidad.md).

Quality gates: **13/13 por lote** (39 checks), todos pass. El check de frescura mide `ingested_at`, no `event_time`, para no castigar tardíos.

## 5. Cuarentena final

| Lote | Fuente | Motivo | Filas |
|---|---|---|---:|
| `historical_2026-10-21` | `orders_historical` | `invalid_price` | 2,517 |
| `events_2026-10-22` | `order_events` | 7 motivos | 12 |
| `events_2026-10-23` | `order_events` | 5 motivos + conflicto | 7 |
| `events_2026-10-24` | `order_events` | 5 motivos | 9 |
| **Total** | | | **2,545** |

Cada fila tiene `quarantine_key`, `source_record_id`, `reason_code`, `raw_payload`, `batch_id` y `processed_at`. La clave incluye `row_index` para no colapsar rechazos idénticos.

## 6. Idempotencia

Prueba realizada sobre el lote más reciente, para evitar que un lote posterior cambie el lifecycle derivado:

```text
ANTES  events_2026-10-24 curated           5013  be58f76bb042c9045b618315863709aae8fa07d67660a8a2c86c1dfee6ce9190
ANTES  events_2026-10-24 curated_lifecycle 4970  9d3bddf26f41c999f4168605900a1af7fa59bae897334cd3e160daa293e61218
RERUN  publish_events('2026-10-24') -> rows_inserted_this_run=0, published=5013
DESPUÉS events_2026-10-24 curated           5013  be58f76bb042c9045b618315863709aae8fa07d67660a8a2c86c1dfee6ce9190
DESPUÉS events_2026-10-24 curated_lifecycle 4970  9d3bddf26f41c999f4168605900a1af7fa59bae897334cd3e160daa293e61218
```

Catálogo:

```text
publish_catalog('2026-10-23', 3) -> inserted=0 changed=0 unchanged=3958
catalog_2026-10-23_v3 curated checksum = 88ce7075938afdf03e9114f6d71e808c3341e87f3ac16b575b42c334a51a823b
product_versions = 5312; current_products = 3958
```

Evidencia cruda: [`docs/evidence/idempotency.txt`](evidence/idempotency.txt).

> El lifecycle de un lote antiguo puede cambiar si después llega un lote nuevo: es un agregado de todo el historial y se recalcula para tardíos. La prueba de idempotencia se hace sobre el lote más reciente o sobre los hechos, no comparando lifecycles históricos entre estados temporales distintos.

## 7. Falla y recuperación

Runs reales en Airflow:

| Run | Estado | Qué demuestra |
|---|---|---|
| `manual__2026-10-26T21:35:23+00:00` | success | flujo normal del lote 10-24 |
| `demo_fail_after_raw` | failed | el lote quedó en S3 raw y `extract_events` falló |
| `demo_fail_after_clean` | failed | el lote quedó limpio y `clean_events` falló antes de publicar |
| `demo_recovery` | success | reejecución normal; conteos y checksums estables |

Los runs de falla se generaron con la política de 0 reintentos para exponer el estado `failed` de inmediato; la configuración entregada usa 2 reintentos con backoff. Al repetir la demo, el fallo tarda más y termina igualmente en `failed` al agotar los reintentos.

Mensajes capturados:

```text
RuntimeError: FALLA SIMULADA: el lote quedo en raw y no se proceso.
Reejecuta la tarea para demostrar recuperacion.

RuntimeError: FALLA SIMULADA: el lote quedo limpio en S3 y no se publico.
Reejecuta publish_events; es idempotente.
```

Evidencia cruda: [`docs/evidence/failure_recovery.txt`](evidence/failure_recovery.txt).

## 8. Resultados de las siete preguntas

Resultados completos: [`docs/evidence/sql_results.txt`](evidence/sql_results.txt).

| # | Hallazgo verificable |
|---|---|
| 1 | Dic-2010 UK: bruto 1,156,460.23 USD; devoluciones 110,773.33; neto **1,045,686.90**. (314 filas en el resultado completo) |
| 2 | General neto **5,701,674.29** USD; Home Decor **3,097,228.44**; Kitchen **2,199,697.47**; Services neto 122,801.28 (excluida del top de productos) |
| 3 | `22423` Regency Cakestand: **261,799.48** USD neto; 855 unidades devueltas. `85123A` neto 159,171.64 |
| 4 | Perú: 6,496 órdenes, 5,171 pagadas, pago medio **17.90 h**, mediana 17.57 h, cancelación **7.47%** |
| 5 | Tardíos publicados: 318 + 367 + 391 = **1,076**; 1,068 órdenes afectadas; 7.80% en el lote 10-24 |
| 6 | Rechazo histórico **0.4645%** (2,517/541,910); eventos **0.0878%** (28/31,907); catálogo 0% |
| 7 | SKUs como `15044C` muestran 3 versiones y cambios de categoría (Stationery → Garden → Stationery), con vigencia actual y `previous_category` |

## 9. Pruebas

```text
docker exec mn_airflow python -m pytest tests -m "not integration" -q
.........  [100%]
9 passed, 2 deselected

docker exec -e RUN_INTEGRATION_TESTS=1 mn_airflow python -m pytest tests -q
...........  [100%]
11 passed
```

Cobertura de las pruebas:

- config/batch IDs/fechas;
- validación histórica, cuarentena única y deduplicación;
- eventos tardíos, duplicado exacto y conflicto;
- catálogo duplicado, sentinel de fecha y guarda SCD2 para snapshots fuera de orden;
- integridad de `event_id` y productos corrientes;
- ejecución de las 7 queries.

Evidencia: [`docs/evidence/pytest_unit.txt`](evidence/pytest_unit.txt), [`docs/evidence/pytest_integration.txt`](evidence/pytest_integration.txt).

## 10. Terraform

```text
fmt_exit=0
Success! The configuration is valid.
validate_exit=0
```

Provider `hashicorp/aws v5.100.0`; **sin apply**. El módulo define:

- bucket S3 con versionado, SSE-S3, bloqueo público, TLS obligatorio y lifecycle;
- rol de **ejecución** MWAA y rol de **despliegue** separado (`deployment_role_arn`), con permisos acotados al proyecto;
- log group, metric filter de `ERROR`, topic SNS y alarmas `PipelineErrors`, `DAGProcessingErrors` y `SchedulerHeartbeat`;
- MWAA opcional (`enable_mwaa=false`).

En local, `src/storage.py` envía `ServerSideEncryption=AES256` en cada `put_object`; prueba: [`docs/evidence/s3_sse.txt`](evidence/s3_sse.txt).

Evidencia: [`docs/evidence/terraform_validate.txt`](evidence/terraform_validate.txt). Costos y limpieza: [`README.md`](../README.md#costos-aproximados-y-limpieza) y [`terraform/README.md`](../terraform/README.md).

## 11. DAGs importados

```text
dag_id              fileloc                              is_paused
01_historical_load  /opt/airflow/dags/historical_dag.py   False
02_daily_events     /opt/airflow/dags/daily_dag.py        False
```

No hay errores de importación. Los DAGs usan 2 reintentos por defecto con backoff exponencial, timeout por tarea, `max_active_runs=1` y `max_active_tasks=4`. Los runs conservados en la metadata de Airflow corresponden al DAG diario; el DAG histórico se ejecutó mediante la misma lógica de `src.pipelines` durante la construcción y puede relanzarse con:

```powershell
docker exec mn_airflow airflow dags trigger 01_historical_load
```

**Advertencia de rerun:** relanzar el DAG histórico después de las versiones v2/v3 no reabre el catálogo —la guarda SCD2 ignora snapshots anteriores al corriente—, pero sí recalcula y reescribe el snapshot histórico en S3. Por la vigencia temporal, los hechos de 2010–2011 siguen apuntando a v1 (se verificó: 534,130 filas con `catalog_version=1`), pero no se reejecutó al final para no alterar la evidencia ni los manifiestos. Para una demo de idempotencia, usar el lote diario más reciente (`2026-10-24`) o el catálogo v3. El estado final entregado ya contiene las tres versiones y los hechos históricos.

## 12. Reproducir esta evidencia

```powershell
.\scripts\run_queries.ps1
.\scripts\verify.ps1
docker exec mn_airflow python -m pytest tests -m "not integration" -q
docker exec -e RUN_INTEGRATION_TESTS=1 mn_airflow python -m pytest tests -q
cd terraform; terraform init -backend=false; terraform fmt -check; terraform validate
```

Los resultados pueden variar solo si se regeneran los datos con otra semilla o se cambia la tasa DEMO.
