# Decisiones técnicas

Este documento reúne las decisiones del MVP y su porqué, en el orden que pide el enunciado: arquitectura, alternativas descartadas, granularidad, deduplicación, datos tardíos, moneda, idempotencia y seguridad. Cada decisión indica dónde está implementada.

## 1. Arquitectura

**Decisión:** pipeline batch en Airflow con tres capas en S3 (`raw → cleaned → curated`, Parquet) y un warehouse dimensional. En local corre con Docker (Airflow, PostgreSQL, Moto S3, API de catálogo); en AWS el mismo código corre en MWAA con S3 y Redshift Serverless (Terraform).

| Tema | Elegido | Descartado | Por qué |
|---|---|---|---|
| Procesamiento | pandas + PyArrow dentro de las tareas de Airflow | Glue/Spark, EMR | ~540k filas históricas + ~54k eventos se procesan en segundos; Spark agrega costo y tiempo de arranque sin beneficio a este volumen. Parquet deja abierta la migración a Glue. |
| Orquestación | Airflow 2.10 (local) / MWAA | Step Functions, cron | El enunciado pide Airflow; MWAA mantiene los mismos DAGs. |
| Warehouse | PostgreSQL 16 local; Redshift Serverless en AWS | Redshift provisionado | Serverless cobra solo por uso y no requiere dimensionar nodos para un MVP. |
| S3 local | Moto | MinIO, disco local | Mismo cliente `boto3` y mismas llaves; cambiar a AWS es solo cambiar el endpoint. |
| Catálogo | API FastAPI con paginación, 429 y 500 simulados | Archivo estático | Permite demostrar reintentos con backoff y versión de extracción. |
| IaC | Terraform con MWAA y Redshift detrás de flags | CDK | Terraform es declarativo y `validate` funciona sin cuenta; los flags evitan costos accidentales. |

## 2. Granularidad del modelo

| Tabla | Grano | Clave |
|---|---|---|
| `fact_order_line` | una línea de factura del histórico | `line_key` (hash del contenido) |
| `fact_order_event` | un evento de cambio de estado | `(source, event_id)` |
| `fact_order_lifecycle` | un pedido, con su estado actual | `(source, order_id)` |
| `dim_product` | una versión de atributos de un SKU (SCD2) | `product_key`; natural `(sku, valid_from)` |
| `dim_customer` | un cliente del histórico + miembro desconocido `-1` | `customer_key` |
| `dim_date`, `dim_country`, `dim_currency` | un día / país / moneda | claves de calendario o sustitutas |

Columnas de auditoría: `source`, `batch_id`, `record_hash` y `created_at`/`ingested_at` en los hechos; `ops.batch_manifest` guarda el checksum SHA-256 de cada objeto por capa.

## 3. Deduplicación

- **Histórico:** clave = hash del contenido normalizado de la línea. Las filas idénticas se cuentan como duplicadas (5,263) y se guardan en `cleaned/.../duplicates.parquet`; no se borran en silencio.
- **Eventos:** clave = `event_id`.
  - Mismo `event_id` y mismo contenido (`record_hash`) → duplicado por reintento; se descarta y se cuenta.
  - Mismo `event_id` con contenido distinto → **conflicto**: va a cuarentena con `duplicate_id_conflict`.
  - Dentro de un lote gana el evento con `event_time` más antiguo y, si empatan, el de menor hash (determinista).
  - Entre lotes gana la versión ya publicada.
  - Límite conocido: esta regla depende del orden de carga. Un backfill en otro orden podría elegir otra versión, aunque ambas quedan trazadas en cuarentena.

## 4. Estado actual del pedido

Regla explícita en `publish_events`:

1. El último evento por `event_time_utc` define el estado.
2. Empate de hora: gana el tipo de mayor rango (`refunded` > `cancelled` > `paid` > `created`).
3. Empate total: gana el `event_id` mayor.

`time_to_pay_hours` = primer `paid` − primer `created`, solo si existen ambos y el pago es posterior.

## 5. Eventos tardíos

- Un evento es tardío si `ingested_at − event_time_utc > 24 h`. `ingested_at` es el cierre del día del lote (determinista, para que una reejecución dé el mismo resultado).
- Ser tardío no es motivo de rechazo: el evento se publica con `is_late = true`.
- Al publicar un lote se recalcula el ciclo de vida de **todos** los pedidos afectados con su historial completo, así un `paid` que llega dos días después corrige `time_to_pay_hours` y el estado.
- Reproceso de un intervalo sin editar código: `.\scripts\backfill.ps1 -Start <fecha> -End <fecha>`, o `airflow dags backfill` con `DAILY_SCHEDULE=@daily` (el DAG usa la fecha lógica `ds`).

## 6. Moneda

- Moneda de reporte: USD.
- **Histórico:** GBP. **Eventos:** COP, MXN y PEN según el país.
- Coherencia país-moneda: un evento de CO/MX/PE con otra moneda va a cuarentena (`country_currency_mismatch`); no se corrige.
- **Conversión:** se usa la tasa de `ops.exchange_rates` con la fecha efectiva más reciente que sea menor o igual a la fecha de la transacción.
- **Las tasas son DEMO** (`source_note = 'DEMO - no es cotizacion real'`), no cotizaciones reales.

## 7. Clientes sin identificador

Las líneas sin `customer_id` se cargan con `customer_key = -1` (miembro desconocido explícito). Cuentan en ventas y unidades, pero no se inventa una identidad ni se agrupan como un cliente real.

## 8. Catálogo y cambios de atributos (SCD2)

- Cada versión del catálogo se compara por hash de atributos.
- Si un SKU cambió, se cierra la versión vigente (`valid_to`, `is_current = false`) y se inserta la nueva. Nunca se sobrescribe.
- La versión que rige cada fecha es: v1 antes del 22-oct, v2 el 22-oct y v3 desde el 23-oct. Por eso un backfill no publica una versión futura antes que una pasada.
- Supuesto: la v1 rige desde 2010-01-01 para poder relacionar el histórico 2010-2011.
- **Límite:** un SKU que desaparece del catálogo no se cierra automáticamente; la baja se representa con `is_active = false` cuando la API la informa.

## 9. Idempotencia y recuperación

- `batch_id` determinista por fuente y fecha; las llaves de S3 son fijas y se sobrescriben.
- **Histórico:** `DELETE` del lote + `INSERT` en una sola transacción.
- **Eventos:** `UNIQUE(source, event_id)` + `ON CONFLICT DO NOTHING`; ciclo de vida con upsert.
- **Validación previa a publicar** (`validate_*`): revisa tasas de rechazo y duplicados y que los conteos cuadren sobre `cleaned`. Si falla, el lote no llega al warehouse.
- **Quality gate final:** reconcilia lo publicado y falla la corrida si algo no cuadra.
- **Falla después de escribir en S3 y antes del warehouse:** `raw/` y `cleaned/` quedan en S3 con su manifiesto. Al reejecutar, se sobrescriben las mismas llaves y la publicación inserta solo lo que falta. Esto está probado en `docs/evidence/failure_recovery.txt`.
- **Carga parcial en el warehouse:** cada publicación es una transacción. Si falla a mitad, PostgreSQL hace rollback y el reintento parte de cero.

## 10. Seguridad

- **Sin secretos en Git:** `.env.example` no trae claves. `run_demo.ps1` genera `.env` con valores aleatorios, y `.env` está en `.gitignore`.
- **En AWS:**
  - MWAA usa su rol de ejecución (credenciales temporales de STS, sin access keys).
  - La clave de Redshift la gestiona Secrets Manager.
  - Los roles de despliegue y ejecución están separados y tienen mínimo privilegio. Los únicos `*` son acciones que AWS no permite acotar por recurso.
- **Bucket:** sin acceso público, solo TLS, cifrado SSE-S3, versionado y lifecycle por capa.
- Diagrama de credenciales y accesos: `docs/arquitectura.md`, sección "Credenciales y accesos".

## 11. Límites conocidos

- No se desplegó en una cuenta AWS real (permitido por el anexo del simulacro). Terraform pasa `validate` y un `apply` contra Moto, y el SQL de Redshift se validó con un analizador. Falta ejecutarlo en un laboratorio.
- El histórico (2010-2011) y los eventos (2026) son series separadas; no se mezclan en una misma tendencia.
- Las líneas de pedido de los eventos (`payload.lines`) se validan pero no se modelan como hecho propio.
- Moto guarda el lake en memoria; al reiniciar el contenedor se regenera re-ejecutando los DAGs desde `data/`.
