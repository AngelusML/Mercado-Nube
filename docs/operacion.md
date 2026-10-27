# Operación y runbook

## 1. Arranque

```powershell
Copy-Item .env.example .env   # si no existe
docker compose up -d --build
docker compose ps
```

El contenedor `mn_airflow` ejecuta al iniciar:

1. espera a PostgreSQL;
2. `airflow db migrate`;
3. `python -m src.bootstrap` (DDL, dimensiones semilla, tasas, bucket Moto);
4. crea el usuario admin;
5. levanta webserver y scheduler.

Verificación:

```powershell
docker exec mn_airflow airflow dags list
curl http://localhost:8001/health
```

DAGs esperados: `01_historical_load`, `02_daily_events`.

### Reintentos, timeouts y concurrencia

- `PIPELINE_RETRIES=2`, `PIPELINE_RETRY_DELAY_SECONDS=30`, backoff exponencial y `max_retry_delay=10 min`; se configuran por entorno.
- `execution_timeout`: 1 h por tarea diaria, 2 h por tarea histórica.
- `max_active_runs=1`, `max_active_tasks=4` y `dagrun_timeout` (2 h diario, 3 h histórico).
- `DAILY_SCHEDULE` vacío deja el DAG diario en disparo manual; en producción se define un cron, por ejemplo `0 2 * * *`.
- La evidencia de fallo se capturó con `PIPELINE_RETRIES=0` para ver el estado `failed` sin esperar el backoff; la configuración entregada usa 2 reintentos. Si repites la demo de fallo, el run tardará más y terminará en `failed` al agotarlos.

## 2. Carga completa

```powershell
# Histórico + catálogo v1
docker exec mn_airflow airflow dags trigger 01_historical_load

# Día 1
docker exec mn_airflow airflow dags trigger 02_daily_events --conf '{"ingest_date":"2026-10-22"}'

# Día 2
docker exec mn_airflow airflow dags trigger 02_daily_events --conf '{"ingest_date":"2026-10-23"}'

# Día 3 / lote con tardíos
docker exec mn_airflow airflow dags trigger 02_daily_events --conf '{"ingest_date":"2026-10-24"}'
```

> Si la base ya tiene el catálogo v2/v3, el DAG histórico no reabre la historia SCD2, pero reescribe el snapshot y las métricas del lote histórico. Para verificar idempotencia en el estado entregado, usa `events_2026-10-24` o `catalog_2026-10-24_v3`.

O:

```powershell
.\scripts\run_demo.ps1
```

`run_demo.ps1` usa run IDs únicos y espera cada DAG consultando `airflow.dag_run` en PostgreSQL. No ejecuta `docker compose down` ni toca otros proyectos. Detecta si el histórico y los lotes diarios ya están publicados y los omite; usa `-IncludeHistorical`/`-ForceDaily` para forzarlos. `-ForceDaily` sobre un stack ya cargado puede reescribir manifests de catálogos antiguos; para una demostración de idempotencia usa el lote más reciente.

## 3. Verificación

```powershell
.\scripts\verify.ps1
```

Hace:

- `docker compose ps`;
- tests unitarios;
- tests de integración + las 7 queries;
- conteos y reconciliación;
- resumen de quality gates.

También puede ejecutarse por separado:

```powershell
docker exec mn_airflow python -m pytest tests -m "not integration" -q
docker exec -e RUN_INTEGRATION_TESTS=1 mn_airflow python -m pytest tests -q
```

## 4. Evidencia de queries

```powershell
.\scripts\run_queries.ps1
```

Genera `docs/evidence/sql_results.txt` con las siete consultas y sus resultados. El archivo es reproducible; los montos usan tasas DEMO.

## 5. Demostrar falla y recuperación

### Falla después de raw

```powershell
docker exec mn_airflow airflow dags trigger 02_daily_events `
  --run-id demo_fail_after_raw `
  --conf '{"ingest_date":"2026-10-24","fail_point":"after_raw"}'
```

Resultado: `extract_events` falla con:

```text
RuntimeError: FALLA SIMULADA: el lote quedo en raw y no se proceso.
Reejecuta la tarea para demostrar recuperacion.
```

`clean_events`, `publish_events` y `quality_gate` quedan `upstream_failed`. El catálogo puede completar porque es una rama independiente.

### Falla después de clean

```powershell
docker exec mn_airflow airflow dags trigger 02_daily_events `
  --run-id demo_fail_after_clean `
  --conf '{"ingest_date":"2026-10-24","fail_point":"after_clean"}'
```

Resultado: el lote queda limpio en S3, pero `clean_events` lanza:

```text
RuntimeError: FALLA SIMULADA: el lote quedo limpio en S3 y no se publico.
Reejecuta publish_events; es idempotente.
```

### Recuperación

```powershell
docker exec mn_airflow airflow dags trigger 02_daily_events `
  --run-id demo_recovery `
  --conf '{"ingest_date":"2026-10-24"}'
```

El rerun vuelve a extraer, limpia (borra y reescribe la cuarentena del batch), publica y ejecuta el gate. Los conteos y checksums quedan iguales a la ejecución original.

Evidencia de los runs usados en la construcción:

| Run | Estado | Observación |
|---|---|---|
| `demo_fail_after_raw` | failed | `extract_events` falló intencionalmente |
| `demo_fail_after_clean` | failed | `clean_events` falló intencionalmente |
| `demo_recovery` | success | todas las tareas verdes; métricas sin cambios |

Logs:

```powershell
docker exec mn_airflow sh -lc "grep -R 'FALLA SIMULADA' -n /opt/airflow/logs/dag_id=02_daily_events"
```

## 6. Idempotencia: cómo comprobarla

### Eventos

```powershell
# Antes
docker exec mn_postgres psql -U mn_app -d mercado_nube -P pager=off -c `
"SELECT layer,row_count,checksum FROM ops.batch_manifest WHERE batch_id='events_2026-10-24' ORDER BY layer;"

# Rerun
docker exec mn_airflow python -c "from src import pipelines; import json; print(json.dumps(pipelines.publish_events('2026-10-24'), default=str))"

# Después: mismos checksums y conteos
```

Resultado observado:

```text
fact_order_event  be58f76bb042c9045b618315863709aae8fa07d67660a8a2c86c1dfee6ce9190
lifecycle         9d3bddf26f41c999f4168605900a1af7fa59bae897334cd3e160daa293e61218
published          5013
total events       31589
```

### Catálogo

```powershell
docker exec mn_airflow python -c "from src import pipelines; import json; print(json.dumps(pipelines.publish_catalog('2026-10-23', 3), default=str))"
```

Resultado esperado: `inserted=0`, `changed=0`, `unchanged=3958`; manifiesto `88ce7075...`; total de versiones 5,312.

### Histórico

El cargador hace `DELETE` por `batch_id` e inserta en una transacción. Un rerun no duplica `line_key`; la reconciliación se mantiene en 534,130.

## 7. Tardíos y reprocesamiento

```sql
SELECT batch_id, count(*) AS events,
       sum(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_events,
       round(100.0 * sum(CASE WHEN is_late THEN 1 ELSE 0 END) / count(*), 2) AS pct
FROM analytics.fact_order_event
GROUP BY batch_id
ORDER BY batch_id;

SELECT order_id, event_type, event_time_utc, ingested_at, late_hours
FROM analytics.fact_order_event
WHERE batch_id = 'events_2026-10-24' AND is_late
ORDER BY late_hours DESC
LIMIT 5;

SELECT order_id, event_count, late_event_count, current_status, time_to_pay_hours
FROM analytics.fact_order_lifecycle
WHERE late_event_count > 0
ORDER BY late_event_count DESC
LIMIT 5;
```

Un lote de eventos tardíos se procesa igual que uno normal: se marca `is_late`, se inserta si es nuevo y se recalcula el lifecycle de las órdenes afectadas con **todo** el historial. La frescura del gate mide `ingested_at`, no la antigüedad del `event_time`.

## 8. Troubleshooting

| Síntoma | Causa probable | Acción |
|---|---|---|
| `mn_api` unhealthy | catálogo JSON faltante o carga lenta | `docker compose logs api`; verificar `data/catalog/catalog_v*.json` |
| API devuelve 500/429 | simulación intencional | esperar; el cliente reintenta con backoff |
| DAG no aparece | parse error | `docker exec mn_airflow airflow dags list-import-errors` |
| `extract_catalog` agota reintentos | API caída | reiniciar `api`; el lote es idempotente |
| Quality gate falla | reconciliación/SCD2/FKs | consultar `ops.data_quality_results` y `ops.alerts` |
| `psql` local no conecta | puerto ocupado | cambiar `POSTGRES_HOST_PORT` en `.env` y recrear |
| Airflow no abre | puerto 8088 ocupado | cambiar `AIRFLOW_HOST_PORT` |
| Moto no responde | contenedor detenido | `docker compose restart moto` |

Comandos útiles:

```powershell
docker compose logs -f airflow
docker compose logs -f api
docker compose logs -f postgres
docker exec mn_postgres psql -U mn_app -d mercado_nube -c "\dt analytics.*"
docker exec mn_postgres psql -U mn_app -d mercado_nube -c "SELECT * FROM ops.alerts ORDER BY created_at DESC;"
```

## 9. Reset controlado (solo este proyecto)

```powershell
# Detiene y elimina SOLO los contenedores/volúmenes de mercado-nube.
docker compose down -v
docker compose up -d --build
```

No usar `docker system prune` ni `docker rm` global: afectaría otros proyectos. El volumen se llama `mercado-nube-postgres-data`; el proyecto Compose es `mercado-nube`.

## 10. Terraform: validar, nunca aplicar en la demo

```powershell
cd terraform
terraform init -backend=false
terraform validate
terraform fmt -check
```

Resultado de la construcción: `Success! The configuration is valid.` con `hashicorp/aws v5.100.0`.

No se ejecutó `plan`/`apply`; no se creó ningún recurso AWS. Para AWS real se necesitan credenciales, presupuesto, VPC/subredes/SGs para MWAA y revisión de seguridad.

## 11. Checklist de cierre

- [ ] `docker compose ps` con servicios healthy.
- [ ] `airflow dags list` muestra los dos DAGs.
- [ ] Histórico: 534,130 hechos.
- [ ] Eventos: 31,589 publicados.
- [ ] Lifecycle: 19,330 órdenes.
- [ ] SCD2: 5,312 versiones, 3,958 corrientes, 0 duplicados/solapamientos.
- [ ] `SELECT * FROM ops.data_quality_results WHERE status='fail'` vacío.
- [ ] Rerun de 2026-10-24 con checksums idénticos.
- [ ] Fallas simuladas y `demo_recovery` exitoso.
- [ ] `pytest` 11 passed.
- [ ] `terraform validate` exitoso.
- [ ] Evidencia SQL generada.
