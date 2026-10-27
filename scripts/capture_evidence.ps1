# Regenera la evidencia de docs/evidence/ despues de una corrida completa.
#
#   .\scripts\run_demo.ps1          # primero: carga historica + 7 lotes
#   .\scripts\capture_evidence.ps1  # luego: conteos, pruebas, idempotencia, SQL
#
# No imprime ni guarda secretos.
param(
    [string]$IdempotencyDate = "2026-10-26"
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root
$out = "docs/evidence"
New-Item -ItemType Directory -Force -Path $out | Out-Null

function Invoke-Sql {
    param([string]$Sql)
    return ($Sql | docker exec -i mn_postgres psql -U mn_app -d mercado_nube -P pager=off 2>&1)
}

$countsSql = @"
SELECT 'historical_rows_read' AS metric, SUM(rows_read)::text AS value FROM ops.pipeline_run_metrics WHERE source = 'orders_historical' AND layer = 'published'
UNION ALL SELECT 'fact_order_line', COUNT(*)::text FROM analytics.fact_order_line
UNION ALL SELECT 'event_rows_read', SUM(rows_read)::text FROM ops.pipeline_run_metrics WHERE source = 'order_events' AND layer = 'published'
UNION ALL SELECT 'fact_order_event', COUNT(*)::text FROM analytics.fact_order_event
UNION ALL SELECT 'fact_order_lifecycle', COUNT(*)::text FROM analytics.fact_order_lifecycle
UNION ALL SELECT 'product_versions_scd2', COUNT(*)::text FROM analytics.dim_product WHERE sku <> '__UNKNOWN__'
UNION ALL SELECT 'current_products', COUNT(*)::text FROM analytics.dim_product WHERE is_current AND sku <> '__UNKNOWN__'
UNION ALL SELECT 'late_events', COUNT(*)::text FROM analytics.fact_order_event WHERE is_late
UNION ALL SELECT 'quarantine_rows', COUNT(*)::text FROM ops.quarantine_records
UNION ALL SELECT 'quality_checks_pass', COUNT(*) FILTER (WHERE status = 'pass')::text || '/' || COUNT(*)::text FROM ops.data_quality_results;
"@

$batchSql = @"
SELECT batch_id, source, layer, rows_read, rows_rejected, rows_duplicated,
       rows_late, rows_published, rows_conflicting
FROM ops.pipeline_run_metrics
ORDER BY batch_id, layer;
"@

Write-Host "1) Conteos finales y metricas por lote" -ForegroundColor Cyan
"=== CONTEOS FINALES ===" | Set-Content "$out/final_counts.txt"
Invoke-Sql $countsSql | Add-Content "$out/final_counts.txt"
"" | Add-Content "$out/final_counts.txt"
"=== METRICAS POR LOTE ===" | Add-Content "$out/final_counts.txt"
Invoke-Sql $batchSql | Add-Content "$out/final_counts.txt"

Write-Host "2) Pruebas unitarias" -ForegroundColor Cyan
docker exec mn_airflow python -m pytest tests -m "not integration" -q 2>&1 | Set-Content "$out/pytest_unit.txt"

Write-Host "3) Pruebas de integracion + 7 queries" -ForegroundColor Cyan
docker exec -e RUN_INTEGRATION_TESTS=1 mn_airflow python -m pytest tests -q 2>&1 | Set-Content "$out/pytest_integration.txt"

Write-Host "4) Idempotencia: reejecutar el lote $IdempotencyDate" -ForegroundColor Cyan
$idemSql = @"
SELECT 'fact_order_line' AS tabla, COUNT(*) AS filas FROM analytics.fact_order_line
UNION ALL SELECT 'fact_order_event', COUNT(*) FROM analytics.fact_order_event
UNION ALL SELECT 'fact_order_lifecycle', COUNT(*) FROM analytics.fact_order_lifecycle
UNION ALL SELECT 'quarantine_records', COUNT(*) FROM ops.quarantine_records;
"@
# La cuarentena guarda processed_at (hora real de proceso), por eso su checksum
# cambia en cada corrida; se compara su conteo, no su checksum.
$manifestSql = "SELECT layer, row_count, CASE WHEN layer = 'quarantine' THEN '(conteo)' ELSE checksum END AS checksum FROM ops.batch_manifest WHERE batch_id = 'events_$IdempotencyDate' ORDER BY layer;"
"=== ANTES de reejecutar events_$IdempotencyDate ===" | Set-Content "$out/idempotency.txt"
Invoke-Sql $idemSql | Add-Content "$out/idempotency.txt"
Invoke-Sql $manifestSql | Add-Content "$out/idempotency.txt"
& (Join-Path $PSScriptRoot "backfill.ps1") -Start $IdempotencyDate -End $IdempotencyDate
"=== DESPUES de reejecutar events_$IdempotencyDate (deben ser iguales) ===" | Add-Content "$out/idempotency.txt"
Invoke-Sql $idemSql | Add-Content "$out/idempotency.txt"
Invoke-Sql $manifestSql | Add-Content "$out/idempotency.txt"

Write-Host "5) Resultados de las 7 preguntas de negocio" -ForegroundColor Cyan
& (Join-Path $PSScriptRoot "run_queries.ps1")

Write-Host "6) Resumen de verificacion" -ForegroundColor Cyan
& (Join-Path $PSScriptRoot "verify.ps1") 2>&1 | Out-String | Set-Content "$out/verify.txt"

Write-Host "Evidencia regenerada en $out" -ForegroundColor Green
Get-Content "$out/final_counts.txt" -TotalCount 16
