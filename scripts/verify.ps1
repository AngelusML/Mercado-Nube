param(
    [switch]$SkipIntegration
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

Write-Host "1) Estado de contenedores" -ForegroundColor Cyan
docker compose ps

Write-Host "2) Pruebas unitarias" -ForegroundColor Cyan
docker exec mn_airflow python -m pytest tests -m "not integration" -q

if (-not $SkipIntegration) {
    Write-Host "3) Pruebas de integracion + las 7 queries" -ForegroundColor Cyan
    docker exec -e RUN_INTEGRATION_TESTS=1 mn_airflow python -m pytest tests -q
}

Write-Host "4) Conteos y reconciliacion" -ForegroundColor Cyan
docker exec mn_postgres psql -U mn_app -d mercado_nube -P pager=off -c @"
SELECT 'fact_order_line' AS metric, count(*)::text AS value FROM analytics.fact_order_line
UNION ALL SELECT 'fact_order_event', count(*)::text FROM analytics.fact_order_event
UNION ALL SELECT 'fact_order_lifecycle', count(*)::text FROM analytics.fact_order_lifecycle
UNION ALL SELECT 'product_versions_scd2', count(*)::text FROM analytics.dim_product WHERE sku <> '__UNKNOWN__'
UNION ALL SELECT 'current_products', count(*)::text FROM analytics.dim_product WHERE is_current AND sku <> '__UNKNOWN__'
UNION ALL SELECT 'late_events', count(*)::text FROM analytics.fact_order_event WHERE is_late
UNION ALL SELECT 'quarantine_rows', count(*)::text FROM ops.quarantine_records;
"@

Write-Host "5) Quality gates" -ForegroundColor Cyan
docker exec mn_postgres psql -U mn_app -d mercado_nube -P pager=off -c @"
SELECT batch_id, count(*) AS checks,
       count(*) FILTER (WHERE status = 'pass') AS passed,
       count(*) FILTER (WHERE status = 'fail') AS failed
FROM ops.data_quality_results
GROUP BY batch_id
ORDER BY batch_id;
"@
