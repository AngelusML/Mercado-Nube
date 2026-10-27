param(
    [switch]$SkipBuild,
    [switch]$SkipHistorical,
    [switch]$IncludeHistorical,
    [switch]$ForceDaily,
    [switch]$SkipQueries
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

function Wait-Container {
    param([string]$Name, [int]$TimeoutSeconds = 300)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $status = (docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $Name 2>$null).Trim()
        if ($status -eq "healthy" -or $status -eq "running") { return }
        Write-Host "  $Name => $status (esperando...)"
        Start-Sleep -Seconds 5
    }
    throw "Timeout esperando el contenedor $Name"
}

function Wait-DagRun {
    param([string]$DagId, [string]$RunId, [int]$TimeoutSeconds = 900)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $state = (docker exec mn_postgres psql -U mn_admin -d airflow -tAc "SELECT state FROM dag_run WHERE dag_id = '$DagId' AND run_id = '$RunId';").Trim()
        Write-Host "  $DagId / $RunId => $state"
        if ($state -eq "success") { return }
        if ($state -eq "failed") { throw "DAG $DagId fallo (run_id=$RunId). Revisa la UI o los logs." }
        Start-Sleep -Seconds 5
    }
    throw "Timeout esperando $DagId / $RunId"
}

if (-not $SkipBuild) {
    Write-Host "Levantando stack local (sin tocar otros proyectos Docker)..." -ForegroundColor Cyan
    docker compose up -d --build
}

Write-Host "Esperando healthchecks..." -ForegroundColor Cyan
Wait-Container -Name "mn_postgres"
Wait-Container -Name "mn_moto"
Wait-Container -Name "mn_api"
Wait-Container -Name "mn_airflow"
docker compose ps

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"

if (-not $SkipHistorical) {
    $historicalRows = 0
    try {
        $historicalRows = [int](docker exec mn_postgres psql -U mn_app -d mercado_nube -tAc "SELECT count(*) FROM analytics.fact_order_line;" 2>$null).Trim()
    } catch {
        $historicalRows = 0
    }
    if ($IncludeHistorical -or $historicalRows -eq 0) {
        $historicalRun = "demo_historical_$stamp"
        Write-Host "Disparando carga historica ($historicalRun)..."
        docker exec mn_airflow airflow dags trigger 01_historical_load --run-id $historicalRun --conf '{"ingest_date":"2026-10-21"}'
        Wait-DagRun -DagId "01_historical_load" -RunId $historicalRun
    } else {
        Write-Host "Historico ya cargado ($historicalRows hechos); se omite. Usa -IncludeHistorical para forzar." -ForegroundColor Yellow
    }
}

foreach ($date in @("2026-10-22", "2026-10-23", "2026-10-24")) {
    $batchId = "events_$date"
    $alreadyPublished = 0
    try {
        $alreadyPublished = [int](docker exec mn_postgres psql -U mn_app -d mercado_nube -tAc "SELECT count(*) FROM ops.pipeline_run_metrics WHERE batch_id = '$batchId' AND layer = 'published';" 2>$null).Trim()
    } catch {
        $alreadyPublished = 0
    }
    if ($alreadyPublished -gt 0 -and -not $ForceDaily) {
        Write-Host "Lote $date ya publicado; se omite. Usa -ForceDaily para reprocesar." -ForegroundColor Yellow
        continue
    }
    $runId = "demo_daily_${date}_$stamp"
    Write-Host "Disparando lote diario $date ($runId)..." -ForegroundColor Cyan
    docker exec mn_airflow airflow dags trigger 02_daily_events --run-id $runId --conf "{`"ingest_date`":`"$date`"}"
    Wait-DagRun -DagId "02_daily_events" -RunId $runId
}

if (-not $SkipQueries) {
    Write-Host "Ejecutando las 7 queries y guardando evidencia..." -ForegroundColor Cyan
    & (Join-Path $PSScriptRoot "run_queries.ps1")
}

Write-Host "Demo completada. Airflow: http://localhost:8088 (admin/admin123)" -ForegroundColor Green
