# Demo completa de Mercado Nube con UNA sola instruccion:
#   .\scripts\run_demo.ps1
#
# 1. Si no existe .env, lo crea desde .env.example con secretos aleatorios
#    (Fernet key, claves de PostgreSQL y de Airflow). .env nunca se versiona.
# 2. Levanta el stack Docker y espera los healthchecks.
# 3. Ejecuta la carga historica y los 7 lotes diarios (2026-10-20 a 2026-10-26).
# 4. Ejecuta las consultas de negocio y guarda la evidencia.
#
# Es idempotente: si un lote ya esta publicado, se omite (usa -ForceDaily
# o -IncludeHistorical para reprocesar).
param(
    [switch]$SkipBuild,
    [switch]$SkipHistorical,
    [switch]$IncludeHistorical,
    [switch]$ForceDaily,
    [switch]$SkipQueries,
    [string]$StartDate = "2026-10-20",
    [string]$EndDate = "2026-10-26"
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

function New-RandomSecret {
    param([int]$Bytes = 24)
    $buffer = New-Object byte[] $Bytes
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($buffer)
    return ([System.BitConverter]::ToString($buffer)).Replace("-", "").ToLower()
}

function New-FernetKey {
    $buffer = New-Object byte[] 32
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($buffer)
    return [Convert]::ToBase64String($buffer).Replace("+", "-").Replace("/", "_")
}

function Set-EnvValue {
    param([string[]]$Lines, [string]$Key, [string]$Value)
    $found = $false
    for ($i = 0; $i -lt $Lines.Count; $i++) {
        if ($Lines[$i] -match "^$Key=") {
            $Lines[$i] = "$Key=$Value"
            $found = $true
        }
    }
    if (-not $found) { $Lines += "$Key=$Value" }
    return ,$Lines
}

function Get-EnvValue {
    param([string]$Key, [string]$Default = "")
    $line = Get-Content ".env" | Where-Object { $_ -match "^$Key=" } | Select-Object -First 1
    if (-not $line) { return $Default }
    $value = $line.Substring($Key.Length + 1).Trim()
    if ($value) { return $value } else { return $Default }
}

if (-not (Test-Path ".env")) {
    Write-Host "No existe .env: se crea desde .env.example con secretos aleatorios..." -ForegroundColor Cyan
    $lines = Get-Content ".env.example"
    $lines = Set-EnvValue $lines "POSTGRES_ADMIN_PASSWORD" (New-RandomSecret)
    $lines = Set-EnvValue $lines "AIRFLOW_DB_PASSWORD" (New-RandomSecret)
    $lines = Set-EnvValue $lines "WAREHOUSE_PASSWORD" (New-RandomSecret)
    $lines = Set-EnvValue $lines "AIRFLOW_ADMIN_PASSWORD" (New-RandomSecret 12)
    $lines = Set-EnvValue $lines "AIRFLOW_FERNET_KEY" (New-FernetKey)
    $lines = Set-EnvValue $lines "AIRFLOW_SECRET_KEY" (New-RandomSecret 32)
    $lines | Set-Content -Encoding ASCII ".env"
    Write-Host ".env creado (excluido de Git)." -ForegroundColor Green
}

$adminUser = Get-EnvValue "POSTGRES_ADMIN_USER" "mn_admin"
$airflowUser = Get-EnvValue "AIRFLOW_ADMIN_USER" "admin"
$airflowPort = Get-EnvValue "AIRFLOW_HOST_PORT" "8088"

function Wait-Container {
    param([string]$Name, [int]$TimeoutSeconds = 300)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $status = (docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $Name 2>$null)
        if ($status) { $status = $status.Trim() }
        if ($status -eq "healthy" -or ($status -eq "running" -and $Name -eq "mn_moto")) { return }
        Write-Host "  $Name => $status (esperando...)"
        Start-Sleep -Seconds 5
    }
    throw "Timeout esperando el contenedor $Name"
}

function Wait-DagRun {
    param([string]$DagId, [string]$RunId, [int]$TimeoutSeconds = 1200)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $state = (docker exec mn_postgres psql -U $adminUser -d airflow -tAc "SELECT state FROM dag_run WHERE dag_id = '$DagId' AND run_id = '$RunId';")
        if ($state) { $state = $state.Trim() }
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

$current = [datetime]::ParseExact($StartDate, "yyyy-MM-dd", $null)
$last = [datetime]::ParseExact($EndDate, "yyyy-MM-dd", $null)
while ($current -le $last) {
    $date = $current.ToString("yyyy-MM-dd")
    $current = $current.AddDays(1)
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

Write-Host "Demo completada. Airflow: http://localhost:$airflowPort" -ForegroundColor Green
Write-Host "Usuario: $airflowUser  |  Clave: valor de AIRFLOW_ADMIN_PASSWORD en tu archivo .env (no se imprime por seguridad)" -ForegroundColor Green
