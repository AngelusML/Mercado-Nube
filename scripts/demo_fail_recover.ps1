param(
    [string]$IngestDate = "2026-10-24"
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

function Run-Dag {
    param([string]$RunId, [string]$Conf)
    docker exec mn_airflow airflow dags trigger 02_daily_events --run-id $RunId --conf $Conf
    Start-Sleep -Seconds 12
    $state = (docker exec mn_postgres psql -U mn_admin -d airflow -tAc "SELECT state FROM dag_run WHERE dag_id = '02_daily_events' AND run_id = '$RunId';").Trim()
    Write-Host "$RunId => $state"
    return $state
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$confRaw = "{`"ingest_date`":`"$IngestDate`",`"fail_point`":`"after_raw`"}"
$rawRun = "demo_fail_raw_$stamp"
Run-Dag -RunId $rawRun -Conf $confRaw | Out-Null

$confClean = "{`"ingest_date`":`"$IngestDate`",`"fail_point`":`"after_clean`"}"
$cleanRun = "demo_fail_clean_$stamp"
Run-Dag -RunId $cleanRun -Conf $confClean | Out-Null

Write-Host "Recuperando el mismo lote con una reejecucion normal..." -ForegroundColor Cyan
$recoveryRun = "demo_recovery_$stamp"
docker exec mn_airflow airflow dags trigger 02_daily_events --run-id $recoveryRun --conf "{`"ingest_date`":`"$IngestDate`"}"
Start-Sleep -Seconds 35
docker exec mn_postgres psql -U mn_admin -d airflow -P pager=off -c "SELECT dag_id, run_id, state FROM dag_run WHERE dag_id='02_daily_events' AND run_id IN ('$rawRun','$cleanRun','$recoveryRun') ORDER BY run_id;"
docker exec mn_postgres psql -U mn_app -d mercado_nube -P pager=off -c "SELECT layer, rows_read, rows_rejected, rows_duplicated, rows_late, rows_published FROM ops.pipeline_run_metrics WHERE batch_id='events_$IngestDate' ORDER BY layer;"
