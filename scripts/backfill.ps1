# Backfill / replay de un intervalo de fechas SIN editar codigo.
#
#   .\scripts\backfill.ps1 -Start 2026-10-20 -End 2026-10-26
#
# Dispara el DAG 02_daily_events una vez por fecha (en orden) pasando
# ingest_date por --conf. Es idempotente: reprocesar un lote ya publicado
# no duplica hechos (UNIQUE(source, event_id) + upsert del lifecycle) y
# recalcula las ordenes afectadas por eventos tardios.
param(
    [Parameter(Mandatory = $true)][string]$Start,
    [Parameter(Mandatory = $true)][string]$End
)

$ErrorActionPreference = "Stop"
& (Join-Path $PSScriptRoot "run_demo.ps1") -SkipBuild -SkipHistorical -SkipQueries -ForceDaily -StartDate $Start -EndDate $End
