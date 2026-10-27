param(
    [string]$Output = "docs/evidence/sql_results.txt"
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root
New-Item -ItemType Directory -Force -Path (Split-Path $Output) | Out-Null

$sections = New-Object System.Collections.Generic.List[string]
$sections.Add("# Resultados de las 7 preguntas de negocio")
$sections.Add("")
$sections.Add("Generado por scripts/run_queries.ps1. Montos en USD; tasas DEMO.")
$sections.Add("")

foreach ($file in Get-ChildItem "sql/queries/*.sql" | Sort-Object Name) {
    Write-Host "Ejecutando $($file.Name)..." -ForegroundColor Cyan
    $sections.Add("## $($file.Name)")
    $sections.Add("")
    $sections.Add('```text')
    $sql = Get-Content -Raw $file.FullName
    $result = $sql | docker exec -i mn_postgres psql -U mn_app -d mercado_nube -P pager=off 2>&1
    $sections.Add(($result -join "`n"))
    $sections.Add('```')
    $sections.Add("")
}

$sections | Set-Content -Encoding UTF8 $Output
Write-Host "Resultados guardados en $Output" -ForegroundColor Green
