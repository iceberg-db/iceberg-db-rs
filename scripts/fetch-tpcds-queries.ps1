# Download official TPC-DS query SQL from DuckDB (queries 01-99).
# Source: https://github.com/duckdb/duckdb/tree/main/extension/tpcds/dsdgen/queries

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$OutDir = Join-Path $Root "benchmarks\tpcds\queries"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$Base = "https://raw.githubusercontent.com/duckdb/duckdb/main/extension/tpcds/dsdgen/queries"

foreach ($n in 1..99) {
    $num = "{0:D2}" -f $n
    $url = "$Base/$num.sql"
    $dest = Join-Path $OutDir "q$num.sql"
    Write-Host "Fetching q$num ..."
    Invoke-WebRequest -Uri $url -OutFile $dest -UseBasicParsing
}

Write-Host "Done. $($OutDir)"
