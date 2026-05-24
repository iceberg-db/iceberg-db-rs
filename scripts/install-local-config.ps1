# Install Hadoop-style local catalog config for idb-cli.
$ErrorActionPreference = "Stop"
$destDir = Join-Path $env:USERPROFILE ".iceberg-db"
$dest = Join-Path $destDir "config.yaml"
$src = Join-Path $PSScriptRoot "..\config\local-hadoop.yaml"
New-Item -ItemType Directory -Force -Path $destDir | Out-Null
Copy-Item -Force $src $dest
if (-not $env:ICEBERG_DB_WAREHOUSE) {
    $env:ICEBERG_DB_WAREHOUSE = (Join-Path $destDir "warehouse").Replace("\", "/")
}
Write-Host "Installed $dest"
Write-Host "Set warehouse: $env:ICEBERG_DB_WAREHOUSE"
Write-Host "Run: cargo run -p idb-cli -- -e `"SELECT COUNT(*) FROM demo.customers`""
