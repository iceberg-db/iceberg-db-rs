# Run Snowflake dev proxy on http://127.0.0.1:8787 (for test-oauth-via-proxy.ps1).
# Keep this window open OR use web-wasm\serve.ps1 (starts proxy + Trunk).
# Stop this (Ctrl+C) before running serve.ps1 if you see "Access is denied" rebuilding idb-sf-proxy.exe.

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Root

$proxyBin = Join-Path $Root "target\release\idb-sf-proxy.exe"
if (-not (Test-Path $proxyBin)) {
    Write-Host "Building idb-sf-proxy (release)..." -ForegroundColor Cyan
    cargo build -p idb-sf-proxy --release
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

try {
    $null = Invoke-WebRequest -Uri "http://127.0.0.1:8787/health" -UseBasicParsing -TimeoutSec 1
    Write-Host "idb-sf-proxy already listening on http://127.0.0.1:8787/health" -ForegroundColor Green
    exit 0
} catch {
    # not running
}

Write-Host "idb-sf-proxy → http://127.0.0.1:8787 (Ctrl+C to stop)" -ForegroundColor Cyan
& $proxyBin
