# Same OAuth as test-horizon.ps1, but through the local dev proxy (same path as the browser).
# Start web-wasm\serve.ps1 first (idb-sf-proxy on :8787).
param(
    [string]$Account = "qtfneqx-er54214",
    [string]$User = "",
    [string]$Scope = "session:role:DATA_ENGINEER_ROLE",
    [string]$Pat = ""
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
. (Join-Path $PSScriptRoot "_resolve-snowflake-pat.ps1")

try {
    $null = Invoke-WebRequest -Uri "http://127.0.0.1:8787/health" -UseBasicParsing -TimeoutSec 2
} catch {
    throw @"
idb-sf-proxy is not running on http://127.0.0.1:8787

Option A — proxy only (second terminal, leave open):
  .\scripts\start-sf-proxy.ps1

Option B — full dev UI (proxy + Trunk on :8080):
  cd web-wasm
  .\serve.ps1
"@
}

$pat = Resolve-SnowflakePat -Pat $Pat -Root $Root
Write-Host ("PAT length: " + $pat.Length) -ForegroundColor DarkGray
$patFile = Join-Path $Root "pat.txt"
Set-Content -Path $patFile -Value $pat -NoNewline

# Trunk uses /sf/ → :8787/ ; curl hits the proxy the same way the browser does after Trunk rewrite.
$oauthUrl = "http://127.0.0.1:8787/$Account/polaris/api/catalog/v1/oauth/tokens"

$curlArgs = @(
    "-i", "--fail",
    "-X", "POST", $oauthUrl,
    "-H", "Content-Type: application/x-www-form-urlencoded",
    "--data-urlencode", "grant_type=client_credentials",
    "--data-urlencode", "scope=$Scope",
    "--data-urlencode", "client_secret@$patFile"
)
if ($User) {
    $curlArgs += "--data-urlencode", "client_id=$User"
}

Write-Host "=== OAuth via local proxy (port 8787) ===" -ForegroundColor Cyan
Write-Host ("curl.exe " + ($curlArgs -join " "))
& curl.exe @curlArgs
exit $LASTEXITCODE
