# Reproduce browser 403: curl with Origin header (proxy now strips this for WASM).
param(
    [string]$Account = "qtfneqx-er54214",
    [string]$Scope = "session:role:DATA_ENGINEER_ROLE",
    [string]$Pat = ""
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
. (Join-Path $PSScriptRoot "_resolve-snowflake-pat.ps1")
. (Join-Path $PSScriptRoot "_stop-sf-proxy.ps1")

$pat = Resolve-SnowflakePat -Pat $Pat -Root $Root
$patFile = Join-Path $Root "pat.txt"
Set-Content -Path $patFile -Value $pat -NoNewline

$oauthUrl = "http://127.0.0.1:8787/$Account/polaris/api/catalog/v1/oauth/tokens"

Write-Host "=== OAuth WITH Origin (like browser) — expect 403 if Snowflake rejects Origin ===" -ForegroundColor Yellow
curl.exe -i `
    -X POST $oauthUrl `
    -H "Origin: http://127.0.0.1:8080" `
    -H "Content-Type: application/x-www-form-urlencoded" `
    --data-urlencode "grant_type=client_credentials" `
    --data-urlencode "scope=$Scope" `
    "--data-urlencode" "client_secret@$patFile"
exit $LASTEXITCODE
