# OAuth through Trunk dev server (same path as the browser: :8080/sf/...).
# Start web-wasm\serve.ps1 first (Trunk :8080 + idb-sf-proxy :8787).
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
    $null = Invoke-WebRequest -Uri "http://127.0.0.1:8080/" -UseBasicParsing -TimeoutSec 2
} catch {
    throw @"
Trunk is not running on http://127.0.0.1:8080

Start the full dev stack (proxy + WASM UI):
  cd web-wasm
  .\serve.ps1
"@
}

$pat = Resolve-SnowflakePat -Pat $Pat -Root $Root
Write-Host ("PAT length: " + $pat.Length) -ForegroundColor DarkGray
$patFile = Join-Path $Root "pat.txt"
Set-Content -Path $patFile -Value $pat -NoNewline

$oauthUrl = "http://127.0.0.1:8080/sf/$Account/polaris/api/catalog/v1/oauth/tokens"

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

Write-Host "=== OAuth via Trunk (:8080/sf/...) ===" -ForegroundColor Cyan
Write-Host ("curl.exe " + ($curlArgs -join " "))
& curl.exe @curlArgs
exit $LASTEXITCODE
