# Test Snowflake Horizon: OAuth curl then idb query.
param(
    [string]$Query = "SELECT 1",
    [string]$Account = "qtfneqx-er54214",
    [string]$User = "",
    [string]$Scope = "session:role:DATA_ENGINEER_ROLE",
    [string]$Warehouse = "ICEBERG_TEST",
    [string]$Pat = ""
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Root
. (Join-Path $PSScriptRoot "_resolve-snowflake-pat.ps1")

$pat = Resolve-SnowflakePat -Pat $Pat -Root $Root
Write-Host ("PAT length: " + $pat.Length)

$oauthUrl = "https://$Account.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens"
$patFile = Join-Path $Root "pat.txt"
Set-Content -Path $patFile -Value $pat -NoNewline

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

Write-Host ""
Write-Host "=== OAuth (curl.exe) ===" -ForegroundColor Cyan
Write-Host ("curl.exe " + ($curlArgs -join " "))
& curl.exe @curlArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host "OAuth failed." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== Build idb ===" -ForegroundColor Cyan
& (Join-Path $Root "build-native.ps1") build -p idb-cli
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host ("=== idb: " + $Query + " ===") -ForegroundColor Cyan
cargo run -p idb-cli -- --log-http -e $Query
exit $LASTEXITCODE
