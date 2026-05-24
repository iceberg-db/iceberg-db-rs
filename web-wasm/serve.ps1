# Start Trunk with LLVM on PATH (required for zstd-sys on wasm32-unknown-unknown).

$ErrorActionPreference = "Stop"



$llvmBin = Join-Path ${env:ProgramFiles} "LLVM\bin"

if (-not (Test-Path (Join-Path $llvmBin "clang.exe"))) {

    Write-Error @"

clang.exe not found at: $llvmBin



Install LLVM, then open a NEW terminal:

  winget install --id LLVM.LLVM -e



Or add LLVM\bin to your user PATH manually.

"@

}



$env:Path = "$llvmBin;$env:Path"

$env:CC_wasm32_unknown_unknown = "clang"

$env:AR_wasm32_unknown_unknown = "llvm-ar"



Write-Host "Using clang: $(Get-Command clang | Select-Object -ExpandProperty Source)"



$repoRoot = Split-Path $PSScriptRoot -Parent

$patchToml = Join-Path $repoRoot "patches\iceberg-0.9.1\Cargo.toml"

if (-not (Test-Path $patchToml)) {

    Write-Host "Vendoring patched iceberg (first run)..."

    & (Join-Path $repoRoot "scripts\vendor-iceberg-patch.ps1")

    if (-not (Test-Path $patchToml)) {

        Write-Error "Vendor failed: $patchToml still missing. Run: cargo fetch -p iceberg"

    }

}



function Invoke-Cargo {
    param(
        [Parameter(Mandatory)][string[]]$Args,
        [switch]$Quiet
    )
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        if ($Quiet) {
            & cargo @Args 2>&1 | Out-Null
        } else {
            & cargo @Args
        }
        if ($LASTEXITCODE -ne 0) {
            throw "cargo $($Args -join ' ') failed with exit code $LASTEXITCODE"
        }
    } finally {
        $ErrorActionPreference = $prev
    }
}



Set-Location $repoRoot

. (Join-Path $repoRoot "scripts\_stop-sf-proxy.ps1")
Stop-IdbSfProxy

Write-Host "Building Snowflake dev proxy (idb-sf-proxy)..."

Invoke-Cargo -Args @("build", "-p", "idb-sf-proxy", "--release")



$proxyBin = Join-Path $repoRoot "target\release\idb-sf-proxy.exe"

if (-not (Test-Path $proxyBin)) {

    Write-Error "Missing proxy binary: $proxyBin"

}



$proxyProc = Start-Process -FilePath $proxyBin -PassThru -WindowStyle Hidden

Write-Host "Snowflake dev proxy PID $($proxyProc.Id) -> http://127.0.0.1:8787"



$healthOk = $false

$prevEa = $ErrorActionPreference

$ErrorActionPreference = "Continue"

for ($i = 0; $i -lt 15; $i++) {

    Start-Sleep -Milliseconds 400

    try {

        $null = Invoke-WebRequest -Uri "http://127.0.0.1:8787/health" -UseBasicParsing -TimeoutSec 2

        $healthOk = $true

        break

    } catch {

        if ($proxyProc.HasExited) {

            Write-Error "idb-sf-proxy exited early (code $($proxyProc.ExitCode))"

        }

    }

}

$ErrorActionPreference = $prevEa



if (-not $healthOk) {

    if (-not $proxyProc.HasExited) {

        Stop-Process -Id $proxyProc.Id -Force -ErrorAction SilentlyContinue

    }

    Write-Error "Snowflake dev proxy did not respond on http://127.0.0.1:8787/health"

}



try {

    Set-Location $PSScriptRoot

    Write-Host "Rebuilding WASM (cargo clean -p idb-wasm) - hard-refresh the browser after Trunk starts."
    Write-Host "  On Run, DevTools should show: idb_query: planning SQL, loadTable, s3 GET, done"

    Set-Location $repoRoot

    Invoke-Cargo -Args @("clean", "-p", "idb-wasm") -Quiet

    Set-Location $PSScriptRoot



    trunk serve index.html --open @args

} finally {

    if ($proxyProc -and -not $proxyProc.HasExited) {

        Stop-Process -Id $proxyProc.Id -Force -ErrorAction SilentlyContinue

        Write-Host "Stopped Snowflake dev proxy"

    }

}


