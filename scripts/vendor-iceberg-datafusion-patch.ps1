# Copies iceberg-datafusion 0.9.1 from the local cargo registry into patches/iceberg-datafusion-0.9.1.
# Re-apply edits listed in patches/iceberg-datafusion-0.9.1/PATCH.md after refresh.
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$dst = Join-Path $root "patches\iceberg-datafusion-0.9.1"

$registry = Join-Path $env:USERPROFILE ".cargo\registry\src"
$src = Get-ChildItem -Path $registry -Recurse -Directory -Filter "iceberg-datafusion-0.9.1" -ErrorAction SilentlyContinue |
    Select-Object -First 1 -ExpandProperty FullName

if (-not $src) {
    Write-Error "iceberg-datafusion-0.9.1 not found under $registry. Run: cargo fetch -p iceberg-datafusion"
}

New-Item -ItemType Directory -Force -Path $dst | Out-Null
robocopy $src $dst /E /NFL /NDL /NJH /NJS /XD target | Out-Null
if ($LASTEXITCODE -ge 8) {
    Write-Error "robocopy failed with exit code $LASTEXITCODE"
}

# Point at patched iceberg crate
$toml = Join-Path $dst "Cargo.toml"
(Get-Content $toml -Raw) -replace '(?m)^\[dependencies\.iceberg\]\r?\nversion = "0\.9\.1"\r?\n', "[dependencies.iceberg]`npath = `"../iceberg-0.9.1`"`n" | Set-Content -Path $toml -Encoding utf8

Write-Host "Vendored iceberg-datafusion into $dst"
Write-Host "IMPORTANT: re-apply PATCH.md changes if this was a refresh over an existing fork."
