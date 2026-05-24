# Copies iceberg 0.9.1 from the local cargo registry into patches/iceberg-0.9.1,
# then overlays wasm-safe src/io/object_cache.rs from patches/iceberg-overlay/.
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$dst = Join-Path $root "patches\iceberg-0.9.1"
$overlay = Join-Path $root "patches\iceberg-overlay\object_cache.rs"
$dest = Join-Path $dst "src\io\object_cache.rs"

if (-not (Test-Path $overlay)) {
    Write-Error @"
Missing patches/iceberg-overlay/object_cache.rs (wasm-safe ObjectCache).
Copy the patched file from patches/iceberg-0.9.1/src/io/ after first edit, or pull latest from git.
"@
}

$registry = Join-Path $env:USERPROFILE ".cargo\registry\src"
$src = Get-ChildItem -Path $registry -Recurse -Directory -Filter "iceberg-0.9.1" -ErrorAction SilentlyContinue |
    Select-Object -First 1 -ExpandProperty FullName

if (-not $src) {
    Write-Error "iceberg-0.9.1 not found under $registry. Run: cargo fetch -p iceberg"
}

New-Item -ItemType Directory -Force -Path $dst | Out-Null
robocopy $src $dst /E /NFL /NDL /NJH /NJS /XD target | Out-Null
if ($LASTEXITCODE -ge 8) {
    Write-Error "robocopy failed with exit code $LASTEXITCODE"
}

New-Item -ItemType Directory -Force -Path (Split-Path $dest -Parent) | Out-Null
Copy-Item $overlay $dest -Force
Write-Host "Patched iceberg vendored at $dst"
