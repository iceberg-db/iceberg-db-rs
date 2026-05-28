# Push live fetch-stats and open a PR against main (parallel S3 is already merged).
$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path $PSScriptRoot -Parent
Set-Location $RepoRoot

function Invoke-Git {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$GitArgs)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & git @GitArgs 2>&1 | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) { Write-Host $_.ToString() } else { $_ }
    }
    $exit = $LASTEXITCODE
    $ErrorActionPreference = $prev
    if ($exit -ne 0) { throw "git $($GitArgs -join ' ') failed (exit $exit)" }
}

$stagePaths = @(
    "crates/idb-catalog/src/wasm_query_io.rs",
    "crates/idb-catalog/src/lib.rs",
    "crates/idb-catalog/src/wasm_local.rs",
    "crates/idb-catalog/src/wasm_s3_storage.rs",
    "crates/idb-sql/src/lib.rs",
    "crates/idb-wasm/src/lib.rs",
    "web-wasm/app.js",
    "web-wasm/index.html",
    "web-wasm/lib/format.mjs",
    "web-wasm/tests/format.test.mjs",
    "scripts/commit-and-pr.ps1",
    "scripts/pr-body-live-fetch-stats.md"
)

$branch = "feat/wasm-live-fetch-stats"
$prBodyFile = Join-Path $PSScriptRoot "pr-body-live-fetch-stats.md"

Write-Host "=== 1. fetch main ==="
Invoke-Git fetch origin main

Write-Host ""
Write-Host "=== 2. branch from origin/main ==="
Invoke-Git checkout -B $branch origin/main

Write-Host ""
Write-Host "=== 3. stage ==="
foreach ($p in $stagePaths) {
    if (Test-Path (Join-Path $RepoRoot $p)) {
        Invoke-Git add -- $p
    }
}

Invoke-Git status -sb
Invoke-Git diff --cached --stat

$prev = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& git diff --cached --quiet
$hasStaged = $LASTEXITCODE -ne 0
$ErrorActionPreference = $prev

if (-not $hasStaged) {
    Write-Host "No staged changes - nothing to commit. Exiting."
    exit 0
}

Write-Host ""
Write-Host "=== 4. commit ==="
$commitTitle = "feat(wasm): live fetch stats in UI (files and bytes)"
$commitBody = "Track distinct S3 objects and response bytes per query. Poll idb_bytes_fetched and idb_files_fetched during runs. Show file count and size in the status bar."
Invoke-Git commit -m $commitTitle -m $commitBody

Write-Host ""
Write-Host "=== 5. push ==="
Invoke-Git push -u origin $branch

Write-Host ""
Write-Host "=== 6. create PR ==="
$prev = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$existing = gh pr list --head $branch --json url --jq ".[0].url" 2>&1
$ErrorActionPreference = $prev

if ($existing -and ($existing -notmatch "error|failed")) {
    $prUrl = $existing.Trim()
    Write-Host "PR already exists: $prUrl"
} else {
    $prUrl = gh pr create --base main --head $branch --title $commitTitle --body-file $prBodyFile
    Write-Host "Created PR: $prUrl"
}

Write-Host ""
Write-Host "=== DONE ==="
Write-Host "Branch: $branch"
Write-Host "HEAD: $(git rev-parse HEAD)"
Write-Host "PR: $prUrl"
