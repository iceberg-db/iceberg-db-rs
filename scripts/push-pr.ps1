# Push current iceberg-db-rs contents as a feature branch to
# https://github.com/iceberg-db/iceberg-db-rs and open a pull request
# against `main` (which is protected). Files live at the repo ROOT — no
# nested iceberg-db-rs/ subfolder.
#
# Usage:
#   .\scripts\push-pr.ps1
#   .\scripts\push-pr.ps1 -Branch chore/my-change -Title "..."
#
# Requires: gh auth login (with `repo` scope on iceberg-db org), git on PATH.

[CmdletBinding()]
param(
    [string]$Owner = "iceberg-db",
    [string]$Repo = "iceberg-db-rs",
    [string]$Branch,
    [string]$Title = "chore: cleanup helpers + tests; fix BigInt elapsed_ms and 120s UI timeout",
    [string]$BodyFile
)

$ErrorActionPreference = "Stop"

function Invoke-Git {
    param([Parameter(ValueFromRemainingArguments)][string[]]$Args)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $out = & git @Args 2>&1
        foreach ($line in $out) {
            $text = "$line"
            if ($text.Trim()) { Write-Host $text }
        }
        if ($LASTEXITCODE -ne 0) {
            throw "git $($Args -join ' ') failed (exit $LASTEXITCODE)"
        }
    } finally {
        $ErrorActionPreference = $prev
    }
}

function Get-Git {
    param([Parameter(ValueFromRemainingArguments)][string[]]$Args)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $lines = @(& git @Args 2>&1 | ForEach-Object { "$_" })
        if ($LASTEXITCODE -ne 0) {
            throw "git $($Args -join ' ') failed (exit $LASTEXITCODE)"
        }
        return ($lines -join "`n").Trim()
    } finally {
        $ErrorActionPreference = $prev
    }
}

# ---- Resolve paths --------------------------------------------------------
$Src = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Parent = Split-Path $Src -Parent
$Clone = Join-Path $Parent "$Repo-git"

Write-Host "Source dir: $Src"
Write-Host "Clone dir : $Clone"
Write-Host "Target    : $Owner/$Repo"

# ---- GitHub auth + repo check --------------------------------------------
$user = gh api user -q .login
Write-Host "GitHub user: $user"
gh auth status

$null = gh repo view "$Owner/$Repo" --json url 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "Repo $Owner/$Repo not found or you lack access. Confirm the org/repo and re-run `gh auth login`."
}

$defaultBranch = gh repo view "$Owner/$Repo" --json defaultBranchRef -q .defaultBranchRef.name 2>$null
if (-not $defaultBranch) { $defaultBranch = "main" }
Write-Host "Default branch: $defaultBranch"

# ---- Branch name ----------------------------------------------------------
if (-not $Branch) {
    $stamp = Get-Date -Format "yyyyMMdd-HHmm"
    $Branch = "chore/cleanup-and-tests-$stamp"
}
Write-Host "Feature branch: $Branch"

# ---- Clone or refresh -----------------------------------------------------
if (-not (Test-Path (Join-Path $Clone ".git"))) {
    Write-Host "Cloning $Owner/$Repo into $Clone ..."
    gh repo clone "$Owner/$Repo" $Clone
}

Set-Location $Clone

Invoke-Git fetch origin
Invoke-Git checkout $defaultBranch
Invoke-Git pull --ff-only origin $defaultBranch

# ---- Git author ----------------------------------------------------------
$userId = gh api user -q .id
$noreplyEmail = "${userId}+${user}@users.noreply.github.com"
$gitName = Get-Git config --global user.name
if (-not $gitName) { $gitName = $user }
Invoke-Git config user.email $noreplyEmail
Invoke-Git config user.name $gitName
Write-Host "Git author: $gitName <$noreplyEmail>"

# ---- Create / reset feature branch ---------------------------------------
$existing = Get-Git branch --list $Branch
if ($existing) {
    Write-Host "Branch $Branch already exists locally — checking out and resetting to $defaultBranch ..."
    Invoke-Git checkout $Branch
    Invoke-Git reset --hard "origin/$defaultBranch"
} else {
    Invoke-Git checkout -b $Branch "origin/$defaultBranch"
}

# ---- Mirror source files to repo root (NOT a nested subfolder) -----------
Write-Host "Mirroring files from $Src into $Clone (excludes target/, web-wasm/dist/, .git, .env) ..."
$robolog = Join-Path $env:TEMP "idb-push-pr-robocopy.log"
robocopy $Src $Clone /MIR /XD target .git "web-wasm\dist" node_modules /XF build.log _git_check.txt pat.txt *.rs.bk .env _push_result.txt /NFL /NDL /NJH /NJS /NP | Tee-Object -FilePath $robolog
if ($LASTEXITCODE -ge 8) { throw "robocopy failed with exit $LASTEXITCODE (see $robolog)" }
Write-Host "Sync done (robocopy exit $LASTEXITCODE; 0-7 = success)."

# ---- Make sure target/ and dist/ stay out of git ------------------------
$gi = Join-Path $Clone ".gitignore"
$giLines = @()
if (Test-Path $gi) { $giLines = Get-Content $gi }
foreach ($line in @("/target/", "web-wasm/dist/", "/_push_result.txt")) {
    if ($giLines -notcontains $line) { Add-Content -Path $gi -Value $line }
}

# ---- Stage + commit ------------------------------------------------------
Invoke-Git add -A
$status = Get-Git status --porcelain
if (-not $status) {
    Write-Host "No file changes vs $defaultBranch — nothing to push."
    return
}

# Use a multi-line commit message (matches the PR title + a one-paragraph body).
$commitMsg = @"
$Title

Reformats idb-config/profile.rs, gates unused REST helpers, hardens
parse_show_tables against non-ASCII input, and extracts pure JS helpers
into web-wasm/lib/format.mjs. Adds Rust unit tests across idb-config,
idb-sql, idb-catalog and Node-runnable tests for the JS formatters.
Also fixes the WASM elapsed_ms BigInt bug and replaces the 120s hard
query timeout with a soft 10-minute UI threshold.
"@

Invoke-Git commit -m $commitMsg

# ---- Push branch ---------------------------------------------------------
Invoke-Git push -u origin $Branch

# ---- Open PR -------------------------------------------------------------
$bodyText = @"
## Summary

- **WASM query UX**: replace the hard 120s ``Promise.race`` failure in
  ``web-wasm/app.js`` with a 10-minute soft threshold that keeps awaiting
  the WASM query; show "Still running (Xs)" while waiting.
- **Live elapsed timer**: status bar now ticks every second during runs,
  drops zero components (``2h 30m``, ``1h 5s``), and resets cleanly on
  completion/error.
- **BigInt fix**: ``QueryResult.elapsed_ms`` is now ``u64`` (was ``u128``,
  which ``serde_wasm_bindgen`` marshalled as JS ``BigInt`` and broke
  ``ms / 1000`` arithmetic in the new timer).
- **Cleanup**:
  - Reformat ``idb-config/profile.rs`` (collapses the per-line blank-line
    spam) and split ``validate_snowflake_horizon`` out of
    ``validate_rest_props``.
  - Hoist imports to the top of ``idb-sql/lib.rs`` and reorder cfgs.
  - Harden ``parse_show_tables`` against non-ASCII inputs (previously
    byte-indexing could panic); add ``strip_ascii_prefix_ci`` helper.
  - Gate the unused ``reqwest::Client`` import on non-wasm targets.
  - Gate ``rest_types`` and ``tables_list_endpoint`` to wasm32 (dead on
    native builds).
  - Extract pure JS helpers into ``web-wasm/lib/format.mjs`` and import
    them from ``app.js`` (constants block, regrouped functions, no
    behavior change).
- **Tests**: added unit tests across ``idb-config``, ``idb-sql``,
  ``idb-catalog`` plus Node-runnable tests for the JS formatters
  (``web-wasm/tests/format.test.mjs``).

## Test plan

- [ ] ``cargo test -p idb-config -p idb-sql -p idb-catalog --lib``
- [ ] ``cd web-wasm && node --test tests``
- [ ] ``trunk build`` from ``web-wasm/``
- [ ] Manual: run ``SELECT COUNT(*) FROM customer`` in the WASM UI;
      verify the elapsed timer ticks, ``Cannot mix BigInt`` error no
      longer occurs, and the query completes past the old 120s wall.
"@

if ($BodyFile) {
    $bodyText = Get-Content -Raw -Path $BodyFile
}

$tmp = New-TemporaryFile
Set-Content -Path $tmp -Value $bodyText -Encoding UTF8
try {
    gh pr create `
        --repo "$Owner/$Repo" `
        --base $defaultBranch `
        --head $Branch `
        --title $Title `
        --body-file $tmp
} finally {
    Remove-Item -Force $tmp -ErrorAction SilentlyContinue
}

$sha = Get-Git rev-parse HEAD
Write-Host ""
Write-Host "Done."
Write-Host "  Repo:   https://github.com/$Owner/$Repo"
Write-Host "  Branch: $Branch"
Write-Host "  Commit: $sha"
