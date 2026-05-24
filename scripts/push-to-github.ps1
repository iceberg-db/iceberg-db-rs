# Sync iceberg-db-rs into GitHub repo iceberg-db and push.
# Run from PowerShell: .\scripts\push-to-github.ps1

$ErrorActionPreference = "Stop"

function Invoke-Git {
    param([Parameter(ValueFromRemainingArguments)][string[]]$Args)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $out = & git @Args 2>&1
        foreach ($line in $out) {
            $text = if ($line -is [System.Management.Automation.ErrorRecord]) { "$line" } else { "$line" }
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

$RsSrc = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$RepoPrimary = Join-Path (Split-Path $RsSrc -Parent) "iceberg-db"
$RepoClone = Join-Path (Split-Path $RsSrc -Parent) "iceberg-db-git"

$user = gh api user -q .login
Write-Host "GitHub user: $user"
gh auth status

$null = gh repo view "$user/iceberg-db" --json url 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "Repo $user/iceberg-db not found or gh not authenticated. Run: gh auth login"
}

if (Test-Path (Join-Path $RepoPrimary ".git")) {
    $RepoRoot = $RepoPrimary
} else {
    if (-not (Test-Path (Join-Path $RepoClone ".git"))) {
        gh repo clone "https://github.com/$user/iceberg-db.git" $RepoClone
    }
    $RepoRoot = $RepoClone
}

Set-Location $RepoRoot

$branch = gh repo view "$user/iceberg-db" --json defaultBranchRef -q .defaultBranchRef.name 2>$null
if (-not $branch) { $branch = "main" }

$currentBranch = Get-Git branch --show-current
if (-not $currentBranch) {
    Invoke-Git checkout -b $branch
} elseif ($currentBranch -ne $branch) {
    Invoke-Git branch -M $branch
}

$userId = gh api user -q .id
$noreplyEmail = "${userId}+${user}@users.noreply.github.com"
$gitName = Get-Git config --global user.name
if (-not $gitName) { $gitName = $user }
Invoke-Git config user.email $noreplyEmail
Invoke-Git config user.name $gitName
Write-Host "Git author for this repo: $gitName <$noreplyEmail>"

$dest = Join-Path $RepoRoot "iceberg-db-rs"
if (-not (Test-Path $dest)) {
    New-Item -ItemType Directory -Path $dest | Out-Null
}

Write-Host "Syncing files (robocopy mirror; excludes target/, web-wasm/dist/) ..."
$robolog = Join-Path $env:TEMP "idb-push-robocopy.log"
robocopy $RsSrc $dest /MIR /XD target .git "web-wasm\dist" /XF build.log _git_check.txt pat.txt *.rs.bk .env /NFL /NDL /NJH /NJS /NP | Tee-Object -FilePath $robolog
if ($LASTEXITCODE -ge 8) { throw "robocopy failed with exit $LASTEXITCODE (see $robolog)" }
Write-Host "Sync done (robocopy exit $LASTEXITCODE; 0-7 = success)."

$gi = Join-Path $dest ".gitignore"
$giLines = @()
if (Test-Path $gi) { $giLines = Get-Content $gi }
foreach ($line in @("/target/", "web-wasm/dist/")) {
    if ($giLines -notcontains $line) { Add-Content -Path $gi -Value $line }
}

Write-Host "Staging changes ..."
Invoke-Git add iceberg-db-rs/

$status = Get-Git status --porcelain
if ($status) {
    Write-Host "Committing ..."
    $commitMsg = @"
Improve WASM SQL editor: case-insensitive columns, selection run, live status logs

Wrap Iceberg tables with lowercase column schemas so JOINs work with unquoted ids.
Run only selected SQL from the editor; stream idb_query progress in the status bar.
"@
    Invoke-Git commit -m $commitMsg
} else {
    Write-Host "No file changes to commit."
}

Write-Host "Pushing to GitHub (fetch/rebase/push may take a minute on slow networks) ..."
$remoteHeads = Get-Git ls-remote --heads origin
$remoteEmpty = -not $remoteHeads

if ($remoteEmpty) {
    Write-Host "Remote has no branches yet - pushing initial $branch ..."
    Invoke-Git push -u origin $branch
} else {
    Invoke-Git fetch origin $branch
    $hasUpstream = Get-Git rev-parse --abbrev-ref '@{u}'
    if ($hasUpstream) {
        Invoke-Git pull --rebase origin $branch
    }
    Invoke-Git push -u origin $branch
}

$sha = Get-Git rev-parse HEAD
$url = gh repo view "$user/iceberg-db" --json url -q .url

Write-Host ""
Write-Host "Done."
Write-Host "  Repo:   $url"
Write-Host "  Branch: $branch"
Write-Host "  Commit: $sha"
