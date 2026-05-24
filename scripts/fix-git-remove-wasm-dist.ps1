# Remove web-wasm/dist from git index and amend the last commit (fixes GH001 >100MB).

$ErrorActionPreference = "Stop"



$RsRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

$RepoGit = Join-Path (Split-Path $RsRoot -Parent) "iceberg-db-git"



if (-not (Test-Path (Join-Path $RepoGit ".git"))) {

    throw "Not a git repo: $RepoGit — run push-to-github.ps1 first"

}



Set-Location $RepoGit

$user = gh api user -q .login
$userId = gh api user -q .id
git config user.email "${userId}+${user}@users.noreply.github.com"
if (-not (git config user.name)) {
    git config user.name (git config --global user.name)
    if (-not (git config user.name)) { git config user.name $user }
}

git rm -r --cached --ignore-unmatch iceberg-db-rs/web-wasm/dist 2>$null



$gi = "iceberg-db-rs/.gitignore"

if (Test-Path $gi) {

    $lines = @(Get-Content $gi)

    if ($lines -notcontains "web-wasm/dist/") {

        Add-Content $gi "web-wasm/dist/"

    }

    git add $gi

}



git status --short



git commit --amend --reset-author -m @"

Add Rust iceberg-db-rs with Snowflake Horizon IRC support



Native SQL engine (DataFusion + iceberg-rust), Snowflake Horizon IRC/PAT auth,

vended S3 credentials, HTTP debug logging. WASM build output (web-wasm/dist) is gitignored.

"@



Write-Host "Amended: $(git rev-parse --short HEAD)"

Write-Host "Then:    git push -u origin main"


