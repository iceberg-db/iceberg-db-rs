# Create branch, commit TPC-DS benchmark work, push, and open a GitHub PR.
# Run from repo root: .\scripts\create-tpcds-bench-pr.ps1

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $repoRoot

function Invoke-Git {
    param([Parameter(ValueFromRemainingArguments)][string[]]$Args)
    & git @Args
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Args -join ' ') failed (exit $LASTEXITCODE)"
    }
}

$branch = "feat/tpcds-bench-duckdb"
$base = "main"
$remoteHead = git ls-remote --heads origin $base 2>$null
if (-not $remoteHead) {
    $base = "master"
}

Write-Host "Repository: $repoRoot"
Write-Host "Base branch: $base"
Write-Host "Feature branch: $branch"

Invoke-Git fetch origin $base
$current = git branch --show-current
if ($current -ne $branch) {
    $exists = git branch --list $branch
    if ($exists) {
        Invoke-Git checkout $branch
    } else {
        Invoke-Git checkout -b $branch
    }
}

Invoke-Git add `
    Cargo.toml `
    README.md `
    .gitignore `
    crates/idb-bench `
    crates/idb-catalog/src/resolved_paths.rs `
    crates/idb-catalog/src/lib.rs `
    crates/idb-core/src/lib.rs `
    benchmarks/tpcds `
    scripts/generate-tpcds-parquet.py `
    scripts/build-iceberg-warehouse.py `
    scripts/setup-local-tpcds.ps1 `
    scripts/fetch-tpcds-queries.ps1 `
    scripts/create-tpcds-bench-pr.ps1

$status = git status --porcelain
if (-not $status) {
    Write-Host "Nothing to commit."
} else {
    $commitMsg = @"
Add TPC-DS benchmark harness (iceberg-db-rs vs DuckDB)

Introduce idb-bench with local Parquet/Iceberg setup scripts, Hadoop
warehouse support with Windows file URI normalization, and starter
TPC-DS query suite with comparison reporting.
"@
    Invoke-Git commit -m $commitMsg
}

Write-Host "Pushing $branch ..."
Invoke-Git push -u origin $branch

$prBody = @"
## Summary
- Add ``idb-bench`` crate to run TPC-DS queries against iceberg-db-rs (Iceberg + DataFusion) and DuckDB (Parquet) with side-by-side timing and row-count checks.
- Add ``benchmarks/tpcds/`` manifest, starter queries (q01, q02, q03, q07), and example config.
- Add PowerShell/Python scripts to generate local TPC-DS Parquet (DuckDB ``dsdgen``), build a Hadoop-style Iceberg warehouse (Spark + Iceberg), and run benchmarks at configurable scale factors.
- Fix Windows/local Iceberg reads: normalize Spark ``file:/C:/...`` URIs in ``resolved_paths``; link ``rstrtmgr`` for bundled DuckDB on Windows; set default schema via ``from_warehouse_with_schema`` (no unsupported ``USE``).

## Test plan
- [ ] ``cargo build -p idb-bench --release``
- [ ] ``.\scripts\setup-local-tpcds.ps1 -ScaleFactor 1 -FreshWarehouse`` (with ``HADOOP_HOME`` on Windows)
- [ ] ``cargo run -p idb-bench --release -- --config benchmarks/tpcds/bench.example.yaml --smoke``
- [ ] ``cargo run -p idb-bench --release -- --config benchmarks/tpcds/bench.yaml`` (after local ``bench.yaml`` is generated)
- [ ] Optional SF10: ``.\scripts\setup-local-tpcds.ps1 -Root .\bench-data\tpcds-sf10 -ScaleFactor 10 -FreshWarehouse``
"@

gh pr create --base $base --head $branch --title "Add TPC-DS benchmark harness (iceberg-db-rs vs DuckDB)" --body $prBody

Write-Host ""
Write-Host "Done. PR created for branch $branch -> $base"
