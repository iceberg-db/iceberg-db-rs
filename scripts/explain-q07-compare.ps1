# Compare EXPLAIN ANALYZE for TPC-DS q07 on iceberg-db-rs vs DuckDB Iceberg.
param(
    [string]$Config = "benchmarks/tpcds/bench.yaml",
    [string]$QueryId = "q07",
    [int]$TargetPartitions = 20
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

$env:RUST_LOG = "warn"

$extraArgs = @("--config", $Config, "--explain", $QueryId)
if ($TargetPartitions -gt 0) {
    $base = Get-Content $Config -Raw
    $tmp = Join-Path $env:TEMP "idb-bench-explain-$QueryId.yaml"
    @(
        $base.TrimEnd()
        "target_partitions: $TargetPartitions"
    ) -join "`n" | Set-Content -Path $tmp -Encoding utf8
    $extraArgs = @("--config", $tmp, "--explain", $QueryId)
}

Write-Host "Running idb-bench EXPLAIN compare for $QueryId ..."
cargo run -p idb-bench --release -- @extraArgs
