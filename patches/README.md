# Vendor patches (`patches/`)

This repository pins **forked copies** of upstream Iceberg crates instead of patching behavior only in `idb-*` crates. That keeps performance fixes in the scan/plan layer where DataFusion and Iceberg meet.

| Patch | Crates.io version | Why forked |
|-------|-------------------|------------|
| [`iceberg-0.9.1/`](iceberg-0.9.1/) | 0.9.1 | WASM-safe S3/object cache, parallel scan tuning, delete-file loader fixes |
| [`iceberg-datafusion-0.9.1/`](iceberg-datafusion-0.9.1/) | 0.9.1 | Dynamic filter pushdown into `IcebergTableScan`, parallel file partitions |

Both are wired in the workspace root [`Cargo.toml`](../Cargo.toml):

```toml
[patch.crates-io]
iceberg = { path = "patches/iceberg-0.9.1" }
iceberg-datafusion = { path = "patches/iceberg-datafusion-0.9.1" }
```

## Refreshing from crates.io

After `cargo fetch`, re-copy upstream sources then re-apply our edits (or merge manually):

```powershell
.\scripts\vendor-iceberg-patch.ps1
.\scripts\vendor-iceberg-datafusion-patch.ps1
```

Each patch directory has a **`PATCH.md`** listing files changed relative to upstream and how to validate.

## Validation

```powershell
cargo test -p idb-sql --lib
cargo build -p idb-cli -p idb-bench --release
```

For scan pushdown / partitioning, run `EXPLAIN ANALYZE` on a join query and confirm `IcebergTableScan` shows `partitions=N` and a non-empty `predicate:[...]` when the fact table is the join probe side.
