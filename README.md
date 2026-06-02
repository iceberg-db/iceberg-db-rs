# iceberg-db-rs

Rust SQL engine over Apache Iceberg (browser/WASM target).

## Stack

- **SQL → plan → execution:** [Apache DataFusion](https://arrow.apache.org/datafusion/)
- **Iceberg:** [iceberg-rust](https://github.com/apache/iceberg-rust) + [iceberg-datafusion](https://crates.io/crates/iceberg-datafusion)
- **Config:** YAML via `~/.iceberg-db/config.yaml`

### Vendor patches (Iceberg + DataFusion integration)

We maintain **forked** copies of upstream crates under [`patches/`](patches/) (see [`patches/README.md`](patches/README.md)) instead of layering workarounds in `idb-*`. Scan and pushdown behavior lives where DataFusion meets Iceberg.

Both are enabled via `[patch.crates-io]` in [`Cargo.toml`](Cargo.toml):

```toml
[patch.crates-io]
iceberg = { path = "patches/iceberg-0.9.1" }
iceberg-datafusion = { path = "patches/iceberg-datafusion-0.9.1" }
```

Refresh from crates.io with:

```powershell
.\scripts\vendor-iceberg-patch.ps1
.\scripts\vendor-iceberg-datafusion-patch.ps1
```

#### `patches/iceberg-0.9.1`

Fork of [iceberg-rust](https://github.com/apache/iceberg-rust) **0.9.1** for correctness and concurrency on native and WASM targets:

| Area | What changed |
|------|----------------|
| **Object cache / S3** | Bounded concurrency for browser/WASM scans (`object_cache` overlay) |
| **Equality deletes** | Fixes inverted delete predicates and missing delete-filter errors in `caching_delete_file_loader` |
| **Scan parallelism** | Tunable `available_parallelism()` for manifest and file I/O on native |

Full file list: [`patches/iceberg-0.9.1/PATCH.md`](patches/iceberg-0.9.1/PATCH.md).

#### `patches/iceberg-datafusion-0.9.1`

Fork of **iceberg-datafusion 0.9.1** (DataFusion 52). Adds **general** scan optimizations—no query-specific SQL rewrites:

| Feature | Problem | Fix |
|---------|---------|-----|
| **Dynamic filter pushdown** | Join and runtime filters from `HashJoinExec` never reached `IcebergTableScan`, so large fact scans showed `predicate:[]` | `handle_child_pushdown_result` + physical → Iceberg `Predicate` conversion (`physical_expr_to_predicate.rs`) |
| **Parallel scan partitions** | Upstream used a single partition and ignored `execute(partition)` | `target_partitions` from session config, `repartitioned()`, size-balanced file groups (`file_groups.rs`) |

**DataFusion options** (also exposed via `idb-cli --target-partitions`):

| Option | Effect |
|--------|--------|
| `execution.target_partitions` | Scan partition count |
| `optimizer.repartition_file_scans` | Allows increasing scan partitions for large tables |
| `optimizer.enable_join_dynamic_filter_pushdown` | Hash-join → probe-side scan filters (default on) |

**Validate:** `idb-cli --explain-analyze` on a selective join—probe-side `IcebergTableScan` should show `partitions>1` on large tables and a non-empty `predicate:[...]` when pushdown applies. Dynamic filters apply to the **probe** side of hash joins; star-schema queries still depend on join order from the planner.

Full change list: [`patches/iceberg-datafusion-0.9.1/PATCH.md`](patches/iceberg-datafusion-0.9.1/PATCH.md).

## Crates

| Crate | Purpose |
|-------|---------|
| `idb-config` | Load YAML, `${ENV}` substitution |
| `idb-catalog` | REST + local warehouse (`MemoryCatalog` / file layout) |
| `idb-sql` | DataFusion `SessionContext` + `IcebergCatalogProvider` |
| `idb-core` | `Engine` facade |
| `idb-cli` | Native binary `idb` |
| `idb-bench` | TPC-DS benchmarks vs DuckDB (`idb-bench`) |
| `idb-wasm` | WASM stub (browser phase 3) |

## Build & run (native)

```bash
cd iceberg-db-rs
cargo build -p idb-cli

# Local warehouse
export ICEBERG_DB_WAREHOUSE=/path/to/warehouse   # optional if using -w
cargo run -p idb-cli -- -w /path/to/warehouse -e "SELECT COUNT(*) FROM demo.customers"

# Or config file
cargo run -p idb-cli -- -c config/local-hadoop.yaml -e "SELECT 1"
```

Seed demo tables into the same warehouse path, then query from Rust.

## Benchmarks (TPC-DS vs DuckDB)

The [`idb-bench`](crates/idb-bench) crate runs the [TPC-DS](https://www.tpc.org/tpcds/) query suite against **iceberg-db-rs** and **DuckDB** (both on the same local Iceberg warehouse), then prints a comparison table.

```powershell
# From repository root (not web-wasm/)
.\scripts\fetch-tpcds-queries.ps1

Copy-Item benchmarks/tpcds/bench.example.yaml benchmarks/tpcds/bench.yaml
# edit warehouse in bench.yaml (shared by both engines)

cargo run -p idb-bench --release -- --config benchmarks/tpcds/bench.yaml
cargo run -p idb-bench --release -- --config benchmarks/tpcds/bench.yaml --json tpcds-report.json
```

See [benchmarks/tpcds/README.md](benchmarks/tpcds/README.md) for data layout and manifest format. Recorded runs and fix notes: [benchmarks/tpcds/HISTORY.md](benchmarks/tpcds/HISTORY.md).

## Tests

```powershell
# Rust unit tests for the pure (no-network) crates
cargo test -p idb-config -p idb-sql -p idb-catalog --lib

# JS helper tests (Node 18+, no deps)
cd web-wasm
npm test                # or: node --test tests
```

The Rust tests cover YAML/profile parsing, SQL statement preprocessing
(`SHOW TABLES`, identifier hardening), and path resolution. The JS tests
cover the pure formatters in `web-wasm/lib/format.mjs` used by the SQL
workspace UI.

## Roadmap

1. **P0 (this scaffold):** native CLI, file + REST catalog, basic `SELECT`
2. **P1:** SQL compliance tests shared with `iceberg-db-sqltest`
3. **P2:** filter/projection pushdown parity — **in progress** via `patches/iceberg-datafusion-0.9.1` (dynamic filters + partitioned scans)
4. **P3:** `idb-wasm` + browser extension UI (Snowsight-lite)
