# iceberg-db-rs

Rust SQL engine over Apache Iceberg (browser/WASM target), developed **in parallel** with the Java [`iceberg-db`](../iceberg-db) project.

## Stack

- **SQL → plan → execution:** [Apache DataFusion](https://arrow.apache.org/datafusion/)
- **Iceberg:** [iceberg-rust](https://github.com/apache/iceberg-rust) + [iceberg-datafusion](https://crates.io/crates/iceberg-datafusion)
- **Config:** YAML compatible with Java `~/.iceberg-db/config.yaml`

## Crates

| Crate | Purpose |
|-------|---------|
| `idb-config` | Load YAML, `${ENV}` substitution |
| `idb-catalog` | REST + local warehouse (`MemoryCatalog` / file layout) |
| `idb-sql` | DataFusion `SessionContext` + `IcebergCatalogProvider` |
| `idb-core` | `Engine` facade |
| `idb-cli` | Native binary `idb` |
| `idb-wasm` | WASM stub (browser phase 3) |

## Build & run (native)

```bash
cd iceberg-db-rs
cargo build -p idb-cli

# Local warehouse (same layout as Java HadoopCatalog tests)
export ICEBERG_DB_WAREHOUSE=/path/to/warehouse   # optional if using -w
cargo run -p idb-cli -- -w /path/to/warehouse -e "SELECT COUNT(*) FROM demo.customers"

# Or config file
cargo run -p idb-cli -- -c config/local-hadoop.yaml -e "SELECT 1"
```

Seed demo tables with the Java seeder into the same warehouse path, then query from Rust.

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
3. **P2:** filter/projection pushdown parity
4. **P3:** `idb-wasm` + browser extension UI (Snowsight-lite)

## Java vs Rust

| | Java `iceberg-db` | `iceberg-db-rs` |
|--|-------------------|-----------------|
| Planner | Calcite | DataFusion |
| Browser | Not targeted | WASM + extension |
| JDBC | Yes | No (planned: local agent only) |
| Local warehouse | HadoopCatalog | `MemoryCatalog` + warehouse path (v0) |
