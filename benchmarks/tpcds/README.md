# TPC-DS benchmarks (iceberg-db-rs vs DuckDB)

This directory feeds the [`idb-bench`](../../crates/idb-bench) crate.

## Layout

| Path | Purpose |
|------|---------|
| `manifest.toml` | Which queries to run |
| `queries/qNN.sql` | TPC-DS query SQL (unqualified table names) |
| `bench.example.yaml` | Example config — copy to `bench.yaml` and edit paths |

## Data prerequisites

Both engines must see the **same logical TPC-DS dataset**:

1. **iceberg-db-rs** — Iceberg tables under a Hadoop-style warehouse:
   - `{warehouse}/tpcds/{table}/metadata/*.metadata.json`
   - Register with `idb-bench` using `warehouse`, `catalog: local`, `schema: tpcds`.

2. **DuckDB** — same Iceberg warehouse via the [Iceberg extension](https://duckdb.org/docs/extensions/iceberg) (`INSTALL iceberg; LOAD iceberg;` + `iceberg_scan` per table).

Parquet under `bench-data/.../parquet` is only used by the setup script to **build** the Iceberg warehouse (Spark CTAS), not by the benchmark runtime.

Generate data with `.\scripts\setup-local-tpcds.ps1` (DuckDB `dsdgen` + Spark → Iceberg).

## Fetch all 99 queries

```powershell
.\scripts\fetch-tpcds-queries.ps1
```

Then enable additional entries in `manifest.toml`.

## Bootstrap local test data (Parquet + Iceberg warehouse)

From repo root:

```powershell
.\scripts\setup-local-tpcds.ps1 -ScaleFactor 1
```

This does:
- generate TPC-DS tables as Parquet under `bench-data/tpcds/parquet`
- create a Hadoop-style Iceberg warehouse under `bench-data/tpcds/warehouse`
- install Python deps (`duckdb`, `pyspark`) if needed

If you use a custom root:

```powershell
.\scripts\setup-local-tpcds.ps1 -Root .\bench-data\tpcds-sf1 -ScaleFactor 1
```

On Windows, Spark requires `HADOOP_HOME`/`hadoop.home.dir` (with `winutils.exe`) to build the Iceberg warehouse.  
If you only want Parquet generation first:

```powershell
.\scripts\setup-local-tpcds.ps1 -ScaleFactor 1 -ParquetOnly
```

## Run

Run these from the **repository root** (`iceberg-db-rs-git/`), not `web-wasm/`.

```powershell
# PowerShell (repo root)
Copy-Item benchmarks/tpcds/bench.example.yaml benchmarks/tpcds/bench.yaml
# edit warehouse paths in bench.yaml (both engines share the same warehouse)

cargo run -p idb-bench --release -- --config benchmarks/tpcds/bench.yaml `
  --notes "What you changed since the last run" `
  --change "path/to/change: short description"

cargo run -p idb-bench --release -- --config benchmarks/tpcds/bench.yaml --json results/tpcds-report.json
```

Every benchmark run **appends to the audit log** unless you pass `--no-history`:

| Artifact | Purpose |
|----------|---------|
| [`results/tpcds-history.jsonl`](../results/tpcds-history.jsonl) | Machine-readable run history (one JSON object per line) |
| [`benchmarks/tpcds/HISTORY.md`](HISTORY.md) | Human-readable audit log (auto-regenerated) |

Use `--label` for a short tag (e.g. `post-pushdown-patch`) and repeat `--change` for structured fix bullets. See [`HISTORY.md`](HISTORY.md) for past runs and deltas.

You can skip copying and pass the example config directly:

```powershell
cargo run -p idb-bench -- --config benchmarks/tpcds/bench.example.yaml --list-queries
```

## Output

- Console table: per-query latency (ms), speedup (DuckDB / iceberg-db), row-count match
- Optional JSON report via `--json`

`speedup_vs_duckdb` &gt; 1.0 means iceberg-db-rs was faster for that query.
