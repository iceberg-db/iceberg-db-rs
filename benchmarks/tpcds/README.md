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

2. **DuckDB** — Parquet per table:
   - `{parquet_root}/store_sales/*.parquet` (or `{parquet_root}/store_sales.parquet`)
   - Other TPC-DS tables likewise.

Generate data with [TPC-DS tools](https://www.tpc.org/tpcds/) (e.g. `dsdgen`) or your existing Java/Spark pipeline, then export Parquet and (optionally) register Iceberg tables.

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
# edit warehouse + duckdb.parquet_root in bench.yaml

cargo run -p idb-bench --release -- --config benchmarks/tpcds/bench.yaml
cargo run -p idb-bench --release -- --config benchmarks/tpcds/bench.yaml --json results/tpcds.json
```

You can skip copying and pass the example config directly:

```powershell
cargo run -p idb-bench -- --config benchmarks/tpcds/bench.example.yaml --list-queries
```

## Output

- Console table: per-query latency (ms), speedup (DuckDB / iceberg-db), row-count match
- Optional JSON report via `--json`

`speedup_vs_duckdb` &gt; 1.0 means iceberg-db-rs was faster for that query.
