#!/usr/bin/env python3
"""
Create a local Hadoop-style Iceberg warehouse from TPC-DS parquet folders.
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
from pathlib import Path

from pyspark.sql import SparkSession


# Sorted + bloom-filtered on write (Tier 1 star-schema layout).
#
# Bloom columns: high-cardinality FKs used in equality / IN semi-joins (q07: cdemo, item).
# Do NOT bloom the leading sort key (date_sk): files are clustered by date; min/max stats
# prune files/ranges. Parquet often omits blooms on sorted low-span columns anyway.
# Promo_sk (~500 NDV) is optional; we set bloom-filter-ndv so Iceberg still writes a filter.
FACT_ICEBERG_WRITE: dict[str, dict[str, object]] = {
    "store_sales": {
        "sort": ["ss_sold_date_sk", "ss_ticket_number", "ss_item_sk"],
        "bloom": ["ss_cdemo_sk", "ss_item_sk", "ss_promo_sk"],
        "bloom_verify": ["ss_cdemo_sk", "ss_item_sk"],
        # Only require blooms on files that can contain d_year=2000 rows (idb-bench q07).
        "bloom_verify_date": {
            "column": "ss_sold_date_sk",
            "min": 2_451_545,
            "max": 2_451_910,
        },
    },
    "catalog_sales": {
        "sort": ["cs_sold_date_sk", "cs_order_number", "cs_item_sk"],
        "bloom": ["cs_bill_customer_sk", "cs_item_sk", "cs_promo_sk"],
        # bill_customer bloom is best-effort (Parquet may skip on some files); item is required.
        "bloom_verify": ["cs_item_sk"],
    },
    "web_sales": {
        "sort": ["ws_sold_date_sk", "ws_order_number", "ws_item_sk"],
        "bloom": ["ws_bill_customer_sk", "ws_item_sk", "ws_promo_sk"],
        "bloom_verify": ["ws_item_sk"],
    },
    "store_returns": {
        "sort": ["sr_returned_date_sk", "sr_ticket_number"],
        "bloom": ["sr_customer_sk", "sr_item_sk"],
        "bloom_verify": ["sr_item_sk"],
    },
    "catalog_returns": {
        "sort": ["cr_returned_date_sk", "cr_order_number"],
        "bloom": ["cr_item_sk"],
        "bloom_verify": ["cr_item_sk"],
    },
    "web_returns": {
        "sort": ["wr_returned_date_sk", "wr_order_number"],
        "bloom": ["wr_item_sk"],
        "bloom_verify": ["wr_item_sk"],
    },
    # sf10 inventory ~133M rows: sorted write often completes without Parquet blooms
    # (driver OOM / GCLocker warnings); not queried by idb-bench q01/q02/q03/q07.
    "inventory": {
        "sort": ["inv_date_sk", "inv_item_sk", "inv_warehouse_sk"],
        "bloom": [],
        "bloom_verify": [],
    },
}

# ~128 MiB target data files (Iceberg will split sorted output).
TARGET_FILE_SIZE_BYTES = 134_217_728
# Parquet bloom bitset size per column per row group (Iceberg default 1 MiB).
BLOOM_FILTER_MAX_BYTES = 1_048_576
# TPC-DS sf10 approximate NDV hints so Iceberg sizes blooms on every output file.
SF10_BLOOM_NDV: dict[str, int] = {
    "ss_cdemo_sk": 1_920_800,
    "ss_item_sk": 102_000,
    "ss_promo_sk": 500,
    "cs_bill_customer_sk": 12_000_000,
    "cs_item_sk": 102_000,
    "cs_promo_sk": 500,
    "ws_bill_customer_sk": 12_000_000,
    "ws_item_sk": 102_000,
    "ws_promo_sk": 500,
    "sr_customer_sk": 12_000_000,
    "sr_item_sk": 102_000,
    "cr_item_sk": 102_000,
    "wr_item_sk": 102_000,
    "inv_item_sk": 102_000,
}

TPCDS_TABLES = [
    "call_center",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]


def file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def bloom_ndv_for_columns(bloom_cols: list[str], extra: dict[str, int] | None) -> dict[str, int]:
    """Merge per-table NDV hints with sf10 defaults for every bloom column."""
    out: dict[str, int] = {}
    for col in bloom_cols:
        if col in SF10_BLOOM_NDV:
            out[col] = SF10_BLOOM_NDV[col]
    if extra:
        out.update(extra)
    return out


def _file_overlaps_date_range(
    con,
    path: Path,
    date_column: str,
    range_min: int,
    range_max: int,
) -> bool:
    p = path.as_posix()
    row = con.execute(
        f"""
        SELECT min(CAST(stats_min AS BIGINT)), max(CAST(stats_max AS BIGINT))
        FROM parquet_metadata('{p}')
        WHERE path_in_schema = '{date_column}'
        """
    ).fetchone()
    if not row or row[0] is None or row[1] is None:
        return True
    fmin, fmax = int(row[0]), int(row[1])
    return not (fmax < range_min or fmin > range_max)


def verify_parquet_bloom_columns(
    data_dir: Path,
    columns: list[str],
    *,
    date_filter: dict[str, int | str] | None = None,
) -> None:
    """Fail if any checked data file lacks bloom filters on required columns."""
    import duckdb

    files = sorted(data_dir.glob("*.parquet"))
    if not files:
        raise RuntimeError(f"no data parquet under {data_dir}")

    con = duckdb.connect()
    checked = 0
    skipped = 0
    missing: list[str] = []
    for path in files:
        if date_filter:
            col = str(date_filter["column"])
            lo = int(date_filter["min"])
            hi = int(date_filter["max"])
            if not _file_overlaps_date_range(con, path, col, lo, hi):
                skipped += 1
                continue
        checked += 1
        p = path.as_posix()
        for col in columns:
            row = con.execute(
                f"""
                SELECT bool_or(bloom_filter_offset IS NOT NULL)
                FROM parquet_metadata('{p}')
                WHERE path_in_schema = '{col}'
                """
            ).fetchone()
            if not row or not row[0]:
                missing.append(f"{path.name}:{col}")

    if missing:
        raise RuntimeError(
            "Parquet bloom filters missing after Iceberg write (use tableProperty "
            f"write.parquet.bloom-filter-enabled.column.*; set bloom-filter-ndv "
            f"for high-cardinality FKs). Missing {len(missing)} file-column pair(s) "
            f"among {checked} checked file(s) ({skipped} skipped by date filter): "
            f"{', '.join(missing[:8])}"
            + (" ..." if len(missing) > 8 else "")
        )
    if date_filter and checked == 0:
        raise RuntimeError(
            f"no data files overlapped date filter {date_filter} under {data_dir}"
        )


def apply_fact_table_properties(
    writer,
    bloom_cols: list[str],
    bloom_ndv: dict[str, int] | None = None,
):
    """Iceberg bloom + file layout must be tableProperty on CTAS, not .option()."""
    writer = (
        writer.tableProperty(
            "write.target-file-size-bytes", str(TARGET_FILE_SIZE_BYTES)
        )
        .tableProperty(
            "write.parquet.row-group-size-bytes", str(128 * 1024 * 1024)
        )
        .tableProperty(
            "write.parquet.bloom-filter-max-bytes", str(BLOOM_FILTER_MAX_BYTES)
        )
    )
    for col in bloom_cols:
        writer = writer.tableProperty(
            f"write.parquet.bloom-filter-enabled.column.{col}", "true"
        )
    for col, ndv in (bloom_ndv or {}).items():
        writer = writer.tableProperty(
            f"write.parquet.bloom-filter-ndv.column.{col}", str(ndv)
        )
    return writer


def parquet_files(parquet_root: Path, table: str) -> list[Path]:
    table_dir = parquet_root / table
    if not table_dir.is_dir():
        return []
    return sorted(table_dir.glob("*.parquet"))


def clean_schema_dir(warehouse_root: Path, schema: str) -> None:
    schema_dir = warehouse_root / schema
    if schema_dir.exists():
        shutil.rmtree(schema_dir)
        print(f"removed existing warehouse schema dir: {schema_dir}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet-root", required=True)
    parser.add_argument("--warehouse-root", required=True)
    parser.add_argument("--catalog", default="local")
    parser.add_argument("--schema", default="tpcds")
    parser.add_argument(
        "--iceberg-version",
        default="1.6.1",
        help="Iceberg runtime package version",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Delete existing {warehouse}/{schema} before loading (recommended after failed runs)",
    )
    parser.add_argument(
        "--driver-memory",
        default="8g",
        help="Spark driver memory (SF10+ needs more headroom)",
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated subset of TPCDS_TABLES (default: all)",
    )
    parser.add_argument(
        "--skip-bloom-verify",
        action="store_true",
        help="Do not assert Parquet bloom footers on fact tables (debug only)",
    )
    args = parser.parse_args()
    if platform.system().lower().startswith("win"):
        has_hadoop = bool(os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir"))
        if not has_hadoop:
            raise RuntimeError(
                "Windows Spark requires HADOOP_HOME (winutils.exe). "
                "Set HADOOP_HOME or hadoop.home.dir, then rerun setup-local-tpcds.ps1."
            )

    parquet_root = Path(args.parquet_root).resolve()
    warehouse_root = Path(args.warehouse_root).resolve()
    warehouse_root.mkdir(parents=True, exist_ok=True)

    if args.fresh:
        clean_schema_dir(warehouse_root, args.schema)

    pkg = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{args.iceberg_version}"
    builder = (
        SparkSession.builder.appName("tpcds-parquet-to-iceberg")
        .master("local[*]")
        .config("spark.jars.packages", pkg)
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config(
            f"spark.sql.catalog.{args.catalog}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{args.catalog}.type", "hadoop")
        .config(
            f"spark.sql.catalog.{args.catalog}.warehouse",
            file_uri(warehouse_root),
        )
    )
    if platform.system().lower().startswith("win"):
        # Prefer file:/// URIs on Windows (Spark/Hadoop mishandle file:/C:/...).
        builder = builder.config(
            "spark.hadoop.fs.file.impl.disable.cache", "true"
        )
    spark = builder.getOrCreate()

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.schema}")

    tables = TPCDS_TABLES
    if args.tables.strip():
        wanted = {t.strip() for t in args.tables.split(",") if t.strip()}
        unknown = wanted - set(TPCDS_TABLES)
        if unknown:
            raise RuntimeError(f"unknown --tables: {sorted(unknown)}")
        tables = [t for t in TPCDS_TABLES if t in wanted]

    loaded = 0
    for table in tables:
        files = parquet_files(parquet_root, table)
        if not files:
            raise RuntimeError(
                f"no parquet for table '{table}' under {parquet_root / table} "
                f"(run generate-tpcds-parquet first)"
            )

        fq = f"{args.catalog}.{args.schema}.{table}"
        spark.sql(f"DROP TABLE IF EXISTS {fq}")

        # Read with explicit paths (avoids backtick glob + file:/ URI quirks on Windows).
        paths = [f.as_posix() for f in files]
        df = spark.read.parquet(*paths)

        write_spec = FACT_ICEBERG_WRITE.get(table)
        if write_spec:
            sort_cols = write_spec["sort"]
            assert isinstance(sort_cols, list)
            df = df.sort(*sort_cols)
            bloom_cols = write_spec.get("bloom", [])
            assert isinstance(bloom_cols, list)
            writer = df.writeTo(fq).using("iceberg")
            if bloom_cols:
                extra_ndv = write_spec.get("bloom_ndv", {})
                assert isinstance(extra_ndv, dict)
                bloom_ndv = bloom_ndv_for_columns(bloom_cols, extra_ndv)
                writer = apply_fact_table_properties(writer, bloom_cols, bloom_ndv)
            else:
                writer = writer.tableProperty(
                    "write.target-file-size-bytes", str(TARGET_FILE_SIZE_BYTES)
                ).tableProperty(
                    "write.parquet.row-group-size-bytes", str(128 * 1024 * 1024)
                )
            writer.create()
        else:
            df.writeTo(fq).using("iceberg").create()

        meta_hint = (
            warehouse_root / args.schema / table / "metadata" / "version-hint.text"
        )
        if not meta_hint.is_file():
            raise RuntimeError(
                f"table {table} missing Iceberg metadata {meta_hint} after write"
            )

        count = spark.sql(f"SELECT COUNT(*) AS c FROM {fq}").collect()[0][0]
        data_files = list((warehouse_root / args.schema / table).rglob("*.parquet"))
        print(
            f"registered iceberg table: {table} rows={count} "
            f"data_parquet_files={len(data_files)}"
        )
        if count == 0:
            raise RuntimeError(f"table {table} has zero rows after load")

        write_spec = FACT_ICEBERG_WRITE.get(table)
        if write_spec and not args.skip_bloom_verify:
            verify_cols = write_spec.get("bloom_verify", write_spec.get("bloom", []))
            if isinstance(verify_cols, list) and verify_cols:
                data_dir = warehouse_root / args.schema / table / "data"
                date_filter = write_spec.get("bloom_verify_date")
                if date_filter is not None and not isinstance(date_filter, dict):
                    raise RuntimeError(f"{table}: bloom_verify_date must be a dict")
                verify_parquet_bloom_columns(
                    data_dir, verify_cols, date_filter=date_filter
                )
                scope = ""
                if date_filter:
                    scope = (
                        f" (files overlapping {date_filter['column']} "
                        f"[{date_filter['min']},{date_filter['max']}])"
                    )
                print(f"  bloom filters OK on {table}: {', '.join(verify_cols)}{scope}")
            elif write_spec.get("bloom"):
                print(f"  note: {table} has bloom columns but no bloom_verify list")

        loaded += 1

    spark.stop()
    print(f"done: loaded {loaded} tables into {warehouse_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
