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

    loaded = 0
    for table in TPCDS_TABLES:
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
        loaded += 1

    spark.stop()
    print(f"done: loaded {loaded} tables into {warehouse_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
