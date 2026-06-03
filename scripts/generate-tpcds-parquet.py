#!/usr/bin/env python3
"""
Generate TPC-DS data as local Parquet files using DuckDB's tpcds extension.

With --layout star (default), fact tables are written as sorted, sharded Parquet with
smaller row groups so Iceberg/DataFusion can prune row groups on FK IN/range predicates.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb

# Fact tables: sort key + FK columns (for shard count / future bloom at Iceberg write).
FACT_LAYOUTS: dict[str, dict[str, object]] = {
    "store_sales": {
        "order_by": "ss_sold_date_sk, ss_ticket_number, ss_item_sk",
        "shard_cols": ("ss_sold_date_sk", "ss_ticket_number"),
        "bloom_cols": ("ss_sold_date_sk", "ss_cdemo_sk", "ss_item_sk", "ss_promo_sk"),
    },
    "catalog_sales": {
        "order_by": "cs_sold_date_sk, cs_order_number, cs_item_sk",
        "shard_cols": ("cs_sold_date_sk", "cs_order_number"),
        "bloom_cols": ("cs_sold_date_sk", "cs_bill_customer_sk", "cs_item_sk", "cs_promo_sk"),
    },
    "web_sales": {
        "order_by": "ws_sold_date_sk, ws_order_number, ws_item_sk",
        "shard_cols": ("ws_sold_date_sk", "ws_order_number"),
        "bloom_cols": ("ws_sold_date_sk", "ws_bill_customer_sk", "ws_item_sk", "ws_promo_sk"),
    },
    "store_returns": {
        "order_by": "sr_returned_date_sk, sr_ticket_number",
        "shard_cols": ("sr_returned_date_sk", "sr_ticket_number"),
        "bloom_cols": ("sr_returned_date_sk", "sr_customer_sk", "sr_item_sk"),
    },
    "catalog_returns": {
        "order_by": "cr_returned_date_sk, cr_order_number",
        "shard_cols": ("cr_returned_date_sk", "cr_order_number"),
        "bloom_cols": ("cr_returned_date_sk", "cr_item_sk"),
    },
    "web_returns": {
        "order_by": "wr_returned_date_sk, wr_order_number",
        "shard_cols": ("wr_returned_date_sk", "wr_order_number"),
        "bloom_cols": ("wr_returned_date_sk", "wr_item_sk"),
    },
    "inventory": {
        "order_by": "inv_date_sk, inv_item_sk, inv_warehouse_sk",
        "shard_cols": ("inv_date_sk", "inv_item_sk"),
        "bloom_cols": ("inv_date_sk", "inv_item_sk", "inv_warehouse_sk"),
    },
}

ROW_GROUP_SIZE = 131_072
COMPRESSION = "ZSTD"


def shard_count(scale_factor: float) -> int:
    """More scale → more Parquet shards (keeps files roughly similar size)."""
    return max(8, min(128, int(8 * scale_factor)))


def write_dimension(con: duckdb.DuckDBPyConnection, table: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (SELECT * FROM {table}) TO '{out_path.as_posix()}' (
            FORMAT PARQUET,
            COMPRESSION {COMPRESSION},
            ROW_GROUP_SIZE {ROW_GROUP_SIZE}
        )
        """
    )
    print(f"wrote {table}: {out_path}")


def write_fact_star(
    con: duckdb.DuckDBPyConnection,
    table: str,
    spec: dict[str, object],
    table_dir: Path,
    num_shards: int,
) -> None:
    if table_dir.exists():
        shutil.rmtree(table_dir)
    table_dir.mkdir(parents=True, exist_ok=True)

    shard_cols = spec["shard_cols"]
    assert isinstance(shard_cols, tuple)
    shard_expr = f"(hash({shard_cols[0]}, {shard_cols[1]}) % {num_shards})"
    order_by = spec["order_by"]
    assert isinstance(order_by, str)

    for shard in range(num_shards):
        out_path = table_dir / f"part-{shard:05d}.parquet"
        con.execute(
            f"""
            COPY (
                SELECT * FROM {table}
                WHERE {shard_expr} = {shard}
                ORDER BY {order_by}
            ) TO '{out_path.as_posix()}' (
                FORMAT PARQUET,
                COMPRESSION {COMPRESSION},
                ROW_GROUP_SIZE {ROW_GROUP_SIZE}
            )
            """
        )
    parts = sorted(table_dir.glob("*.parquet"))
    print(f"wrote {table}: {len(parts)} parquet file(s) under {table_dir}")


def write_fact_legacy(con: duckdb.DuckDBPyConnection, table: str, out_path: Path) -> None:
    write_dimension(con, table, out_path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="Output parquet root directory")
    parser.add_argument("--sf", type=float, default=1.0, help="TPC-DS scale factor")
    parser.add_argument(
        "--layout",
        choices=("star", "legacy"),
        default="star",
        help="star: sorted sharded facts with smaller row groups; legacy: one file per table",
    )
    parser.add_argument(
        "--duckdb-file",
        default="tpcds-gen.duckdb",
        help="Temporary DuckDB database file",
    )
    args = parser.parse_args()

    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    db_file = Path(args.duckdb_file).resolve()

    con = duckdb.connect(str(db_file))
    con.execute("INSTALL tpcds;")
    con.execute("LOAD tpcds;")
    con.execute(f"CALL dsdgen(sf={args.sf});")

    tables = [
        row[0]
        for row in con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        ).fetchall()
    ]

    shards = shard_count(args.sf)
    use_star = args.layout == "star"

    for table in tables:
        if table == "dbgen_version":
            continue
        if use_star and table in FACT_LAYOUTS:
            write_fact_star(con, table, FACT_LAYOUTS[table], out / table, shards)
        else:
            write_fact_legacy(con, table, out / table / "part-00000.parquet")

    con.close()
    print(f"done: parquet_root={out} layout={args.layout} fact_shards={shards if use_star else 'n/a'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
