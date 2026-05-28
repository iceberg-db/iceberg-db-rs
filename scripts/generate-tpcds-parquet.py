#!/usr/bin/env python3
"""
Generate TPC-DS data as local Parquet files using DuckDB's tpcds extension.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="Output parquet root directory")
    parser.add_argument("--sf", type=float, default=1.0, help="TPC-DS scale factor")
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

    for table in tables:
        if table == "dbgen_version":
            continue
        table_dir = out / table
        table_dir.mkdir(parents=True, exist_ok=True)
        file_path = table_dir / "part-00000.parquet"
        con.execute(
            f"COPY (SELECT * FROM {table}) TO '{file_path.as_posix()}' (FORMAT PARQUET);"
        )
        print(f"wrote {table}: {file_path}")

    con.close()
    print(f"done: parquet_root={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
