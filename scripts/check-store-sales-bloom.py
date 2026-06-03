"""Check Parquet bloom filters and row-group layout for store_sales (or any fact table)."""
import argparse
import glob
import sys

import duckdb


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--warehouse",
        default="bench-data/tpcds-sf10/warehouse/tpcds/store_sales/data",
        help="Path to table data/ directory or warehouse root + --table",
    )
    parser.add_argument("--table", default="")
    parser.add_argument(
        "--required",
        default="ss_cdemo_sk,ss_item_sk",
        help="Columns that must have bloom_filter_offset on every sampled file",
    )
    parser.add_argument(
        "--optional",
        default="ss_promo_sk",
        help="Columns to report but not fail if bloom is absent (low NDV)",
    )
    parser.add_argument(
        "--date-column",
        default="ss_sold_date_sk",
        help="Date/sort key column for per-file min/max envelope (uses min/max, not bloom)",
    )
    args = parser.parse_args()

    data_dir = args.warehouse.replace("\\", "/")
    if args.table:
        data_dir = f"{data_dir.rstrip('/')}/{args.table}/data"
    elif not data_dir.endswith("/data"):
        data_dir = f"{data_dir.rstrip('/')}/data"

    required = [c.strip() for c in args.required.split(",") if c.strip()]
    optional = [c.strip() for c in args.optional.split(",") if c.strip()]
    files = sorted(glob.glob(f"{data_dir}/*.parquet"))
    if not files:
        print(f"no parquet under {data_dir}", file=sys.stderr)
        return 1

    con = duckdb.connect()
    print(f"data_dir={data_dir} data_files={len(files)}")
    return _report(con, files, required, optional, args.date_column)


def _report(
    con,
    files: list[str],
    required: list[str],
    optional: list[str],
    date_column: str,
) -> int:
    ok = True
    columns = required + optional
    for path in files[:3]:
        p = path.replace("\\", "/")
        print(f"\n--- {p.split('/')[-1]} ---")
        in_list = ",".join(f"'{c}'" for c in columns)
        rows = con.execute(
            f"""
            SELECT path_in_schema,
                   count(DISTINCT row_group_id) AS row_groups,
                   bool_or(bloom_filter_offset IS NOT NULL) AS has_bloom,
                   min(CAST(stats_min AS BIGINT)) AS min_sk,
                   max(CAST(stats_max AS BIGINT)) AS max_sk
            FROM parquet_metadata('{p}')
            WHERE path_in_schema IN ({in_list})
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()
        for r in rows:
            tag = "required" if r[0] in required else "optional"
            print(f"  {r[0]} ({tag}): rgs={r[1]} bloom={r[2]} min={r[3]} max={r[4]}")
            if r[0] in required and not r[2]:
                ok = False

    if date_column:
        print(f"\n--- per-file {date_column} envelope (all files) ---")
        for path in files:
            p = path.replace("\\", "/")
            lo, hi, rgs = con.execute(
                f"""
                SELECT min(CAST(stats_min AS BIGINT)), max(CAST(stats_max AS BIGINT)),
                       count(DISTINCT row_group_id)
                FROM parquet_metadata('{p}')
                WHERE path_in_schema = '{date_column}'
                """
            ).fetchone()
            print(f"  {p.split('/')[-1]}: [{lo},{hi}] row_groups={rgs}")

    if not ok:
        print(
            "\nFAIL: required column(s) missing bloom_filter_offset",
            file=sys.stderr,
        )
        return 1
    print("\nOK: required bloom filters present on sampled files")
    if optional:
        print(f"  (optional columns {optional} may omit bloom at low NDV)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
