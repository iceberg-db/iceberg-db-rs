"""Export tpcds-history.jsonl to JSON for canvas charts."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HISTORY = ROOT / "results" / "tpcds-history.jsonl"
QUERIES = ["q01", "q02", "q03", "q07"]


def extract_run(r: dict) -> dict | None:
    rep = r.get("report", {})
    comps = {c["query_id"]: c for c in (rep.get("comparisons") or [])}
    points = {}
    for q in QUERIES:
        c = comps.get(q)
        if c:
            ib = c.get("iceberg_ms")
            if c.get("iceberg_error"):
                ib = None
            points[q] = {
                "iceberg_ms": ib,
                "duckdb_ms": c.get("duckdb_ms"),
                "rows_ok": c.get("row_count_match", False) and not c.get("iceberg_error"),
            }
        else:
            points[q] = {"iceberg_ms": None, "duckdb_ms": None, "rows_ok": False}
    if not any(p.get("iceberg_ms") for p in points.values()):
        return None
    return {
        "label": r.get("label") or "unlabeled",
        "recorded_at": (r.get("recorded_at") or "")[:19],
        "queries": points,
    }


def main() -> None:
    runs = []
    with HISTORY.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            ex = extract_run(r)
            if ex:
                runs.append(ex)

    out = ROOT / "results" / "bench-history-export.json"
    out.write_text(json.dumps({"runs": runs, "queries": QUERIES}, indent=2), encoding="utf-8")
    print(f"wrote {len(runs)} runs to {out}")
    for run in runs:
        q07 = run["queries"].get("q07", {})
        ib = q07.get("iceberg_ms")
        dk = q07.get("duckdb_ms")
        print(
            f"  {run['recorded_at']} {run['label'][:28]:28} q07 ib={ib} dk={dk}"
        )


if __name__ == "__main__":
    main()
