//! Benchmark results and iceberg-db vs DuckDB comparison.

use serde::{Deserialize, Serialize};

use crate::engine::QueryRunResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchReport {
    pub suite: String,
    pub iceberg_db: EngineRunSummary,
    pub duckdb: EngineRunSummary,
    pub comparisons: Vec<QueryComparison>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineRunSummary {
    pub engine: String,
    pub queries_run: usize,
    pub queries_ok: usize,
    pub queries_failed: usize,
    pub total_elapsed_ms: u64,
    pub results: Vec<QueryRunResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryComparison {
    pub query_id: String,
    pub iceberg_ms: Option<u64>,
    pub duckdb_ms: Option<u64>,
    pub speedup_vs_duckdb: Option<f64>,
    pub iceberg_rows: Option<usize>,
    pub duckdb_rows: Option<usize>,
    pub row_count_match: Option<bool>,
    pub iceberg_error: Option<String>,
    pub duckdb_error: Option<String>,
}

pub fn summarize(engine: &str, results: &[QueryRunResult]) -> EngineRunSummary {
    let queries_run = results.len();
    let queries_ok = results.iter().filter(|r| r.error.is_none()).count();
    let total_elapsed_ms = results.iter().map(|r| r.elapsed_ms).sum();
    EngineRunSummary {
        engine: engine.to_string(),
        queries_run,
        queries_ok,
        queries_failed: queries_run - queries_ok,
        total_elapsed_ms,
        results: results.to_vec(),
    }
}

pub fn compare_runs(
    suite: &str,
    iceberg: EngineRunSummary,
    duckdb: EngineRunSummary,
) -> BenchReport {
    let mut ids: Vec<String> = iceberg
        .results
        .iter()
        .map(|r| r.query_id.clone())
        .chain(duckdb.results.iter().map(|r| r.query_id.clone()))
        .collect();
    ids.sort();
    ids.dedup();

    let comparisons: Vec<QueryComparison> = ids
        .into_iter()
        .map(|id| {
            let i = iceberg.results.iter().find(|r| r.query_id == id);
            let d = duckdb.results.iter().find(|r| r.query_id == id);
            let iceberg_ms = i.map(|r| r.elapsed_ms);
            let duckdb_ms = d.map(|r| r.elapsed_ms);
            let speedup = match (iceberg_ms, duckdb_ms) {
                (Some(i_ms), Some(d_ms)) if i_ms > 0 => Some(d_ms as f64 / i_ms as f64),
                _ => None,
            };
            let iceberg_rows = i.map(|r| r.row_count);
            let duckdb_rows = d.map(|r| r.row_count);
            let row_count_match = match (iceberg_rows, duckdb_rows) {
                (Some(a), Some(b)) => Some(a == b),
                _ => None,
            };
            QueryComparison {
                query_id: id,
                iceberg_ms,
                duckdb_ms,
                speedup_vs_duckdb: speedup,
                iceberg_rows,
                duckdb_rows,
                row_count_match,
                iceberg_error: i.and_then(|r| r.error.clone()),
                duckdb_error: d.and_then(|r| r.error.clone()),
            }
        })
        .collect();

    BenchReport {
        suite: suite.to_string(),
        iceberg_db: iceberg,
        duckdb,
        comparisons,
    }
}

pub fn print_report(report: &BenchReport) {
    println!("\n=== TPC-DS benchmark: {} ===\n", report.suite);
    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>8}",
        "engine", "ok", "failed", "total_ms", "queries"
    );
    for s in [&report.iceberg_db, &report.duckdb] {
        println!(
            "{:<12} {:>10} {:>10} {:>10} {:>8}",
            s.engine, s.queries_ok, s.queries_failed, s.total_elapsed_ms, s.queries_run
        );
    }
    println!(
        "\n{:<8} {:>12} {:>12} {:>10} {:>8}",
        "query", "iceberg_ms", "duckdb_ms", "speedup", "rows_ok"
    );
    for c in &report.comparisons {
        let speedup = c
            .speedup_vs_duckdb
            .map(|x| format!("{x:.2}x"))
            .unwrap_or_else(|| "-".into());
        let rows = c
            .row_count_match
            .map(|m| if m { "yes" } else { "NO" })
            .unwrap_or("-");
        let iceberg_ms = if c.iceberg_error.is_some() {
            "ERR".to_string()
        } else {
            c.iceberg_ms
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into())
        };
        let duckdb_ms = if c.duckdb_error.is_some() {
            "ERR".to_string()
        } else {
            c.duckdb_ms
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into())
        };
        println!(
            "{:<8} {:>12} {:>12} {:>10} {:>8}",
            c.query_id, iceberg_ms, duckdb_ms, speedup, rows
        );
        if let Some(err) = &c.iceberg_error {
            println!("  iceberg-db-rs: {err}");
        }
        if let Some(err) = &c.duckdb_error {
            println!("  duckdb: {err}");
        }
    }
}
