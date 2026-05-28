//! Run a full TPC-DS suite against both engines.

use anyhow::Result;

use crate::config::BenchConfig;
use crate::duckdb::DuckDbBench;
use crate::engine::{BenchEngine, QueryRunResult};
use crate::iceberg_db::IcebergDbEngine;
use crate::report::{compare_runs, summarize, BenchReport};
use crate::tpcds::load_manifest;

pub async fn run_tpcds_suite(config: &BenchConfig) -> Result<BenchReport> {
    let queries = load_manifest(&config.manifest)?;

    let mut iceberg = IcebergDbEngine::open(config).await?;
    let mut duckdb = DuckDbBench::open(&config.duckdb)?;

    iceberg.prepare().await?;
    duckdb.prepare().await?;

    let iceberg_results = run_engine(&mut iceberg, &queries, config.warmup, config.iterations).await;

    let duckdb_results = run_engine(&mut duckdb, &queries, config.warmup, config.iterations).await;

    Ok(compare_runs(
        "tpcds",
        summarize(iceberg.name(), &iceberg_results),
        summarize(duckdb.name(), &duckdb_results),
    ))
}

async fn run_engine(
    engine: &mut dyn BenchEngine,
    queries: &[crate::tpcds::TpcdsQuery],
    warmup: bool,
    iterations: u32,
) -> Vec<QueryRunResult> {
    let mut results = Vec::new();
    for q in queries {
        let sql = &q.sql;
        if warmup {
            let _ = engine.run_query(&q.id, &sql).await;
        }
        let mut last = QueryRunResult {
            query_id: q.id.clone(),
            elapsed_ms: 0,
            row_count: 0,
            error: Some("no iterations".into()),
        };
        for _ in 0..iterations.max(1) {
            last = engine.run_query(&q.id, &sql).await;
        }
        results.push(last);
    }
    results
}
