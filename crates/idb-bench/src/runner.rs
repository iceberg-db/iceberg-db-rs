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
    let mut duckdb = DuckDbBench::open(config)?;

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
    let timed_iters = iterations.max(1);
    let mut results = Vec::new();
    for q in queries {
        let sql = &q.sql;
        if warmup {
            let _ = engine.run_query(&q.id, &sql).await;
        }
        let mut samples_ms = Vec::with_capacity(timed_iters as usize);
        let mut last = QueryRunResult {
            query_id: q.id.clone(),
            elapsed_ms: 0,
            mean_elapsed_ms: None,
            timed_iterations: None,
            row_count: 0,
            error: Some("no iterations".into()),
        };
        for _ in 0..timed_iters {
            last = engine.run_query(&q.id, &sql).await;
            if last.error.is_none() {
                samples_ms.push(last.elapsed_ms);
            }
        }
        if timed_iters > 1 && !samples_ms.is_empty() {
            last.elapsed_ms = median_ms(&samples_ms);
            last.mean_elapsed_ms = Some(mean_ms(&samples_ms));
            last.timed_iterations = Some(timed_iters);
        }
        results.push(last);
    }
    results
}

fn median_ms(samples: &[u64]) -> u64 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }
    (sorted[(n - 1) / 2] + sorted[n / 2]) / 2
}

fn mean_ms(samples: &[u64]) -> u64 {
    let sum: u64 = samples.iter().sum();
    sum / samples.len() as u64
}
