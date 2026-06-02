//! Benchmark harness comparing **iceberg-db-rs** (Iceberg + DataFusion) to **DuckDB** (Iceberg extension).
//!
//! The primary suite is [TPC-DS](https://www.tpc.org/tpcds/): load query SQL from
//! `benchmarks/tpcds/queries/` and point both engines at the same dataset layout.

pub mod config;
pub mod engine;
pub mod explain;
pub mod history;
pub mod report;
pub mod runner;
pub mod tpcds;

#[cfg(feature = "duckdb-engine")]
pub mod duckdb;
pub mod iceberg_db;
pub mod qualify;

pub use config::BenchConfig;
pub use engine::{BenchEngine, QueryRunResult};
pub use report::{compare_runs, print_report, BenchReport, EngineRunSummary, QueryComparison};
pub use history::{
    record_run, RunHistoryEntry, DEFAULT_HISTORY_JSONL, DEFAULT_HISTORY_MARKDOWN,
};
pub use runner::run_tpcds_suite;
