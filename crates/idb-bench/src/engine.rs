//! Shared benchmark engine interface.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRunResult {
    pub query_id: String,
    /// Primary latency: median when `iterations` > 1, else the single timed run.
    pub elapsed_ms: u64,
    /// Mean across timed iterations (present when `iterations` > 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean_elapsed_ms: Option<u64>,
    /// Number of timed iterations aggregated into `elapsed_ms` / `mean_elapsed_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timed_iterations: Option<u32>,
    pub row_count: usize,
    pub error: Option<String>,
}

#[async_trait]
pub trait BenchEngine: Send {
    fn name(&self) -> &'static str;

    /// One-time setup (register tables / views).
    async fn prepare(&mut self) -> Result<()>;

    /// Execute SQL and return timing + row count. Implementations should fully consume results.
    async fn run_query(&mut self, query_id: &str, sql: &str) -> QueryRunResult;
}
