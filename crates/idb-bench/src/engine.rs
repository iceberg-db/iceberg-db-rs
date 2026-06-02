//! Shared benchmark engine interface.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRunResult {
    pub query_id: String,
    pub elapsed_ms: u64,
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
