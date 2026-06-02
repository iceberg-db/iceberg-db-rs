//! Benchmark configuration (YAML).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct BenchConfig {
    /// Iceberg Hadoop-style warehouse root (absolute path).
    pub warehouse: PathBuf,
    /// DataFusion / Iceberg catalog name (default `local`).
    #[serde(default = "default_catalog")]
    pub catalog: String,
    /// Default schema / Iceberg namespace for TPC-DS tables (default `tpcds`).
    #[serde(default = "default_schema")]
    pub schema: String,
    /// Optional `idb` config file (REST catalogs); if set, `warehouse` is ignored for iceberg-db.
    pub iceberg_config: Option<PathBuf>,
    pub duckdb: DuckDbConfig,
    /// Query manifest TOML (lists `[[query]]` entries).
    #[serde(default = "default_manifest")]
    pub manifest: PathBuf,
    /// Number of timed iterations per query per engine (default 1).
    #[serde(default = "default_iterations")]
    pub iterations: u32,
    /// Optional warmup iteration before timing (not recorded).
    #[serde(default = "default_warmup")]
    pub warmup: bool,
    /// DataFusion `target_partitions` for iceberg-db-rs (unset = CPU count).
    pub target_partitions: Option<usize>,
    /// DataFusion `optimizer.repartition_file_scans` (unset = default true).
    pub repartition_file_scans: Option<bool>,
    /// DataFusion `optimizer.enable_join_dynamic_filter_pushdown` (unset = default true).
    pub enable_join_dynamic_filter_pushdown: Option<bool>,
    /// DataFusion `optimizer.repartition_joins` (unset = default true).
    pub repartition_joins: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DuckDbConfig {
    /// Iceberg Hadoop warehouse root (defaults to top-level `warehouse` in bench.yaml).
    pub warehouse: Option<PathBuf>,
    /// Deprecated: use `warehouse` + DuckDB Iceberg extension instead of raw Parquet.
    pub parquet_root: Option<PathBuf>,
    /// Optional on-disk DuckDB file; if omitted, uses an in-memory database.
    pub database: Option<PathBuf>,
    /// Schema / namespace for TPC-DS tables (default `tpcds`).
    #[serde(default = "default_schema")]
    pub schema: String,
}

fn default_catalog() -> String {
    "local".to_string()
}

fn default_schema() -> String {
    "tpcds".to_string()
}

fn default_manifest() -> PathBuf {
    PathBuf::from("benchmarks/tpcds/manifest.toml")
}

fn default_iterations() -> u32 {
    1
}

fn default_warmup() -> bool {
    true
}

impl BenchConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read bench config {}", path.display()))?;
        serde_yaml::from_str(&text).context("parse bench YAML")
    }
}
