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
}

#[derive(Debug, Clone, Deserialize)]
pub struct DuckDbConfig {
    /// Directory with one sub-folder (or parquet files) per TPC-DS table name.
    pub parquet_root: PathBuf,
    /// Optional on-disk DuckDB file; if omitted, uses an in-memory database.
    pub database: Option<PathBuf>,
    /// Schema name for DuckDB views (default `tpcds`).
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
