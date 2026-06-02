//! Side-by-side EXPLAIN ANALYZE for iceberg-db-rs vs DuckDB on one TPC-DS query.

use anyhow::{Context, Result};

use crate::config::BenchConfig;
use crate::duckdb::DuckDbBench;
use crate::iceberg_db::IcebergDbEngine;
use crate::qualify::qualify_tpcds_sql;
use crate::tpcds::load_manifest;

pub async fn explain_compare(config: &BenchConfig, query_id: &str) -> Result<String> {
    let queries = load_manifest(&config.manifest)?;
    let query = queries
        .iter()
        .find(|q| q.id.eq_ignore_ascii_case(query_id))
        .with_context(|| format!("query '{query_id}' not found in {}", config.manifest.display()))?;

    let qualified = qualify_tpcds_sql(&query.sql, &config.schema);

    let mut sections = vec![
        format!("=== Query {query_id} ==="),
        format!("--- SQL (qualified) ---\n{qualified}\n"),
    ];

    let iceberg = IcebergDbEngine::open(config).await?;
    let iceberg_plan = iceberg.explain_analyze(&query.sql).await?;
    sections.push(format!(
        "=== iceberg-db-rs EXPLAIN ANALYZE (target_partitions={:?}) ===\n{iceberg_plan}",
        config.target_partitions
    ));

    #[cfg(feature = "duckdb-engine")]
    {
        let duckdb = DuckDbBench::open(config)?;
        let duckdb_plan = duckdb.explain_analyze(&query.sql)?;
        sections.push(format!("=== duckdb-iceberg EXPLAIN ANALYZE ===\n{duckdb_plan}"));
    }

    #[cfg(not(feature = "duckdb-engine"))]
    sections.push("=== duckdb-iceberg (disabled; rebuild with default features) ===".into());

    Ok(sections.join("\n\n"))
}
