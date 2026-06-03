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
    sections.push(summarize_planned_files(&iceberg_plan));
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

/// One-line-per-scan summary of `plan_files()` task counts (set during EXPLAIN ANALYZE execute).
fn summarize_planned_files(explain_text: &str) -> String {
    let mut lines = vec![
        "=== iceberg-db-rs: planned_files (plan_files task count per IcebergTableScan) ==="
            .to_string(),
    ];

    for chunk in explain_text.split("IcebergTableScan") {
        if chunk.is_empty() {
            continue;
        }
        let Some(planned) = parse_planned_files_count(chunk) else {
            continue;
        };
        let label = infer_scan_table_label(chunk);
        let date_filter = if chunk.contains("ss_sold_date_sk >=") || chunk.contains("d_date_sk >=") {
            "range"
        } else if chunk.contains("ss_sold_date_sk IN") || chunk.contains("d_date_sk IN") {
            "IN"
        } else {
            "-"
        };
        lines.push(format!(
            "  {label}: planned_files={planned} (date FK pushdown: {date_filter})"
        ));
    }

    if lines.len() == 1 {
        lines.push(
            "  (no planned_files= in plan — run EXPLAIN ANALYZE, not EXPLAIN only)".into(),
        );
    }

    lines.join("\n")
}

fn parse_planned_files_count(scan_suffix: &str) -> Option<usize> {
    let needle = "planned_files=";
    let start = scan_suffix.find(needle)? + needle.len();
    let num: usize = scan_suffix[start..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()?;
    (num > 0).then_some(num)
}

fn infer_scan_table_label(scan_suffix: &str) -> &'static str {
    if scan_suffix.contains("projection:[ss_") {
        "store_sales"
    } else if scan_suffix.contains("projection:[d_") {
        "date_dim"
    } else if scan_suffix.contains("projection:[cd_") {
        "customer_demographics"
    } else if scan_suffix.contains("projection:[i_") {
        "item"
    } else if scan_suffix.contains("projection:[p_") {
        "promotion"
    } else if scan_suffix.contains("projection:[c_") {
        "customer"
    } else {
        "other"
    }
}
