//! `idb-bench` — compare iceberg-db-rs vs DuckDB on TPC-DS.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use idb_bench::config::BenchConfig;
use idb_bench::history::{record_run, DEFAULT_HISTORY_JSONL, DEFAULT_HISTORY_MARKDOWN};
use idb_bench::iceberg_db::IcebergDbEngine;
use idb_bench::report::print_report;
use idb_bench::runner::run_tpcds_suite;
use idb_bench::tpcds::load_manifest;

#[derive(Parser)]
#[command(name = "idb-bench", about = "TPC-DS benchmarks: iceberg-db-rs vs DuckDB")]
struct Args {
    /// Benchmark YAML config (see benchmarks/tpcds/bench.example.yaml).
    #[arg(short, long, default_value = "benchmarks/tpcds/bench.example.yaml")]
    config: PathBuf,

    /// Write JSON report to this path.
    #[arg(long)]
    json: Option<PathBuf>,

    /// Skip writing to the audit history (default: record every run).
    #[arg(long)]
    no_history: bool,

    /// Path to JSONL history file.
    #[arg(long, default_value = DEFAULT_HISTORY_JSONL)]
    history_file: PathBuf,

    /// Path to regenerated markdown audit log.
    #[arg(long, default_value = DEFAULT_HISTORY_MARKDOWN)]
    history_markdown: PathBuf,

    /// Short label for this run (e.g. post-pushdown-patch).
    #[arg(long)]
    label: Option<String>,

    /// What changed since the last run — required for meaningful audit trails.
    #[arg(long)]
    notes: Option<String>,

    /// Structured change line (repeat for multiple items).
    #[arg(long = "change")]
    changes: Vec<String>,

    /// Regenerate benchmarks/tpcds/HISTORY.md from the JSONL file (no benchmark run).
    #[arg(long)]
    regenerate_history: bool,

    /// Only list queries from the manifest (no execution).
    #[arg(long)]
    list_queries: bool,

    /// Run iceberg-db-rs smoke queries and print errors (no DuckDB).
    #[arg(long)]
    smoke: bool,

    /// Run EXPLAIN ANALYZE for one manifest query id (e.g. q07) on both engines.
    #[arg(long)]
    explain: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let config = BenchConfig::load(&args.config)?;

    if args.regenerate_history {
        idb_bench::history::regenerate_audit_markdown(&args.history_file, &args.history_markdown)?;
        eprintln!("Regenerated {}", args.history_markdown.display());
        return Ok(());
    }

    if args.list_queries {
        let queries = load_manifest(&config.manifest)?;
        for q in queries {
            println!("{}  {}", q.id, q.path.display());
        }
        return Ok(());
    }

    if args.smoke {
        return run_smoke(&config).await;
    }

    if let Some(query_id) = args.explain {
        let report = idb_bench::explain::explain_compare(&config, &query_id).await?;
        println!("{report}");
        return Ok(());
    }

    let queries = load_manifest(&config.manifest)?;
    let query_ids: Vec<String> = queries.iter().map(|q| q.id.clone()).collect();

    let report = run_tpcds_suite(&config).await?;
    print_report(&report);

    if let Some(path) = args.json {
        let json = serde_json::to_string_pretty(&report)?;
        std::fs::write(&path, json)?;
        eprintln!("Wrote {}", path.display());
    }

    if !args.no_history {
        let notes = args.notes.unwrap_or_else(|| {
            eprintln!(
                "hint: pass --notes \"what changed\" so the audit log stays useful (see benchmarks/tpcds/HISTORY.md)"
            );
            String::new()
        });
        let history_path = record_run(
            &args.history_file,
            &args.history_markdown,
            &args.config,
            &config,
            &query_ids,
            &report,
            args.label,
            notes,
            args.changes,
        )?;
        eprintln!(
            "Recorded run → {} (audit log: {})",
            history_path.display(),
            args.history_markdown.display()
        );
    }

    if report.iceberg_db.queries_failed > 0 || report.duckdb.queries_failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

async fn run_smoke(config: &BenchConfig) -> Result<()> {
    let engine = IcebergDbEngine::open(config).await?;
    let checks = [
        ("tables", "SHOW TABLES"),
        ("count", "SELECT COUNT(*) AS c FROM store_sales"),
        (
            "q03",
            "SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand, \
             sum(ss_ext_sales_price) sum_agg FROM date_dim dt, store_sales, item \
             WHERE dt.d_date_sk = store_sales.ss_sold_date_sk \
             AND store_sales.ss_item_sk = item.i_item_sk AND item.i_manufact_id = 128 \
             AND dt.d_moy = 11 \
             GROUP BY dt.d_year, item.i_brand, item.i_brand_id LIMIT 5",
        ),
    ];
    let mut failed = 0usize;
    for (name, sql) in checks {
        println!("\n--- smoke:{name} ---");
        match engine.run_sql(sql).await {
            Ok((rows, ms)) => println!("ok rows={rows} elapsed_ms={ms}"),
            Err(e) => {
                failed += 1;
                eprintln!("FAILED: {e:#}");
            }
        }
    }
    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}
