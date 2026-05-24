use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use idb_config::default_config_path;
use idb_core::Engine;

#[derive(Parser)]
#[command(name = "idb", about = "iceberg-db Rust SQL engine (native)")]
struct Args {
    /// Path to config.yaml (same format as Java iceberg-db)
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Local warehouse directory (Hadoop-style layout; ignores config file)
    #[arg(short, long)]
    warehouse: Option<PathBuf>,

    /// Default catalog name when using --warehouse
    #[arg(long, default_value = "local")]
    catalog: String,

    /// Run a single SQL statement and print row count
    #[arg(short, long)]
    execute: Option<String>,

    /// Print logical plan instead of executing
    #[arg(long)]
    explain: bool,

    /// Log each HTTP request to the REST catalog (secrets redacted)
    #[arg(long)]
    log_http: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    if args.log_http {
        // Set before catalog HTTP; idb-catalog reads IDB_LOG_HTTP.
        std::env::set_var("IDB_LOG_HTTP", "1");
        eprintln!("HTTP logging enabled (secrets redacted).");
    }

    let engine = if let Some(warehouse) = args.warehouse {
        eprintln!(
            "Using Hadoop-style warehouse: {}",
            warehouse.display()
        );
        Engine::from_warehouse(&warehouse, &args.catalog).await?
    } else {
        let config = args.config.unwrap_or_else(default_config_path);
        eprintln!("Using config: {}", config.display());
        let cfg = idb_config::load(&config)?;
        eprintln!(
            "Default catalog: {} ({} configured)",
            cfg.default_catalog.as_deref().unwrap_or("(first)"),
            cfg.catalogs.len()
        );
        for (name, spec) in &cfg.catalogs {
            let profile = spec.profile_name().unwrap_or_default();
            let wh = spec.property("warehouse").unwrap_or_default();
            eprintln!(
                "  - {name}: type={} profile={profile} warehouse={wh}",
                spec.catalog_type
            );
        }
        Engine::from_config_file(&config).await?
    };

    let sql = match args.execute {
        Some(sql) => sql,
        None => {
            eprintln!("No SQL provided. Use -e \"SELECT ...\" (interactive shell: later phase).");
            return Ok(());
        }
    };

    if args.explain {
        println!("{}", engine.explain(&sql).await?);
    } else {
        let result = engine.query(&sql).await?;
        eprintln!(
            "{} row(s) in {} ms",
            result.row_count, result.elapsed_ms
        );
        for batch in &result.batches {
            print_batches(batch);
        }
    }
    Ok(())
}

fn print_batches(batch: &datafusion::arrow::record_batch::RecordBatch) {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    match pretty_format_batches(&[batch.clone()]) {
        Ok(table) => println!("{table}"),
        Err(e) => eprintln!("(could not format batch: {e})"),
    }
}
