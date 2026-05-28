//! DuckDB runner (bundled engine, Parquet-backed TPC-DS views).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use async_trait::async_trait;
use duckdb::Connection;

use crate::config::DuckDbConfig;
use crate::engine::{BenchEngine, QueryRunResult};
use crate::tpcds::tables::TPCDS_TABLES;

pub struct DuckDbBench {
    inner: Arc<Mutex<DuckDbState>>,
}

struct DuckDbState {
    conn: Connection,
    parquet_root: PathBuf,
    schema: String,
    prepared: bool,
}

impl DuckDbBench {
    pub fn open(config: &DuckDbConfig) -> Result<Self> {
        let conn = match &config.database {
            Some(path) => Connection::open(path)
                .with_context(|| format!("open DuckDB database {}", path.display()))?,
            None => Connection::open_in_memory().context("open in-memory DuckDB")?,
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(DuckDbState {
                conn,
                parquet_root: config.parquet_root.clone(),
                schema: config.schema.clone(),
                prepared: false,
            })),
        })
    }

    fn with_conn<F, T>(inner: &Arc<Mutex<DuckDbState>>, f: F) -> Result<T>
    where
        F: FnOnce(&mut DuckDbState) -> Result<T>,
    {
        let mut guard = inner.lock().map_err(|e| anyhow::anyhow!("duckdb lock: {e}"))?;
        f(&mut guard)
    }
}

#[async_trait]
impl BenchEngine for DuckDbBench {
    fn name(&self) -> &'static str {
        "duckdb"
    }

    async fn prepare(&mut self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            Self::with_conn(&inner, |state| {
                if state.prepared {
                    return Ok(());
                }
                let schema = &state.schema;
                state
                    .conn
                    .execute_batch(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
                    .context("create DuckDB schema")?;
                let mut registered = 0usize;
                for table in TPCDS_TABLES {
                    if let Some(glob) = parquet_glob(&state.parquet_root, table) {
                        let sql = format!(
                            "CREATE OR REPLACE TABLE {schema}.{table} AS \
                             SELECT * FROM read_parquet('{}');",
                            glob.replace('\\', "/")
                        );
                        state
                            .conn
                            .execute(&sql, [])
                            .with_context(|| format!("register {schema}.{table}"))?;
                        registered += 1;
                    }
                }
                if registered == 0 {
                    anyhow::bail!(
                        "no TPC-DS parquet under {}",
                        state.parquet_root.display()
                    );
                }
                state
                    .conn
                    .execute(&format!("USE {schema};"), [])
                    .with_context(|| format!("USE {schema}"))?;
                tracing::info!(registered, "DuckDB TPC-DS tables registered from parquet");
                state.prepared = true;
                Ok(())
            })
        })
        .await
        .context("duckdb prepare join")?
    }

    async fn run_query(&mut self, query_id: &str, sql: &str) -> QueryRunResult {
        let inner = Arc::clone(&self.inner);
        let query_id_owned = query_id.to_string();
        let sql = sql.to_string();
        match tokio::task::spawn_blocking(move || {
            Self::with_conn(&inner, |state| run_query_sync(&state.conn, &query_id_owned, &sql))
        })
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => QueryRunResult {
                query_id: query_id.to_string(),
                elapsed_ms: 0,
                row_count: 0,
                error: Some(format!("{e:#}")),
            },
            Err(e) => QueryRunResult {
                query_id: query_id.to_string(),
                elapsed_ms: 0,
                row_count: 0,
                error: Some(format!("duckdb task failed: {e}")),
            },
        }
    }
}

fn run_query_sync(conn: &Connection, query_id: &str, sql: &str) -> Result<QueryRunResult> {
    let started = Instant::now();
    let mut stmt = conn.prepare(sql).map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut rows = stmt.query([]).map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut count = 0usize;
    loop {
        match rows.next() {
            Ok(Some(_)) => count += 1,
            Ok(None) => break,
            Err(e) => {
                return Ok(QueryRunResult {
                    query_id: query_id.to_string(),
                    elapsed_ms: started.elapsed().as_millis() as u64,
                    row_count: 0,
                    error: Some(format!("{e}")),
                });
            }
        }
    }
    Ok(QueryRunResult {
        query_id: query_id.to_string(),
        elapsed_ms: started.elapsed().as_millis() as u64,
        row_count: count,
        error: None,
    })
}

fn parquet_glob(root: &Path, table: &str) -> Option<String> {
    let dir = root.join(table);
    if dir.is_dir() {
        let pattern = dir.join("*.parquet");
        if pattern.parent().map(|p| p.exists()).unwrap_or(false) {
            return Some(pattern.to_string_lossy().into_owned());
        }
    }
    let single = dir.with_extension("parquet");
    if single.is_file() {
        return Some(single.to_string_lossy().into_owned());
    }
    let flat = root.join(format!("{table}.parquet"));
    if flat.is_file() {
        return Some(flat.to_string_lossy().into_owned());
    }
    None
}
