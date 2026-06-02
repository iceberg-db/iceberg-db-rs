//! DuckDB runner using the Iceberg extension (`iceberg_scan`) on the same warehouse as iceberg-db-rs.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use async_trait::async_trait;
use duckdb::Connection;

use crate::config::BenchConfig;
use crate::engine::{BenchEngine, QueryRunResult};
use crate::qualify::qualify_tpcds_sql;
use crate::tpcds::tables::TPCDS_TABLES;

pub struct DuckDbBench {
    inner: Arc<Mutex<DuckDbState>>,
    schema: String,
}

struct DuckDbState {
    conn: Connection,
    warehouse_root: PathBuf,
    schema: String,
    prepared: bool,
}

impl DuckDbBench {
    pub fn open(config: &BenchConfig) -> Result<Self> {
        let duckdb = &config.duckdb;
        let warehouse = duckdb
            .warehouse
            .clone()
            .unwrap_or_else(|| config.warehouse.clone());
        if duckdb.parquet_root.is_some() {
            anyhow::bail!(
                "duckdb.parquet_root is deprecated; remove it and set duckdb.warehouse (or top-level warehouse) \
                 so DuckDB uses the Iceberg extension on the same tables as iceberg-db-rs"
            );
        }
        let conn = match &duckdb.database {
            Some(path) => Connection::open(path)
                .with_context(|| format!("open DuckDB database {}", path.display()))?,
            None => Connection::open_in_memory().context("open in-memory DuckDB")?,
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(DuckDbState {
                conn,
                warehouse_root: warehouse,
                schema: duckdb.schema.clone(),
                prepared: false,
            })),
            schema: duckdb.schema.clone(),
        })
    }

    fn with_conn<F, T>(inner: &Arc<Mutex<DuckDbState>>, f: F) -> Result<T>
    where
        F: FnOnce(&mut DuckDbState) -> Result<T>,
    {
        let mut guard = inner.lock().map_err(|e| anyhow::anyhow!("duckdb lock: {e}"))?;
        f(&mut guard)
    }

    pub fn explain_analyze(&self, sql: &str) -> Result<String> {
        let inner = Arc::clone(&self.inner);
        let sql = qualify_tpcds_sql(sql, &self.schema);
        Self::with_conn(&inner, |state| {
            prepare_iceberg_tables(state)?;
            explain_analyze_sync(&state.conn, &sql)
        })
    }
}

#[async_trait]
impl BenchEngine for DuckDbBench {
    fn name(&self) -> &'static str {
        "duckdb-iceberg"
    }

    async fn prepare(&mut self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            Self::with_conn(&inner, |state| prepare_iceberg_tables(state))
        })
        .await
        .context("duckdb prepare join")?
    }

    async fn run_query(&mut self, query_id: &str, sql: &str) -> QueryRunResult {
        let inner = Arc::clone(&self.inner);
        let query_id_owned = query_id.to_string();
        let sql = qualify_tpcds_sql(sql, &self.schema);
        match tokio::task::spawn_blocking(move || {
            Self::with_conn(&inner, |state| run_query_sync(&state.conn, &query_id_owned, &sql))
        })
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                let msg = format!("{e:#}");
                eprintln!("{query_id} duckdb-iceberg: {msg}");
                QueryRunResult {
                    query_id: query_id.to_string(),
                    elapsed_ms: 0,
                    row_count: 0,
                    error: Some(msg),
                }
            }
            Err(e) => QueryRunResult {
                query_id: query_id.to_string(),
                elapsed_ms: 0,
                row_count: 0,
                error: Some(format!("duckdb task failed: {e}")),
            },
        }
    }
}

fn prepare_iceberg_tables(state: &mut DuckDbState) -> Result<()> {
    if state.prepared {
        return Ok(());
    }

    state
        .conn
        .execute_batch(
            "INSTALL iceberg;\nLOAD iceberg;\nSET unsafe_enable_version_guessing = true;",
        )
        .context(
            "load DuckDB iceberg extension (requires network on first INSTALL iceberg)",
        )?;

    let schema = &state.schema;
    state
        .conn
        .execute_batch(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
        .context("create DuckDB schema")?;

    let mut registered = 0usize;
    for table in TPCDS_TABLES {
        let table_dir = state.warehouse_root.join(schema).join(table);
        let metadata_hint = table_dir.join("metadata").join("version-hint.text");
        let metadata_v1 = table_dir.join("metadata").join("v1.metadata.json");
        if !metadata_hint.is_file() && !metadata_v1.is_file() {
            continue;
        }

        let scan_path = path_for_iceberg_scan(&table_dir);
        let sql = format!(
            "CREATE OR REPLACE VIEW {schema}.{table} AS \
             SELECT * FROM iceberg_scan('{scan_path}', allow_moved_paths := true);"
        );
        state
            .conn
            .execute(&sql, [])
            .with_context(|| format!("register iceberg view {schema}.{table}"))?;
        registered += 1;
    }

    if registered == 0 {
        anyhow::bail!(
            "no Iceberg tables under {}/{}/{{table}}/metadata — run setup-local-tpcds.ps1 first",
            state.warehouse_root.display(),
            schema
        );
    }

    state
        .conn
        .execute(&format!("USE {schema};"), [])
        .context("USE schema")?;
    tracing::info!(registered, "DuckDB Iceberg views registered from warehouse");
    state.prepared = true;
    Ok(())
}

/// Path string for `iceberg_scan` (table root directory).
fn path_for_iceberg_scan(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn explain_analyze_sync(conn: &Connection, sql: &str) -> Result<String> {
    let mut stmt = conn
        .prepare(&format!("EXPLAIN ANALYZE {sql}"))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut rows = stmt.query([]).map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut lines = Vec::new();
    while let Some(row) = rows.next().map_err(|e| anyhow::anyhow!("{e}"))? {
        let text: String = row.get(1).map_err(|e| anyhow::anyhow!("{e}"))?;
        lines.push(text);
    }
    Ok(lines.join("\n"))
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
