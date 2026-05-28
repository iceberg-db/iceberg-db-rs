//! iceberg-db-rs runner (`idb-core` + Iceberg warehouse or REST config).

use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use idb_core::Engine;

use crate::config::BenchConfig;
use crate::engine::{BenchEngine, QueryRunResult};
use crate::qualify::qualify_tpcds_sql;

pub struct IcebergDbEngine {
    engine: Engine,
    schema: String,
}

impl IcebergDbEngine {
    pub async fn open(config: &BenchConfig) -> Result<Self> {
        let engine = if let Some(path) = &config.iceberg_config {
            Engine::from_config_file(path).await?
        } else {
            Engine::from_warehouse_with_schema(
                &config.warehouse,
                &config.catalog,
                &config.schema,
            )
            .await?
        };
        Ok(Self {
            engine,
            schema: config.schema.clone(),
        })
    }

    pub async fn run_sql(&self, sql: &str) -> Result<(usize, u64)> {
        let sql = qualify_tpcds_sql(sql, &self.schema);
        let result = self.engine.query(&sql).await?;
        Ok((result.row_count, result.elapsed_ms))
    }
}

#[async_trait]
impl BenchEngine for IcebergDbEngine {
    fn name(&self) -> &'static str {
        "iceberg-db-rs"
    }

    async fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    async fn run_query(&mut self, query_id: &str, sql: &str) -> QueryRunResult {
        let started = Instant::now();
        let sql = qualify_tpcds_sql(sql, &self.schema);
        match self.engine.query(&sql).await {
            Ok(result) => QueryRunResult {
                query_id: query_id.to_string(),
                elapsed_ms: result.elapsed_ms.max(started.elapsed().as_millis() as u64),
                row_count: result.row_count,
                error: None,
            },
            Err(e) => {
                let msg = format!("{e:#}");
                eprintln!("{query_id} iceberg-db-rs: {msg}");
                QueryRunResult {
                    query_id: query_id.to_string(),
                    elapsed_ms: started.elapsed().as_millis() as u64,
                    row_count: 0,
                    error: Some(msg),
                }
            }
        }
    }
}

/// Open with explicit warehouse path (tests).
pub async fn open_warehouse(
    warehouse: &Path,
    catalog: &str,
    schema: &str,
) -> Result<IcebergDbEngine> {
    let engine = Engine::from_warehouse_with_schema(warehouse, catalog, schema).await?;
    Ok(IcebergDbEngine {
        engine,
        schema: schema.to_string(),
    })
}
