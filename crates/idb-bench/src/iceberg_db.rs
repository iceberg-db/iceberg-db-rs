//! iceberg-db-rs runner (`idb-core` + Iceberg warehouse or REST config).

use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use idb_core::Engine;
use idb_sql::SessionOptions;

use crate::config::BenchConfig;
use crate::engine::{BenchEngine, QueryRunResult};
use crate::qualify::qualify_tpcds_sql;

pub struct IcebergDbEngine {
    engine: Engine,
    schema: String,
}

impl IcebergDbEngine {
    pub async fn open(config: &BenchConfig) -> Result<Self> {
        let options = SessionOptions {
            target_partitions: config.target_partitions,
            repartition_file_scans: config.repartition_file_scans,
            enable_join_dynamic_filter_pushdown: config.enable_join_dynamic_filter_pushdown,
            repartition_joins: config.repartition_joins,
            ..Default::default()
        };
        let engine = if let Some(path) = &config.iceberg_config {
            Engine::from_config_with_options(idb_config::load(path)?, options).await?
        } else {
            Engine::from_warehouse_with_options(
                &config.warehouse,
                &config.catalog,
                &config.schema,
                options,
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

    pub async fn explain_analyze(&self, sql: &str) -> Result<String> {
        let sql = qualify_tpcds_sql(sql, &self.schema);
        self.engine.explain_analyze(&sql).await
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
                mean_elapsed_ms: None,
                timed_iterations: None,
                row_count: result.row_count,
                error: None,
            },
            Err(e) => {
                let msg = format!("{e:#}");
                eprintln!("{query_id} iceberg-db-rs: {msg}");
                QueryRunResult {
                    query_id: query_id.to_string(),
                    elapsed_ms: started.elapsed().as_millis() as u64,
                    mean_elapsed_ms: None,
                    timed_iterations: None,
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
