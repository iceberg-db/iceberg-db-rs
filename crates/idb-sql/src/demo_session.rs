//! Demo session without external Iceberg storage.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;

#[cfg(not(target_arch = "wasm32"))]
use datafusion::prelude::SessionConfig;

use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::CatalogBuilder;

use crate::SqlSession;

const CATALOG: &str = "local";
const SCHEMA: &str = "demo";
const TABLE: &str = "customers";

/// In-memory `demo.customers` (3 rows) for demos and native smoke builds.
pub async fn open_demo_session() -> Result<SqlSession> {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carlos"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["US", "US", "EU"])) as ArrayRef,
        ],
    )
    .context("demo customers batch")?;

    let mem_table = MemTable::try_new(arrow_schema, vec![vec![batch]]).context("demo mem table")?;

    let schema_provider = MemorySchemaProvider::new();
    schema_provider.register_table(TABLE.to_string(), Arc::new(mem_table))?;

    let catalog_provider = MemoryCatalogProvider::new();
    catalog_provider.register_schema(SCHEMA, Arc::new(schema_provider))?;

    #[cfg(target_arch = "wasm32")]
    let config = crate::wasm_session_config(CATALOG, "public");
    #[cfg(not(target_arch = "wasm32"))]
    let config = SessionConfig::new()
        .with_information_schema(true)
        .with_create_default_catalog_and_schema(false)
        .with_default_catalog_and_schema(CATALOG, "public");
    let ctx = SessionContext::new_with_config(config);
    ctx.register_catalog(CATALOG, Arc::new(catalog_provider));
    ctx.catalog(CATALOG)
        .with_context(|| format!("catalog '{CATALOG}' not registered"))?;

    // Placeholder Iceberg catalog (SHOW TABLES uses DataFusion for this demo session).
    let iceberg_catalog: Arc<dyn iceberg::Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "demo",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory://".to_string(),
                )]),
            )
            .await
            .context("demo memory catalog")?,
    );

    Ok(SqlSession {
        ctx,
        default_catalog: CATALOG.to_string(),
        default_schema: SCHEMA.to_string(),
        iceberg_catalog,
        wasm_demo: true,
    })
}
