//! Browser demo session without Iceberg (avoids Moka cache using `std::time` on wasm).

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

use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::CatalogBuilder;

use crate::SqlSession;

const CATALOG: &str = "local";
const SCHEMA: &str = "demo";
const TABLE: &str = "customers";

/// In-memory `demo.customers` (3 rows) for WASM — same shape as the Java/Rust seeders.
pub async fn open_wasm_demo_session() -> Result<SqlSession> {
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

    let mem_table =
        MemTable::try_new(arrow_schema, vec![vec![batch]]).context("demo mem table")?;

    let schema_provider = MemorySchemaProvider::new();
    schema_provider.register_table(TABLE.to_string(), Arc::new(mem_table))?;

    let catalog_provider = MemoryCatalogProvider::new();
    catalog_provider.register_schema(SCHEMA, Arc::new(schema_provider))?;

    let config = crate::wasm_session_config(CATALOG, "public");
    let ctx = SessionContext::new_with_config(config);
    ctx.register_catalog(CATALOG, Arc::new(catalog_provider));
    ctx.catalog(CATALOG)
        .with_context(|| format!("catalog '{CATALOG}' not registered"))?;

    // Placeholder Iceberg catalog (SHOW TABLES uses DataFusion for this demo session).
    let iceberg_catalog: Arc<dyn iceberg::Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "wasm",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory://".to_string(),
                )]),
            )
            .await
            .context("wasm memory catalog")?,
    );

    Ok(SqlSession {
        ctx,
        default_catalog: CATALOG.to_string(),
        default_schema: SCHEMA.to_string(),
        iceberg_catalog,
        wasm_demo: true,
    })
}
