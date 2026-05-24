//! WASM: register Iceberg tables lazily so Connect does not load every table (and
//! trigger moka/std::time before queries).
//!
//! Table metadata is reloaded from the REST catalog on every query (no in-memory cache).
//! Iceberg tables evolve when rows are inserted in Snowflake; caching `IcebergStaticTableProvider`
//! would keep serving the first snapshot.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionContext;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergStaticTableProvider;
use iceberg_datafusion::to_datafusion_error;

use crate::case_insensitive;

pub async fn open_wasm_horizon_session(
    catalog_name: String,
    default_schema: String,
    iceberg_catalog: Arc<dyn Catalog>,
) -> Result<super::SqlSession> {
    let config = super::wasm_session_config(&catalog_name, &default_schema);
    let ctx = SessionContext::new_with_config(config);

    let provider = WasmLazyIcebergCatalogProvider::try_new(iceberg_catalog.clone()).await?;
    let provider = case_insensitive::wrap_catalog(Arc::new(provider));
    ctx.register_catalog(&catalog_name, provider);
    ctx.catalog(&catalog_name)
        .with_context(|| format!("catalog '{catalog_name}' not registered"))?;

    Ok(super::SqlSession {
        ctx,
        default_catalog: catalog_name,
        default_schema,
        iceberg_catalog,
        wasm_demo: false,
    })
}

#[derive(Debug)]
struct WasmLazyIcebergCatalogProvider {
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl WasmLazyIcebergCatalogProvider {
    async fn try_new(client: Arc<dyn Catalog>) -> Result<Self> {
        let schema_names: Vec<String> = client
            .list_namespaces(None)
            .await?
            .iter()
            .flat_map(|ns| ns.as_ref().clone())
            .collect();

        let mut schemas = HashMap::new();
        for name in schema_names {
            let ns = NamespaceIdent::new(name.clone());
            let provider =
                WasmLazyIcebergSchemaProvider::try_new(client.clone(), ns).await?;
            schemas.insert(name, Arc::new(provider) as Arc<dyn SchemaProvider>);
        }

        Ok(Self { schemas })
    }
}

impl CatalogProvider for WasmLazyIcebergCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}

#[derive(Debug)]
struct WasmLazyIcebergSchemaProvider {
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
    table_names: Vec<String>,
}

impl WasmLazyIcebergSchemaProvider {
    async fn try_new(catalog: Arc<dyn Catalog>, namespace: NamespaceIdent) -> Result<Self> {
        let table_names: Vec<String> = catalog
            .list_tables(&namespace)
            .await?
            .iter()
            .map(|t| t.name().to_string())
            .collect();

        Ok(Self {
            catalog,
            namespace,
            table_names,
        })
    }

    async fn load_table_provider(
        &self,
        table_name: &str,
    ) -> DFResult<Arc<dyn TableProvider>> {
        web_sys::console::log_1(
            &format!("idb_query: reload Iceberg table {table_name} from catalog").into(),
        );
        let table_ident = TableIdent::new(self.namespace.clone(), table_name.to_string());
        let table = self
            .catalog
            .load_table(&table_ident)
            .await
            .map_err(to_datafusion_error)?;
        let provider = IcebergStaticTableProvider::try_new_from_table(table)
            .await
            .map_err(to_datafusion_error)?;
        web_sys::console::log_1(
            &format!("idb_query: table {table_name} ready for scan").into(),
        );

        Ok(Arc::new(provider))
    }
}

#[async_trait]
impl SchemaProvider for WasmLazyIcebergSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.table_names.clone()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.iter().any(|t| t == name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if !self.table_exist(name) {
            return Ok(None);
        }

        let table = self.load_table_provider(name).await?;
        Ok(Some(table))
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::NotImplemented(
            "CREATE TABLE is not supported in browser WASM yet".into(),
        ))
    }

    fn deregister_table(&self, _name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::NotImplemented(
            "DROP TABLE is not supported in browser WASM yet".into(),
        ))
    }
}
