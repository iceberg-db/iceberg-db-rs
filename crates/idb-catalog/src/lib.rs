//! Build Iceberg `Catalog` instances from `idb-config` entries.

pub mod demo_memory;

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
mod hadoop;
#[cfg(feature = "native")]
mod resolved_paths;
#[cfg(feature = "native")]
mod http_log;
/// REST JSON types are only deserialized by the WASM build (native uses iceberg-catalog-rest types).
#[cfg(all(feature = "native", target_arch = "wasm32"))]
mod rest_types;
#[cfg(feature = "native")]
mod rest_vended;
#[cfg(feature = "native")]
mod snowflake_auth;
#[cfg(all(feature = "native", target_arch = "wasm32"))]
mod wasm_local;
#[cfg(all(feature = "native", target_arch = "wasm32"))]
mod wasm_s3_storage;

#[cfg(feature = "native")]
use std::collections::HashMap;
#[cfg(feature = "native")]
use std::path::Path;
#[cfg(feature = "native")]
use std::sync::Arc;

#[cfg(feature = "native")]
use anyhow::{anyhow, bail, Context, Result};
#[cfg(feature = "native")]
use iceberg::io::{LocalFsStorageFactory, StorageFactory};
#[cfg(feature = "native")]
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
#[cfg(feature = "native")]
use iceberg::{Catalog, CatalogBuilder};
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
use iceberg_catalog_rest::{RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
#[cfg(all(feature = "native", target_arch = "wasm32"))]
use rest_types::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
use iceberg_storage_opendal::OpenDalStorageFactory;
#[cfg(all(feature = "native", target_arch = "wasm32"))]
use wasm_s3_storage::WasmS3StorageFactory;
#[cfg(feature = "native")]
use rest_vended::VendedRestCatalog;
#[cfg(feature = "native")]
use idb_config::{resolve_value, AppConfig, CatalogSpec};
#[cfg(feature = "native")]
use tracing::info;

#[cfg(feature = "native")]
pub struct CatalogRegistry {
    default_name: String,
    /// Schema (namespace) for unqualified table names and bare `SHOW TABLES`.
    default_schema: String,
    catalogs: HashMap<String, Arc<dyn Catalog>>,
}

#[cfg(feature = "native")]
impl CatalogRegistry {
    pub async fn from_config(config: &AppConfig) -> Result<Self> {
        if config.catalogs.is_empty() {
            bail!("no catalogs in config");
        }
        let default_name = config
            .default_catalog
            .clone()
            .or_else(|| config.catalogs.keys().next().cloned())
            .context("default catalog")?;
        let mut catalogs = HashMap::new();
        for (name, spec) in &config.catalogs {
            info!(catalog = %name, r#type = %spec.catalog_type, "opening catalog");
            let catalog = open_catalog(name, spec).await?;
            catalogs.insert(name.clone(), catalog);
        }
        if !catalogs.contains_key(&default_name) {
            bail!("unknown default catalog: {default_name}");
        }
        let default_schema = config
            .catalogs
            .get(&default_name)
            .and_then(default_schema_from_spec)
            .unwrap_or_else(|| "public".to_string());
        Ok(Self {
            default_name,
            default_schema,
            catalogs,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn from_file_warehouse(catalog_name: &str, warehouse: &Path) -> Result<Self> {
        let warehouse = warehouse
            .canonicalize()
            .unwrap_or_else(|_| warehouse.to_path_buf());
        let catalog = open_file_warehouse_path(catalog_name, &warehouse).await?;
        Ok(Self {
            default_name: catalog_name.to_string(),
            default_schema: "public".to_string(),
            catalogs: HashMap::from([(catalog_name.to_string(), catalog)]),
        })
    }

    pub fn default_name(&self) -> &str {
        &self.default_name
    }

    pub fn default_schema(&self) -> &str {
        &self.default_schema
    }

    pub fn get(&self, name: &str) -> Result<Arc<dyn Catalog>> {
        self.catalogs
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("unknown catalog: {name}"))
    }

    pub fn default(&self) -> Arc<dyn Catalog> {
        self.catalogs
            .get(&self.default_name)
            .expect("default catalog")
            .clone()
    }
}

#[cfg(feature = "native")]
async fn open_catalog(name: &str, spec: &CatalogSpec) -> Result<Arc<dyn Catalog>> {
    match spec.catalog_type.to_ascii_lowercase().as_str() {
        "hadoop" | "file" => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                open_file_warehouse(name, spec).await
            }
            #[cfg(target_arch = "wasm32")]
            {
                let _ = (name, spec);
                bail!("hadoop/file catalog is not available in the browser WASM build; use type: rest")
            }
        }
        "rest" => open_rest(name, spec).await,
        other => bail!("unsupported catalog type: {other}"),
    }
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
async fn open_file_warehouse(name: &str, spec: &CatalogSpec) -> Result<Arc<dyn Catalog>> {
    let warehouse = spec
        .property("warehouse")
        .filter(|s| !s.is_empty())
        .map(|s| resolve_value(&s))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("catalog '{name}' requires warehouse"))?;
    let warehouse = Path::new(&warehouse);
    open_file_warehouse_path(name, warehouse).await
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
async fn open_file_warehouse_path(name: &str, warehouse: &Path) -> Result<Arc<dyn Catalog>> {
    if !warehouse.is_absolute() {
        bail!(
            "warehouse must be an absolute path for catalog '{name}': {}",
            warehouse.display()
        );
    }
    let props = HashMap::from([(
        MEMORY_CATALOG_WAREHOUSE.to_string(),
        warehouse.to_string_lossy().replace('\\', "/"),
    )]);
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(name, props)
        .await
        .map_err(|e| anyhow!("memory catalog: {e}"))?;
    let catalog: Arc<dyn Catalog> = Arc::new(catalog);
    hadoop::bootstrap_hadoop_tables(catalog.clone(), warehouse)
        .await
        .context("bootstrap Hadoop-style warehouse tables")?;
    Ok(Arc::new(resolved_paths::ResolvedPathCatalog::new(catalog)))
}

#[cfg(feature = "native")]
async fn open_rest(name: &str, spec: &CatalogSpec) -> Result<Arc<dyn Catalog>> {
    let mut props = spec.rest_catalog_properties();
    if let Err(msg) =
        idb_config::profile::validate_rest_props(name, &props, spec.profile_name().as_deref())
    {
        bail!("{msg}");
    }
    let uri = props
        .remove("uri")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("REST catalog '{name}' requires uri"))?;
    props.entry(REST_CATALOG_PROP_URI.to_string()).or_insert(uri);
    if let Some(warehouse) = props.get("warehouse").cloned() {
        props
            .entry(REST_CATALOG_PROP_WAREHOUSE.to_string())
            .or_insert(warehouse);
    }
    let mut props: HashMap<String, String> = props.into_iter().collect();
    if let Some(schema) = default_schema_from_spec(spec) {
        props
            .entry("default-schema".to_string())
            .or_insert(schema);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let storage_factory: Arc<dyn StorageFactory> = Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
        customized_credential_load: None,
    });
    #[cfg(target_arch = "wasm32")]
    let storage_factory: Arc<dyn StorageFactory> = Arc::new(WasmS3StorageFactory::s3());

    let mut rest_props = props.clone();
    rest_props.remove("header.X-Iceberg-Access-Delegation");

    let is_snowflake = idb_config::profile::is_snowflake_horizon_profile(
        spec.profile_name().as_deref(),
        rest_props.get("uri").map(String::as_str),
    );

    if is_snowflake {
        #[cfg(target_arch = "wasm32")]
        props
            .entry("s3.dev-proxy".to_string())
            .or_insert_with(|| "http://127.0.0.1:8787".to_string());
        let bearer = snowflake_auth::exchange_pat(&rest_props).await.map_err(|e| {
            anyhow!(
                "{e:#}\n\nHorizon hint: scope must match PAT ROLE_RESTRICTION exactly \
(SHOW USER PROGRAMMATIC ACCESS TOKENS). Some accounts also need username: <login> in config."
            )
        })?;
        rest_props.insert("token".to_string(), bearer.clone());
        rest_props.remove("credential");
        props.insert("token".to_string(), bearer);
        props.remove("credential");
    }

    http_log::log_catalog_bootstrap(&rest_props);

    #[cfg(not(target_arch = "wasm32"))]
    let catalog = {
        let inner = RestCatalogBuilder::default()
            .with_storage_factory(storage_factory.clone())
            .load(name, rest_props)
            .await
            .map_err(|e| anyhow!("rest catalog: {e}"))?;
        VendedRestCatalog::new(inner, props, storage_factory).context("vended REST catalog")?
    };

    #[cfg(target_arch = "wasm32")]
    let catalog =
        VendedRestCatalog::new(props, storage_factory).context("vended REST catalog (wasm)")?;

    Ok(Arc::new(catalog))
}

#[cfg(feature = "native")]
fn default_schema_from_spec(spec: &CatalogSpec) -> Option<String> {
    spec.property("default-schema")
        .or_else(|| spec.property("schema"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}
