//! Embedded engine facade for native and WASM targets.

use std::path::Path;

use anyhow::{Context, Result};
use idb_catalog::CatalogRegistry;
use idb_config::{load, AppConfig};
use idb_sql::{QueryResult, SqlSession};

pub struct Engine {
    registry: CatalogRegistry,
    session: SqlSession,
}

impl Engine {
    pub async fn from_config_file(path: &Path) -> Result<Self> {
        let config = load(path)?;
        Self::from_config(config).await
    }

    pub async fn from_config(config: AppConfig) -> Result<Self> {
        let registry = CatalogRegistry::from_config(&config).await?;
        let session = SqlSession::from_registry(&registry).await?;
        Ok(Self { registry, session })
    }

    pub async fn from_warehouse(warehouse: &Path, catalog_name: &str) -> Result<Self> {
        let registry = CatalogRegistry::from_file_warehouse(catalog_name, warehouse).await?;
        let session = SqlSession::from_registry(&registry).await?;
        Ok(Self { registry, session })
    }

    pub fn registry(&self) -> &CatalogRegistry {
        &self.registry
    }

    pub fn session(&self) -> &SqlSession {
        &self.session
    }

    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        self.session.query(sql).await
    }

    pub async fn explain(&self, sql: &str) -> Result<String> {
        self.session.explain(sql).await
    }

    pub async fn set_default_catalog(&mut self, name: &str) -> Result<()> {
        let catalog = self.registry.get(name)?;
        self.session = SqlSession::from_iceberg_catalog(
            name.to_string(),
            self.registry.default_schema().to_string(),
            catalog,
        )
        .await?;
        Ok(())
    }
}

pub async fn open_config_path(path: &Path) -> Result<Engine> {
    Engine::from_config_file(path)
        .await
        .with_context(|| format!("open engine config {}", path.display()))
}
