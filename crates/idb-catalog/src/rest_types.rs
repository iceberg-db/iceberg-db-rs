//! REST catalog JSON types (subset of `iceberg-catalog-rest` for wasm builds).

pub const REST_CATALOG_PROP_URI: &str = "uri";
pub const REST_CATALOG_PROP_WAREHOUSE: &str = "warehouse";

use std::collections::HashMap;

use iceberg::spec::TableMetadata;
use iceberg::TableIdent;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ListTablesResponse {
    pub identifiers: Vec<TableIdent>,
    #[serde(default)]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableResult {
    pub metadata_location: Option<String>,
    pub metadata: TableMetadata,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub config: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_credentials: Option<Vec<StorageCredential>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageCredential {
    pub prefix: String,
    pub config: HashMap<String, String>,
}
