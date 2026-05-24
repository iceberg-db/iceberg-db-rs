//! REST catalog wrapper: merges Snowflake / IRC `storage-credentials` into table FileIO.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use iceberg::io::{FileIO, FileIOBuilder, StorageFactory};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION};
use reqwest::{Client, StatusCode};

#[cfg(not(target_arch = "wasm32"))]
use iceberg_catalog_rest::{LoadTableResult, RestCatalog, StorageCredential};

#[cfg(target_arch = "wasm32")]
use crate::rest_types::{ListTablesResponse, LoadTableResult, StorageCredential};

use crate::http_log;
use crate::snowflake_auth;

#[cfg(target_arch = "wasm32")]
use crate::wasm_local;

/// REST catalog that applies vended S3 credentials from `loadTable` responses.
pub struct VendedRestCatalog {
    #[cfg(not(target_arch = "wasm32"))]
    inner: RestCatalog,
    props: HashMap<String, String>,
    storage_factory: Arc<dyn StorageFactory>,
    http: Client,
}

impl VendedRestCatalog {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(
        inner: RestCatalog,
        props: HashMap<String, String>,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> Result<Self> {
        let http = build_http_client(&props)?;
        Ok(Self {
            inner,
            props,
            storage_factory,
            http,
        })
    }

    #[cfg(target_arch = "wasm32")]
    pub fn new(
        props: HashMap<String, String>,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> Result<Self> {
        let http = build_http_client(&props)?;
        Ok(Self {
            props,
            storage_factory,
            http,
        })
    }

    async fn load_table_vended(&self, table_ident: &TableIdent) -> Result<Table> {
        let uri = self
            .props
            .get("uri")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("REST catalog missing uri"))?;
        let url = table_endpoint(uri, table_ident, &self.props);

        let bearer = snowflake_auth::exchange_pat(&self.props).await?;

        let auth_header = format!("Bearer {}", http_log::redact_secret(&bearer));
        let mut req_headers = catalog_request_headers(&self.props);
        req_headers.push(("Authorization".into(), auth_header));

        http_log::log_outbound("GET", &url, &req_headers, None);

        let http = self.http.clone();
        let bearer2 = bearer.clone();
        let http_resp = http_get(http, url.clone(), bearer2).await.context("load_table HTTP")?;

        if http_log::enabled() {
            eprintln!("--- idb HTTP response ---");
            eprintln!("GET {url}");
            eprintln!("status: {}", http_resp.status);
            eprintln!("--- end ---");
        }
        let status = http_resp.status;
        let body = http_resp.body;

        if status != StatusCode::OK {
            let hint = if idb_config::profile::is_snowflake_horizon_uri(uri) {
                "\nSnowflake IRC path is /v1/<warehouse>/namespaces/<schema>/tables/<table> \
(warehouse = database in config.yaml, e.g. ICEBERG_TEST). \
Use schema.table in SQL, e.g. SELECT * FROM iceberg_test.employee — \
not database.table (database is only the warehouse/path prefix)."
            } else {
                "\nHint: verify catalog URI, namespace, and table name."
            };
            return Err(anyhow!(
                "load_table failed ({status}): {}{}",
                String::from_utf8_lossy(&body),
                hint
            ));
        }

        let load: LoadTableResult =
            serde_json::from_slice(&body).context("parse load_table JSON")?;

        let mut config: HashMap<String, String> = load
            .config
            .into_iter()
            .chain(self.props.clone())
            .collect();
        merge_storage_credentials(&mut config, load.storage_credentials.as_ref());

        let file_io = build_file_io(
            &self.storage_factory,
            load.metadata_location.as_deref(),
            &config,
        )?;

        let mut builder = Table::builder()
            .identifier(table_ident.clone())
            .file_io(file_io)
            .metadata(load.metadata);

        if let Some(metadata_location) = load.metadata_location {
            builder = builder.metadata_location(metadata_location);
        }

        #[cfg(target_arch = "wasm32")]
        {
            builder = builder.disable_cache();
        }

        builder.build().map_err(|e| anyhow!("build table: {e}"))
    }

    #[cfg(target_arch = "wasm32")]
    async fn list_namespaces_wasm(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        if parent.is_some() {
            return Ok(vec![]);
        }
        let schema = self
            .props
            .get("default-schema")
            .or_else(|| self.props.get("schema"))
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_else(|| "public".to_string());
        let ns = NamespaceIdent::from_vec(vec![schema]).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, format!("invalid default schema: {e}"))
        })?;
        Ok(vec![ns])
    }

    #[cfg(target_arch = "wasm32")]
    async fn list_tables_wasm(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        let uri = self
            .props
            .get("uri")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::new(ErrorKind::Unexpected, "REST catalog missing uri".to_string())
            })?;
        let bearer = snowflake_auth::exchange_pat(&self.props)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        let mut identifiers = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let mut url = tables_list_endpoint(uri, namespace, &self.props);
            if let Some(ref token) = next_token {
                url = format!("{url}?pageToken={token}");
            }

            let http = self.http.clone();
            let bearer2 = bearer.clone();
            let http_resp = http_get(http, url.clone(), bearer2)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

            let status = http_resp.status;
            let body = http_resp.body;

            if status == StatusCode::NOT_FOUND {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "namespace does not exist".to_string(),
                ));
            }
            if status != StatusCode::OK {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "list_tables failed ({status}): {}",
                        String::from_utf8_lossy(&body)
                    ),
                ));
            }

            let page: ListTablesResponse = serde_json::from_slice(&body).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("parse list_tables JSON: {e}"),
                )
            })?;
            identifiers.extend(page.identifiers);
            match page.next_page_token {
                Some(token) if !token.is_empty() => next_token = Some(token),
                _ => break,
            }
        }

        Ok(identifiers)
    }
}

#[cfg(target_arch = "wasm32")]
struct HttpPayload {
    status: StatusCode,
    body: bytes::Bytes,
}

#[cfg(target_arch = "wasm32")]
async fn http_get(http: Client, url: String, bearer: String) -> Result<HttpPayload> {
    let r = wasm_local::client_get(http, url, bearer).await?;
    Ok(HttpPayload {
        status: r.status,
        body: r.body,
    })
}

#[cfg(not(target_arch = "wasm32"))]
struct HttpPayload {
    status: StatusCode,
    body: bytes::Bytes,
}

#[cfg(not(target_arch = "wasm32"))]
async fn http_get(http: Client, url: String, bearer: String) -> Result<HttpPayload> {
    let response = http
        .get(&url)
        .header(AUTHORIZATION, format!("Bearer {bearer}"))
        .send()
        .await
        .context("HTTP GET")?;
    let status = response.status();
    let body = response.bytes().await.context("read body")?;
    Ok(HttpPayload { status, body })
}

fn merge_storage_credentials(
    config: &mut HashMap<String, String>,
    storage_credentials: Option<&Vec<StorageCredential>>,
) {
    let Some(creds) = storage_credentials else {
        return;
    };
    for cred in creds {
        config.extend(cred.config.clone());
    }
}

fn build_file_io(
    factory: &Arc<dyn StorageFactory>,
    metadata_location: Option<&str>,
    props: &HashMap<String, String>,
) -> Result<FileIO> {
    if metadata_location.is_none() && !props.contains_key("warehouse") {
        return Err(anyhow!(
            "cannot build FileIO: missing metadata_location and warehouse"
        ));
    }
    Ok(FileIOBuilder::new(factory.clone())
        .with_props(props.clone())
        .build())
}

fn catalog_request_headers(props: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (key, value) in props {
        let Some(name) = key.strip_prefix("header.") else {
            continue;
        };
        out.push((name.to_string(), value.clone()));
    }
    let (vended_name, vended_value) = idb_config::profile::snowflake_vended_credentials_header();
    out.push((vended_name.to_string(), vended_value.to_string()));
    out
}

fn build_http_client(props: &HashMap<String, String>) -> Result<Client> {
    let mut headers = HeaderMap::new();
    for (key, value) in props {
        let Some(name) = key.strip_prefix("header.") else {
            continue;
        };
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| anyhow!("invalid header name {name}: {e}"))?;
        let value = HeaderValue::from_str(value)
            .map_err(|e| anyhow!("invalid header value for {name}: {e}"))?;
        headers.insert(name, value);
    }
    let (vended_name, vended_value) = idb_config::profile::snowflake_vended_credentials_header();
    headers.insert(
        HeaderName::from_bytes(vended_name.as_bytes()).unwrap(),
        HeaderValue::from_str(vended_value).unwrap(),
    );
    #[cfg(target_arch = "wasm32")]
    {
        use reqwest::header::{CACHE_CONTROL, HeaderValue, PRAGMA};
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        headers.insert(PRAGMA, HeaderValue::from_static("no-cache"));
    }
    Client::builder()
        .default_headers(headers)
        .build()
        .context("build HTTP client")
}

fn table_endpoint(uri: &str, table: &TableIdent, props: &HashMap<String, String>) -> String {
    let base = uri.trim_end_matches('/');
    let ns = snowflake_path_segment(&table.namespace().to_url_string());
    let name = snowflake_path_segment(table.name());

    if idb_config::profile::is_snowflake_horizon_uri(uri) {
        if let Some(db) = props
            .get("warehouse")
            .or_else(|| props.get("prefix"))
            .filter(|w| !w.is_empty() && !w.contains("://"))
        {
            let db = snowflake_path_segment(db);
            return format!("{base}/v1/{db}/namespaces/{ns}/tables/{name}");
        }
    }

    if let Some(prefix) = props.get("prefix").filter(|s| !s.is_empty()) {
        let prefix = prefix.trim_matches('/');
        return format!("{base}/v1/{prefix}/namespaces/{ns}/tables/{name}");
    }

    format!("{base}/v1/namespaces/{ns}/tables/{name}")
}

#[cfg(target_arch = "wasm32")]
fn tables_list_endpoint(uri: &str, namespace: &NamespaceIdent, props: &HashMap<String, String>) -> String {
    let base = uri.trim_end_matches('/');
    let ns = snowflake_path_segment(&namespace.to_url_string());

    if idb_config::profile::is_snowflake_horizon_uri(uri) {
        if let Some(db) = props
            .get("warehouse")
            .or_else(|| props.get("prefix"))
            .filter(|w| !w.is_empty() && !w.contains("://"))
        {
            let db = snowflake_path_segment(db);
            return format!("{base}/v1/{db}/namespaces/{ns}/tables");
        }
    }

    if let Some(prefix) = props.get("prefix").filter(|s| !s.is_empty()) {
        let prefix = prefix.trim_matches('/');
        return format!("{base}/v1/{prefix}/namespaces/{ns}/tables");
    }

    format!("{base}/v1/namespaces/{ns}/tables")
}

fn snowflake_path_segment(ident: &str) -> String {
    ident.to_uppercase()
}

#[cfg(target_arch = "wasm32")]
fn wasm_unsupported() -> Error {
    Error::new(
        ErrorKind::FeatureUnsupported,
        "not supported in the browser WASM catalog".to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::NamespaceIdent;

    #[test]
    fn snowflake_load_table_url_includes_database_prefix() {
        let uri = "https://xy.snowflakecomputing.com/polaris/api/catalog";
        let table = TableIdent::new(
            NamespaceIdent::from_vec(vec!["public".into()]).unwrap(),
            "employee".into(),
        );
        let props = HashMap::from([
            ("warehouse".into(), "ICEBERG_TEST".into()),
            ("uri".into(), uri.into()),
        ]);
        let url = table_endpoint(uri, &table, &props);
        assert_eq!(
            url,
            "https://xy.snowflakecomputing.com/polaris/api/catalog/v1/ICEBERG_TEST/namespaces/PUBLIC/tables/EMPLOYEE"
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
macro_rules! delegate {
    ($self:expr, $method:ident ( $($arg:expr),* $(,)? )) => {
        $self.inner.$method($($arg),*).await
    };
}

#[async_trait]
impl Catalog for VendedRestCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        #[cfg(target_arch = "wasm32")]
        return self.list_namespaces_wasm(parent).await;
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, list_namespaces(parent))
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (namespace, properties);
            return Err(wasm_unsupported());
        }
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, create_namespace(namespace, properties))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = namespace;
            return Err(wasm_unsupported());
        }
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, get_namespace(namespace))
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = namespace;
            return Err(wasm_unsupported());
        }
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, namespace_exists(namespace))
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (namespace, properties);
            return Err(wasm_unsupported());
        }
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, update_namespace(namespace, properties))
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = namespace;
            return Err(wasm_unsupported());
        }
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, drop_namespace(namespace))
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        #[cfg(target_arch = "wasm32")]
        return self.list_tables_wasm(namespace).await;
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, list_tables(namespace))
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (namespace, creation);
            return Err(wasm_unsupported());
        }
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, create_table(namespace, creation))
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        self.load_table_vended(table)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = table;
            return Err(wasm_unsupported());
        }
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, drop_table(table))
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = table;
            return Err(wasm_unsupported());
        }
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, table_exists(table))
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (src, dest);
            return Err(wasm_unsupported());
        }
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, rename_table(src, dest))
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (table, metadata_location);
            return Err(wasm_unsupported());
        }
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, register_table(table, metadata_location))
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = commit;
            return Err(wasm_unsupported());
        }
        #[cfg(not(target_arch = "wasm32"))]
        delegate!(self, update_table(commit))
    }
}

impl std::fmt::Debug for VendedRestCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendedRestCatalog")
            .field("uri", &self.props.get("uri"))
            .field("warehouse", &self.props.get("warehouse"))
            .finish()
    }
}
