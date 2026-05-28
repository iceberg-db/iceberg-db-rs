//! Resolve manifest data-file paths relative to each table's `metadata.location`.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{
    FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, LocalFsStorage,
    OutputFile, Storage, StorageConfig, StorageFactory,
};
use iceberg::table::Table;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent,
};
use serde::{Deserialize, Serialize};

/// Catalog wrapper that attaches per-table `FileIO` with Hadoop-style relative path resolution.
pub struct ResolvedPathCatalog {
    inner: Arc<dyn Catalog>,
}

impl std::fmt::Debug for ResolvedPathCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedPathCatalog").finish_non_exhaustive()
    }
}

impl ResolvedPathCatalog {
    pub fn new(inner: Arc<dyn Catalog>) -> Self {
        Self { inner }
    }

    fn rewrap_table(&self, table: Table) -> Result<Table> {
        let base = table.metadata().location().to_string();
        let file_io = FileIOBuilder::new(Arc::new(TableRelativeStorageFactory { base })).build();

        let mut builder = Table::builder()
            .identifier(table.identifier().clone())
            .metadata(table.metadata().clone())
            .file_io(file_io);
        if let Some(loc) = table.metadata_location() {
            builder = builder.metadata_location(loc.to_string());
        }
        builder.build()
    }
}

#[async_trait]
impl Catalog for ResolvedPathCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.rewrap_table(self.inner.create_table(namespace, creation).await?)
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        self.rewrap_table(self.inner.load_table(table).await?)
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table> {
        self.rewrap_table(
            self.inner
                .register_table(table, metadata_location)
                .await?,
        )
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.rewrap_table(self.inner.update_table(commit).await?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableRelativeStorageFactory {
    base: String,
}

#[typetag::serde(name = "idb_table_relative_fs_factory")]
impl StorageFactory for TableRelativeStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(TableRelativeStorage {
            base: self.base.clone(),
            inner: LocalFsStorage::new(),
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableRelativeStorage {
    base: String,
    #[serde(skip, default)]
    inner: LocalFsStorage,
}

impl TableRelativeStorage {
    fn resolve(&self, path: &str) -> String {
        resolve_path(&self.base, path)
    }
}

#[async_trait]
#[typetag::serde(name = "idb_table_relative_fs")]
impl Storage for TableRelativeStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.inner.exists(&self.resolve(path)).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.inner.metadata(&self.resolve(path)).await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.inner.read(&self.resolve(path)).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.inner.reader(&self.resolve(path)).await
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.inner.write(&self.resolve(path), bs).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.inner.writer(&self.resolve(path)).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(&self.resolve(path)).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.inner.delete_prefix(&self.resolve(path)).await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.inner.new_input(&self.resolve(path))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.inner.new_output(&self.resolve(path))
    }
}

/// Joins `path` to `base` when it is not already absolute (Hadoop-catalog relative data files).
pub fn resolve_path(base: &str, path: &str) -> String {
    let path = path.trim();
    if path.is_empty() {
        return path.to_string();
    }
    // Spark/Iceberg on Windows often emit `file:/C:/...` (not `file:///...`).
    if let Some(local) = file_uri_to_local_path(path) {
        return local;
    }
    if path.contains("://") || is_absolute_path(path) {
        return path.replace('\\', "/");
    }
    let base = base.trim_end_matches('/').replace('\\', "/");
    let relative = path.trim_start_matches('/').replace('\\', "/");
    format!("{base}/{relative}")
}

fn is_absolute_path(path: &str) -> bool {
    path.starts_with('/')
        || path
            .chars()
            .nth(1)
            .is_some_and(|c| c == ':')
}

/// Converts `file:` URIs (including Spark's `file:/C:/...`) to a local path for `LocalFsStorage`.
fn file_uri_to_local_path(path: &str) -> Option<String> {
    let rest = path.strip_prefix("file:")?;
    let local = rest.trim_start_matches('/').replace('\\', "/");
    if local.is_empty() {
        return None;
    }
    Some(local)
}

#[cfg(test)]
mod tests {
    use super::{is_absolute_path, resolve_path};

    #[test]
    fn resolves_relative_data_file() {
        let base = "C:/warehouse/demo/customers";
        assert_eq!(
            resolve_path(base, "data-1.parquet"),
            "C:/warehouse/demo/customers/data-1.parquet"
        );
    }

    #[test]
    fn leaves_absolute_path_unchanged() {
        let base = "C:/warehouse/demo/customers";
        let abs = "C:/warehouse/demo/customers/data-1.parquet";
        assert_eq!(resolve_path(base, abs), abs);
    }

    #[test]
    fn passes_through_s3_uri() {
        let base = "C:/warehouse/demo/customers";
        let s3 = "s3://bucket/key/data.parquet";
        assert_eq!(resolve_path(base, s3), s3);
    }

    #[test]
    fn normalizes_backslashes_in_relative_path() {
        let base = "C:\\warehouse\\demo\\customers";
        assert_eq!(
            resolve_path(base, "subdir\\data-1.parquet"),
            "C:/warehouse/demo/customers/subdir/data-1.parquet"
        );
    }

    #[test]
    fn empty_path_returns_empty() {
        assert_eq!(resolve_path("C:/warehouse", ""), "");
        assert_eq!(resolve_path("", ""), "");
    }

    #[test]
    fn unix_absolute_path_is_absolute() {
        assert!(is_absolute_path("/abs/path"));
        assert!(is_absolute_path("C:/abs"));
        assert!(!is_absolute_path("relative/path.parquet"));
        assert!(!is_absolute_path(""));
    }

    #[test]
    fn unix_absolute_path_passes_through() {
        // `is_absolute_path` short-circuits before the relative-trim branch.
        assert_eq!(resolve_path("C:/wh", "/abs/data.parquet"), "/abs/data.parquet");
    }

    #[test]
    fn trims_trailing_slash_from_base() {
        let base = "C:/warehouse/demo/customers/";
        assert_eq!(
            resolve_path(base, "data-1.parquet"),
            "C:/warehouse/demo/customers/data-1.parquet"
        );
    }

    #[test]
    fn normalizes_spark_file_uri_on_windows() {
        let base = "C:/warehouse/tpcds/store_sales";
        let spark = "file:/C:/warehouse/tpcds/store_sales/metadata/snap.avro";
        assert_eq!(
            resolve_path(base, spark),
            "C:/warehouse/tpcds/store_sales/metadata/snap.avro"
        );
    }

    #[test]
    fn normalizes_file_triple_slash_uri() {
        let base = "C:/warehouse/tpcds/store_sales";
        let uri = "file:///C:/warehouse/tpcds/store_sales/data/part-00000.parquet";
        assert_eq!(
            resolve_path(base, uri),
            "C:/warehouse/tpcds/store_sales/data/part-00000.parquet"
        );
    }
}
