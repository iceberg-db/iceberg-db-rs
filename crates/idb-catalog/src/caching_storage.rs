//! `Storage` wrapper that caches `FileRead::read` ranges through [`byte_cache`].

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::Result;
use serde::{Deserialize, Serialize};

use crate::byte_cache::{self, ByteCacheStore};

fn wrap_storage(
    inner: Arc<dyn Storage>,
    config: &StorageConfig,
) -> Result<Arc<dyn Storage>> {
    let props: HashMap<String, String> = config.props().clone();
    let cache = byte_cache::global_byte_cache()
        .unwrap_or_else(|| byte_cache::open_byte_cache(&props));
    let cred_fp = byte_cache::global_cred_fingerprint();
    Ok(Arc::new(CachingStorage::new(inner, cache, cred_fp)))
}

/// Native CLI: OpenDAL S3 with read-through byte cache on `FileRead::read`.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachingOpenDalS3Factory {
    configured_scheme: String,
}

#[cfg(not(target_arch = "wasm32"))]
impl CachingOpenDalS3Factory {
    pub fn s3() -> Self {
        Self {
            configured_scheme: "s3".to_string(),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[typetag::serde(name = "idb_caching_opendal_s3")]
impl StorageFactory for CachingOpenDalS3Factory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        use iceberg_storage_opendal::OpenDalStorageFactory;

        let inner_factory = OpenDalStorageFactory::S3 {
            configured_scheme: self.configured_scheme.clone(),
            customized_credential_load: None,
        };
        let inner = inner_factory.build(config)?;
        wrap_storage(inner, config)
    }
}

/// WASM: `WasmS3Storage` with the same read-through cache helper as native.
#[cfg(target_arch = "wasm32")]
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CachingWasmS3Factory;

#[cfg(target_arch = "wasm32")]
impl CachingWasmS3Factory {
    pub fn s3() -> Self {
        Self
    }
}

#[cfg(target_arch = "wasm32")]
#[typetag::serde(name = "idb_caching_wasm_s3")]
impl StorageFactory for CachingWasmS3Factory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        use crate::wasm_s3_storage::WasmS3StorageFactory;

        let inner = WasmS3StorageFactory::s3().build(config)?;
        wrap_storage(inner, config)
    }
}

/// Runtime fields are not persisted across serde (see Iceberg `MemoryStorage`).
#[derive(Clone, Serialize, Deserialize)]
pub struct CachingStorage {
    #[serde(skip, default = "CachingStorage::placeholder")]
    _cache_marker: (),
    #[serde(skip, default = "CachingStorage::empty_inner")]
    inner: Arc<dyn Storage>,
    #[serde(skip, default = "CachingStorage::empty_cache")]
    cache: Arc<dyn ByteCacheStore>,
    #[serde(skip, default)]
    cred_fp: u64,
}

impl CachingStorage {
    fn placeholder() -> () {}

    fn empty_inner() -> Arc<dyn Storage> {
        Arc::new(iceberg::io::MemoryStorage::new())
    }

    fn empty_cache() -> Arc<dyn ByteCacheStore> {
        byte_cache::global_byte_cache().unwrap_or_else(|| Arc::new(byte_cache::NoopByteCache))
    }

    pub fn new(
        inner: Arc<dyn Storage>,
        cache: Arc<dyn ByteCacheStore>,
        cred_fp: u64,
    ) -> Self {
        Self {
            _cache_marker: (),
            inner,
            cache,
            cred_fp,
        }
    }
}

impl fmt::Debug for CachingStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachingStorage")
            .field("cred_fp", &self.cred_fp)
            .finish_non_exhaustive()
    }
}

#[async_trait]
#[typetag::serde(name = "idb_byte_cache_storage")]
impl Storage for CachingStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.inner.exists(path).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.inner.metadata(path).await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        if crate::wasm_query_io::query_cancelled() {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "query cancelled",
            ));
        }
        if let Some(hit) =
            byte_cache::lookup_full_object(self.cache.as_ref(), path, self.cred_fp)
        {
            crate::wasm_query_io::record_byte_cache_hit();
            return Ok(hit);
        }
        let meta = self.inner.metadata(path).await?;
        if meta.size == 0 {
            return self.inner.read(path).await;
        }
        let range = 0..meta.size;
        if let Some(hit) =
            byte_cache::lookup_range(self.cache.as_ref(), path, range.clone(), self.cred_fp)
        {
            crate::wasm_query_io::record_byte_cache_hit();
            return Ok(hit);
        }
        let data = self.inner.read(path).await?;
        if !data.is_empty() {
            let key = byte_cache::ByteCacheKey::new(path, range, self.cred_fp);
            self.cache.put(&key, data.clone());
        }
        Ok(data)
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let inner = self.inner.reader(path).await?;
        Ok(Box::new(CachingFileRead {
            inner,
            path: path.to_string(),
            cache: self.cache.clone(),
            cred_fp: self.cred_fp,
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.inner.write(path, bs).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.inner.writer(path).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.inner.delete_prefix(path).await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        // Must not delegate to `inner.new_input` — that attaches the uncached storage to
        // `InputFile`, so every manifest/parquet read bypasses this wrapper.
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

struct CachingFileRead {
    inner: Box<dyn FileRead>,
    path: String,
    cache: Arc<dyn ByteCacheStore>,
    cred_fp: u64,
}

#[async_trait]
impl FileRead for CachingFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        if crate::wasm_query_io::query_cancelled() {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "query cancelled",
            ));
        }
        if let Some(hit) = byte_cache::lookup_range(
            self.cache.as_ref(),
            &self.path,
            range.clone(),
            self.cred_fp,
        ) {
            crate::wasm_query_io::record_byte_cache_hit();
            return Ok(hit);
        }
        let data = self.inner.read(range.clone()).await?;
        let key = byte_cache::ByteCacheKey::new(&self.path, range, self.cred_fp);
        if !data.is_empty() {
            self.cache.put(&key, data.clone());
        }
        Ok(data)
    }
}
