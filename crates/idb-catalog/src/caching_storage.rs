//! Storage factory wrappers for optional object byte caching.

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use iceberg::io::{Storage, StorageConfig, StorageFactory};
#[cfg(not(target_arch = "wasm32"))]
use iceberg::Result;
#[cfg(not(target_arch = "wasm32"))]
use iceberg_storage_opendal::OpenDalStorageFactory;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CachingOpenDalS3Factory {
    inner: OpenDalStorageFactory,
}

#[cfg(not(target_arch = "wasm32"))]
impl CachingOpenDalS3Factory {
    pub fn s3() -> Self {
        Self {
            inner: OpenDalStorageFactory::S3 {
                configured_scheme: "s3".to_string(),
                customized_credential_load: None,
            },
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[typetag::serde(name = "CachingOpenDalS3Factory")]
impl StorageFactory for CachingOpenDalS3Factory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        self.inner.build(config)
    }
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug)]
pub struct CachingWasmS3Factory;

#[cfg(target_arch = "wasm32")]
impl CachingWasmS3Factory {
    pub fn s3() -> Self {
        Self
    }
}
