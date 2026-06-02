//! Local byte cache for ranged object reads (shared by CLI and WASM).
//!
//! Query cancel is in [`crate::wasm_query_io`].
//!
//! Keys are logical Iceberg paths plus byte range and a credential fingerprint so
//! vended S3 credentials do not collide across tables.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use iceberg::io::{CLIENT_REGION, S3_ENDPOINT, S3_PATH_STYLE_ACCESS};
use iceberg::Result;

/// Whether byte caching is enabled (default: on unless `IDB_BYTE_CACHE=0`).
pub fn byte_cache_enabled_from_env() -> bool {
    match std::env::var("IDB_BYTE_CACHE").ok().as_deref() {
        Some("0") | Some("false") | Some("FALSE") | Some("off") => false,
        _ => true,
    }
}

pub fn byte_cache_enabled_from_props(props: &HashMap<String, String>) -> bool {
    if let Some(v) = props.get("idb.byte-cache") {
        let v = v.trim();
        if v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off") {
            return false;
        }
        return true;
    }
    byte_cache_enabled_from_env()
}

/// Namespace byte-cache entries per catalog connection (not per vended STS key).
///
/// Snowflake/IRC returns fresh `s3.access-key-id` / session tokens on every `loadTable`.
/// Hashing those would make the cache miss on every query even when reading the same
/// parquet paths. Object paths in [`ByteCacheKey`] already isolate files; stable props
/// isolate warehouses and endpoints.
pub fn cred_fingerprint(props: &HashMap<String, String>) -> u64 {
    let mut h = DefaultHasher::new();
    for key in [
        "uri",
        "warehouse",
        "prefix",
        S3_ENDPOINT,
        CLIENT_REGION,
        S3_PATH_STYLE_ACCESS,
        "s3.region",
        "s3.bucket",
    ] {
        if let Some(v) = props.get(key).filter(|s| !s.is_empty()) {
            v.hash(&mut h);
        }
    }
    h.finish()
}

fn max_cache_bytes_from_props(props: &HashMap<String, String>) -> usize {
    if let Some(mib) = props
        .get("idb.byte-cache-max-mib")
        .and_then(|s| s.trim().parse::<usize>().ok())
    {
        return mib.saturating_mul(1024 * 1024);
    }
    #[cfg(target_arch = "wasm32")]
    {
        return 1024 * 1024 * 1024;
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = props;
        0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ByteCacheKey {
    pub path: String,
    pub range: Range<u64>,
    pub cred_fingerprint: u64,
}

impl ByteCacheKey {
    pub fn new(path: impl Into<String>, range: Range<u64>, cred_fingerprint: u64) -> Self {
        Self {
            path: path.into(),
            range,
            cred_fingerprint,
        }
    }

    pub fn file_name(&self) -> String {
        let mut h = DefaultHasher::new();
        self.path.hash(&mut h);
        self.range.start.hash(&mut h);
        self.range.end.hash(&mut h);
        self.cred_fingerprint.hash(&mut h);
        format!("{:016x}", h.finish())
    }
}

pub trait ByteCacheStore: Send + Sync {
    fn get(&self, key: &ByteCacheKey) -> Option<Bytes>;

    /// Exact range match, or a slice from a previously cached superset range.
    fn get_range(&self, path: &str, range: Range<u64>, cred_fingerprint: u64) -> Option<Bytes> {
        let key = ByteCacheKey::new(path, range.clone(), cred_fingerprint);
        self.get(&key)
    }

    /// Whole-file reads (manifests, metadata) — largest cached span from offset 0.
    fn get_full_object(&self, path: &str, cred_fingerprint: u64) -> Option<Bytes> {
        let _ = (path, cred_fingerprint);
        None
    }

    fn put(&self, key: &ByteCacheKey, data: Bytes);
}

/// Look up cached bytes for `range`, including subsets of a larger cached fetch.
pub fn lookup_range(
    cache: &dyn ByteCacheStore,
    path: &str,
    range: Range<u64>,
    cred_fingerprint: u64,
) -> Option<Bytes> {
    cache.get_range(path, range, cred_fingerprint)
}

/// Return the largest cached chunk for `path` that starts at byte 0 (full-object reads).
pub fn lookup_full_object(
    cache: &dyn ByteCacheStore,
    path: &str,
    cred_fingerprint: u64,
) -> Option<Bytes> {
    cache.get_full_object(path, cred_fingerprint)
}

fn slice_cached_range(cached: &Range<u64>, data: &Bytes, want: &Range<u64>) -> Option<Bytes> {
    if cached.start <= want.start && cached.end >= want.end {
        let start = (want.start - cached.start) as usize;
        let len = (want.end - want.start) as usize;
        if start + len <= data.len() {
            return Some(data.slice(start..start + len));
        }
    }
    None
}

type PathCredKey = (String, u64);

/// Read-through helper used by native OpenDAL and WASM S3 readers.
pub async fn read_range_cached<F, Fut>(
    cache: &Arc<dyn ByteCacheStore>,
    path: &str,
    cred_fingerprint: u64,
    range: Range<u64>,
    fetch: F,
) -> Result<Bytes>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Bytes>>,
{
    if let Some(hit) = lookup_range(cache.as_ref(), path, range.clone(), cred_fingerprint) {
        return Ok(hit);
    }
    let data = fetch().await?;
    if !data.is_empty() {
        let key = ByteCacheKey::new(path, range, cred_fingerprint);
        cache.put(&key, data.clone());
    }
    Ok(data)
}

static GLOBAL_BYTE_CACHE: std::sync::OnceLock<Arc<dyn ByteCacheStore>> = std::sync::OnceLock::new();
static GLOBAL_CRED_FINGERPRINT: AtomicU64 = AtomicU64::new(0);

pub fn install_global_byte_cache(store: Arc<dyn ByteCacheStore>, props: &HashMap<String, String>) {
    GLOBAL_CRED_FINGERPRINT.store(cred_fingerprint(props), Ordering::Relaxed);
    let _ = GLOBAL_BYTE_CACHE.set(store);
}

pub fn global_byte_cache() -> Option<Arc<dyn ByteCacheStore>> {
    GLOBAL_BYTE_CACHE.get().cloned()
}

pub fn global_cred_fingerprint() -> u64 {
    GLOBAL_CRED_FINGERPRINT.load(Ordering::Relaxed)
}

/// Create the platform-appropriate cache backend.
pub fn open_byte_cache(props: &HashMap<String, String>) -> Arc<dyn ByteCacheStore> {
    if !byte_cache_enabled_from_props(props) {
        return Arc::new(NoopByteCache);
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let root = props
            .get("idb.byte-cache-dir")
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(default_native_cache_dir);
        return Arc::new(FsByteCache::new(root));
    }
    #[cfg(target_arch = "wasm32")]
    {
        let max_bytes = max_cache_bytes_from_props(props);
        Arc::new(MemoryByteCache::new(max_bytes))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn default_native_cache_dir() -> PathBuf {
    std::env::var("IDB_BYTE_CACHE_DIR")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(PathBuf::from)
        })
        .map(|h| h.join(".cache").join("iceberg-db").join("byte-cache"))
        .unwrap_or_else(|| std::env::temp_dir().join("iceberg-db-byte-cache"))
}

pub struct NoopByteCache;

impl ByteCacheStore for NoopByteCache {
    fn get(&self, _key: &ByteCacheKey) -> Option<Bytes> {
        None
    }
    fn put(&self, _key: &ByteCacheKey, _data: Bytes) {}
}

#[derive(Clone)]
struct CachedChunk {
    range: Range<u64>,
    data: Bytes,
}

#[derive(Default)]
struct PathByteCache {
    chunks: Vec<CachedChunk>,
}

impl PathByteCache {
    fn find_range(&self, want: &Range<u64>) -> Option<Bytes> {
        for chunk in &self.chunks {
            if let Some(slice) = slice_cached_range(&chunk.range, &chunk.data, want) {
                return Some(slice);
            }
        }
        None
    }

    fn find_full_object(&self) -> Option<Bytes> {
        self.chunks
            .iter()
            .filter(|c| c.range.start == 0)
            .max_by_key(|c| c.range.end)
            .map(|c| c.data.clone())
    }

    fn insert(&mut self, key: &ByteCacheKey, data: Bytes) -> Option<usize> {
        if data.is_empty() {
            return None;
        }
        let mut replaced = 0usize;
        if let Some(idx) = self
            .chunks
            .iter()
            .position(|c| c.range == key.range)
        {
            replaced = self.chunks[idx].data.len();
            self.chunks[idx].data = data;
            return Some(replaced);
        }
        self.chunks.push(CachedChunk {
            range: key.range.clone(),
            data,
        });
        None
    }

    fn bytes_used(&self) -> usize {
        self.chunks.iter().map(|c| c.data.len()).sum()
    }
}

/// On-disk cache for native CLI (and tests).
#[cfg(not(target_arch = "wasm32"))]
pub struct FsByteCache {
    root: PathBuf,
    by_path: RwLock<HashMap<PathCredKey, PathByteCache>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl FsByteCache {
    pub fn new(root: PathBuf) -> Self {
        let _ = std::fs::create_dir_all(&root);
        Self {
            root,
            by_path: RwLock::new(HashMap::new()),
        }
    }

    fn path_for(&self, key: &ByteCacheKey) -> PathBuf {
        let name = key.file_name();
        self.root
            .join(&name[..2])
            .join(&name[2..4])
            .join(format!("{name}.bin"))
    }

}

#[cfg(not(target_arch = "wasm32"))]
impl ByteCacheStore for FsByteCache {
    fn get(&self, key: &ByteCacheKey) -> Option<Bytes> {
        let path = self.path_for(key);
        let data = std::fs::read(&path).ok()?;
        Some(Bytes::from(data))
    }

    fn get_range(&self, path: &str, range: Range<u64>, cred_fingerprint: u64) -> Option<Bytes> {
        if let Some(hit) = self.get(&ByteCacheKey::new(path, range.clone(), cred_fingerprint)) {
            return Some(hit);
        }
        let key = (path.to_string(), cred_fingerprint);
        self.by_path
            .read()
            .expect("by_path lock")
            .get(&key)
            .and_then(|p| p.find_range(&range))
    }

    fn get_full_object(&self, path: &str, cred_fingerprint: u64) -> Option<Bytes> {
        let key = (path.to_string(), cred_fingerprint);
        self.by_path
            .read()
            .expect("by_path lock")
            .get(&key)
            .and_then(PathByteCache::find_full_object)
    }

    fn put(&self, key: &ByteCacheKey, data: Bytes) {
        if data.is_empty() {
            return;
        }
        let disk_path = self.path_for(key);
        if let Some(parent) = disk_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let tmp = disk_path.with_extension("tmp");
        if std::fs::write(&tmp, &data).is_ok() {
            let _ = std::fs::rename(tmp, disk_path);
        }
        let path_key = (key.path.clone(), key.cred_fingerprint);
        let mut by_path = self.by_path.write().expect("by_path lock");
        let entry = by_path.entry(path_key).or_default();
        entry.insert(key, data);
    }
}

/// In-process cache for WASM (persists for the browser tab session).
pub struct MemoryByteCache {
    by_path: RwLock<HashMap<PathCredKey, PathByteCache>>,
    bytes_used: Mutex<usize>,
    max_bytes: usize,
}

impl MemoryByteCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            by_path: RwLock::new(HashMap::new()),
            bytes_used: Mutex::new(0),
            max_bytes,
        }
    }

    fn evict_if_needed(&self, incoming: usize) {
        let mut used = *self.bytes_used.lock().expect("bytes_used lock");
        if used + incoming <= self.max_bytes {
            return;
        }
        let mut by_path = self.by_path.write().expect("by_path lock");
        while used + incoming > self.max_bytes && !by_path.is_empty() {
            let Some(removed_key) = by_path.keys().next().cloned() else {
                break;
            };
            if let Some(removed) = by_path.remove(&removed_key) {
                used = used.saturating_sub(removed.bytes_used());
            }
        }
        *self.bytes_used.lock().expect("bytes_used lock") = used;
    }
}

impl ByteCacheStore for MemoryByteCache {
    fn get(&self, key: &ByteCacheKey) -> Option<Bytes> {
        let path_key = (key.path.clone(), key.cred_fingerprint);
        self.by_path
            .read()
            .expect("by_path lock")
            .get(&path_key)
            .and_then(|p| p.find_range(&key.range))
    }

    fn get_range(&self, path: &str, range: Range<u64>, cred_fingerprint: u64) -> Option<Bytes> {
        self.get(&ByteCacheKey::new(path, range, cred_fingerprint))
    }

    fn get_full_object(&self, path: &str, cred_fingerprint: u64) -> Option<Bytes> {
        let key = (path.to_string(), cred_fingerprint);
        self.by_path
            .read()
            .expect("by_path lock")
            .get(&key)
            .and_then(PathByteCache::find_full_object)
    }

    fn put(&self, key: &ByteCacheKey, data: Bytes) {
        if data.is_empty() {
            return;
        }
        self.evict_if_needed(data.len());
        let path_key = (key.path.clone(), key.cred_fingerprint);
        let mut by_path = self.by_path.write().expect("by_path lock");
        let entry = by_path.entry(path_key).or_default();
        entry.insert(key, data);
        let total: usize = by_path.values().map(PathByteCache::bytes_used).sum();
        *self.bytes_used.lock().expect("bytes_used lock") = total;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_cache_serves_subset_of_cached_range() {
        let cache = MemoryByteCache::new(4 * 1024 * 1024);
        let path = "s3://bucket/data.parquet";
        let key = ByteCacheKey::new(path, 0..1000, 42);
        cache.put(&key, Bytes::from(vec![7u8; 1000]));
        let hit = cache
            .get_range(path, 100..250, 42)
            .expect("subset range should hit");
        assert_eq!(hit.len(), 150);
        assert!(hit.iter().all(|&b| b == 7));
    }
}
