//! Per-query HTTP/S3 I/O stats for the browser status bar (reset at query start).

#[cfg(target_arch = "wasm32")]
use std::collections::HashSet;
#[cfg(target_arch = "wasm32")]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(target_arch = "wasm32")]
use std::sync::Mutex;

#[cfg(target_arch = "wasm32")]
static BYTES_FETCHED: AtomicU64 = AtomicU64::new(0);

#[cfg(target_arch = "wasm32")]
static S3_FILES_FETCHED: AtomicU64 = AtomicU64::new(0);

/// Distinct S3 object URLs seen this query (one parquet file may use many range GETs).
#[cfg(target_arch = "wasm32")]
static SEEN_S3_OBJECTS: Mutex<Option<HashSet<String>>> = Mutex::new(None);

#[cfg(target_arch = "wasm32")]
static QUERY_CANCEL_REQUESTED: AtomicBool = AtomicBool::new(false);

#[cfg(target_arch = "wasm32")]
static BYTE_CACHE_HITS: AtomicU64 = AtomicU64::new(0);

#[cfg(target_arch = "wasm32")]
static BYTE_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

/// Reset I/O stats and cancel flag before each SQL query.
pub fn reset_query_state() {
    #[cfg(target_arch = "wasm32")]
    {
        QUERY_CANCEL_REQUESTED.store(false, Ordering::Relaxed);
        BYTES_FETCHED.store(0, Ordering::Relaxed);
        S3_FILES_FETCHED.store(0, Ordering::Relaxed);
        BYTE_CACHE_HITS.store(0, Ordering::Relaxed);
        BYTE_CACHE_MISSES.store(0, Ordering::Relaxed);
        *SEEN_S3_OBJECTS.lock().expect("s3 object set lock") = Some(HashSet::new());
    }
}

/// Record a byte-cache hit (served from memory/disk, no S3 GET).
pub fn record_byte_cache_hit() {
    #[cfg(target_arch = "wasm32")]
    BYTE_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
}

/// Record a byte-cache miss (will fetch from storage).
pub fn record_byte_cache_miss() {
    #[cfg(target_arch = "wasm32")]
    BYTE_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
}

pub fn byte_cache_hits() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        return BYTE_CACHE_HITS.load(Ordering::Relaxed);
    }
    #[cfg(not(target_arch = "wasm32"))]
    0
}

pub fn byte_cache_misses() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        return BYTE_CACHE_MISSES.load(Ordering::Relaxed);
    }
    #[cfg(not(target_arch = "wasm32"))]
    0
}

/// Reset before each SQL query so the status bar shows fetch size for that run only.
pub fn reset_bytes_fetched() {
    reset_query_state();
}

/// Request cancellation of the in-flight query (checked before new S3/HTTP reads).
pub fn request_query_cancel() {
    #[cfg(target_arch = "wasm32")]
    QUERY_CANCEL_REQUESTED.store(true, Ordering::Relaxed);
}

pub fn query_cancelled() -> bool {
    #[cfg(target_arch = "wasm32")]
    {
        return QUERY_CANCEL_REQUESTED.load(Ordering::Relaxed);
    }
    #[cfg(not(target_arch = "wasm32"))]
    false
}

pub fn add_bytes_fetched(n: u64) {
    #[cfg(target_arch = "wasm32")]
    if n > 0 {
        BYTES_FETCHED.fetch_add(n, Ordering::Relaxed);
    }
}

pub fn bytes_fetched() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        return BYTES_FETCHED.load(Ordering::Relaxed);
    }
    #[cfg(not(target_arch = "wasm32"))]
    0
}

/// Count each distinct S3 object URL once per query (range reads share the same URL).
pub fn record_s3_object_fetch(url: &str) {
    #[cfg(target_arch = "wasm32")]
    {
        let mut guard = SEEN_S3_OBJECTS.lock().expect("s3 object set lock");
        if guard.is_none() {
            *guard = Some(HashSet::new());
        }
        if guard
            .as_mut()
            .expect("s3 object set")
            .insert(url.to_string())
        {
            S3_FILES_FETCHED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn files_fetched() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        return S3_FILES_FETCHED.load(Ordering::Relaxed);
    }
    #[cfg(not(target_arch = "wasm32"))]
    0
}
