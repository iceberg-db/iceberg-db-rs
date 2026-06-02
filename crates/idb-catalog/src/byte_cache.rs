//! Optional byte-cache hooks for object storage reads.
//!
//! The native benchmark path does not configure a byte cache. Keep these
//! hooks no-op unless a cache implementation is wired through catalog props.

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ByteCache;

pub fn open_byte_cache(_props: &HashMap<String, String>) -> Option<ByteCache> {
    None
}

pub fn install_global_byte_cache(_cache: Option<ByteCache>, _props: &HashMap<String, String>) {}
