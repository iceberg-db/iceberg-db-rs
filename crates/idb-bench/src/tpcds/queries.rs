//! Load TPC-DS query SQL from a manifest + query directory.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct QueryManifest {
    pub suite: String,
    #[serde(default)]
    pub version: u32,
    pub query: Vec<ManifestEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ManifestEntry {
    pub id: String,
    pub file: String,
    /// Omit or `true` to run; set `false` to skip.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone)]
pub struct TpcdsQuery {
    pub id: String,
    pub sql: String,
    pub path: PathBuf,
}

impl QueryManifest {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read manifest {}", path.display()))?;
        toml::from_str(&text).context("parse TPC-DS manifest TOML")
    }
}

pub fn load_manifest(manifest_path: &Path) -> Result<Vec<TpcdsQuery>> {
    let manifest = QueryManifest::load(manifest_path)?;
    let base = manifest_path
        .parent()
        .context("manifest path has no parent directory")?;

    let mut out = Vec::new();
    for entry in manifest.query {
        if !entry.enabled {
            continue;
        }
        let path = base.join(&entry.file);
        let sql = std::fs::read_to_string(&path)
            .with_context(|| format!("read query {} ({})", entry.id, path.display()))?;
        out.push(TpcdsQuery {
            id: entry.id,
            sql: sql.trim().to_string(),
            path,
        });
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn load_starter_manifest() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmarks/tpcds/manifest.toml");
        let queries = load_manifest(&root).expect("manifest");
        assert!(!queries.is_empty());
        assert!(queries.iter().any(|q| q.id == "q01"));
    }
}
