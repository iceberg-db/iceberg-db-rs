//! Discover tables written by Java {@code HadoopCatalog} under a warehouse directory.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use tracing::debug;

const LOCATION: &str = "location";

/// Registers namespaces and tables found under `{warehouse}/{namespace}/{table}/metadata/`.
pub async fn bootstrap_hadoop_tables(catalog: Arc<dyn Catalog>, warehouse: &Path) -> Result<()> {
    let warehouse_path = warehouse.to_path_buf();
    let warehouse = normalize_path(warehouse);
    let namespace_entries = fs::read_dir(&warehouse_path)
        .with_context(|| format!("read warehouse {}", warehouse_path.display()))?;

    for namespace_entry in namespace_entries {
        let namespace_entry = namespace_entry?;
        let namespace_path = namespace_entry.path();
        if !namespace_path.is_dir() {
            continue;
        }
        let namespace_name = namespace_entry.file_name().to_string_lossy().into_owned();
        if namespace_name.starts_with('.') {
            continue;
        }

        let namespace = NamespaceIdent::new(namespace_name.clone());
        if !catalog.namespace_exists(&namespace).await? {
            let ns_location = format!("{warehouse}/{namespace_name}");
            catalog
                .create_namespace(
                    &namespace,
                    HashMap::from([(LOCATION.to_string(), ns_location)]),
                )
                .await
                .with_context(|| format!("create namespace '{namespace_name}'"))?;
        }

        let table_entries = fs::read_dir(&namespace_path)?;
        for table_entry in table_entries {
            let table_entry = table_entry?;
            let table_path = table_entry.path();
            if !table_path.is_dir() {
                continue;
            }
            let table_name = table_entry.file_name().to_string_lossy().into_owned();
            if table_name.starts_with('.') {
                continue;
            }

            let metadata_dir = table_path.join("metadata");
            let Some(metadata_location) = resolve_metadata_file(&metadata_dir)? else {
                debug!(
                    namespace = %namespace_name,
                    table = %table_name,
                    "skipping path without Iceberg metadata"
                );
                continue;
            };

            let table_ident = TableIdent::new(namespace.clone(), table_name.clone());
            if catalog.table_exists(&table_ident).await? {
                continue;
            }

            catalog
                .register_table(&table_ident, metadata_location)
                .await
                .with_context(|| {
                    format!("register table '{namespace_name}.{table_name}' from warehouse")
                })?;
            debug!(
                namespace = %namespace_name,
                table = %table_name,
                "registered Hadoop-catalog table"
            );
        }
    }

    Ok(())
}

fn resolve_metadata_file(metadata_dir: &Path) -> Result<Option<String>> {
    if !metadata_dir.is_dir() {
        return Ok(None);
    }

    if let Some(path) = metadata_from_version_hint(metadata_dir)? {
        if path.is_file() {
            return Ok(Some(normalize_path(&path)));
        }
    }

    Ok(latest_metadata_json(metadata_dir)?.map(|path| normalize_path(&path)))
}

fn metadata_from_version_hint(metadata_dir: &Path) -> Result<Option<PathBuf>> {
    let hint_path = metadata_dir.join("version-hint.text");
    if !hint_path.is_file() {
        return Ok(None);
    }
    let version = fs::read_to_string(&hint_path)?.trim().to_string();
    if version.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        metadata_dir.join(format!("v{version}.metadata.json")),
    ))
}

fn latest_metadata_json(metadata_dir: &Path) -> Result<Option<PathBuf>> {
    let mut best: Option<(u32, PathBuf)> = None;
    for entry in fs::read_dir(metadata_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(version) = parse_metadata_version(&name) else {
            continue;
        };
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        match &best {
            Some((best_version, _)) if *best_version >= version => {}
            _ => best = Some((version, path)),
        }
    }
    Ok(best.map(|(_, path)| path))
}

fn parse_metadata_version(file_name: &str) -> Option<u32> {
    let rest = file_name.strip_prefix('v')?;
    let version_str = rest.strip_suffix(".metadata.json")?;
    version_str.parse().ok()
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
