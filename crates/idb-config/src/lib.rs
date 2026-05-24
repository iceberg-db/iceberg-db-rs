//! YAML catalog configuration (aligned with Java `YamlCatalogConfigLoader`).

pub mod profile;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(rename = "default-catalog")]
    pub default_catalog: Option<String>,
    pub catalogs: BTreeMap<String, CatalogSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CatalogSpec {
    #[serde(rename = "type", default = "default_rest_type")]
    pub catalog_type: String,
    #[serde(flatten)]
    pub properties: BTreeMap<String, serde_yaml::Value>,
}

fn default_rest_type() -> String {
    "rest".to_string()
}

impl CatalogSpec {
    pub fn property(&self, key: &str) -> Option<String> {
        self.properties.get(key).map(value_to_string)
    }

    pub fn profile_name(&self) -> Option<String> {
        self.property("profile")
    }

    /// Properties for `iceberg-catalog-rest`, including profile transforms.
    pub fn rest_catalog_properties(&self) -> BTreeMap<String, String> {
        let mut props = self.resolved_properties();
        profile::apply_profile(self.profile_name().as_deref(), &mut props);
        props
    }

    pub fn resolved_properties(&self) -> BTreeMap<String, String> {
        self.properties
            .iter()
            .filter(|(k, _)| k.as_str() != "type" && k.as_str() != "profile")
            .map(|(k, v)| (k.clone(), resolve_value(&value_to_string(v))))
            .collect()
    }
}

fn value_to_string(v: &serde_yaml::Value) -> String {
    match v {
        serde_yaml::Value::Null => String::new(),
        serde_yaml::Value::Bool(b) => b.to_string(),
        serde_yaml::Value::Number(n) => n.to_string(),
        serde_yaml::Value::String(s) => s.clone(),
        other => serde_yaml::to_string(other).unwrap_or_default(),
    }
}

/// Substitute `${ENV_VAR}` from environment, then Java system-property-style fallback.
pub fn resolve_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next();
            let mut name = String::new();
            for ch in chars.by_ref() {
                if ch == '}' {
                    break;
                }
                name.push(ch);
            }
            let replacement = std::env::var(&name)
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_default();
            out.push_str(&replacement);
        } else {
            out.push(c);
        }
    }
    out
}

fn mapping_key<'a>(mapping: &'a serde_yaml::Mapping, name: &str) -> Option<&'a serde_yaml::Value> {
    mapping.get(&serde_yaml::Value::String(name.into())).or_else(|| {
        mapping.iter().find_map(|(k, v)| {
            k.as_str()
                .filter(|k| k.eq_ignore_ascii_case(name))
                .map(|_| v)
        })
    })
}

/// Parse catalog config from a YAML string (used by WASM and tests).
pub fn load_str(text: &str) -> Result<AppConfig> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let trimmed = text.trim_start();

    if trimmed.starts_with('{') {
        anyhow::bail!(
            "config looks like JSON (starts with '{{'). Use YAML with a top-level `catalogs:` map."
        );
    }

    let root: serde_yaml::Value =
        serde_yaml::from_str(text).map_err(|e| anyhow::anyhow!("parse config (invalid YAML): {e}"))?;

    let Some(mapping) = root.as_mapping() else {
        anyhow::bail!("parse config: expected a YAML mapping at the root, got {root:?}");
    };

    let default_catalog = mapping_key(mapping, "default-catalog")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let catalogs_value = mapping_key(mapping, "catalogs").ok_or_else(|| {
        let keys: Vec<_> = mapping
            .keys()
            .filter_map(|k| k.as_str().map(str::to_string))
            .collect();
        anyhow::anyhow!("parse config: missing top-level `catalogs:` (found keys: {keys:?})")
    })?;

    let catalogs: BTreeMap<String, CatalogSpec> =
        serde_yaml::from_value(catalogs_value.clone()).map_err(|e| {
            anyhow::anyhow!(
                "parse config `catalogs` section: {e}\n\
Hint: quote Windows paths, e.g. warehouse: \"C:/path/to/warehouse\""
            )
        })?;

    if catalogs.is_empty() {
        anyhow::bail!("config has no catalogs; add at least one entry under `catalogs:`");
    }

    Ok(AppConfig {
        default_catalog,
        catalogs,
    })
}

pub fn load(path: &Path) -> Result<AppConfig> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("read config {}", path.display()))?;
    load_str(&text).with_context(|| format!("config {}", path.display()))
}

pub fn default_config_path() -> PathBuf {
    if let Ok(path) = std::env::var("ICEBERG_DB_CONFIG") {
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
        return PathBuf::from(home)
            .join(".iceberg-db")
            .join("config.yaml");
    }
    PathBuf::from("config.yaml")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_env_placeholder() {
        std::env::set_var("ICEBERG_DB_WAREHOUSE", "/tmp/wh");
        assert_eq!(
            resolve_value("${ICEBERG_DB_WAREHOUSE}"),
            "/tmp/wh".to_string()
        );
    }

    #[test]
    fn resolve_value_passes_through_literals() {
        assert_eq!(resolve_value("plain"), "plain");
        assert_eq!(resolve_value(""), "");
    }

    #[test]
    fn resolve_value_missing_env_var_becomes_empty() {
        let name = "IDB_TEST_NEVER_SET_MISSING_d4f1";
        std::env::remove_var(name);
        let placeholder = format!("${{{name}}}");
        assert_eq!(resolve_value(&placeholder), "");
        assert_eq!(
            resolve_value(&format!("prefix-{placeholder}-suffix")),
            "prefix--suffix"
        );
    }

    #[test]
    fn resolve_value_handles_multiple_placeholders() {
        let name_a = "IDB_TEST_MULTI_A_3c2b";
        let name_b = "IDB_TEST_MULTI_B_3c2b";
        std::env::set_var(name_a, "alpha");
        std::env::set_var(name_b, "beta");
        let input = format!("${{{name_a}}}/${{{name_b}}}");
        assert_eq!(resolve_value(&input), "alpha/beta");
        std::env::remove_var(name_a);
        std::env::remove_var(name_b);
    }

    #[test]
    fn resolve_value_unterminated_placeholder_consumes_remainder() {
        // The implementation reads chars until `}`; without one, it consumes the
        // entire suffix as the variable name. Verify the unset → empty case so
        // the function never panics on a typo'd template.
        let name = "IDB_TEST_UNCLOSED_PLACEHOLDER_9a7f";
        std::env::remove_var(name);
        assert_eq!(resolve_value(&format!("${{{name}")), "");
    }

    #[test]
    fn parses_hadoop_catalog_yaml() {
        let yaml = r#"default-catalog: local

catalogs:
  local:
    type: hadoop
    warehouse: "C:/Users/chand/.iceberg-db/warehouse"
"#;
        let cfg: AppConfig = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(cfg.default_catalog.as_deref(), Some("local"));
        assert_eq!(cfg.catalogs.len(), 1);
        assert_eq!(cfg.catalogs["local"].catalog_type, "hadoop");
    }

    #[test]
    fn unquoted_windows_path_breaks_catalogs_key() {
        let yaml = r#"default-catalog: local

catalogs:
  local:
    type: hadoop
    warehouse: C:/Users/chand/.iceberg-db/warehouse
"#;
        let root: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        assert!(
            root.get("catalogs").is_some(),
            "catalogs key should exist; root={root:?}"
        );
    }

    #[test]
    fn horizon_scope_with_colons_round_trips_in_yaml() {
        // YAML plain scalars allow colons in the value as long as the colon
        // isn't followed by a space (which would start a mapping). Both quoted
        // and unquoted forms therefore round-trip identically. The JS UI still
        // double-quotes for safety, but the parser does not depend on it.
        let pat = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.test-pat-value-part";
        let scope = "session:role:DATA_ENGINEER_ROLE";

        let unquoted = format!(
            r#"default-catalog: snowflake_horizon
catalogs:
  snowflake_horizon:
    type: rest
    profile: snowflake-horizon
    uri: https://xy.snowflakecomputing.com/polaris/api/catalog
    token: {pat}
    scope: {scope}
"#
        );
        let quoted = format!(
            r#"default-catalog: snowflake_horizon
catalogs:
  snowflake_horizon:
    type: rest
    profile: snowflake-horizon
    uri: "https://xy.snowflakecomputing.com/polaris/api/catalog"
    token: "{pat}"
    scope: "{scope}"
"#
        );

        let read_scope = |yaml: &str| -> String {
            load_str(yaml).expect("parse").catalogs["snowflake_horizon"]
                .property("scope")
                .unwrap_or_default()
        };
        assert_eq!(read_scope(&quoted), scope);
        assert_eq!(read_scope(&unquoted), scope);
    }

    #[test]
    fn load_strips_bom() {
        let dir = std::env::temp_dir().join("idb-config-test");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.yaml");
        fs::write(
            &path,
            "\u{feff}default-catalog: local\n\ncatalogs:\n  local:\n    type: hadoop\n    warehouse: /tmp/wh\n",
        )
        .unwrap();
        let cfg = load(&path).expect("load with BOM");
        assert_eq!(cfg.catalogs.len(), 1);
    }
}
