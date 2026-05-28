//! DataFusion SQL session over Iceberg catalogs.

mod case_insensitive;

#[cfg(target_arch = "wasm32")]
mod wasm_demo;
#[cfg(all(target_arch = "wasm32", feature = "native"))]
mod wasm_lazy_catalog;

use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use iceberg::Catalog;
use iceberg_datafusion::IcebergCatalogProvider;
#[cfg(feature = "native")]
use idb_catalog::CatalogRegistry;

#[cfg(target_arch = "wasm32")]
macro_rules! wasm_query_step {
    ($($t:tt)*) => {
        web_sys::console::log_1(&format!("idb_query: {}", format!($($t)*)).into());
    };
}

#[cfg(target_arch = "wasm32")]
async fn collect_batches_wasm(
    df: datafusion::dataframe::DataFrame,
) -> Result<Vec<RecordBatch>> {
    // `collect()` avoids DataFusion `execute_stream` spawning extra tokio tasks on wasm32.
    let batches = df.collect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    wasm_query_step!("collect finished, {} batch(es), {} row(s)", batches.len(), n);
    Ok(batches)
}

/// DataFusion on wasm32: avoid parallel tokio tasks that never run on a single-thread runtime.
#[cfg(target_arch = "wasm32")]
pub(crate) fn wasm_session_config(catalog: &str, schema: &str) -> SessionConfig {
    let mut config = SessionConfig::new()
        .with_information_schema(true)
        .with_create_default_catalog_and_schema(false)
        .with_default_catalog_and_schema(catalog, schema);
    config.options_mut().execution.target_partitions = 1;
    config
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub batches: Vec<RecordBatch>,
    pub row_count: usize,
    pub elapsed_ms: u64,
    /// HTTP/S3 response bytes read during this query (WASM Horizon; 0 for demo/native).
    pub bytes_fetched: u64,
    /// Distinct S3 objects fetched (WASM Horizon; 0 for demo/native).
    pub files_fetched: u64,
}

pub struct SqlSession {
    ctx: SessionContext,
    default_catalog: String,
    default_schema: String,
    iceberg_catalog: Arc<dyn Catalog>,
    /// WASM demo uses a DataFusion memory catalog; `SHOW TABLES` reads from it.
    wasm_demo: bool,
}

impl SqlSession {
    /// Browser demo: `demo.customers` in a DataFusion memory catalog (no Iceberg/Moka).
    #[cfg(target_arch = "wasm32")]
    pub async fn from_wasm_demo() -> Result<Self> {
        wasm_demo::open_wasm_demo_session().await
    }

    #[cfg(feature = "native")]
    pub async fn from_registry(registry: &CatalogRegistry) -> Result<Self> {
        let default_catalog = registry.default_name().to_string();
        let default_schema = registry.default_schema().to_string();
        let iceberg_catalog = registry.default();
        #[cfg(target_arch = "wasm32")]
        {
            return wasm_lazy_catalog::open_wasm_horizon_session(
                default_catalog,
                default_schema,
                iceberg_catalog,
            )
            .await;
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            Self::from_iceberg_catalog(default_catalog, default_schema, iceberg_catalog).await
        }
    }

    pub async fn from_iceberg_catalog(
        catalog_name: String,
        default_schema: String,
        iceberg_catalog: Arc<dyn Catalog>,
    ) -> Result<Self> {
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_create_default_catalog_and_schema(false)
            .with_default_catalog_and_schema(&catalog_name, &default_schema);
        let ctx = SessionContext::new_with_config(config);
        let provider = IcebergCatalogProvider::try_new(iceberg_catalog.clone())
            .await
            .map_err(|e| {
                let msg = format!("{e}");
                let hint = if msg.contains("500") || msg.contains("status") {
                    "\nHorizon hint: use a Snowflake PAT (exchanged via OAuth), correct account URI, \
warehouse = database name, and scope session:role:<role> matching the PAT."
                } else {
                    ""
                };
                anyhow::anyhow!("iceberg catalog provider: {msg}{hint}")
            })?;
        let provider = case_insensitive::wrap_catalog(Arc::new(provider));
        ctx.register_catalog(&catalog_name, provider);
        ctx.catalog(&catalog_name)
            .with_context(|| format!("catalog '{catalog_name}' not registered"))?;
        Ok(Self {
            ctx,
            default_catalog: catalog_name,
            default_schema,
            iceberg_catalog,
            wasm_demo: false,
        })
    }

    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }

    pub fn default_catalog(&self) -> &str {
        &self.default_catalog
    }

    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        #[cfg(all(target_arch = "wasm32", feature = "native"))]
        idb_catalog::reset_bytes_fetched();
        let started = QueryTimer::start();
        if let Some(schema) = parse_show_tables(sql) {
            let schema = schema.unwrap_or_else(|| self.default_schema.clone());
            return self.show_tables(&schema, started).await;
        }
        #[cfg(target_arch = "wasm32")]
        wasm_query_step!("planning SQL");
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| plan_sql_error(&e, &self.default_catalog, &self.default_schema))?;
        #[cfg(target_arch = "wasm32")]
        wasm_query_step!("executing (Iceberg scan / S3)");
        let schema = Arc::new(df.schema().as_arrow().clone());
        #[cfg(target_arch = "wasm32")]
        let batches = collect_batches_wasm(df).await.context("execute sql")?;
        #[cfg(not(target_arch = "wasm32"))]
        let batches = df.collect().await.context("execute sql")?;
        let columns = schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().clone(),
                data_type: format!("{:?}", f.data_type()),
            })
            .collect();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        Ok(QueryResult {
            columns,
            batches,
            row_count,
            elapsed_ms: started.elapsed_ms(),
            bytes_fetched: query_bytes_fetched(),
            files_fetched: query_files_fetched(),
        })
    }

    /// Flatten result batches into display strings for the WASM UI grid.
    pub fn rows_as_strings(batches: &[RecordBatch]) -> Vec<Vec<String>> {
        use datafusion::arrow::util::display::array_value_to_string;
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let mut cells = Vec::with_capacity(batch.num_columns());
                for col in 0..batch.num_columns() {
                    let value = array_value_to_string(batch.column(col), row)
                        .unwrap_or_else(|_| "NULL".to_string());
                    cells.push(value);
                }
                rows.push(cells);
            }
        }
        rows
    }

    pub fn format_batches_table(batches: &[RecordBatch]) -> String {
        use datafusion::arrow::util::pretty::pretty_format_batches;
        pretty_format_batches(batches)
            .map(|t| t.to_string())
            .unwrap_or_else(|e| format!("(could not format: {e})"))
    }

    async fn show_tables(&self, schema: &str, started: QueryTimer) -> Result<QueryResult> {
        let names: Vec<String> = if self.wasm_demo {
            let catalog = self
                .ctx
                .catalog(&self.default_catalog)
                .with_context(|| format!("catalog '{}' not found", self.default_catalog))?;
            let schema_provider = catalog
                .schema(schema)
                .with_context(|| format!("schema '{schema}' not found"))?;
            schema_provider.table_names()
        } else {
            use iceberg::NamespaceIdent;

            let namespace = NamespaceIdent::from_vec(vec![schema.to_string()])
                .map_err(|e| anyhow::anyhow!("invalid namespace '{schema}': {e}"))?;
            let tables = self
                .iceberg_catalog
                .list_tables(&namespace)
                .await
                .map_err(|e| anyhow::anyhow!("list tables in '{schema}': {e}"))?;
            tables.iter().map(|t| t.name().to_string()).collect()
        };
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let schema_arrow = Arc::new(Schema::new(vec![Field::new(
            "table_name",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema_arrow.clone(),
            vec![Arc::new(StringArray::from(names))],
        )
        .context("build SHOW TABLES result")?;
        let columns = schema_arrow
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().clone(),
                data_type: format!("{:?}", f.data_type()),
            })
            .collect();
        let row_count = batch.num_rows();
        Ok(QueryResult {
            columns,
            batches: vec![batch],
            row_count,
            elapsed_ms: started.elapsed_ms(),
            bytes_fetched: query_bytes_fetched(),
            files_fetched: query_files_fetched(),
        })
    }

    pub async fn explain(&self, sql: &str) -> Result<String> {
        let plan = self
            .ctx
            .sql(&format!("EXPLAIN {sql}"))
            .await
            .context("explain plan")?;
        let batches = plan.collect().await.context("collect explain")?;
        let mut lines = Vec::new();
        for batch in batches {
            let col = batch.column(0);
            push_utf8_column(col, &mut lines);
        }
        Ok(lines.join("\n"))
    }
}

fn query_bytes_fetched() -> u64 {
    #[cfg(all(target_arch = "wasm32", feature = "native"))]
    {
        return idb_catalog::bytes_fetched();
    }
    #[cfg(not(all(target_arch = "wasm32", feature = "native")))]
    0
}

fn query_files_fetched() -> u64 {
    #[cfg(all(target_arch = "wasm32", feature = "native"))]
    {
        return idb_catalog::files_fetched();
    }
    #[cfg(not(all(target_arch = "wasm32", feature = "native")))]
    0
}

struct QueryTimer {
    #[cfg(not(target_arch = "wasm32"))]
    inner: std::time::Instant,
    #[cfg(target_arch = "wasm32")]
    inner: web_time::Instant,
}

impl QueryTimer {
    fn start() -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        return Self {
            inner: std::time::Instant::now(),
        };
        #[cfg(target_arch = "wasm32")]
        return Self {
            inner: web_time::Instant::now(),
        };
    }

    fn elapsed_ms(&self) -> u64 {
        self.inner
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64
    }
}

/// Strip a leading ASCII (case-insensitive) prefix, returning the rest of `s`.
///
/// Safe for any UTF-8 input: matching is done on raw bytes against an ASCII
/// prefix, so slicing at `prefix.len()` always lands on a `char` boundary.
fn strip_ascii_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let prefix_bytes = prefix.as_bytes();
    let s_bytes = s.as_bytes();
    if s_bytes.len() < prefix_bytes.len() {
        return None;
    }
    if !s_bytes[..prefix_bytes.len()].eq_ignore_ascii_case(prefix_bytes) {
        return None;
    }
    Some(&s[prefix_bytes.len()..])
}

/// Parse `SHOW TABLES [IN <schema>]`.
///
/// Returns:
/// - `None`            — not a `SHOW TABLES` statement (fall through to DataFusion).
/// - `Some(None)`      — bare `SHOW TABLES`, use session default schema.
/// - `Some(Some(s))`   — explicit schema. For `catalog.schema`, only `schema` is kept.
fn parse_show_tables(sql: &str) -> Option<Option<String>> {
    let s = sql.trim().trim_end_matches(';').trim();
    let after = strip_ascii_prefix_ci(s, "show tables")?;

    // Anything other than whitespace immediately after the literal disqualifies
    // (e.g. "SHOW TABLESS"); same logic catches "SHOW TABLESIN foo".
    if !after.is_empty() && !after.starts_with(|c: char| c.is_whitespace()) {
        return None;
    }

    let tail = after.trim();
    if tail.is_empty() {
        return Some(None);
    }

    let after_in = strip_ascii_prefix_ci(tail, "in")?;
    if !after_in.starts_with(|c: char| c.is_whitespace()) {
        return None;
    }

    let schema = after_in.trim();
    if schema.is_empty() {
        return None;
    }

    // `catalog.schema` → use schema segment for Iceberg namespace.
    let schema = schema
        .rsplit('.')
        .next()
        .unwrap_or(schema)
        .trim_matches('"')
        .trim_matches('`')
        .to_string();
    Some(Some(schema))
}

fn plan_sql_error(
    err: &datafusion::error::DataFusionError,
    default_catalog: &str,
    default_schema: &str,
) -> anyhow::Error {
    let msg = err.to_string();
    let hint = if msg.contains("not found") {
        format!(
            "\nNaming: `{default_catalog}` is the **catalog** name from config.yaml (not the Snowflake database). \
The database is `warehouse` (e.g. ICEBERG_TEST). \
Default schema is `{default_schema}`. \
Snowflake IRC identifiers are often uppercase in the catalog — try \
`SELECT * FROM {default_schema}.employee` or `ICEBERG_TEST.EMPLOYEE` if needed."
        )
    } else {
        String::new()
    };
    anyhow::anyhow!("plan sql: {msg}{hint}")
}

fn push_utf8_column(col: &dyn Array, lines: &mut Vec<String>) {
    use datafusion::arrow::array::LargeStringArray;

    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        for row in 0..arr.len() {
            if !arr.is_null(row) {
                lines.push(arr.value(row).to_string());
            }
        }
    } else if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
        for row in 0..arr.len() {
            if !arr.is_null(row) {
                lines.push(arr.value(row).to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn show_tables_bare_forms() {
        assert_eq!(parse_show_tables("SHOW TABLES"), Some(None));
        assert_eq!(parse_show_tables("show tables;"), Some(None));
        assert_eq!(parse_show_tables("  show tables  "), Some(None));
        assert_eq!(parse_show_tables("Show TaBLes"), Some(None));
        assert_eq!(parse_show_tables("show tables ;"), Some(None));
    }

    #[test]
    fn show_tables_in_schema_variants() {
        assert_eq!(
            parse_show_tables("SHOW TABLES IN iceberg_test"),
            Some(Some("iceberg_test".into()))
        );
        assert_eq!(
            parse_show_tables("show tables in iceberg_test"),
            Some(Some("iceberg_test".into()))
        );
        assert_eq!(
            parse_show_tables("SHOW TABLES IN snowflake_horizon.iceberg_test"),
            Some(Some("iceberg_test".into()))
        );
        assert_eq!(
            parse_show_tables(r#"SHOW TABLES IN "iceberg_test""#),
            Some(Some("iceberg_test".into()))
        );
        assert_eq!(
            parse_show_tables("SHOW TABLES IN `iceberg_test`"),
            Some(Some("iceberg_test".into()))
        );
        assert_eq!(
            parse_show_tables("SHOW TABLES IN iceberg_test;"),
            Some(Some("iceberg_test".into()))
        );
    }

    #[test]
    fn show_tables_not_matching() {
        assert_eq!(parse_show_tables("SELECT 1"), None);
        assert_eq!(parse_show_tables(""), None);
        assert_eq!(parse_show_tables(";"), None);
        // "SHOW TABLESS" — non-whitespace after literal disqualifies.
        assert_eq!(parse_show_tables("SHOW TABLESS"), None);
        // "SHOW TABLE" alone is not the prefix.
        assert_eq!(parse_show_tables("SHOW TABLE"), None);
        // "SHOW TABLES INFOO" — IN not followed by space.
        assert_eq!(parse_show_tables("SHOW TABLES INFOO"), None);
        // "SHOW TABLES IN" with no schema.
        assert_eq!(parse_show_tables("SHOW TABLES IN"), None);
    }

    #[test]
    fn show_tables_does_not_panic_on_unicode_prefix() {
        // The byte-indexed implementation must not panic on multibyte inputs.
        assert_eq!(parse_show_tables("αβγδ"), None);
        assert_eq!(parse_show_tables("ΣΗΟΨ ΤΑΒΛΕΣ"), None);
        assert_eq!(parse_show_tables("→ select 1"), None);
    }

    #[test]
    fn strip_ascii_prefix_ci_basic() {
        assert_eq!(strip_ascii_prefix_ci("FOObar", "foo"), Some("bar"));
        assert_eq!(strip_ascii_prefix_ci("foobar", "FOO"), Some("bar"));
        assert_eq!(strip_ascii_prefix_ci("baz", "foo"), None);
        assert_eq!(strip_ascii_prefix_ci("fo", "foo"), None);
        assert_eq!(strip_ascii_prefix_ci("αβγ", "foo"), None);
    }

    #[test]
    fn query_timer_elapsed_is_nonnegative() {
        let timer = QueryTimer::start();
        let elapsed = timer.elapsed_ms();
        // Just a smoke test; we cannot assert it's strictly positive on fast machines.
        assert!(elapsed <= u64::MAX);
    }
}
