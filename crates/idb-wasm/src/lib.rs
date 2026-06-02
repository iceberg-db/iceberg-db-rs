//! iceberg-db in the browser via WebAssembly.
//!
//! With feature `horizon` (default): Snowflake Horizon IRC REST catalog + DataFusion SQL.
//! With `idb_init_demo()`: in-memory `demo.customers` only.

use std::sync::{Arc, OnceLock};

#[cfg(target_arch = "wasm32")]
use tokio::runtime::Runtime;

use idb_sql::SqlSession;
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

static SESSION: OnceLock<Arc<SqlSession>> = OnceLock::new();

#[cfg(target_arch = "wasm32")]
const MAX_WASM_GRID_ROWS: usize = 5_000;

#[cfg(target_arch = "wasm32")]
static TOKIO: OnceLock<Runtime> = OnceLock::new();

#[cfg(target_arch = "wasm32")]
fn tokio_runtime() -> &'static Runtime {
    TOKIO.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("tokio runtime")
    })
}

#[cfg(target_arch = "wasm32")]
fn log_init(step: &str) {
    web_sys::console::log_1(&format!("idb_init: {step}").into());
}

#[cfg(target_arch = "wasm32")]
fn log_query(step: &str) {
    web_sys::console::log_1(&format!("idb_query: {step}").into());
}

fn store_session(session: SqlSession) -> Result<JsValue, JsValue> {
    SESSION
        .set(Arc::new(session))
        .map_err(|_| JsValue::from_str("engine already initialized; reload the page to reconfigure"))?;
    Ok(JsValue::from_str("ready"))
}

#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub fn idb_wasm_version() -> String {
    format!(
        "{}+{}+{}",
        env!("CARGO_PKG_VERSION"),
        env!("IDB_WASM_BUILD"),
        env!("IDB_WASM_BUILD_TAG")
    )
}

/// In-memory `demo.customers` (3 rows). No Snowflake / network.
#[wasm_bindgen]
pub fn idb_init_demo() -> js_sys::Promise {
    future_to_promise(async {
        #[cfg(target_arch = "wasm32")]
        {
            let _guard = tokio_runtime().enter();
            init_demo().await
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            init_demo().await
        }
    })
}

/// Legacy entry: use `idb_init_horizon` or `idb_init_demo` from JavaScript.
#[wasm_bindgen]
pub fn idb_init() -> js_sys::Promise {
    idb_init_demo()
}

/// Connect to Snowflake Horizon IRC using YAML config (same shape as `~/.iceberg-db/config.yaml`).
/// Pass the PAT as `token:` in YAML (browser cannot read `$env:SNOWFLAKE_ACCESS_TOKEN`).
#[cfg(feature = "horizon")]
#[wasm_bindgen]
pub fn idb_init_horizon(config_yaml: String) -> js_sys::Promise {
    future_to_promise(async move {
        #[cfg(target_arch = "wasm32")]
        {
            let _guard = tokio_runtime().enter();
            init_horizon(config_yaml).await
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            init_horizon(config_yaml).await
        }
    })
}

#[cfg(not(feature = "horizon"))]
#[wasm_bindgen]
pub fn idb_init_horizon(_config_yaml: String) -> js_sys::Promise {
    future_to_promise(async {
        Err(JsValue::from_str(
            "idb-wasm built without `horizon` feature; use idb_init_demo()",
        ))
    })
}

#[wasm_bindgen]
pub fn idb_query(sql: String) -> js_sys::Promise {
    future_to_promise(async move {
        #[cfg(target_arch = "wasm32")]
        let _guard = tokio_runtime().enter();
        run_query(sql).await
    })
}

/// Live HTTP/S3 bytes read for the in-flight query (poll from JS while `idb_query` runs).
#[wasm_bindgen]
pub fn idb_bytes_fetched() -> u64 {
    #[cfg(all(target_arch = "wasm32", feature = "horizon"))]
    {
        return idb_catalog::bytes_fetched();
    }
    0
}

/// Stop the in-flight query: new S3 reads fail fast; UI should ignore the eventual promise.
#[wasm_bindgen]
pub fn idb_cancel_query() {
    #[cfg(all(target_arch = "wasm32", feature = "horizon"))]
    {
        idb_catalog::request_query_cancel();
    }
}

/// Distinct S3 objects fetched so far for the in-flight query (poll from JS).
#[wasm_bindgen]
pub fn idb_files_fetched() -> u64 {
    #[cfg(all(target_arch = "wasm32", feature = "horizon"))]
    {
        return idb_catalog::files_fetched();
    }
    0
}

async fn init_demo() -> Result<JsValue, JsValue> {
    #[cfg(target_arch = "wasm32")]
    log_init("demo tables");

    let session = SqlSession::from_wasm_demo().await.map_err(js_error)?;

    #[cfg(target_arch = "wasm32")]
    log_init("done");

    store_session(session)
}

#[cfg(feature = "horizon")]
async fn init_horizon(config_yaml: String) -> Result<JsValue, JsValue> {
    use idb_catalog::CatalogRegistry;
    use idb_config::load_str;

    #[cfg(target_arch = "wasm32")]
    log_init("horizon catalog");

    if config_yaml.trim().is_empty() {
        return Err(JsValue::from_str(
            "Horizon config YAML is empty — use the Connect button in the UI (PAT + account host)",
        ));
    }

    let config = load_str(&config_yaml).map_err(js_error)?;
    let registry = CatalogRegistry::from_config(&config)
        .await
        .map_err(js_error)?;
    let session = SqlSession::from_registry(&registry)
        .await
        .map_err(js_error)?;

    #[cfg(target_arch = "wasm32")]
    log_init("done");

    store_session(session)
}

async fn run_query(sql: String) -> Result<JsValue, JsValue> {
    let session = SESSION
        .get()
        .ok_or_else(|| JsValue::from_str("call idb_init() or idb_init_horizon() first"))?
        .clone();

    #[cfg(target_arch = "wasm32")]
    {
        #[cfg(all(target_arch = "wasm32", feature = "horizon"))]
        idb_catalog::reset_query_state();
        log_query(&format!("start: {}", truncate_log(&sql, 80)));
        log_query("entering DataFusion session.query");
    }

    if query_cancelled_wasm() {
        return Err(JsValue::from_str("Query cancelled"));
    }

    let result = session.query(&sql).await.map_err(js_error)?;

    if query_cancelled_wasm() {
        return Err(JsValue::from_str("Query cancelled"));
    }

    #[cfg(target_arch = "wasm32")]
    {
        log_query(&format!(
            "done: {} row(s) in {} ms, {} file(s), {} bytes fetched, byte-cache hits={} misses={}",
            result.row_count,
            result.elapsed_ms,
            result.files_fetched,
            result.bytes_fetched,
            idb_catalog::byte_cache_hits(),
            idb_catalog::byte_cache_misses()
        ));
        log_query("serializing for UI");
    }

    let response = QueryResponse::from_result(&result).map_err(js_error)?;

    serde_wasm_bindgen::to_value(&response).map_err(js_error)
}

#[cfg(target_arch = "wasm32")]
fn truncate_log(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

#[derive(Serialize)]
struct QueryResponse {
    row_count: usize,
    elapsed_ms: u64,
    bytes_fetched: u64,
    files_fetched: u64,
    columns: Vec<ColumnDto>,
    rows: Vec<Vec<String>>,
    text: String,
}

#[derive(Serialize)]
struct ColumnDto {
    name: String,
    data_type: String,
}

impl QueryResponse {
    fn from_result(result: &idb_sql::QueryResult) -> anyhow::Result<Self> {
        #[cfg(target_arch = "wasm32")]
        {
            let mut rows = SqlSession::rows_as_strings(&result.batches);
            let truncated = rows.len() > MAX_WASM_GRID_ROWS;
            if truncated {
                rows.truncate(MAX_WASM_GRID_ROWS);
            }
            let text = if result.row_count > 500 {
                format!(
                    "{} row(s) in {} ms (text tab omitted for large results)",
                    result.row_count, result.elapsed_ms
                )
            } else {
                SqlSession::format_batches_table(&result.batches)
            };
            let text = if truncated {
                format!(
                    "{text}\n\n(grid shows first {MAX_WASM_GRID_ROWS} of {} rows)",
                    result.row_count
                )
            } else {
                text
            };
            return Ok(Self {
                row_count: result.row_count,
                elapsed_ms: result.elapsed_ms,
                bytes_fetched: result.bytes_fetched,
                files_fetched: result.files_fetched,
                columns: result
                    .columns
                    .iter()
                    .map(|c| ColumnDto {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                    })
                    .collect(),
                rows,
                text,
            });
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let text = SqlSession::format_batches_table(&result.batches);
            Ok(Self {
                row_count: result.row_count,
                elapsed_ms: result.elapsed_ms,
                bytes_fetched: result.bytes_fetched,
                files_fetched: result.files_fetched,
                columns: result
                    .columns
                    .iter()
                    .map(|c| ColumnDto {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                    })
                    .collect(),
                rows: SqlSession::rows_as_strings(&result.batches),
                text,
            })
        }
    }
}

#[cfg(all(target_arch = "wasm32", feature = "horizon"))]
fn query_cancelled_wasm() -> bool {
    idb_catalog::query_cancelled()
}

#[cfg(not(all(target_arch = "wasm32", feature = "horizon")))]
fn query_cancelled_wasm() -> bool {
    false
}

fn js_error(err: impl std::fmt::Display) -> JsValue {
    // anyhow chains: {:#} includes Snowflake OAuth status + JSON body
    JsValue::from_str(&format!("{err:#}"))
}
