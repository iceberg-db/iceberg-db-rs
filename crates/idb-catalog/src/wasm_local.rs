//! Browser HTTP runs on the JS executor; bridge to `Send` futures for `Catalog: Send`.
//!
//! `async fn run(future)` would store the non-Send `future` in its own state machine.
//! `run_local` moves `inner` into `spawn_local` synchronously and only awaits a Send oneshot.

#[cfg(target_arch = "wasm32")]
use std::future::Future;

#[cfg(target_arch = "wasm32")]
use anyhow::Context;
#[cfg(target_arch = "wasm32")]
use bytes::Bytes;
#[cfg(target_arch = "wasm32")]
use reqwest::header::CONTENT_LENGTH;
#[cfg(target_arch = "wasm32")]
use reqwest::{Client, Request, StatusCode};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;

#[cfg(target_arch = "wasm32")]
pub struct WasmHttpResponse {
    pub status: StatusCode,
    pub body: Bytes,
    pub content_length: Option<u64>,
}

/// Yield one turn to the browser event loop so a nested `spawn_local` task can run.
#[cfg(target_arch = "wasm32")]
async fn yield_to_event_loop() {
    let _ = JsFuture::from(js_sys::Promise::resolve(&wasm_bindgen::JsValue::UNDEFINED)).await;
}

/// Run a browser-only future on the JS microtask queue; the returned future is `Send`.
///
/// The non-`Send` reqwest/js future runs only inside `spawn_local`; the caller awaits a
/// `Send` oneshot receiver. We yield once before awaiting so nested tasks (S3 via proxy)
/// actually start when the parent is already on the wasm-bindgen executor.
#[cfg(target_arch = "wasm32")]
pub fn run_local<T>(inner: impl Future<Output = T> + 'static) -> impl Future<Output = T> + Send
where
    T: Send + 'static,
{
    let (tx, rx) = futures::channel::oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        // Yield inside the browser task (non-Send `JsFuture` is OK here).
        yield_to_event_loop().await;
        let result = inner.await;
        let _ = tx.send(result);
    });
    async move {
        rx.await
            .expect("browser HTTP task dropped before completion")
    }
}

#[cfg(target_arch = "wasm32")]
async fn from_response(resp: reqwest::Response) -> anyhow::Result<WasmHttpResponse> {
    let status = resp.status();
    let content_length = resp
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    let body = resp.bytes().await.context("read HTTP body")?;
    Ok(WasmHttpResponse {
        status,
        body,
        content_length,
    })
}

#[cfg(target_arch = "wasm32")]
pub async fn client_get(
    client: Client,
    url: String,
    bearer: String,
) -> anyhow::Result<WasmHttpResponse> {
    run_local(async move {
        web_sys::console::log_1(
            &format!(
                "idb_query: REST GET …{}",
                url.rsplit('/').next().unwrap_or("")
            )
            .into(),
        );
        let resp = client
            .get(&url)
            .header(reqwest::header::CACHE_CONTROL, "no-cache")
            .header(reqwest::header::PRAGMA, "no-cache")
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {bearer}"),
            )
            .send()
            .await
            .context("HTTP GET")?;
        from_response(resp).await
    })
    .await
}

#[cfg(target_arch = "wasm32")]
fn urlencode_form_body(form: &[(String, String)]) -> String {
    form.iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                urlencoding::encode(k),
                urlencoding::encode(v)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(target_arch = "wasm32")]
pub async fn client_post_form(
    url: String,
    form: Vec<(String, String)>,
) -> anyhow::Result<WasmHttpResponse> {
    run_local(async move {
        let body = urlencode_form_body(&form);
        let resp = Client::new()
            .post(&url)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(body)
            .send()
            .await
            .context("HTTP POST")?;
        from_response(resp).await
    })
    .await
}

#[cfg(target_arch = "wasm32")]
pub async fn client_execute(client: Client, request: Request) -> anyhow::Result<WasmHttpResponse> {
    run_local(async move {
        let resp = client.execute(request).await.context("HTTP execute")?;
        from_response(resp).await
    })
    .await
}
