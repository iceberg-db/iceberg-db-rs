//! Dev proxy: `/{account}/polaris/...` → `https://{account}.snowflakecomputing.com/polaris/...`
//! Trunk rewrites `/sf/` → `http://127.0.0.1:8787/`.

use std::io::Read;
use std::sync::Arc;
use std::thread;

use anyhow::{Context, Result};
use reqsign::{AwsCredential, AwsV4Signer};
use reqwest::blocking::Client;
use reqwest::header::{HeaderName, HeaderValue, CONTENT_TYPE, HOST, RANGE};
use reqwest::Method as HttpMethod;
use serde::Deserialize;
use tiny_http::{Header, Method, Response, Server, StatusCode};

const DEFAULT_PORT: u16 = 8787;

/// Dev origins for the Trunk UI (WASM calls this proxy directly; Trunk /sf/ may drop POST bodies).
fn cors_origin(request: &tiny_http::Request) -> Option<String> {
    let origin = request
        .headers()
        .iter()
        .find(|h| h.field.equiv("Origin"))
        .map(|h| h.value.as_str())?;
    if origin.starts_with("http://127.0.0.1:")
        || origin.starts_with("http://localhost:")
        || origin.starts_with("http://[::1]:")
    {
        Some(origin.to_string())
    } else {
        None
    }
}

const CORS_ALLOW_HEADERS_DEFAULT: &str = "Authorization, Content-Type, Range, X-Amz-Date, X-Amz-Security-Token, X-Amz-Content-Sha256, X-Amz-Algorithm, X-Amz-Credential, X-Amz-SignedHeaders, X-Amz-Signature, X-Iceberg-Access-Delegation";

/// Echo browser preflight `Access-Control-Request-Headers` so SigV4 / AWS headers are allowed.
fn cors_allow_headers(request: &tiny_http::Request) -> String {
    request
        .headers()
        .iter()
        .find(|h| h.field.equiv("Access-Control-Request-Headers"))
        .map(|h| h.value.as_str().to_string())
        .unwrap_or_else(|| CORS_ALLOW_HEADERS_DEFAULT.to_string())
}

fn with_cors<R: Read>(mut response: Response<R>, origin: Option<&str>) -> Response<R> {
    let Some(origin) = origin else {
        return response;
    };
    let _ = response.add_header(
        Header::from_bytes(b"Access-Control-Allow-Origin", origin.as_bytes()).unwrap(),
    );
    let _ = response.add_header(
        Header::from_bytes(b"Access-Control-Allow-Credentials", b"true").unwrap(),
    );
    let _ = response.add_header(
        Header::from_bytes(
            b"Vary",
            b"Origin, Access-Control-Request-Method, Access-Control-Request-Headers",
        )
        .unwrap(),
    );
    response
}

fn main() -> Result<()> {
    let port = std::env::var("SNOWFLAKE_PROXY_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PORT);
    let addr = format!("127.0.0.1:{port}");
    let server = Server::http(&addr).map_err(|e| anyhow::anyhow!("bind http://{addr}/: {e}"))?;
    let client = Arc::new(
        Client::builder()
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .context("build HTTP client")?,
    );

    eprintln!("idb-sf-proxy listening on http://{addr}/ (browser WASM + optional Trunk /sf/)");

    for request in server.incoming_requests() {
        let client = client.clone();
        thread::spawn(move || {
            if let Err(e) = handle(client, request) {
                eprintln!("idb-sf-proxy: {e:#}");
            }
        });
    }
    Ok(())
}

fn path_and_query(url: &str) -> (&str, Option<&str>) {
    let path_q = if let Some(after) = url.split("://").nth(1) {
        after.find('/').map(|i| &after[i..]).unwrap_or("/")
    } else {
        url
    };
    let path_q = path_q.trim_start_matches('/');
    match path_q.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path_q, None),
    }
}

fn handle(client: Arc<Client>, mut request: tiny_http::Request) -> Result<()> {
    let method = request.method().clone();
    let cors = cors_origin(&request);
    let request_url = request.url().to_string();
    let (path, query) = path_and_query(&request_url);

    if method == Method::Options {
        let mut response = Response::empty(StatusCode(204));
        if let Some(origin) = cors.as_deref() {
            let allow_headers = cors_allow_headers(&request);
            let _ = response.add_header(
                Header::from_bytes(b"Access-Control-Allow-Origin", origin.as_bytes()).unwrap(),
            );
            let _ = response.add_header(
                Header::from_bytes(b"Access-Control-Allow-Credentials", b"true").unwrap(),
            );
            let _ = response.add_header(
                Header::from_bytes(
                    b"Access-Control-Allow-Methods",
                    b"GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS",
                )
                .unwrap(),
            );
            let _ = response.add_header(
                Header::from_bytes(b"Access-Control-Allow-Headers", allow_headers.as_bytes()).unwrap(),
            );
            let _ = response.add_header(Header::from_bytes(b"Access-Control-Max-Age", b"86400").unwrap());
        }
        request.respond(response)?;
        return Ok(());
    }

    if path == "health" {
        let response = with_cors(
            Response::from_string("ok").with_status_code(StatusCode(200)),
            cors.as_deref(),
        );
        request.respond(response)?;
        return Ok(());
    }

    if path == "_s3_signed" {
        return handle_s3_signed(client, request, cors);
    }

    if path == "_s3" {
        return handle_s3(client, request, query, cors);
    }

    let mut segments = path.splitn(2, '/');
    let account = segments
        .next()
        .filter(|s| !s.is_empty())
        .context("missing account in path (expected /{account}/polaris/...)")?;
    let rest = segments.next().unwrap_or("").to_string();
    let oauth_tokens = rest.contains("oauth/tokens");

    let mut upstream = format!("https://{account}.snowflakecomputing.com/{rest}");
    if let Some(q) = query {
        upstream.push('?');
        upstream.push_str(q);
    }

    eprintln!("{method} {upstream}");

    let inbound_content_type = request
        .headers()
        .iter()
        .find(|h| h.field.equiv("content-type"))
        .map(|h| h.value.as_str().to_string());

    let mut body = Vec::new();
    request.as_reader().read_to_end(&mut body)?;

    if oauth_tokens {
        eprintln!(
            "  oauth/tokens: inbound body {} bytes, content-type {:?}",
            body.len(),
            inbound_content_type
        );
        log_oauth_form_field_lengths(&body);
        if body.is_empty() && matches!(method, Method::Post) {
            eprintln!("  oauth/tokens: WARNING empty POST body — OAuth will fail (403)");
        }
    }

    let mut content_type: Option<String> = None;
    let mut req = client.request(map_method(&method), &upstream);
    for header in request.headers() {
        if header.field.equiv("content-type") {
            content_type = Some(header.value.as_str().to_string());
            continue;
        }
        let name = header.field.as_str().as_str();
        if strip_upstream_header(name) {
            if oauth_tokens && name.eq_ignore_ascii_case("origin") {
                eprintln!("  oauth/tokens: stripping Origin (browser-only; curl does not send this)");
            }
            continue;
        }
        if let Ok(hname) = HeaderName::from_bytes(name.as_bytes()) {
            if let Ok(value) = HeaderValue::from_str(header.value.as_str()) {
                req = req.header(hname, value);
            }
        }
    }
    if !body.is_empty() {
        if let Some(ct) = content_type.as_deref() {
            req = req.header(CONTENT_TYPE, ct);
        } else if matches!(
            method,
            Method::Post | Method::Put | Method::Patch
        ) {
            req = req.header(CONTENT_TYPE, "application/x-www-form-urlencoded");
        }
        req = req.body(body);
    }

    let resp = req.send().context("upstream Snowflake request")?;
    let upstream_status = resp.status();
    let status = StatusCode(upstream_status.as_u16());
    let out_headers: Vec<Header> = resp
        .headers()
        .iter()
        .filter(|(name, _)| !strip_upstream_header(name.as_str()))
        .filter_map(|(name, value)| {
            Header::from_bytes(name.as_str().as_bytes(), value.as_bytes()).ok()
        })
        .collect();

    let bytes = resp.bytes().context("read upstream body")?;
    if oauth_tokens {
        eprintln!(
            "  oauth/tokens: upstream {upstream_status} body {} bytes",
            bytes.len()
        );
    }
    let mut response = Response::from_data(bytes).with_status_code(status);
    for h in out_headers {
        response = response.with_header(h);
    }
    let response = with_cors(response, cors.as_deref());

    request.respond(response)?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct S3SignedProxyBody {
    url: String,
    method: String,
    range: Option<String>,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    region: String,
}

/// Browser sends JSON + vended creds; proxy signs and fetches S3 (no SigV4 headers in the browser).
fn handle_s3_signed(
    client: Arc<Client>,
    mut request: tiny_http::Request,
    cors: Option<String>,
) -> Result<()> {
    if request.method() != &Method::Post {
        anyhow::bail!("_s3_signed expects POST");
    }
    let mut body = Vec::new();
    request.as_reader().read_to_end(&mut body)?;
    let body: S3SignedProxyBody =
        serde_json::from_slice(&body).context("parse _s3_signed JSON body")?;

    let object = body
        .url
        .rsplit('/')
        .next()
        .unwrap_or("?");
    eprintln!("POST _s3_signed {:?} …/{object}", body.method);

    let http_method = match body.method.to_ascii_uppercase().as_str() {
        "HEAD" => HttpMethod::HEAD,
        _ => HttpMethod::GET,
    };
    let mut req = client
        .request(http_method, &body.url)
        .build()
        .context("build S3 request")?;
    if let Some(range) = body.range.filter(|s| !s.is_empty()) {
        req.headers_mut().insert(RANGE, HeaderValue::from_str(&range)?);
    }
    let cred = AwsCredential {
        access_key_id: body.access_key_id,
        secret_access_key: body.secret_access_key,
        session_token: body.session_token.filter(|s| !s.is_empty()),
        expires_in: None,
    };
    let signer = AwsV4Signer::new("s3", &body.region);
    signer
        .sign(&mut req, &cred)
        .map_err(|e| anyhow::anyhow!("sigv4: {e}"))?;

    let resp = client.execute(req).context("upstream S3 request")?;
    let upstream_status = resp.status();
    let status = StatusCode(upstream_status.as_u16());
    let out_headers: Vec<Header> = resp
        .headers()
        .iter()
        .filter(|(name, _)| !strip_upstream_header(name.as_str()))
        .filter_map(|(name, value)| {
            Header::from_bytes(name.as_str().as_bytes(), value.as_bytes()).ok()
        })
        .collect();
    let bytes = resp.bytes().context("read S3 body")?;
    eprintln!("  _s3_signed: upstream {upstream_status} {} bytes", bytes.len());

    let mut response = Response::from_data(bytes).with_status_code(status);
    for h in out_headers {
        response = response.with_header(h);
    }
    let response = with_cors(response, cors.as_deref());
    request.respond(response)?;
    Ok(())
}

fn query_param<'a>(query: Option<&'a str>, key: &str) -> Option<String> {
    let query = query?;
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=')?;
        if k == key {
            return urlencoding::decode(v)
                .ok()
                .map(|s| s.into_owned());
        }
    }
    None
}

/// Forward a browser-signed S3 GET/HEAD to the real object URL (no S3 CORS in dev).
fn handle_s3(
    client: Arc<Client>,
    mut request: tiny_http::Request,
    query: Option<&str>,
    cors: Option<String>,
) -> Result<()> {
    let target = query_param(query, "u").context("_s3 missing query param u=<https://...>")?;
    let target_host = target
        .split("://")
        .nth(1)
        .and_then(|rest| rest.split('/').next())
        .unwrap_or("s3");
    eprintln!("{} _s3 → {}", request.method(), target_host);

    let mut body = Vec::new();
    request.as_reader().read_to_end(&mut body)?;

    let mut req = client.request(map_method(request.method()), &target);
    if let Some(host) = target
        .split("://")
        .nth(1)
        .and_then(|rest| rest.split('/').next())
    {
        req = req.header(HOST, host);
    }
    for header in request.headers() {
        let name = header.field.as_str().as_str();
        if strip_upstream_header(name) || name.eq_ignore_ascii_case("host") {
            continue;
        }
        if let Ok(hname) = HeaderName::from_bytes(name.as_bytes()) {
            if let Ok(value) = HeaderValue::from_str(header.value.as_str()) {
                req = req.header(hname, value);
            }
        }
    }
    if !body.is_empty() {
        req = req.body(body);
    }

    let resp = req.send().context("upstream S3 request")?;
    let upstream_status = resp.status();
    let status = StatusCode(upstream_status.as_u16());
    let out_headers: Vec<Header> = resp
        .headers()
        .iter()
        .filter(|(name, _)| !strip_upstream_header(name.as_str()))
        .filter_map(|(name, value)| {
            Header::from_bytes(name.as_str().as_bytes(), value.as_bytes()).ok()
        })
        .collect();

    let bytes = resp.bytes().context("read upstream S3 body")?;
    let mut response = Response::from_data(bytes).with_status_code(status);
    for h in out_headers {
        response = response.with_header(h);
    }
    let response = with_cors(response, cors.as_deref());
    request.respond(response)?;
    Ok(())
}

fn map_method(method: &Method) -> reqwest::Method {
    match method {
        Method::Get => reqwest::Method::GET,
        Method::Post => reqwest::Method::POST,
        Method::Put => reqwest::Method::PUT,
        Method::Delete => reqwest::Method::DELETE,
        Method::Head => reqwest::Method::HEAD,
        Method::Options => reqwest::Method::OPTIONS,
        Method::Patch => reqwest::Method::PATCH,
        _ => reqwest::Method::GET,
    }
}

fn is_hop_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "host" | "connection" | "content-length" | "transfer-encoding" | "keep-alive" | "proxy-connection"
    )
}

/// Headers sent by browsers but not by curl — Snowflake OAuth often returns 403 if forwarded.
fn strip_upstream_header(name: &str) -> bool {
    if is_hop_header(name) {
        return true;
    }
    let n = name.to_ascii_lowercase();
    if n.starts_with("sec-") {
        return true;
    }
    matches!(
        n.as_str(),
        "origin"
            | "referer"
            | "access-control-request-method"
            | "access-control-request-headers"
            | "priority"
            | "x-requested-with"
    )
}

fn log_oauth_form_field_lengths(body: &[u8]) {
    let Ok(s) = std::str::from_utf8(body) else {
        return;
    };
    for pair in s.split('&') {
        let (key, enc_len) = pair
            .split_once('=')
            .map(|(k, v)| (k, v.len()))
            .unwrap_or((pair, 0));
        eprintln!("  oauth form field {key} (url-encoded value len {enc_len})");
    }
}
