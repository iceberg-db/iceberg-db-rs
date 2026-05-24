//! S3 storage for `wasm32` without OpenDAL (avoids reqwest/stream → tokio/fs).

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory, CLIENT_REGION, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS,
    S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
use iceberg::{Error, ErrorKind, Result};
use reqsign::AwsCredential;
use reqsign::AwsV4Signer;
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};

#[cfg(target_arch = "wasm32")]
fn wasm_log(msg: &str) {
    web_sys::console::log_1(&format!("{msg}").into());
}

use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasmS3StorageFactory {
    configured_scheme: String,
}

impl WasmS3StorageFactory {
    pub fn s3() -> Self {
        Self {
            configured_scheme: "s3".to_string(),
        }
    }
}

#[typetag::serde(name = "WasmS3StorageFactory")]
impl StorageFactory for WasmS3StorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let cfg = parse_s3_props(config.props().clone())?;
        Ok(Arc::new(WasmS3Storage {
            configured_scheme: self.configured_scheme.clone(),
            cfg,
            client: default_http_client(),
        }))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct S3Props {
    endpoint: Option<String>,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    region: String,
    path_style: bool,
    /// Local `idb-sf-proxy` base (e.g. http://127.0.0.1:8787) — forwards signed S3 reads.
    dev_proxy_base: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WasmS3Storage {
    configured_scheme: String,
    cfg: S3Props,
    #[serde(skip, default = "default_http_client")]
    client: Client,
}

fn default_http_client() -> Client {
    Client::new()
}

fn parse_s3_props(mut m: HashMap<String, String>) -> Result<S3Props> {
    let access_key_id = m
        .remove(S3_ACCESS_KEY_ID)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("missing {S3_ACCESS_KEY_ID} for wasm S3 storage"),
            )
        })?;
    let secret_access_key = m
        .remove(S3_SECRET_ACCESS_KEY)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("missing {S3_SECRET_ACCESS_KEY} for wasm S3 storage"),
            )
        })?;
    let session_token = m.remove(S3_SESSION_TOKEN).filter(|s| !s.is_empty());
    let endpoint = m.remove(S3_ENDPOINT).filter(|s| !s.is_empty());
    let region = m
        .remove(S3_REGION)
        .or_else(|| m.remove(CLIENT_REGION))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "us-east-1".to_string());
    let path_style = m
        .remove(S3_PATH_STYLE_ACCESS)
        .map(|s| is_truthy(&s.to_ascii_lowercase()))
        .unwrap_or(false);
    let dev_proxy_base = m
        .remove("s3.dev-proxy")
        .filter(|s| !s.is_empty());

    Ok(S3Props {
        endpoint,
        access_key_id,
        secret_access_key,
        session_token,
        region,
        path_style,
        dev_proxy_base,
    })
}

fn is_truthy(value: &str) -> bool {
    matches!(value, "1" | "true" | "yes" | "on")
}

struct ObjectRef {
    bucket: String,
    key: String,
}

impl WasmS3Storage {
    fn parse_location(&self, path: &str) -> Result<ObjectRef> {
        let url = Url::parse(path).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("invalid s3 url {path}: {e}"),
            )
        })?;
        let scheme = url.scheme();
        if scheme != "s3" && scheme != "s3a" && scheme != self.configured_scheme {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("expected s3/s3a URL, got scheme {scheme}"),
            ));
        }
        let bucket = url.host_str().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, format!("missing bucket in {path}"))
        })?;
        let key = url.path().trim_start_matches('/').to_string();
        if key.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("missing object key in {path}"),
            ));
        }
        Ok(ObjectRef {
            bucket: bucket.to_string(),
            key,
        })
    }

    fn object_url(&self, obj: &ObjectRef) -> Result<Url> {
        if let Some(endpoint) = &self.cfg.endpoint {
            let mut base = Url::parse(endpoint).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("invalid {S3_ENDPOINT} {endpoint}: {e}"),
                )
            })?;
            if self.cfg.path_style {
                let mut path = base.path().trim_end_matches('/').to_string();
                path.push('/');
                path.push_str(&obj.bucket);
                path.push('/');
                path.push_str(&obj.key);
                base.set_path(&path);
            } else {
                base.set_host(Some(&obj.bucket))
                    .map_err(|_| Error::new(ErrorKind::Unexpected, "invalid bucket host"))?;
                base.set_path(&format!("/{}", obj.key));
            }
            return Ok(base);
        }

        if self.cfg.path_style {
            let host = if self.cfg.region == "us-east-1" {
                "s3.amazonaws.com".to_string()
            } else {
                format!("s3.{}.amazonaws.com", self.cfg.region)
            };
            Ok(Url::parse(&format!(
                "https://{}/{}/{}",
                host, obj.bucket, obj.key
            ))
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("url: {e}")))?)
        } else {
            let host = if self.cfg.region == "us-east-1" {
                format!("{}.s3.amazonaws.com", obj.bucket)
            } else {
                format!("{}.s3.{}.amazonaws.com", obj.bucket, self.cfg.region)
            };
            Ok(Url::parse(&format!("https://{}/{}", host, obj.key))
                .map_err(|e| Error::new(ErrorKind::Unexpected, format!("url: {e}")))?)
        }
    }

    fn credential(&self) -> AwsCredential {
        AwsCredential {
            access_key_id: self.cfg.access_key_id.clone(),
            secret_access_key: self.cfg.secret_access_key.clone(),
            session_token: self.cfg.session_token.clone(),
            expires_in: None,
        }
    }

    async fn signed_request(
        &self,
        method: reqwest::Method,
        url: Url,
        range: Option<Range<u64>>,
    ) -> Result<crate::wasm_local::WasmHttpResponse> {
        if self.cfg.dev_proxy_base.is_some() {
            return self
                .signed_request_via_dev_proxy(method, url, range)
                .await;
        }
        self.signed_request_direct(method, url, range).await
    }

    async fn signed_request_direct(
        &self,
        method: reqwest::Method,
        url: Url,
        range: Option<Range<u64>>,
    ) -> Result<crate::wasm_local::WasmHttpResponse> {
        let client = self.client.clone();
        let region = self.cfg.region.clone();
        let cred = self.credential();
        crate::wasm_local::run_local(async move {
            let mut req = build_signed_s3_request(&client, &region, &cred, method, &url, range)?;
            let resp = client
                .execute(req)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, format!("s3 {url}: {e}")))?;
            wasm_response_from_reqwest(resp).await
        })
        .await
    }

    async fn signed_request_via_dev_proxy(
        &self,
        method: reqwest::Method,
        url: Url,
        range: Option<Range<u64>>,
    ) -> Result<crate::wasm_local::WasmHttpResponse> {
        let object = url
            .path_segments()
            .and_then(|s| s.last())
            .unwrap_or("?")
            .to_string();
        wasm_log(&format!("idb_query: s3 {method:?} …/{object} (via _s3)"));
        let proxy_base = self
            .cfg
            .dev_proxy_base
            .as_ref()
            .expect("dev_proxy_base")
            .trim_end_matches('/')
            .to_string();
        let client = self.client.clone();
        let region = self.cfg.region.clone();
        let cred = self.credential();
        crate::wasm_local::run_local(async move {
            wasm_log(&format!("idb_query: s3 signing …/{object}"));
            let signed = build_signed_s3_request(
                &client,
                &region,
                &cred,
                method.clone(),
                &url,
                range,
            )?;
            let proxy_url = format!(
                "{proxy_base}/_s3?u={}",
                urlencoding::encode(url.as_str())
            );
            let mut rb = client
                .request(method, &proxy_url)
                .header(reqwest::header::CACHE_CONTROL, "no-cache")
                .header(reqwest::header::PRAGMA, "no-cache");
            for (name, value) in signed.headers().iter() {
                if name == reqwest::header::HOST {
                    continue;
                }
                rb = rb.header(name, value.clone());
            }
            wasm_log(&format!("idb_query: s3 fetch …/{object}"));
            let resp = rb
                .send()
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, format!("s3 proxy {url}: {e}")))?;
            let status = resp.status();
            let out = wasm_response_from_reqwest(resp).await?;
            wasm_log(&format!(
                "idb_query: s3 done …/{object} {status} {} bytes",
                out.body.len()
            ));
            Ok(out)
        })
        .await
    }
}

fn build_signed_s3_request(
    client: &Client,
    region: &str,
    cred: &AwsCredential,
    method: reqwest::Method,
    url: &Url,
    range: Option<Range<u64>>,
) -> Result<reqwest::Request> {
    let mut rb = client.request(method, url.clone());
    if let Some(r) = range {
        let end = r.end.saturating_sub(1);
        rb = rb.header(
            reqwest::header::RANGE,
            format!("bytes={}-{}", r.start, end),
        );
    }
    let mut req = rb
        .build()
        .map_err(|e| Error::new(ErrorKind::Unexpected, format!("build request: {e}")))?;
    let signer = AwsV4Signer::new("s3", region);
    signer
        .sign(&mut req, cred)
        .map_err(|e| Error::new(ErrorKind::Unexpected, format!("sigv4: {e}")))?;
    Ok(req)
}

async fn wasm_response_from_reqwest(
    resp: reqwest::Response,
) -> Result<crate::wasm_local::WasmHttpResponse> {
    let status = resp.status();
    let content_length = resp
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    let body = resp
        .bytes()
        .await
        .map_err(|e| Error::new(ErrorKind::Unexpected, format!("read body: {e}")))?;
    Ok(crate::wasm_local::WasmHttpResponse {
        status,
        body,
        content_length,
    })
}

#[async_trait]
#[typetag::serde(name = "WasmS3Storage")]
impl Storage for WasmS3Storage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let obj = self.parse_location(path)?;
        let url = self.object_url(&obj)?;
        let resp = self.signed_request(Method::HEAD, url, None).await?;
        Ok(resp.status.is_success())
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let obj = self.parse_location(path)?;
        let url = self.object_url(&obj)?;
        let resp = self.signed_request(Method::HEAD, url, None).await?;
        if !resp.status.is_success() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("s3 HEAD failed: {}", resp.status),
            ));
        }
        Ok(FileMetadata {
            size: resp.content_length.unwrap_or(0),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let obj = self.parse_location(path)?;
        let url = self.object_url(&obj)?;
        let resp = self.signed_request(Method::GET, url, None).await?;
        if !resp.status.is_success() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("s3 GET failed: {}", resp.status),
            ));
        }
        Ok(resp.body)
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        Ok(Box::new(WasmS3Reader {
            storage: self.clone(),
            path: path.to_string(),
        }))
    }

    async fn write(&self, _path: &str, _bs: Bytes) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "S3 writes are not supported in the browser WASM build",
        ))
    }

    async fn writer(&self, _path: &str) -> Result<Box<dyn FileWrite>> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "S3 writes are not supported in the browser WASM build",
        ))
    }

    async fn delete(&self, _path: &str) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "S3 deletes are not supported in the browser WASM build",
        ))
    }

    async fn delete_prefix(&self, _path: &str) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "S3 deletes are not supported in the browser WASM build",
        ))
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

struct WasmS3Reader {
    storage: WasmS3Storage,
    path: String,
}

#[async_trait]
impl FileRead for WasmS3Reader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let obj = self.storage.parse_location(&self.path)?;
        let url = self.storage.object_url(&obj)?;
        let resp = self
            .storage
            .signed_request(Method::GET, url, Some(range))
            .await?;
        if !resp.status.is_success() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("s3 ranged GET failed: {}", resp.status),
            ));
        }
        Ok(resp.body)
    }
}
