//! Snowflake Horizon IRC PAT → bearer exchange.
//!
//! Snowflake docs: `grant_type`, `scope=session:role:<ROLE>`, `client_secret=<PAT>`.
//! Some accounts also accept optional `client_id` (login name).

use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use idb_config::profile;
#[cfg(not(target_arch = "wasm32"))]
use reqwest::Client;
use serde::Deserialize;

use crate::http_log;

#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
}

pub fn oauth_client_id(props: &HashMap<String, String>) -> Option<String> {
    props
        .get("oauth2-client-id")
        .filter(|s| !s.is_empty())
        .cloned()
}

pub fn pat_from_props(props: &HashMap<String, String>) -> Result<String> {
    let cred = props
        .get("credential")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("Snowflake auth requires `token` / credential (PAT)"))?;
    let pat = profile::snowflake_pat_secret(cred).to_string();
    if pat.len() < 32 {
        return Err(anyhow!(
            "SNOWFLAKE_ACCESS_TOKEN is missing or not a PAT ({} chars). \
Set it in this same shell: $env:SNOWFLAKE_ACCESS_TOKEN = '<paste PAT>'",
            pat.len()
        ));
    }
    Ok(pat)
}

pub fn oauth_scope(props: &HashMap<String, String>) -> Result<&str> {
    let scope = props
        .get("oauth2-scope")
        .or_else(|| props.get("scope"))
        .map(String::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "Snowflake Horizon requires scope: session:role:<ROLE> matching the PAT ROLE_RESTRICTION"
            )
        })?;
    if !scope.starts_with("session:role:") {
        return Err(anyhow!(
            "Snowflake scope must be session:role:<ROLE> (got {scope})"
        ));
    }
    Ok(scope)
}

/// Exchange a Snowflake PAT for a short-lived catalog bearer token.
pub async fn exchange_pat(props: &HashMap<String, String>) -> Result<String> {
    if let Some(token) = props.get("token").filter(|t| !t.is_empty()) {
        if http_log::enabled() {
            eprintln!("--- idb auth: using pre-set bearer token (skipping PAT exchange) ---");
        }
        return Ok(token.clone());
    }

    let pat = pat_from_props(props)?;
    let client_id = oauth_client_id(props);
    let oauth_uri = props
        .get("oauth2-server-uri")
        .ok_or_else(|| anyhow!("missing oauth2-server-uri (snowflake-horizon profile)"))?;
    let scope = oauth_scope(props)?;

    let mut form: Vec<(&str, &str)> = vec![
        ("grant_type", "client_credentials"),
        ("scope", scope),
    ];
    if let Some(ref id) = client_id {
        form.push(("client_id", id.as_str()));
    }
    form.push(("client_secret", pat.as_str()));

    if http_log::enabled() {
        eprintln!("--- idb auth: Snowflake PAT OAuth exchange ---");
        if let Some(id) = &client_id {
            eprintln!("  client_id={id}");
        } else {
            eprintln!("  client_id=(not sent — PAT-only, per Snowflake IRC docs)");
        }
        eprintln!("  scope={scope}");
        eprintln!("  pat_len={}", pat.len());
    }

    let body = http_log::form_body(&form);

    http_log::log_outbound(
        "POST",
        oauth_uri,
        &[(
            "Content-Type".into(),
            "application/x-www-form-urlencoded".into(),
        )],
        Some(&body),
    );
    http_log::log_curl_oauth_pat(oauth_uri, scope, client_id.as_deref());

    #[cfg(not(target_arch = "wasm32"))]
    let (status, body) = {
        let response = Client::new()
            .post(oauth_uri)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .form(&form)
            .send()
            .await
            .context("Snowflake PAT OAuth (POST …/v1/oauth/tokens)")?;
        (response.status(), response.bytes().await.context("oauth token body")?)
    };

    #[cfg(target_arch = "wasm32")]
    let (status, body) = {
        use crate::wasm_local;
        eprintln!(
            "idb oauth (wasm): POST {oauth_uri} scope={scope} pat_len={} client_id={}",
            pat.len(),
            client_id.as_deref().unwrap_or("(none)")
        );
        let owned: Vec<(String, String)> = form
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let resp = wasm_local::client_post_form(oauth_uri.to_string(), owned)
            .await
            .context("Snowflake PAT OAuth (POST …/v1/oauth/tokens)")?;
        (resp.status, resp.body)
    };

    if http_log::enabled() {
        eprintln!("--- idb HTTP response ---");
        eprintln!("POST {oauth_uri}");
        eprintln!("status: {status}");
        eprintln!(
            "body: {}",
            if body.is_empty() {
                "<empty>".to_string()
            } else {
                String::from_utf8_lossy(&body).to_string()
            }
        );
        eprintln!("--- end ---");
    }

    if !status.is_success() {
        return Err(anyhow!(
            "Snowflake OAuth failed ({status}): {}. \
Check scope matches PAT ROLE_RESTRICTION (SHOW USER PROGRAMMATIC ACCESS TOKENS).",
            if body.is_empty() {
                "<empty body>".to_string()
            } else {
                String::from_utf8_lossy(&body).to_string()
            }
        ));
    }

    let token: OAuthTokenResponse =
        serde_json::from_slice(&body).context("parse OAuth token JSON")?;
    Ok(token.access_token)
}
