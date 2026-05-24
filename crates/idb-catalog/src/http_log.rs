//! Log outbound REST catalog HTTP requests (secrets redacted).

use std::collections::HashMap;

pub fn enabled() -> bool {
    matches!(
        std::env::var("IDB_LOG_HTTP").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes")
    )
}

pub fn log_outbound(method: &str, url: &str, headers: &[(String, String)], body: Option<&str>) {
    if !enabled() {
        return;
    }
    eprintln!("--- idb HTTP → Snowflake / REST catalog ---");
    eprintln!("{method} {url}");
    for (k, v) in headers {
        eprintln!("{k}: {v}");
    }
    if let Some(b) = body {
        eprintln!();
        eprintln!("{b}");
    }
    eprintln!("--- end ---");
}

pub fn redact_secret(value: &str) -> String {
    if value.len() <= 8 {
        "***".to_string()
    } else {
        format!("{}…{} ({} chars)", &value[..4], &value[value.len() - 4..], value.len())
    }
}

#[allow(dead_code)]
pub fn redact_header(name: &str, value: &str) -> String {
    let lower = name.to_ascii_lowercase();
    if lower == "authorization" || lower.contains("secret") || lower.contains("token") {
        if let Some(token) = value.strip_prefix("Bearer ") {
            return format!("Bearer {}", redact_secret(token));
        }
        return redact_secret(value);
    }
    value.to_string()
}

pub fn form_body(params: &[(&str, &str)]) -> String {
    params
        .iter()
        .map(|(k, v)| {
            let v = if *k == "client_secret" {
                redact_secret(v)
            } else {
                (*v).to_string()
            };
            format!("{k}={v}")
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// URLs used by `iceberg-catalog-rest` (for logging delegated calls on the inner catalog).
pub fn iceberg_rest_urls(props: &HashMap<String, String>) -> Vec<(String, String)> {
    let Some(uri) = props.get("uri") else {
        return Vec::new();
    };
    let base = uri.trim_end_matches('/');
    let mut urls = Vec::new();

    let mut config = format!("{base}/v1/config");
    if let Some(wh) = props.get("warehouse") {
        config.push_str(&format!("?warehouse={wh}"));
    }
    urls.push(("GET (init)".into(), config));

    if let Some(oauth) = props.get("oauth2-server-uri") {
        urls.push(("POST (auth)".into(), oauth.clone()));
    } else {
        urls.push((
            "POST (auth)".into(),
            format!("{base}/v1/oauth/tokens"),
        ));
    }

    urls.push((
        "GET".into(),
        format!("{base}/v1/namespaces"),
    ));

    urls
}

pub fn log_catalog_bootstrap(props: &HashMap<String, String>) {
    if !enabled() {
        return;
    }
    eprintln!("--- idb REST catalog bootstrap (inner iceberg-catalog-rest) ---");
    eprintln!("Resolved catalog properties (secrets redacted):");
    let mut keys: Vec<_> = props.keys().collect();
    keys.sort();
    for k in keys {
        let v = props.get(k).unwrap();
        let v = if k.contains("token") || k.contains("credential") || k.contains("secret") {
            redact_secret(v)
        } else {
            v.clone()
        };
        eprintln!("  {k} = {v}");
    }
    eprintln!("Expected HTTP sequence:");
    for (method, url) in iceberg_rest_urls(props) {
        eprintln!("  {method} {url}");
    }
    eprintln!("--- end ---");
}

/// Snowflake Horizon PAT OAuth (`client_id` + PAT + scope).
pub fn log_curl_oauth_pat(url: &str, scope: &str, client_id: Option<&str>) {
    if !enabled() {
        return;
    }
    eprintln!("--- reproduce Snowflake PAT OAuth (use curl.exe on Windows) ---");
    eprintln!(r#"curl.exe -i -X POST "{url}" `"#);
    eprintln!(r#"  -H "Content-Type: application/x-www-form-urlencoded" `"#);
    eprintln!(r#"  --data-urlencode "grant_type=client_credentials" `"#);
    if let Some(id) = client_id {
        eprintln!(r#"  --data-urlencode "client_id={id}" `"#);
    }
    eprintln!(r#"  --data-urlencode "scope={scope}" `"#);
    eprintln!(r#"  --data-urlencode "client_secret=$env:SNOWFLAKE_ACCESS_TOKEN""#);
    eprintln!("--- end ---");
}
