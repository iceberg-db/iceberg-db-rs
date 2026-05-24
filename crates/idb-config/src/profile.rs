//! Catalog profile presets (aligned with Java `CatalogProfile`).

use std::collections::BTreeMap;

const VENDED_CREDENTIALS: &str = "vended-credentials";

/// Snowflake PAT exchange treats the credential as the PAT secret.
/// We tolerate the legacy `user:pat` form by stripping the prefix.
pub fn snowflake_pat_secret(credential: &str) -> &str {
    credential
        .split_once(':')
        .map(|(_, pat)| pat)
        .unwrap_or(credential)
}

/// Validate REST catalog props after profile expansion and env substitution.
pub fn validate_rest_props(
    catalog_name: &str,
    props: &BTreeMap<String, String>,
    profile: Option<&str>,
) -> Result<(), String> {
    let uri = props
        .get("uri")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("catalog '{catalog_name}': missing `uri`"))?;

    if uri.contains('<') || uri.contains('>') {
        return Err(format!(
            "catalog '{catalog_name}': `uri` still contains placeholders ({uri}). \
Set your Snowflake account, e.g. uri: xy12345.us-east-1 or \
uri: https://xy12345.us-east-1.snowflakecomputing.com/polaris/api/catalog"
        ));
    }

    if !uri.starts_with("http://") && !uri.starts_with("https://") {
        return Err(format!(
            "catalog '{catalog_name}': `uri` must start with http:// or https:// after profile expansion (got {uri}). \
Use account shorthand like xy12345.us-east-1 or a full https://…snowflakecomputing.com/polaris/api/catalog URL"
        ));
    }

    if props.get("token").is_none_or(|t| t.is_empty())
        && props.get("credential").is_none_or(|c| c.is_empty())
    {
        return Err(format!(
            "catalog '{catalog_name}': set `token: ${{SNOWFLAKE_ACCESS_TOKEN}}` to your Snowflake PAT"
        ));
    }

    if is_snowflake_horizon_profile(profile, props.get("uri").map(String::as_str)) {
        validate_snowflake_horizon(catalog_name, props)?;
    }

    Ok(())
}

fn validate_snowflake_horizon(
    catalog_name: &str,
    props: &BTreeMap<String, String>,
) -> Result<(), String> {
    let scope = props.get("scope").or_else(|| props.get("oauth2-scope"));
    if scope.is_none_or(|s| s.is_empty() || !s.starts_with("session:role:")) {
        return Err(format!(
            "catalog '{catalog_name}': snowflake-horizon requires \
`scope: session:role:<ROLE>` matching the PAT ROLE_RESTRICTION (e.g. session:role:DATA_ENGINEER_ROLE)"
        ));
    }

    let cred = props
        .get("credential")
        .filter(|s| !s.is_empty())
        .or_else(|| props.get("token").filter(|s| !s.is_empty()))
        .map(String::as_str)
        .unwrap_or("");
    let pat = snowflake_pat_secret(cred);
    if pat.len() < 32 {
        return Err(format!(
            "catalog '{catalog_name}': SNOWFLAKE_ACCESS_TOKEN is missing or not a PAT \
({pat_len} chars after resolving ${{SNOWFLAKE_ACCESS_TOKEN}}; expected ~200 for a JWT PAT). \
In the same PowerShell window run:\n  \
$env:SNOWFLAKE_ACCESS_TOKEN = '<paste PAT from Snowsight>'\n  \
$env:SNOWFLAKE_ACCESS_TOKEN.Length",
            pat_len = pat.len()
        ));
    }

    Ok(())
}

pub fn is_snowflake_horizon_profile(profile: Option<&str>, uri: Option<&str>) -> bool {
    if profile
        .map(str::to_ascii_lowercase)
        .as_deref()
        .is_some_and(|p| p == "snowflake-horizon")
    {
        return true;
    }
    uri.is_some_and(is_snowflake_horizon_uri)
}

/// Snowflake Horizon IRC (`…/polaris/api/catalog`), including local dev proxy URLs.
pub fn is_snowflake_horizon_uri(uri: &str) -> bool {
    uri.contains("/polaris/api/catalog")
        && (uri.contains("snowflakecomputing.com")
            || uri.contains("/sf/")
            || uri.contains("127.0.0.1:8787")
            || uri.contains("localhost:8787"))
}

/// Apply a named profile and normalize REST property names for `iceberg-catalog-rest`.
pub fn apply_profile(profile: Option<&str>, props: &mut BTreeMap<String, String>) {
    normalize_rest_headers(props);
    if profile
        .map(str::to_ascii_lowercase)
        .as_deref()
        == Some("snowflake-horizon")
    {
        apply_snowflake_horizon(props);
    }
}

fn normalize_rest_headers(props: &mut BTreeMap<String, String>) {
    let java_headers: Vec<(String, String)> = props
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix("rest.headers.")
                .map(|name| (format!("header.{name}"), v.clone()))
        })
        .collect();
    for (k, v) in java_headers {
        props.entry(k).or_insert(v);
    }
}

fn apply_snowflake_horizon(props: &mut BTreeMap<String, String>) {
    if let Some(username) = props.remove("username") {
        props
            .entry("oauth2-client-id".into())
            .or_insert(username);
    }

    if let Some(uri) = props.get("uri").cloned() {
        if !uri.contains("snowflakecomputing.com") && !uri.starts_with("http") {
            props.insert(
                "uri".into(),
                format!("https://{uri}.snowflakecomputing.com/polaris/api/catalog"),
            );
        }
    }

    if let Some(pat) = props.remove("token") {
        props.insert("credential".into(), pat.trim().to_string());
    }

    if let Some(scope) = props.remove("scope") {
        props.insert("oauth2-scope".into(), scope.trim().to_string());
    }

    if let Some(uri) = props.get("uri").cloned() {
        let oauth = if uri.ends_with('/') {
            format!("{uri}v1/oauth/tokens")
        } else {
            format!("{uri}/v1/oauth/tokens")
        };
        props
            .entry("oauth2-server-uri".into())
            .or_insert(oauth);
    }
}

/// Header value for Snowflake loadTable (not for OAuth).
pub fn snowflake_vended_credentials_header() -> (&'static str, &'static str) {
    ("X-Iceberg-Access-Delegation", VENDED_CREDENTIALS)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_PAT: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.test-pat-secret";

    fn horizon_props() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("uri".into(), "xy12345.us-east-1".into()),
            ("warehouse".into(), "ANALYTICS_DB".into()),
            ("token".into(), SAMPLE_PAT.into()),
            ("username".into(), "CHANDRANSURAJ".into()),
            ("scope".into(), "session:role:SYSADMIN".into()),
        ])
    }

    #[test]
    fn snowflake_horizon_expands_uri_and_oauth_client() {
        let mut props = horizon_props();
        apply_profile(Some("snowflake-horizon"), &mut props);
        assert_eq!(
            props.get("uri").map(String::as_str),
            Some("https://xy12345.us-east-1.snowflakecomputing.com/polaris/api/catalog")
        );
        assert!(props.get("credential").unwrap().starts_with("eyJ"));
        assert_eq!(
            props.get("oauth2-client-id").map(String::as_str),
            Some("CHANDRANSURAJ")
        );
        assert_eq!(
            props.get("oauth2-scope").map(String::as_str),
            Some("session:role:SYSADMIN")
        );
        assert!(!props.contains_key("scope"));
        assert!(validate_rest_props("sf", &props, Some("snowflake-horizon")).is_ok());
    }

    #[test]
    fn snowflake_pat_only_without_username_validates() {
        let props = BTreeMap::from([
            ("uri".into(), "https://xy.snowflakecomputing.com/polaris/api/catalog".into()),
            ("credential".into(), SAMPLE_PAT.into()),
            ("scope".into(), "session:role:DATA_ENGINEER_ROLE".into()),
        ]);
        assert!(validate_rest_props("sf", &props, Some("snowflake-horizon")).is_ok());
    }

    #[test]
    fn snowflake_rejects_username_only_credential() {
        let props = BTreeMap::from([
            ("uri".into(), "https://xy.snowflakecomputing.com/polaris/api/catalog".into()),
            ("credential".into(), "CHANDRANSURAJ:".into()),
            ("oauth2-client-id".into(), "CHANDRANSURAJ".into()),
            ("scope".into(), "session:role:SYSADMIN".into()),
        ]);
        assert!(validate_rest_props("sf", &props, Some("snowflake-horizon")).is_err());
    }

    #[test]
    fn snowflake_pat_secret_handles_legacy_user_colon_pat() {
        assert_eq!(snowflake_pat_secret(SAMPLE_PAT), SAMPLE_PAT);
        assert_eq!(
            snowflake_pat_secret(&format!("CHANDRANSURAJ:{SAMPLE_PAT}")),
            SAMPLE_PAT
        );
        assert_eq!(snowflake_pat_secret(""), "");
    }

    #[test]
    fn is_snowflake_horizon_uri_matches_known_hosts() {
        assert!(is_snowflake_horizon_uri(
            "https://xy.snowflakecomputing.com/polaris/api/catalog"
        ));
        assert!(is_snowflake_horizon_uri(
            "http://127.0.0.1:8787/abc/polaris/api/catalog"
        ));
        assert!(is_snowflake_horizon_uri(
            "http://localhost:8787/abc/polaris/api/catalog"
        ));
        assert!(!is_snowflake_horizon_uri(
            "https://example.com/iceberg/v1"
        ));
        assert!(!is_snowflake_horizon_uri(""));
    }

    #[test]
    fn is_snowflake_horizon_profile_picks_up_either_signal() {
        assert!(is_snowflake_horizon_profile(Some("snowflake-horizon"), None));
        assert!(is_snowflake_horizon_profile(
            Some("Snowflake-Horizon"),
            None
        ));
        assert!(is_snowflake_horizon_profile(
            None,
            Some("https://xy.snowflakecomputing.com/polaris/api/catalog")
        ));
        assert!(!is_snowflake_horizon_profile(
            Some("rest"),
            Some("https://example.com/iceberg/v1")
        ));
    }

    #[test]
    fn apply_profile_leaves_full_url_alone() {
        let mut props = BTreeMap::from([
            (
                "uri".into(),
                "https://xy.snowflakecomputing.com/polaris/api/catalog".into(),
            ),
            ("token".into(), SAMPLE_PAT.into()),
            ("scope".into(), "session:role:R".into()),
        ]);
        apply_profile(Some("snowflake-horizon"), &mut props);
        assert_eq!(
            props.get("uri").map(String::as_str),
            Some("https://xy.snowflakecomputing.com/polaris/api/catalog")
        );
        assert_eq!(
            props.get("oauth2-server-uri").map(String::as_str),
            Some("https://xy.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens")
        );
    }

    #[test]
    fn apply_profile_handles_trailing_slash_uri() {
        let mut props = BTreeMap::from([
            (
                "uri".into(),
                "https://xy.snowflakecomputing.com/polaris/api/catalog/".into(),
            ),
            ("token".into(), SAMPLE_PAT.into()),
            ("scope".into(), "session:role:R".into()),
        ]);
        apply_profile(Some("snowflake-horizon"), &mut props);
        assert_eq!(
            props.get("oauth2-server-uri").map(String::as_str),
            Some("https://xy.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens")
        );
    }

    #[test]
    fn validate_rejects_missing_uri() {
        let props = BTreeMap::new();
        let err = validate_rest_props("sf", &props, None).unwrap_err();
        assert!(err.contains("missing `uri`"));
    }

    #[test]
    fn validate_rejects_placeholder_uri() {
        let props = BTreeMap::from([
            ("uri".into(), "https://<account>.snowflakecomputing.com/polaris/api/catalog".into()),
            ("token".into(), SAMPLE_PAT.into()),
        ]);
        let err = validate_rest_props("sf", &props, None).unwrap_err();
        assert!(err.contains("placeholders"));
    }

    #[test]
    fn validate_rejects_non_http_uri() {
        let props = BTreeMap::from([
            ("uri".into(), "xy.snowflakecomputing.com/polaris/api/catalog".into()),
            ("token".into(), SAMPLE_PAT.into()),
        ]);
        let err = validate_rest_props("sf", &props, None).unwrap_err();
        assert!(err.contains("http://"));
    }

    #[test]
    fn validate_rejects_missing_credentials() {
        let props = BTreeMap::from([
            ("uri".into(), "https://xy.snowflakecomputing.com/polaris/api/catalog".into()),
        ]);
        let err = validate_rest_props("sf", &props, None).unwrap_err();
        assert!(err.contains("SNOWFLAKE_ACCESS_TOKEN"));
    }

    #[test]
    fn validate_rejects_horizon_scope_without_session_role() {
        let props = BTreeMap::from([
            ("uri".into(), "https://xy.snowflakecomputing.com/polaris/api/catalog".into()),
            ("credential".into(), SAMPLE_PAT.into()),
            ("scope".into(), "openid".into()),
        ]);
        let err = validate_rest_props("sf", &props, Some("snowflake-horizon")).unwrap_err();
        assert!(err.contains("session:role:"));
    }

    #[test]
    fn normalize_rest_headers_maps_java_prefix() {
        let mut props = BTreeMap::from([
            ("rest.headers.X-Custom".into(), "value".into()),
        ]);
        apply_profile(None, &mut props);
        assert_eq!(
            props.get("header.X-Custom").map(String::as_str),
            Some("value")
        );
    }

    #[test]
    fn snowflake_vended_credentials_header_value() {
        assert_eq!(
            snowflake_vended_credentials_header(),
            ("X-Iceberg-Access-Delegation", "vended-credentials")
        );
    }
}
