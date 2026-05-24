# Shared PAT resolution for test-horizon.ps1 and OAuth proxy scripts.
function Resolve-SnowflakePat {
    param(
        [string]$Pat,
        [string]$Root
    )

    if ($Pat -and $Pat.Trim().Length -ge 32) {
        return $Pat.Trim()
    }

    if ($env:SNOWFLAKE_ACCESS_TOKEN -and $env:SNOWFLAKE_ACCESS_TOKEN.Trim().Length -ge 32) {
        return $env:SNOWFLAKE_ACCESS_TOKEN.Trim()
    }

    $patFile = Join-Path $Root "pat.txt"
    if (Test-Path $patFile) {
        $fromFile = (Get-Content -Path $patFile -Raw -ErrorAction Stop).Trim()
        if ($fromFile.Length -ge 32) {
            Write-Host "Using PAT from $patFile ($($fromFile.Length) chars)" -ForegroundColor DarkGray
            return $fromFile
        }
    }

    throw @"
Snowflake PAT not found. Use any one of:

  `$env:SNOWFLAKE_ACCESS_TOKEN = '<paste PAT from Snowsight>'
  .\scripts\test-oauth-via-proxy.ps1 -Pat '<paste PAT>'
  # one-line file (gitignored): iceberg-db-rs\pat.txt

PAT is under Snowsight → your user → Programmatic access tokens.
Scope in scripts/UI must match ROLE_RESTRICTION on that token.
"@
}
