# Dev reverse proxy: /{account}/polaris/... → https://{account}.snowflakecomputing.com/...
# Trunk [[proxy]] rewrite="/sf/" → http://127.0.0.1:8787/
$ErrorActionPreference = "Continue"

$Port = if ($env:SNOWFLAKE_PROXY_PORT) { [int]$env:SNOWFLAKE_PROXY_PORT } else { 8787 }
$Prefix = "http://127.0.0.1:${Port}/"
$LogFile = Join-Path $PSScriptRoot "snowflake-proxy.log"

function Write-ProxyLog($msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $msg"
    Add-Content -Path $LogFile -Value $line -ErrorAction SilentlyContinue
    Write-Host $line
}

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$listener = [System.Net.HttpListener]::new()
$listener.Prefixes.Add($Prefix)
try {
    $listener.Start()
} catch {
    Write-ProxyLog "FATAL: cannot bind $Prefix — $($_.Exception.Message)"
    Write-ProxyLog "Try (Admin): netsh http add urlacl url=$Prefix user=$env:USERNAME"
    exit 1
}

Write-ProxyLog "listening on $Prefix (log: $LogFile)"

function Write-ProxyBody($res, [string]$text) {
    $bytes = [Text.Encoding]::UTF8.GetBytes($text)
    $res.ContentType = "text/plain; charset=utf-8"
    $res.ContentLength64 = $bytes.Length
    $res.OutputStream.Write($bytes, 0, $bytes.Length)
}

try {
    while ($listener.IsListening) {
        $ctx = $listener.GetContext()
        $req = $ctx.Request
        $res = $ctx.Response

        try {
            $path = $req.Url.AbsolutePath.TrimStart("/")
            if ($path -eq "_proxy/health" -or $path -eq "health") {
                $ok = [Text.Encoding]::UTF8.GetBytes("ok")
                $res.StatusCode = 200
                $res.ContentType = "text/plain"
                $res.ContentLength64 = $ok.Length
                $res.OutputStream.Write($ok, 0, $ok.Length)
                continue
            }

            $segments = $path -split "/", 2
            $account = $segments[0]
            if (-not $account) {
                $res.StatusCode = 400
                Write-ProxyBody $res "expected /{account}/polaris/..."
                continue
            }

            $rest = if ($segments.Count -gt 1 -and $segments[1]) { $segments[1] } else { "" }
            $upstream = "https://${account}.snowflakecomputing.com/${rest}$($req.Url.Query)"

            $body = $null
            if ($req.ContentLength64 -gt 0) {
                $reader = New-Object System.IO.StreamReader($req.InputStream, $req.ContentEncoding)
                $body = $reader.ReadToEnd()
            }

            $headers = @{}
            foreach ($key in $req.Headers.AllKeys) {
                if ($key -in @("Host", "Connection", "Content-Length", "Transfer-Encoding")) {
                    continue
                }
                $headers[$key] = $req.Headers[$key]
            }

            $iwr = @{
                Uri             = $upstream
                Method          = $req.HttpMethod
                Headers         = $headers
                UseBasicParsing = $true
            }
            if ($null -ne $body -and $body.Length -gt 0) {
                $iwr.Body = $body
            }
            if ($req.ContentType) {
                $iwr.ContentType = $req.ContentType
            }

            Write-ProxyLog "$($req.HttpMethod) $upstream (body=$($body.Length) bytes)"

            try {
                $resp = Invoke-WebRequest @iwr
                $res.StatusCode = [int]$resp.StatusCode
                if ($resp.Headers["Content-Type"]) {
                    $res.ContentType = $resp.Headers["Content-Type"]
                }
                $bytes = $resp.Content
                if ($bytes -is [string]) {
                    $bytes = [Text.Encoding]::UTF8.GetBytes($bytes)
                }
                if ($bytes.Length -gt 0) {
                    $res.ContentLength64 = $bytes.Length
                    $res.OutputStream.Write($bytes, 0, $bytes.Length)
                }
            } catch [System.Net.WebException] {
                $web = $_.Exception.Response
                if ($web) {
                    $res.StatusCode = [int]$web.StatusCode
                    $stream = $web.GetResponseStream()
                    if ($stream) {
                        $ms = New-Object System.IO.MemoryStream
                        $stream.CopyTo($ms)
                        $bytes = $ms.ToArray()
                        if ($bytes.Length -gt 0) {
                            $res.ContentLength64 = $bytes.Length
                            $res.OutputStream.Write($bytes, 0, $bytes.Length)
                        }
                    }
                    Write-ProxyLog "upstream $($web.StatusCode) $upstream"
                } else {
                    throw
                }
            }
        } catch {
            Write-ProxyLog "ERROR $($req.HttpMethod) $($req.Url): $_"
            try {
                $res.StatusCode = 502
                Write-ProxyBody $res "Bad gateway: $_"
            } catch {
                Write-ProxyLog "response error: $_"
            }
        } finally {
            try { $res.Close() } catch { }
        }
    }
} finally {
    $listener.Stop()
    $listener.Close()
}
