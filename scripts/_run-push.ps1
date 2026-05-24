$ErrorActionPreference = "Stop"
$log = Join-Path $PSScriptRoot "..\_push.log"
$script = Join-Path $PSScriptRoot "push-to-github.ps1"

try {
    & $script *>&1 | Tee-Object -FilePath $log
    "EXIT=$LASTEXITCODE" | Add-Content $log
} catch {
    "ERROR=$_" | Add-Content $log
    exit 1
}
