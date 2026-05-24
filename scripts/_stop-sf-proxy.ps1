# Stop any running idb-sf-proxy so cargo can overwrite target\release\idb-sf-proxy.exe

function Stop-IdbSfProxy {
    $procs = Get-Process -Name "idb-sf-proxy" -ErrorAction SilentlyContinue
    if (-not $procs) {
        return
    }
    foreach ($p in $procs) {
        Write-Host "Stopping idb-sf-proxy (PID $($p.Id))..." -ForegroundColor DarkGray
        Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Milliseconds 400
}
