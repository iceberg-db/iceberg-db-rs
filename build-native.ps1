# Build/run native crates (idb-cli). Uses MSVC on Windows — do not inherit CC=clang from web-wasm.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

Remove-Item Env:CC -ErrorAction SilentlyContinue
Remove-Item Env:CXX -ErrorAction SilentlyContinue
Remove-Item Env:CFLAGS -ErrorAction SilentlyContinue

cargo @args
