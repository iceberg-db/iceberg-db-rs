# One-shot push with an up-to-date commit message. Run from any directory.
$ErrorActionPreference = "Stop"
& (Join-Path $PSScriptRoot "push-to-github.ps1")
