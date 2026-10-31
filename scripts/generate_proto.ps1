param(
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    & $Python "scripts/generate_proto.py"
}
finally {
    Pop-Location
}
