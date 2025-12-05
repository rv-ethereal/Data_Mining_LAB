# Run Superset Server
# This script starts the Superset web server

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$supersetEnv = Join-Path $projectRoot "superset_env"
$supersetPython = Join-Path $supersetEnv "Scripts\python.exe"

# Set Superset configuration
$env:SUPERSET_SECRET_KEY = 'your-secret-key-change-me-in-production-123456789'
$env:FLASK_APP = 'superset'
$env:SUPERSET_CONFIG_PATH = "$projectRoot\superset_config.py"
$env:SUPERSET_HOME = "$projectRoot\superset_home"

Write-Host "=== Starting Superset Server ===" -ForegroundColor Cyan
Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  Home Directory: $env:SUPERSET_HOME" -ForegroundColor Gray
Write-Host "  Config File: $env:SUPERSET_CONFIG_PATH" -ForegroundColor Gray
Write-Host "`nSuperset will be available at: http://localhost:8088" -ForegroundColor Green
Write-Host "  Username: admin" -ForegroundColor Yellow
Write-Host "  Password: admin" -ForegroundColor Yellow
Write-Host "`nPress Ctrl+C to stop the server`n" -ForegroundColor Cyan

# Start Superset server
& $supersetPython -m superset run -p 8088 --with-threads --reload
