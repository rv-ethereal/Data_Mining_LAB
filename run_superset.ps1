# Run Superset Server
$PSScriptRoot = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition
$env:SUPERSET_SECRET_KEY = 'thisismysecretkey123456789'
$env:FLASK_APP = 'superset'
$env:SUPERSET_CONFIG_PATH = Join-Path $PSScriptRoot 'superset_config.py'

Write-Host "Starting Superset on http://localhost:8088" -ForegroundColor Green
Write-Host "Username: admin" -ForegroundColor Yellow
Write-Host "Password: admin" -ForegroundColor Yellow
Write-Host "`nPress Ctrl+C to stop the server`n" -ForegroundColor Cyan

# Use the python/superset from the local venv
$SupersetExe = Join-Path $PSScriptRoot 'venv\Scripts\superset.exe'

& $SupersetExe run -p 8088 --with-threads --reload --debugger
