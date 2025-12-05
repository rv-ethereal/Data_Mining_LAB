# Activate virtual environment (if not already activated)
& ".\venv\Scripts\Activate.ps1"

# Set environment variables
$env:SUPERSET_SECRET_KEY = 'thisismysecretkey123456789'
$env:FLASK_APP = 'superset'
$env:SUPERSET_CONFIG_PATH = "$PWD\superset_config.py"

# Path to superset executable in current venv
$supersetExe = Join-Path $PWD "venv\Scripts\superset.exe"

# Check if superset executable exists
if (-Not (Test-Path $supersetExe)) {
    Write-Host "Superset executable not found at $supersetExe" -ForegroundColor Red
    exit
}

# Info
Write-Host "Starting Superset on http://localhost:8088" -ForegroundColor Green
Write-Host "Username: admin" -ForegroundColor Yellow
Write-Host "Password: admin" -ForegroundColor Yellow
Write-Host "`nPress Ctrl+C to stop the server`n" -ForegroundColor Cyan

# Run Superset server
& $supersetExe run -p 8088 --with-threads --reload --debugger
