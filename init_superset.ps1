# Initialize Superset Environment
# This script sets up Superset with database and default admin user

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$supersetEnv = Join-Path $projectRoot "superset_env"
$supersetPython = Join-Path $supersetEnv "Scripts\python.exe"

Write-Host "=== Superset Initialization ===" -ForegroundColor Cyan
Write-Host "Project Root: $projectRoot" -ForegroundColor Gray

# Activate virtual environment
Write-Host "Activating Superset virtual environment..." -ForegroundColor Yellow
& $supersetPython -m pip install --upgrade pip setuptools wheel -q

# Set Superset configuration
$env:SUPERSET_SECRET_KEY = 'your-secret-key-change-me-in-production-123456789'
$env:FLASK_APP = 'superset'
$env:SUPERSET_CONFIG_PATH = "$projectRoot\superset_config.py"
$env:SUPERSET_HOME = "$projectRoot\superset_home"

Write-Host "Creating Superset home directory..." -ForegroundColor Yellow
if (-not (Test-Path $env:SUPERSET_HOME)) {
    New-Item -ItemType Directory -Path $env:SUPERSET_HOME -Force | Out-Null
}

# Initialize database
Write-Host "Initializing Superset database..." -ForegroundColor Yellow
& $supersetPython -m superset db upgrade

# Create admin user
Write-Host "Creating admin user..." -ForegroundColor Yellow
$adminPassword = "admin"
& $supersetPython -m superset fab create-admin --username "admin" --firstname "Admin" --lastname "User" --email "admin@example.com" --password "$adminPassword" 2>$null

# Load examples
Write-Host "Loading example data..." -ForegroundColor Yellow
& $supersetPython -m superset load-examples

# Initialize roles and permissions
Write-Host "Initializing roles and permissions..." -ForegroundColor Yellow
& $supersetPython -m superset init

Write-Host "`n=== Initialization Complete ===" -ForegroundColor Green
Write-Host "Admin User: admin" -ForegroundColor Yellow
Write-Host "Admin Password: admin" -ForegroundColor Yellow
Write-Host "`nYou can now run: .\run_superset.ps1" -ForegroundColor Cyan
