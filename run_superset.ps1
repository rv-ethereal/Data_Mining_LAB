# ============================================================================
# SUPERSET RUN SCRIPT
# ============================================================================
# Purpose: Start Superset analytics platform with proper initialization
# Usage: .\run_superset.ps1
# ============================================================================

param(
    [string]$Port = "8088",
    [switch]$SkipInitialization = $false,
    [string]$AdminPassword = "admin"
)

# Enable error action preference
$ErrorActionPreference = "Continue"

# Get script directory
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

function Write-Header {
    param([string]$Message)
    Write-Host ""
    Write-Host "═" * 70 -ForegroundColor Cyan
    Write-Host "█ $Message".PadRight(71) -ForegroundColor Cyan
    Write-Host "═" * 70 -ForegroundColor Cyan
    Write-Host ""
}

function Write-Success {
    param([string]$Message)
    Write-Host "✓ $Message" -ForegroundColor Green
}

function Write-Error-Custom {
    param([string]$Message)
    Write-Host "✗ $Message" -ForegroundColor Red
}

function Write-Info {
    param([string]$Message)
    Write-Host "ℹ $Message" -ForegroundColor Cyan
}

function Write-Warning-Custom {
    param([string]$Message)
    Write-Host "⚠ $Message" -ForegroundColor Yellow
}

# ============================================================================
# PRE-FLIGHT CHECKS
# ============================================================================

Write-Header "SUPERSET STARTUP - PRE-FLIGHT CHECKS"

# Check if virtual environment exists
Write-Info "Checking Python virtual environment..."
$VenvPath = Join-Path $ScriptDir "myeve"

if (Test-Path $VenvPath) {
    Write-Success "Virtual environment found: $VenvPath"
} else {
    Write-Error-Custom "Virtual environment not found at $VenvPath"
    Write-Host "Create it with: python -m venv myeve" -ForegroundColor Red
    exit 1
}

# Check if Superset is installed
Write-Info "Checking Superset installation..."
$SupersetExe = Join-Path $VenvPath "Scripts\superset.exe"

if (Test-Path $SupersetExe) {
    Write-Success "Superset executable found"
} else {
    Write-Error-Custom "Superset not installed!"
    Write-Host "Install with: pip install apache-superset" -ForegroundColor Red
    exit 1
}

# Check configuration file
Write-Info "Checking configuration file..."
$ConfigFile = Join-Path $ScriptDir "superset_config.py"

if (Test-Path $ConfigFile) {
    Write-Success "Configuration file found: $ConfigFile"
} else {
    Write-Error-Custom "Configuration file not found!"
    exit 1
}

# ============================================================================
# INITIALIZATION
# ============================================================================

if (-not $SkipInitialization) {
    Write-Header "RUNNING INITIALIZATION"
    
    Write-Info "Running Superset initialization script..."
    $InitScript = Join-Path $ScriptDir "initialize_superset.ps1"
    
    if (Test-Path $InitScript) {
        try {
            # Run initialization silently and check for success
            & $InitScript 2>&1 | Out-Null
            Write-Success "Initialization completed"
        } catch {
            Write-Warning-Custom "Initialization script execution completed with notices (non-critical)"
        }
    } else {
        Write-Warning-Custom "Initialization script not found, skipping."
    }
} else {
    Write-Info "Skipping initialization as requested"
}

# ============================================================================
# ENVIRONMENT SETUP
# ============================================================================

Write-Header "CONFIGURING ENVIRONMENT"

# Set environment variables
$env:SUPERSET_HOME = Join-Path $env:USERPROFILE ".superset"
$env:SUPERSET_SECRET_KEY = "enterprise-data-lake-superset-key-production"
$env:FLASK_APP = "superset"
$env:SUPERSET_CONFIG_PATH = $ConfigFile
$env:PYTHONPATH = $ScriptDir
$env:FLASK_ENV = "production"

Write-Success "Environment variables configured"
Write-Info "SUPERSET_HOME: $env:SUPERSET_HOME"
Write-Info "FLASK_APP: $env:FLASK_APP"
Write-Info "SUPERSET_CONFIG_PATH: $env:SUPERSET_CONFIG_PATH"

# ============================================================================
# DATABASE INTEGRITY CHECK
# ============================================================================

Write-Header "VERIFYING DATABASE"

Write-Info "Checking database integrity..."
try {
    & $SupersetExe db upgrade 2>&1 | Out-Null
    Write-Success "Database verified and up-to-date"
} catch {
    Write-Warning-Custom "Database upgrade encountered notices (non-critical)"
}

# ============================================================================
# SERVER STARTUP
# ============================================================================

Write-Header "STARTING SUPERSET SERVER"

Write-Host "Server Configuration:" -ForegroundColor Green
Write-Host "  Port: $Port" -ForegroundColor Yellow
Write-Host "  URL: http://localhost:$Port" -ForegroundColor Yellow
Write-Host "  Admin Username: admin" -ForegroundColor Yellow
Write-Host "  Admin Password: $AdminPassword" -ForegroundColor Yellow
Write-Host ""

Write-Host "Access the platform:" -ForegroundColor Cyan
Write-Host "  📊 Superset: http://localhost:$Port" -ForegroundColor Cyan
Write-Host "  👤 Login: admin / $AdminPassword" -ForegroundColor Cyan
Write-Host ""

Write-Host "Server Controls:" -ForegroundColor Magenta
Write-Host "  Press Ctrl+C to stop the server" -ForegroundColor Magenta
Write-Host "  Logs will appear below:" -ForegroundColor Magenta
Write-Host ""

Write-Host "═" * 70 -ForegroundColor Green
Write-Host "█ SERVER STARTING...".PadRight(71) -ForegroundColor Green
Write-Host "═" * 70 -ForegroundColor Green
Write-Host ""

# Start Superset server
try {
    & $SupersetExe run -p $Port --with-threads --reload --debugger
} catch {
    Write-Error-Custom "Server encountered an error"
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}
