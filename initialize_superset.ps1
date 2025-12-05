# ============================================================================
# SUPERSET INITIALIZATION SCRIPT
# ============================================================================
# Purpose: Initialize Superset database, create admin user, and setup platform
# Usage: .\initialize_superset.ps1
# ============================================================================

param(
    [string]$AdminUsername = "admin",
    [string]$AdminPassword = "admin",
    [string]$AdminFirstName = "Admin",
    [string]$AdminLastName = "User",
    [string]$AdminEmail = "admin@superset.local"
)

# Enable error action preference
$ErrorActionPreference = "Stop"

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

function Write-Warning-Custom {
    param([string]$Message)
    Write-Host "⚠ $Message" -ForegroundColor Yellow
}

function Write-Error-Custom {
    param([string]$Message)
    Write-Host "✗ $Message" -ForegroundColor Red
}

function Write-Info {
    param([string]$Message)
    Write-Host "ℹ $Message" -ForegroundColor Cyan
}

# ============================================================================
# PRE-FLIGHT CHECKS
# ============================================================================

Write-Header "SUPERSET INITIALIZATION - PRE-FLIGHT CHECKS"

# Check if virtual environment exists
Write-Info "Checking Python virtual environment..."
$VenvPath = Join-Path $ScriptDir "myeve"

if (Test-Path $VenvPath) {
    Write-Success "Virtual environment found at: $VenvPath"
} else {
    Write-Error-Custom "Virtual environment not found!"
    Write-Host "Please create it first: python -m venv myeve" -ForegroundColor Red
    exit 1
}

# Check if Superset is installed
Write-Info "Checking Superset installation..."
$SupersetExe = Join-Path $VenvPath "Scripts\superset.exe"

if (Test-Path $SupersetExe) {
    Write-Success "Superset executable found"
} else {
    Write-Error-Custom "Superset not installed!"
    Write-Host "Install it first: pip install apache-superset" -ForegroundColor Red
    exit 1
}

# Check configuration file
Write-Info "Checking Superset configuration file..."
$ConfigFile = Join-Path $ScriptDir "superset_config.py"

if (Test-Path $ConfigFile) {
    Write-Success "Configuration file found: $ConfigFile"
} else {
    Write-Error-Custom "Configuration file not found!"
    exit 1
}

# ============================================================================
# ENVIRONMENT SETUP
# ============================================================================

Write-Header "SETTING UP ENVIRONMENT VARIABLES"

# Define environment variables
$env:SUPERSET_HOME = Join-Path $env:USERPROFILE ".superset"
$env:SUPERSET_SECRET_KEY = "enterprise-data-lake-secret-key-$(Get-Random -Minimum 100000 -Maximum 999999)"
$env:FLASK_APP = "superset"
$env:SUPERSET_CONFIG_PATH = $ConfigFile
$env:PYTHONPATH = $ScriptDir

Write-Success "SUPERSET_HOME set to: $env:SUPERSET_HOME"
Write-Success "FLASK_APP set to: $env:FLASK_APP"
Write-Success "SUPERSET_CONFIG_PATH set to: $env:SUPERSET_CONFIG_PATH"
Write-Success "PYTHONPATH set to: $env:PYTHONPATH"

# Create Superset home directory if it doesn't exist
if (-not (Test-Path $env:SUPERSET_HOME)) {
    New-Item -ItemType Directory -Path $env:SUPERSET_HOME -Force | Out-Null
    Write-Success "Created Superset home directory"
}

# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================

Write-Header "INITIALIZING SUPERSET DATABASE"

Write-Info "Initializing database schema..."
try {
    & $SupersetExe db upgrade 2>&1 | ForEach-Object { Write-Host "  $_" }
    Write-Success "Database schema initialized successfully"
} catch {
    Write-Error-Custom "Failed to initialize database"
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

# ============================================================================
# ADMIN USER CREATION
# ============================================================================

Write-Header "CREATING ADMIN USER"

Write-Info "Creating admin user with the following credentials:"
Write-Host "  Username: $AdminUsername" -ForegroundColor Yellow
Write-Host "  Password: $AdminPassword" -ForegroundColor Yellow
Write-Host "  Email: $AdminEmail" -ForegroundColor Yellow

# Check if admin user already exists
Write-Info "Checking if admin user already exists..."

$PythonScript = @"
from superset.security import _SQLITE_REGEX_ESCAPE, DEFAULT_ROLE_NAMES
try:
    from superset import db
    from superset.security.manager import SupersetSecurityManager
    from superset.models.core import User
    
    db.create_all()
    user = User.query.filter_by(username='$AdminUsername').first()
    if user:
        print("ADMIN_EXISTS")
    else:
        print("CREATE_ADMIN")
except Exception as e:
    print(f"ERROR: {e}")
"@

try {
    $CheckResult = & $SupersetExe fab list-users 2>&1 | Select-String -Pattern $AdminUsername
    if ($CheckResult) {
        Write-Warning-Custom "Admin user '$AdminUsername' already exists. Skipping creation."
    } else {
        Write-Info "Creating new admin user..."
        & $SupersetExe fab create-admin `
            --username $AdminUsername `
            --firstname $AdminFirstName `
            --lastname $AdminLastName `
            --email $AdminEmail `
            --password $AdminPassword 2>&1 | ForEach-Object { Write-Host "  $_" }
        Write-Success "Admin user created successfully"
    }
} catch {
    Write-Warning-Custom "Could not verify admin user. Will attempt creation anyway."
    & $SupersetExe fab create-admin `
        --username $AdminUsername `
        --firstname $AdminFirstName `
        --lastname $AdminLastName `
        --email $AdminEmail `
        --password $AdminPassword 2>&1 | Out-Null
    Write-Success "Admin user creation command executed"
}

# ============================================================================
# LOAD EXAMPLE DATA (OPTIONAL)
# ============================================================================

Write-Header "LOADING INITIAL DATA"

Write-Info "Loading initial data and examples..."
try {
    & $SupersetExe load_examples 2>&1 | ForEach-Object { Write-Host "  $_" }
    Write-Success "Initial data loaded successfully"
} catch {
    Write-Warning-Custom "Failed to load example data (non-critical)"
}

# ============================================================================
# VALIDATE INITIALIZATION
# ============================================================================

Write-Header "VALIDATING INITIALIZATION"

# Check database file
$DbFile = Join-Path $env:SUPERSET_HOME "superset.db"
if (Test-Path $DbFile) {
    $DbSize = (Get-Item $DbFile).Length / 1MB
    Write-Success "Database file created: $DbFile (Size: $($DbSize.ToString('F2')) MB)"
} else {
    Write-Warning-Custom "Database file location: $DbFile"
}

# Summary of environment
Write-Header "INITIALIZATION SUMMARY"

Write-Host "Superset Configuration Details:" -ForegroundColor Green
Write-Host "  Superset Home: $env:SUPERSET_HOME"
Write-Host "  Database: SQLite at $DbFile"
Write-Host "  Config File: $ConfigFile"
Write-Host "  Virtual Env: $VenvPath"
Write-Host ""
Write-Host "Admin Credentials:" -ForegroundColor Green
Write-Host "  Username: $AdminUsername"
Write-Host "  Password: $AdminPassword"
Write-Host "  Email: $AdminEmail"
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Green
Write-Host "  1. Run the Superset server: .\run_superset.ps1"
Write-Host "  2. Access at http://localhost:8088"
Write-Host "  3. Login with credentials above"
Write-Host "  4. Configure data sources and create dashboards"
Write-Host ""

Write-Success "✓ SUPERSET INITIALIZATION COMPLETED SUCCESSFULLY!"
Write-Host ""
