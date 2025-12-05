# Virtual Environment Activation
# Run this script to activate the virtual environment

if (Test-Path "venv\Scripts\Activate.ps1") {
    Write-Host "Activating virtual environment..."
    & .\venv\Scripts\Activate.ps1
    Write-Host ""
    Write-Host "✓ Virtual environment activated!"
    Write-Host ""
    Write-Host "You can now run:"
    Write-Host "  - python scripts\generate_sample_data.py"
    Write-Host "  - python spark\etl_pipeline.py"
    Write-Host "  - .\quick_start.ps1"
    Write-Host ""
    Write-Host "To deactivate: deactivate"
}
else {
    Write-Host "Virtual environment not found!"
    Write-Host ""
    Write-Host "Please run setup first:"
    Write-Host "  .\setup.ps1"
}
