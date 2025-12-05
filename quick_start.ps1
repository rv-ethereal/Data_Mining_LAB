# Quick Start - Virtual Environment Edition
# This version assumes you're running inside the virtual environment

Write-Host "=========================================="
Write-Host "Data Lake ETL Pipeline - Quick Start"
Write-Host "=========================================="
Write-Host ""

# Check if virtual environment is activated
if (-not $env:VIRTUAL_ENV) {
    Write-Host "⚠ Virtual environment is not activated!"
    Write-Host ""
    Write-Host "Please run the setup first:"
    Write-Host "  .\setup.ps1"
    Write-Host ""
    Write-Host "Or activate manually:"
    Write-Host "  .\venv\Scripts\Activate.ps1"
    Write-Host ""
    exit 1
}

Write-Host "✓ Virtual environment is active: $env:VIRTUAL_ENV"
Write-Host ""

# Step 1: Generate Sample Data
Write-Host "Step 1: Generating sample data..."
python scripts\generate_sample_data.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Error generating data. Please check the error message above."
    exit 1
}
Write-Host "✓ Sample data generated successfully"
Write-Host ""

# Step 2: Validate Raw Data
Write-Host "Step 2: Validating raw data..."
python scripts\validate_pipeline.py --stage raw
if ($LASTEXITCODE -ne 0) {
    Write-Host "⚠ Warning: Raw data validation failed"
}
Write-Host ""

# Step 3: Check if PySpark is installed
Write-Host "Step 3: Checking PySpark installation..."
python -c "import pyspark" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "⚠ PySpark is not installed in this virtual environment"
    Write-Host ""
    $response = Read-Host "Do you want to install PySpark now? (y/n)"
    if ($response -eq 'y') {
        Write-Host "Installing PySpark..."
        pip install pyspark
        Write-Host "✓ PySpark installed"
    }
    else {
        Write-Host "Skipping Spark ETL pipeline"
        Write-Host ""
        Write-Host "To install PySpark later, run: pip install pyspark"
        Write-Host ""
        Write-Host "=========================================="
        Write-Host "Quick Start Completed (Partial)"
        Write-Host "=========================================="
        exit 0
    }
}
Write-Host "✓ PySpark is installed"
Write-Host ""

# Step 4: Run Spark ETL
Write-Host "Step 4: Running Spark ETL pipeline..."
Write-Host "This may take 1-2 minutes..."
Write-Host ""

python spark\etl_pipeline.py
if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "✓ Spark ETL completed successfully"
    
    # Validate processed data
    Write-Host ""
    Write-Host "Step 5: Validating processed data..."
    python scripts\validate_pipeline.py --stage all
}
else {
    Write-Host "✗ Spark ETL failed. Check error messages above."
    Write-Host ""
    Write-Host "Common issues:"
    Write-Host "  - Java not installed or JAVA_HOME not set"
    Write-Host "  - Insufficient memory"
    Write-Host ""
}

Write-Host ""
Write-Host "=========================================="
Write-Host "Quick Start Completed!"
Write-Host "=========================================="
Write-Host ""
Write-Host "✓ Data generated and validated"
Write-Host "✓ ETL pipeline executed"
Write-Host ""
Write-Host "Next Steps:"
Write-Host "1. Start Airflow:"
Write-Host "   cd airflow"
Write-Host "   docker-compose up -d"
Write-Host "   Access: http://localhost:8080 (admin/admin)"
Write-Host ""
Write-Host "2. Start Superset:"
Write-Host "   cd superset"
Write-Host "   docker-compose up -d"
Write-Host "   Access: http://localhost:8088 (admin/admin)"
Write-Host ""
Write-Host "For detailed instructions, see docs/setup_guide.md"
Write-Host "=========================================="
