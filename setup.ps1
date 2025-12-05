# Setup Script for Data Lake Project
Write-Host "Data Lake Project - Environment Setup"
Write-Host ""

# Create Virtual Environment
Write-Host "Step 1: Creating virtual environment..."
python -m venv venv
Write-Host "Done"
Write-Host ""

# Install Dependencies  
Write-Host "Step 2: Installing dependencies..."
.\venv\Scripts\python.exe -m pip install --upgrade pip
.\venv\Scripts\pip.exe install -r requirements.txt
Write-Host "Done"
Write-Host ""

Write-Host "Setup completed! Activate with: .\venv\Scripts\Activate.ps1"
