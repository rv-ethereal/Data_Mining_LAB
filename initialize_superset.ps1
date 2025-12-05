# Initialize Superset - Create Admin User
$env:SUPERSET_SECRET_KEY = 'thisismysecretkey123456789'
$env:FLASK_APP = 'superset'

Write-Host "Creating Superset admin user..." -ForegroundColor Green
Write-Host "Username: admin" -ForegroundColor Yellow
Write-Host "Password: admin" -ForegroundColor Yellow

& "d:\assingment\third semester assingment\onprem-datalake\venv\Scripts\Activate.ps1"
& "d:\assingment\third semester assingment\onprem-datalake\venv\Scripts\python.exe" -m superset fab create-admin --username admin --firstname Admin --lastname User --email admin@superset.com --password admin
