# Run Superset Server
$env:SUPERSET_SECRET_KEY = 'thisismysecretkey123456789'
$env:FLASK_APP = 'superset'
$env:SUPERSET_CONFIG_PATH = 'd:\assingment\third semester assingment\onprem-datalake\superset_config.py'

Write-Host "Starting Superset on http://localhost:8088" -ForegroundColor Green
Write-Host "Username: admin" -ForegroundColor Yellow
Write-Host "Password: admin" -ForegroundColor Yellow
Write-Host "`nPress Ctrl+C to stop the server`n" -ForegroundColor Cyan

& "d:\assingment\third semester assingment\onprem-datalake\venv\Scripts\Activate.ps1"
& "d:\assingment\third semester assingment\onprem-datalake\venv\Scripts\python.exe" -m superset run -p 8088 --with-threads --reload --debugger
