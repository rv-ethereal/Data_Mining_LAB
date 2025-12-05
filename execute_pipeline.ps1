# Execute Complete Data Lakehouse Pipeline

Write-Host "Starting Data Lakehouse Pipeline Execution..." -ForegroundColor Cyan

# 1. Start Services
Write-Host "`nChecking Services..." -ForegroundColor Yellow
docker-compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to start services."
    exit 1
}

# 2. Wait for Airflow
Write-Host "`nWaiting for Airflow to be ready (this may take a moment)..." -ForegroundColor Yellow
$retries = 30
while ($retries -gt 0) {
    $status = docker exec lakehouse-airflow-webserver airflow jobs check 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Airflow is ready!" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 5
    $retries--
    Write-Host "." -NoNewline
}

if ($retries -eq 0) {
    Write-Warning "Airflow might still be starting up. Proceeding anyway..."
}

# 3. Generate Data
Write-Host "`nGenerating Sample Data..." -ForegroundColor Yellow
python data/sample/generator.py
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to generate data."
    exit 1
}

# 4. Upload to MinIO
Write-Host "`nUploading to MinIO..." -ForegroundColor Yellow
python scripts/upload_to_minio.py
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to upload data."
    exit 1
}

# 5. Trigger Pipeline
Write-Host "`nTriggering Airflow DAG 'lakehouse_pipeline'..." -ForegroundColor Yellow
docker exec lakehouse-airflow-webserver airflow dags unpause lakehouse_pipeline
docker exec lakehouse-airflow-webserver airflow dags trigger lakehouse_pipeline

# 6. Monitor Status
Write-Host "`nMonitoring Pipeline Status..." -ForegroundColor Yellow
Write-Host "Go to: http://localhost:8082/dags/lakehouse_pipeline/grid" -ForegroundColor Cyan
Write-Host "Login: admin / admin"

# Poll for status
for ($i=0; $i -lt 10; $i++) {
    Start-Sleep -Seconds 5
    docker exec lakehouse-airflow-webserver airflow dags list-runs -d lakehouse_pipeline --state running
}

Write-Host "`nExecution Triggered!" -ForegroundColor Green
Write-Host "Check Superset Dashboard: http://localhost:8088" -ForegroundColor Cyan
