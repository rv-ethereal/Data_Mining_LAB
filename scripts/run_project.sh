#!/bin/bash

# =============================================================================
# Run Script for Data Lake Project
# Author: Susanta Kumar Mohanty
# =============================================================================

PROJECT_DIR="/home/sushi/Downloads/spark-airflow-datalake-project"
export AIRFLOW_HOME=$PROJECT_DIR/airflow

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_info() {
    echo -e "${YELLOW}[i]${NC} $1"
}

echo "============================================================"
echo "   Starting Data Lake Project"
echo "============================================================"

# Activate virtual environment
cd $PROJECT_DIR
source venv/bin/activate

# Check if data exists
if [ ! -f "datalake/raw/sales.csv" ]; then
    print_info "No data found. Generating sample data..."
    python3 scripts/generate_data.py
fi

# Start services in background
print_info "Starting Airflow Webserver on port 8080..."
nohup airflow webserver --port 8080 > logs/airflow-webserver.log 2>&1 &
WEBSERVER_PID=$!
sleep 5

print_info "Starting Airflow Scheduler..."
nohup airflow scheduler > logs/airflow-scheduler.log 2>&1 &
SCHEDULER_PID=$!
sleep 5

print_info "Starting Superset on port 8088..."
nohup superset run -h 0.0.0.0 -p 8088 --with-threads > logs/superset.log 2>&1 &
SUPERSET_PID=$!

# Wait for services to start
sleep 10

# Display status
echo ""
echo "============================================================"
echo "                  SERVICES STARTED"
echo "============================================================"
echo ""
print_status "Airflow Webserver: http://localhost:8080 (admin/admin)"
print_status "Airflow Scheduler: Running (PID: $SCHEDULER_PID)"
print_status "Superset: http://localhost:8088 (admin/admin)"
echo ""
echo "Process IDs saved to: $PROJECT_DIR/pids.txt"
echo "$WEBSERVER_PID" > pids.txt
echo "$SCHEDULER_PID" >> pids.txt
echo "$SUPERSET_PID" >> pids.txt
echo ""
echo "To run ETL manually:"
echo "  cd $PROJECT_DIR && source venv/bin/activate"
echo "  python3 spark_jobs/etl_pipeline.py"
echo ""
echo "To stop all services:"
echo "  ./scripts/stop_project.sh"
echo ""
echo "Logs are in: $PROJECT_DIR/logs/"
echo "============================================================"
