#!/bin/bash

# =============================================================================
# Stop Script for Data Lake Project
# Author: Susanta Kumar Mohanty
# =============================================================================

PROJECT_DIR="/home/claude/spark-airflow-datalake-project"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

echo "============================================================"
echo "   Stopping Data Lake Project Services"
echo "============================================================"

# Stop using PID file if exists
if [ -f "$PROJECT_DIR/pids.txt" ]; then
    echo "Stopping services using PID file..."
    while read pid; do
        if ps -p $pid > /dev/null; then
            kill $pid
            print_status "Stopped process: $pid"
        fi
    done < "$PROJECT_DIR/pids.txt"
    rm "$PROJECT_DIR/pids.txt"
fi

# Kill any remaining processes
echo "Checking for remaining processes..."

# Stop Airflow
pkill -f "airflow webserver"
pkill -f "airflow scheduler"
print_status "Airflow processes stopped"

# Stop Superset
pkill -f "superset run"
print_status "Superset processes stopped"

echo ""
echo "============================================================"
echo "              ALL SERVICES STOPPED"
echo "============================================================"
