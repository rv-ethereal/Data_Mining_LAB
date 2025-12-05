#!/bin/bash

# =============================================================================
# Complete Setup Script for Data Lake Project
# Author: Susanta Kumar Mohanty
# =============================================================================

echo "============================================================"
echo "   Data Lake with Spark, Airflow & Superset - Setup"
echo "============================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Project directory
PROJECT_DIR="/home/sushi/Downloads/spark-airflow-datalake-project"

# Function to print status
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_info() {
    echo -e "${YELLOW}[i]${NC} $1"
}

# Check if running as root
if [ "$EUID" -eq 0 ]; then 
    print_error "Please do not run this script as root"
    exit 1
fi

# Step 1: Check system requirements
echo ""
print_info "Checking system requirements..."

# Check Python
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    print_status "Python3 installed: $PYTHON_VERSION"
else
    print_error "Python3 not found. Please install Python 3.8+"
    exit 1
fi

# Check Java (required for Spark)
if command -v java &> /dev/null; then
    JAVA_VERSION=$(java -version 2>&1 | head -n 1)
    print_status "Java installed: $JAVA_VERSION"
else
    print_info "Java not found. Installing OpenJDK 11..."
    sudo apt-get update
    sudo apt-get install -y openjdk-11-jdk
fi

# Step 2: Create virtual environment
echo ""
print_info "Creating virtual environment..."
cd $PROJECT_DIR

if [ -d "venv" ]; then
    print_info "Virtual environment already exists"
else
    python3 -m venv venv
    print_status "Virtual environment created"
fi

# Activate virtual environment
source venv/bin/activate
print_status "Virtual environment activated"

# Step 3: Install Python dependencies
echo ""
print_info "Installing Python dependencies (this may take a few minutes)..."
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
print_status "All dependencies installed"

# Step 4: Setup Airflow
echo ""
print_info "Setting up Apache Airflow..."

export AIRFLOW_HOME=$PROJECT_DIR/airflow

# Initialize Airflow database
if [ ! -d "$AIRFLOW_HOME" ]; then
    print_info "Initializing Airflow database..."
    airflow db init
    print_status "Airflow database initialized"
    
    # Create admin user
    print_info "Creating Airflow admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
    print_status "Airflow admin user created (username: admin, password: admin)"
else
    print_status "Airflow already initialized"
fi

# Copy DAGs to Airflow
print_info "Copying DAGs to Airflow..."
mkdir -p $AIRFLOW_HOME/dags
cp -r $PROJECT_DIR/dags/* $AIRFLOW_HOME/dags/
print_status "DAGs copied to Airflow"

# Step 5: Setup Superset
echo ""
print_info "Setting up Apache Superset..."

# Initialize Superset
if [ ! -f "$HOME/.superset/superset.db" ]; then
    print_info "Initializing Superset database..."
    superset db upgrade
    
    # Create admin user
    print_info "Creating Superset admin user..."
    superset fab create-admin \
        --username admin \
        --firstname Admin \
        --lastname User \
        --email admin@example.com \
        --password admin
    
    # Load examples (optional)
    # superset load_examples
    
    # Initialize
    superset init
    
    print_status "Superset initialized (username: admin, password: admin)"
else
    print_status "Superset already initialized"
fi

# Step 6: Generate sample data
echo ""
print_info "Generating sample data..."
cd $PROJECT_DIR
python3 scripts/generate_data.py
print_status "Sample data generated"

# Step 7: Verify directory structure
echo ""
print_info "Verifying directory structure..."
for dir in raw staging processed warehouse; do
    if [ -d "datalake/$dir" ]; then
        print_status "datalake/$dir exists"
    else
        print_error "datalake/$dir missing!"
    fi
done

# Step 8: Test Spark
echo ""
print_info "Testing Spark installation..."
python3 -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('test').getOrCreate(); print('✓ Spark is working'); spark.stop()"

# Final summary
echo ""
echo "============================================================"
echo "                    SETUP COMPLETE!"
echo "============================================================"
echo ""
echo "Installation Summary:"
echo "  ✓ Python environment configured"
echo "  ✓ All dependencies installed"
echo "  ✓ Apache Spark ready"
echo "  ✓ Apache Airflow initialized"
echo "  ✓ Apache Superset initialized"
echo "  ✓ Sample data generated"
echo ""
echo "Credentials:"
echo "  Airflow:  http://localhost:8080  (admin/admin)"
echo "  Superset: http://localhost:8088  (admin/admin)"
echo ""
echo "Next Steps:"
echo "  1. Run: ./scripts/run_project.sh"
echo "  2. Access Airflow at http://localhost:8080"
echo "  3. Access Superset at http://localhost:8088"
echo ""
echo "============================================================"

# Deactivate virtual environment
deactivate
