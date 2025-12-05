#!/bin/bash

# =============================================================================
# Test ETL Pipeline Standalone
# Author: Susanta Kumar Mohanty
# =============================================================================

PROJECT_DIR="/home/claude/spark-airflow-datalake-project"

echo "============================================================"
echo "   Testing Spark ETL Pipeline"
echo "============================================================"

cd $PROJECT_DIR
source venv/bin/activate

# Check if data exists
if [ ! -f "datalake/raw/sales.csv" ]; then
    echo "Generating sample data..."
    python3 scripts/generate_data.py
fi

echo ""
echo "Running Spark ETL Pipeline..."
echo "============================================================"

# Run the ETL
python3 spark_jobs/etl_pipeline.py

# Check results
echo ""
echo "============================================================"
echo "   Checking Results"
echo "============================================================"

echo ""
echo "Processed Data:"
ls -lh datalake/processed/

echo ""
echo "Warehouse Tables:"
ls -lh datalake/warehouse/

echo ""
echo "============================================================"
echo "   ETL Test Complete!"
echo "============================================================"
