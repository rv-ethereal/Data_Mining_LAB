#!/bin/bash

# Superset Initialization Script
# This script initializes Superset with admin user and database connections

echo "=========================================="
echo "Initializing Apache Superset"
echo "=========================================="

# Wait for Superset to be ready
echo "Waiting for Superset to start..."
sleep 10

# Create admin user (if not exists)
echo "Creating admin user..."
docker exec superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin

# Initialize database
echo "Initializing Superset database..."
docker exec superset superset db upgrade

# Load examples (optional)
# docker exec superset superset load_examples

# Initialize Superset
echo "Running Superset init..."
docker exec superset superset init

echo "=========================================="
echo "Superset initialized successfully!"
echo "=========================================="
echo "Access Superset at: http://localhost:8088"
echo "Username: admin"
echo "Password: admin"
echo "=========================================="
