#!/bin/bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "============================================================"
echo "   Setting up Airflow + Superset with Docker"
echo "============================================================"
echo ""

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker not installed!${NC}"
    echo "Installing Docker..."
    sudo apt-get update
    sudo apt-get install docker.io docker-compose -y
    sudo usermod -aG docker $USER
    echo -e "${YELLOW}Please log out and back in, then run this script again${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker installed${NC}"
echo ""

# Set environment
echo -e "${YELLOW}[1/6]${NC} Setting up environment..."
echo "AIRFLOW_UID=$(id -u)" > .env
echo -e "${GREEN}✓ Environment configured${NC}"
echo ""

# Create directories
echo -e "${YELLOW}[2/6]${NC} Creating directories..."
mkdir -p logs dags plugins
chmod -R 777 logs dags plugins
echo -e "${GREEN}✓ Directories created${NC}"
echo ""

# Initialize Airflow
echo -e "${YELLOW}[3/6]${NC} Initializing Airflow..."
docker-compose up airflow-init
echo -e "${GREEN}✓ Airflow initialized${NC}"
echo ""

# Start services
echo -e "${YELLOW}[4/6]${NC} Starting services..."
docker-compose up -d
echo -e "${GREEN}✓ Services started${NC}"
echo ""

# Wait
echo -e "${YELLOW}[5/6]${NC} Waiting for services..."
sleep 30
echo -e "${GREEN}✓ Services ready${NC}"
echo ""

# Status
echo -e "${YELLOW}[6/6]${NC} Checking status..."
docker-compose ps
echo ""

echo "============================================================"
echo -e "${GREEN}   SETUP COMPLETE!${NC}"
echo "============================================================"
echo ""
echo "Airflow:  http://localhost:8080 (admin/admin)"
echo "Superset: http://localhost:8088 (admin/admin)"
echo ""
echo "To stop: docker-compose down"
echo "============================================================"
