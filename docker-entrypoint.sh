#!/bin/bash
set -e

# Initialize Superset db
superset db upgrade

# Create default roles and permissions
superset init

# Create admin user if not exists (you might want to customize this)
# Check if admin already exists by listing users (simple hack, or just try creating and fail silently)
# better: allow env vars for admin creation
if [ "$SUPERSET_ADMIN_CREATE" = "true" ]; then
    superset fab create-admin \
        --username admin \
        --firstname Superset \
        --lastname Admin \
        --email admin@superset.com \
        --password admin
fi

# Run setup script
# python /app/setup_superset_demo.py

# Run the server
gunicorn \
    -w 10 \
    -k gthread \
    --threads 4 \
    --timeout 120 \
    -b  0.0.0.0:8088 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
