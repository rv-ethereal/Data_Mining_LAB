# Base image
FROM python:3.9-slim

# Set environment variables
ENV SUPERSET_HOME=/app/superset_home
ENV PYTHONPATH=/app
ENV FLASK_APP=superset

# Create directories
WORKDIR /app
RUN mkdir -p $SUPERSET_HOME

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    libsasl2-dev \
    libldap2-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Superset and additional dependencies
# Explicitly installing conflicting/specific versions if needed, or just pure superset
RUN pip install --no-cache-dir apache-superset
RUN pip install --no-cache-dir \
    pandas>=2.1.0 \
    numpy>=1.26.0 \
    pyarrow>=14.0.0 \
    pyspark>=3.5.0 \
    duckdb==0.8.1 \
    python-dateutil==2.8.2 \
    psycopg2-binary \
    gevent

# Initialize Superset (this is usually done in an entrypoint, but for a simple build we can set up the basics)
# We'll use an entrypoint script to handle init if the db is fresh
COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

# Create an admin user and init (Note: In a real prod setup, this should be cleaner)
# For now, we'll keep it simple and just expose the port, assuming the user might run init commands manually or via a script.
# Actually, let's create a simple entrypoint script.

COPY docker-entrypoint.sh /usr/bin/
COPY setup_superset_demo.py /app/
RUN chmod +x /usr/bin/docker-entrypoint.sh

EXPOSE 8088

ENTRYPOINT ["docker-entrypoint.sh"]
