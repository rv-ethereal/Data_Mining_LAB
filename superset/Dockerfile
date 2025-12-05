FROM apache/superset:latest
USER root
# Install Trino driver directly into the application's virtual environment
RUN pip install --target=/app/.venv/lib/python3.10/site-packages sqlalchemy-trino "sqlalchemy<2.0"
USER superset