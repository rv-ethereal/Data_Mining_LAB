import os

# Secret key for session management
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "thisismysecretkey123456789")

# Disable SQLite unsafe connection warning (since we're using local SQLite)
PREVENT_UNSAFE_DB_CONNECTIONS = False

# Optional: Configure default database connection for Superset
# Update the path to your warehouse.db file
SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(os.getcwd(), "datalake", "warehouse.db")
