"""Superset Configuration - Analytics Platform Settings"""

# Security Configuration
PREVENT_UNSAFE_DB_CONNECTIONS = False

# Feature Flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALLOW_ADHOC_SUBQUERIES": True,
    "ENABLE_JAVASCRIPT_CONTROLS": True,
    "ENABLE_EXPLORE_JSON_CSRF_PROTECTION": True,
}

# Default Configuration Values
SQLALCHEMY_DATABASE_URI = "sqlite:////tmp/superset.db"
SECRET_KEY = "change_me_on_production"

# Session Configuration
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_SAMESITE = "Lax"
