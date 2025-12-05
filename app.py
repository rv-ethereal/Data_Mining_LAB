"""Analytics Platform - Superset Application Factory"""

from superset.app import create_app

def initialize_analytics_platform():
    """Initialize and configure Superset analytics platform."""
    return create_app()

# Application instance
app = initialize_analytics_platform()

if __name__ == "__main__":
    app.run(debug=True)
