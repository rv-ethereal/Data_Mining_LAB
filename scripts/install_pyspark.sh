#!/bin/bash
# Install PySpark and dependencies if not already installed
if ! python -c "import pyspark" 2>/dev/null; then
    echo "Installing PySpark and dependencies..."
    pip install --no-cache-dir pyspark==3.5.0 pandas==2.1.4 pyarrow==14.0.1 faker==22.0.0 pyyaml==6.0.1
    echo "PySpark installed successfully!"
else
    echo "PySpark already installed"
fi
