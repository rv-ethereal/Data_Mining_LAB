"""
Configuration settings for Data Lake ETL Pipeline
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for ETL pipeline"""
    
    # Data Lake Paths
    DATALAKE_ROOT = os.getenv('DATALAKE_ROOT', 'c:/Users/tb619/Videos/Dm assignment/datalake')
    RAW_DATA_PATH = os.path.join(DATALAKE_ROOT, 'raw')
    STAGING_PATH = os.path.join(DATALAKE_ROOT, 'staging')
    PROCESSED_PATH = os.path.join(DATALAKE_ROOT, 'processed')
    WAREHOUSE_PATH = os.path.join(DATALAKE_ROOT, 'warehouse')
    
    # Spark Configuration
    SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
    SPARK_APP_NAME = os.getenv('SPARK_APP_NAME', 'DataLakeETL')
    
    # PostgreSQL Configuration
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'datalake_warehouse')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'datalake_user')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'datalake_password')
    
    @classmethod
    def get_postgres_url(cls):
        """Get PostgreSQL JDBC URL"""
        return f"jdbc:postgresql://{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"
    
    @classmethod
    def get_postgres_properties(cls):
        """Get PostgreSQL connection properties"""
        return {
            "user": cls.POSTGRES_USER,
            "password": cls.POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
    
    # File Paths
    SALES_FILE = os.path.join(RAW_DATA_PATH, 'sales.csv')
    PRODUCTS_FILE = os.path.join(RAW_DATA_PATH, 'products.json')
    CUSTOMERS_FILE = os.path.join(RAW_DATA_PATH, 'customers.csv')
    
    # Output Paths
    PROCESSED_SALES = os.path.join(PROCESSED_PATH, 'sales')
    PROCESSED_PRODUCTS = os.path.join(PROCESSED_PATH, 'products')
    PROCESSED_CUSTOMERS = os.path.join(PROCESSED_PATH, 'customers')
    
    WAREHOUSE_FACT_SALES = os.path.join(WAREHOUSE_PATH, 'fact_sales')
    WAREHOUSE_DIM_PRODUCTS = os.path.join(WAREHOUSE_PATH, 'dim_products')
    WAREHOUSE_DIM_CUSTOMERS = os.path.join(WAREHOUSE_PATH, 'dim_customers')
    WAREHOUSE_AGG_MONTHLY = os.path.join(WAREHOUSE_PATH, 'agg_monthly_revenue')
    WAREHOUSE_AGG_PRODUCTS = os.path.join(WAREHOUSE_PATH, 'agg_top_products')
    WAREHOUSE_AGG_REGIONS = os.path.join(WAREHOUSE_PATH, 'agg_regional_sales')
