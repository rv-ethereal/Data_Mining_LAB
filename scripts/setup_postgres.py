"""
PostgreSQL Data Warehouse Setup Script
Creates database schema and tables for the data warehouse
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
import os

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'user': 'postgres',  # Default PostgreSQL user
    'password': 'postgres',  # Change this to your PostgreSQL password
}

WAREHOUSE_DB = 'datalake_warehouse'
WAREHOUSE_USER = 'datalake_user'
WAREHOUSE_PASSWORD = 'datalake_password'

def create_database():
    """Create warehouse database and user"""
    print("=" * 60)
    print("Creating Data Warehouse Database")
    print("=" * 60)
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Create user
        print(f"Creating user: {WAREHOUSE_USER}")
        cursor.execute(f"DROP USER IF EXISTS {WAREHOUSE_USER};")
        cursor.execute(f"CREATE USER {WAREHOUSE_USER} WITH PASSWORD '{WAREHOUSE_PASSWORD}';")
        
        # Create database
        print(f"Creating database: {WAREHOUSE_DB}")
        cursor.execute(f"DROP DATABASE IF EXISTS {WAREHOUSE_DB};")
        cursor.execute(f"CREATE DATABASE {WAREHOUSE_DB} OWNER {WAREHOUSE_USER};")
        
        # Grant privileges
        cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {WAREHOUSE_DB} TO {WAREHOUSE_USER};")
        
        cursor.close()
        conn.close()
        
        print("✓ Database and user created successfully")
        return True
        
    except Exception as e:
        print(f"✗ Error creating database: {e}")
        return False

def create_schema():
    """Create warehouse schema and tables"""
    print("\n" + "=" * 60)
    print("Creating Warehouse Schema")
    print("=" * 60)
    
    try:
        # Connect to warehouse database
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=WAREHOUSE_DB,
            user=WAREHOUSE_USER,
            password=WAREHOUSE_PASSWORD
        )
        cursor = conn.cursor()
        
        # Create fact_sales table
        print("Creating fact_sales table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                transaction_id VARCHAR(50) PRIMARY KEY,
                transaction_date DATE,
                year INTEGER,
                month INTEGER,
                customer_id VARCHAR(50),
                customer_name VARCHAR(200),
                customer_segment VARCHAR(50),
                city VARCHAR(100),
                state VARCHAR(100),
                product_id VARCHAR(50),
                product_name VARCHAR(200),
                category VARCHAR(100),
                quantity INTEGER,
                sale_unit_price DECIMAL(10, 2),
                discount_percent DECIMAL(5, 2),
                discount_amount DECIMAL(10, 2),
                subtotal DECIMAL(10, 2),
                tax_amount DECIMAL(10, 2),
                total_amount DECIMAL(10, 2),
                payment_method VARCHAR(50),
                shipping_method VARCHAR(50),
                order_status VARCHAR(50),
                store_location VARCHAR(100)
            );
        """)
        
        # Create dim_products table
        print("Creating dim_products table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_products (
                product_id VARCHAR(50) PRIMARY KEY,
                product_name VARCHAR(200),
                category VARCHAR(100),
                subcategory VARCHAR(100),
                brand VARCHAR(100),
                unit_price DECIMAL(10, 2),
                cost_price DECIMAL(10, 2),
                profit_margin DECIMAL(5, 2),
                stock_quantity INTEGER,
                supplier VARCHAR(200),
                weight_kg DECIMAL(10, 2),
                launch_date DATE
            );
        """)
        
        # Create dim_customers table
        print("Creating dim_customers table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_customers (
                customer_id VARCHAR(50) PRIMARY KEY,
                full_name VARCHAR(200),
                email VARCHAR(200),
                phone VARCHAR(50),
                city VARCHAR(100),
                state VARCHAR(100),
                zip_code VARCHAR(20),
                country VARCHAR(50),
                registration_date DATE,
                customer_segment VARCHAR(50),
                age INTEGER,
                gender VARCHAR(20)
            );
        """)
        
        # Create agg_monthly_revenue table
        print("Creating agg_monthly_revenue table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_monthly_revenue (
                year INTEGER,
                month INTEGER,
                total_revenue DECIMAL(15, 2),
                total_quantity INTEGER,
                transaction_count INTEGER,
                avg_order_value DECIMAL(10, 2),
                total_discounts DECIMAL(15, 2),
                PRIMARY KEY (year, month)
            );
        """)
        
        # Create agg_top_products table
        print("Creating agg_top_products table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_top_products (
                product_id VARCHAR(50) PRIMARY KEY,
                product_name VARCHAR(200),
                category VARCHAR(100),
                total_quantity_sold INTEGER,
                total_revenue DECIMAL(15, 2),
                transaction_count INTEGER,
                avg_sale_amount DECIMAL(10, 2),
                revenue_rank INTEGER
            );
        """)
        
        # Create agg_regional_sales table
        print("Creating agg_regional_sales table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_regional_sales (
                state VARCHAR(100),
                city VARCHAR(100),
                total_revenue DECIMAL(15, 2),
                transaction_count INTEGER,
                avg_order_value DECIMAL(10, 2),
                unique_customers INTEGER,
                PRIMARY KEY (state, city)
            );
        """)
        
        # Create indexes
        print("Creating indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON fact_sales(transaction_date);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_customer ON fact_sales(customer_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_product ON fact_sales(product_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_status ON fact_sales(order_status);")
        
        # Create views
        print("Creating views...")
        cursor.execute("""
            CREATE OR REPLACE VIEW v_sales_summary AS
            SELECT 
                DATE_TRUNC('month', transaction_date) as month,
                COUNT(*) as total_transactions,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_order_value
            FROM fact_sales
            WHERE order_status = 'Completed'
            GROUP BY DATE_TRUNC('month', transaction_date)
            ORDER BY month DESC;
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✓ Schema created successfully")
        return True
        
    except Exception as e:
        print(f"✗ Error creating schema: {e}")
        return False

def main():
    """Main function"""
    print("\n" + "=" * 60)
    print("PostgreSQL Data Warehouse Setup")
    print("=" * 60)
    print("\nNOTE: Make sure PostgreSQL is running on localhost:5432")
    print("      Update DB_CONFIG in this script with your credentials\n")
    
    # Create database
    if not create_database():
        print("\n✗ Failed to create database")
        sys.exit(1)
    
    # Create schema
    if not create_schema():
        print("\n✗ Failed to create schema")
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("✓ Data Warehouse Setup Completed Successfully!")
    print("=" * 60)
    print(f"\nDatabase: {WAREHOUSE_DB}")
    print(f"User: {WAREHOUSE_USER}")
    print(f"Password: {WAREHOUSE_PASSWORD}")
    print(f"Host: {DB_CONFIG['host']}")
    print(f"Port: {DB_CONFIG['port']}")
    print("\nTables created:")
    print("  - fact_sales")
    print("  - dim_products")
    print("  - dim_customers")
    print("  - agg_monthly_revenue")
    print("  - agg_top_products")
    print("  - agg_regional_sales")
    print("\nViews created:")
    print("  - v_sales_summary")
    print("=" * 60)

if __name__ == "__main__":
    main()
