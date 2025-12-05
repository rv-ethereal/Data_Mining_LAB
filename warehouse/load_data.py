"""
Load data from Parquet files to SQLite warehouse database
"""
import sqlite3
import pandas as pd
import os
import sys

def get_base_path():
    """Get the base project path"""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def create_database(db_path, schema_path):
    """Create SQLite database and execute schema"""
    print("=" * 60)
    print("CREATING DATABASE")
    print("=" * 60)
    
    # Remove existing database
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"✓ Removed existing database")
    
    # Create new database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Execute schema
    with open(schema_path, 'r') as f:
        schema_sql = f.read()
        cursor.executescript(schema_sql)
    
    conn.commit()
    print(f"✓ Created database: {db_path}")
    print(f"✓ Executed schema: {schema_path}")
    
    return conn

def load_parquet_to_sqlite(parquet_path, table_name, conn):
    """Load a Parquet file into SQLite table"""
    # Read Parquet file
    df = pd.read_parquet(parquet_path)
    
    # Convert date columns to string format for SQLite
    date_columns = df.select_dtypes(include=['datetime64']).columns
    for col in date_columns:
        df[col] = df[col].astype(str)
    
    # Load to SQLite
    df.to_sql(table_name, conn, if_exists='append', index=False)
    
    return len(df)

def load_warehouse_data(conn, warehouse_path):
    """Load all warehouse tables from Parquet files"""
    print("\n" + "=" * 60)
    print("LOADING WAREHOUSE DATA")
    print("=" * 60)
    
    # Load dimension tables first (to satisfy foreign key constraints)
    tables = [
        ('dim_products.parquet', 'dim_products'),
        ('dim_customers.parquet', 'dim_customers'),
        ('fact_sales.parquet', 'fact_sales')
    ]
    
    total_records = 0
    
    for parquet_file, table_name in tables:
        parquet_path = os.path.join(warehouse_path, parquet_file)
        
        if not os.path.exists(parquet_path):
            print(f"✗ Warning: {parquet_file} not found, skipping...")
            continue
        
        record_count = load_parquet_to_sqlite(parquet_path, table_name, conn)
        total_records += record_count
        print(f"✓ Loaded {record_count:,} records into {table_name}")
    
    conn.commit()
    print(f"\n✓ Total records loaded: {total_records:,}")

def verify_data(conn):
    """Verify loaded data with queries"""
    print("\n" + "=" * 60)
    print("VERIFYING DATA")
    print("=" * 60)
    
    cursor = conn.cursor()
    
    # Count records
    tables = ['dim_products', 'dim_customers', 'fact_sales']
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"✓ {table}: {count:,} records")
    
    # Check referential integrity
    cursor.execute("""
        SELECT COUNT(*) 
        FROM fact_sales f
        LEFT JOIN dim_products p ON f.product_id = p.product_id
        WHERE p.product_id IS NULL
    """)
    orphan_products = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT COUNT(*) 
        FROM fact_sales f
        LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)
    orphan_customers = cursor.fetchone()[0]
    
    if orphan_products == 0 and orphan_customers == 0:
        print("✓ Referential integrity: PASSED")
    else:
        print(f"✗ Warning: {orphan_products} orphan products, {orphan_customers} orphan customers")
    
    # Sample analytics query
    print("\n" + "=" * 60)
    print("SAMPLE ANALYTICS")
    print("=" * 60)
    
    cursor.execute("""
        SELECT 
            SUM(total_revenue) as total_revenue,
            COUNT(*) as transaction_count,
            AVG(total_revenue) as avg_transaction_value
        FROM fact_sales
    """)
    
    result = cursor.fetchone()
    print(f"Total Revenue: ${result[0]:,.2f}")
    print(f"Total Transactions: {result[1]:,}")
    print(f"Average Transaction: ${result[2]:,.2f}")

def main():
    """Main execution"""
    print("\n" + "=" * 60)
    print("WAREHOUSE DATA LOADER")
    print("=" * 60)
    
    base_path = get_base_path()
    warehouse_path = os.path.join(base_path, 'datalake', 'warehouse')
    db_path = os.path.join(base_path, 'warehouse', 'datawarehouse.db')
    schema_path = os.path.join(base_path, 'warehouse', 'schema.sql')
    
    try:
        # Create database and schema
        conn = create_database(db_path, schema_path)
        
        # Load data from Parquet files
        load_warehouse_data(conn, warehouse_path)
        
        # Verify data
        verify_data(conn)
        
        print("\n" + "=" * 60)
        print("✓ WAREHOUSE LOADING COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"\nDatabase location: {db_path}")
        
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}", file=sys.stderr)
        raise
    
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    # Check if --verify flag is passed
    if len(sys.argv) > 1 and sys.argv[1] == '--verify':
        base_path = get_base_path()
        db_path = os.path.join(base_path, 'warehouse', 'datawarehouse.db')
        
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            verify_data(conn)
            conn.close()
        else:
            print("Database not found. Run without --verify flag first.")
    else:
        main()
