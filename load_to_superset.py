"""
Load Parquet warehouse data into SQLite database for Superset connectivity
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime

def create_superset_database(warehouse_path, db_path):
    """Convert Parquet warehouse tables to SQLite"""
    print(f"\n[START] Loading warehouse data into SQLite - {datetime.now()}\n")
    
    # Initialize SQLite connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    tables_to_load = {
        'enriched_sales': 'enriched_sales.parquet',
        'revenue_by_month': 'revenue_by_month.parquet',
        'revenue_by_region': 'revenue_by_region.parquet',
        'top_products': 'top_products.parquet',
    }
    
    for table_name, parquet_file in tables_to_load.items():
        parquet_path = os.path.join(warehouse_path, parquet_file)
        
        if not os.path.exists(parquet_path):
            print(f"[WARNING] Parquet file not found: {parquet_path}")
            continue
        
        try:
            # Read parquet and convert to SQLite
            df = pd.read_parquet(parquet_path)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"[OK] Loaded {table_name} ({len(df)} rows)")
        except Exception as e:
            print(f"[ERROR] Failed to load {table_name}: {str(e)}")
    
    # Create summary views
    try:
        cursor.execute('''
            CREATE VIEW IF NOT EXISTS dashboard_summary AS
            SELECT 
                (SELECT COUNT(*) FROM enriched_sales) as total_sales,
                (SELECT SUM(total_revenue) FROM revenue_by_month) as total_revenue,
                (SELECT COUNT(DISTINCT region) FROM enriched_sales) as regions_count,
                (SELECT COUNT(DISTINCT product_id) FROM enriched_sales) as products_count
        ''')
        print("[OK] Created dashboard_summary view")
    except Exception as e:
        print(f"[WARNING] Could not create views: {str(e)}")
    
    conn.commit()
    conn.close()
    
    print(f"\n[SUCCESS] SQLite database created: {db_path}\n")
    return db_path

if __name__ == "__main__":
    warehouse_path = "/home/claude/data_mining_lab/datalake/warehouse"
    db_path = "/home/claude/data_mining_lab/superset_config/datalake.db"
    
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    create_superset_database(warehouse_path, db_path)