

import pandas as pd
import sqlite3
import os
import glob

# setting paths
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
WAREHOUSE_PATH = os.path.join(BASE_PATH, "datalake/warehouse")
DB_PATH = os.path.join(BASE_PATH, "datalake/warehouse.db")

def parquet_to_sqlite():
    # connecting to sqlite db (it creates it if doesnt exist)
    conn = sqlite3.connect(DB_PATH)
    
    # getting all warehouse tables
    tables = [
        "revenue_by_product",
        "revenue_by_region", 
        "payment_analysis",
        "status_summary",
        "customer_summary",
        "monthly_sales"
    ]
    
    print("converting parquet files to sqlite...")
    
    for table_name in tables:
        table_path = os.path.join(WAREHOUSE_PATH, table_name)
        
        if not os.path.exists(table_path):
            print(f"skipping {table_name} - folder not found")
            continue
        
        # reading all parquet files in the folder
        parquet_files = glob.glob(f"{table_path}/*.parquet")
        
        if not parquet_files:
            print(f"no parquet files found in {table_name}")
            continue
        
        # reading and combining all parquet files
        dfs = []
        for file in parquet_files:
            df = pd.read_parquet(file)
            dfs.append(df)
        
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # writing to sqlite
        combined_df.to_sql(table_name, conn, if_exists="replace", index=False)
        
        print(f"loaded {table_name}: {len(combined_df)} rows")
    
    conn.close()
    print(f"\ndatabase created at: {DB_PATH}")
    print("you can now connect superset to this sqlite db")

if __name__ == "__main__":
    parquet_to_sqlite()
