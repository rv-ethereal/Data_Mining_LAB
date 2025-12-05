"""Warehouse Data Integration - Parquet to SQLite Bridge"""

import pandas as pd
import sqlite3
from pathlib import Path
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class WarehouseDataExporter:
    """Export analytical warehouse data from Parquet to SQLite."""
    
    def __init__(self):
        self.base_path = Path(__file__).parent.parent
        self.warehouse_dir = self.base_path / "datalake/warehouse"
        self.database_path = self.base_path / "datalake/warehouse.db"
        self.table_mappings = [
            "revenue_by_product", "revenue_by_region", "payment_analysis",
            "status_summary", "customer_summary", "monthly_sales"
        ]
    
    def _connect_database(self) -> sqlite3.Connection:
        """Establish SQLite database connection."""
        logger.info(f"Connecting to database: {self.database_path}")
        return sqlite3.connect(str(self.database_path))
    
    def _load_parquet_files(self, table_dir: Path) -> pd.DataFrame:
        """Load and combine all parquet files from directory."""
        parquet_files = list(table_dir.glob("*.parquet"))
        
        if not parquet_files:
            logger.warning(f"No parquet files found in {table_dir}")
            return None
        
        dataframes = [pd.read_parquet(str(file)) for file in parquet_files]
        return pd.concat(dataframes, ignore_index=True) if dataframes else None
    
    def export_table(self, connection: sqlite3.Connection, table_name: str) -> bool:
        """Export single warehouse table to SQLite."""
        table_path = self.warehouse_dir / table_name
        
        if not table_path.exists():
            logger.warning(f"Table directory not found: {table_name}")
            return False
        
        dataframe = self._load_parquet_files(table_path)
        if dataframe is None:
            return False
        
        dataframe.to_sql(table_name, connection, if_exists="replace", index=False)
        logger.info(f"Exported {table_name}: {len(dataframe):,} rows")
        return True
    
    def execute(self):
        """Execute warehouse data export process."""
        logger.info("Starting warehouse data export process...")
        
        try:
            with self._connect_database() as connection:
                exported_count = sum(
                    self.export_table(connection, table) 
                    for table in self.table_mappings
                )
                
                logger.info(f"\nExport completed: {exported_count}/{len(self.table_mappings)} tables")
                logger.info(f"Database location: {self.database_path}")
                logger.info("SQLite database ready for visualization tools")
        
        except Exception as error:
            logger.error(f"Export process failed: {str(error)}")
            raise

def main():
    """Execute warehouse data export."""
    exporter = WarehouseDataExporter()
    exporter.execute()

if __name__ == "__main__":
    main()
