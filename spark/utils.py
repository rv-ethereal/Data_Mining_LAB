"""
Utility functions for Spark ETL Pipeline
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, avg, min as spark_min, max as spark_max
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def log_dataframe_info(df: DataFrame, name: str):
    """Log information about a DataFrame"""
    logger.info(f"DataFrame: {name}")
    logger.info(f"  Row count: {df.count()}")
    logger.info(f"  Columns: {', '.join(df.columns)}")
    logger.info(f"  Schema:")
    df.printSchema()

def check_null_values(df: DataFrame, name: str):
    """Check and log null values in DataFrame"""
    logger.info(f"Checking null values in {name}...")
    
    null_counts = df.select([
        count(col(c)).alias(c) for c in df.columns
    ]).collect()[0].asDict()
    
    total_rows = df.count()
    
    for column, non_null_count in null_counts.items():
        null_count = total_rows - non_null_count
        if null_count > 0:
            logger.warning(f"  {column}: {null_count} null values ({null_count/total_rows*100:.2f}%)")
    
    return null_counts

def check_duplicates(df: DataFrame, key_columns: list, name: str):
    """Check for duplicate records"""
    logger.info(f"Checking duplicates in {name} based on {key_columns}...")
    
    total_rows = df.count()
    distinct_rows = df.select(key_columns).distinct().count()
    duplicates = total_rows - distinct_rows
    
    if duplicates > 0:
        logger.warning(f"  Found {duplicates} duplicate records")
    else:
        logger.info(f"  No duplicates found")
    
    return duplicates

def get_data_quality_report(df: DataFrame, name: str):
    """Generate comprehensive data quality report"""
    logger.info(f"Generating data quality report for {name}...")
    
    report = {
        'name': name,
        'row_count': df.count(),
        'column_count': len(df.columns),
        'columns': df.columns
    }
    
    # Numeric column statistics
    numeric_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ['int', 'bigint', 'double', 'float', 'decimal']]
    
    if numeric_cols:
        stats = df.select([
            spark_min(col(c)).alias(f'{c}_min'),
            spark_max(col(c)).alias(f'{c}_max'),
            avg(col(c)).alias(f'{c}_avg')
        ] for c in numeric_cols).collect()[0].asDict()
        
        report['numeric_stats'] = stats
    
    return report

def validate_schema(df: DataFrame, expected_columns: list, name: str):
    """Validate DataFrame schema against expected columns"""
    logger.info(f"Validating schema for {name}...")
    
    actual_columns = set(df.columns)
    expected_columns_set = set(expected_columns)
    
    missing_columns = expected_columns_set - actual_columns
    extra_columns = actual_columns - expected_columns_set
    
    if missing_columns:
        logger.error(f"  Missing columns: {missing_columns}")
        return False
    
    if extra_columns:
        logger.warning(f"  Extra columns: {extra_columns}")
    
    logger.info(f"  Schema validation passed")
    return True

def write_parquet(df: DataFrame, path: str, mode: str = "overwrite"):
    """Write DataFrame to Parquet with logging"""
    logger.info(f"Writing DataFrame to {path} (mode: {mode})...")
    
    df.write.mode(mode).parquet(path)
    
    logger.info(f"  Successfully written {df.count()} rows")

def read_parquet(spark, path: str):
    """Read Parquet file with logging"""
    logger.info(f"Reading Parquet from {path}...")
    
    df = spark.read.parquet(path)
    
    logger.info(f"  Successfully read {df.count()} rows")
    return df
