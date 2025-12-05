"""
Data Pipeline Validation Script
Validates data quality at each stage of the pipeline
"""

import os
import sys
import argparse
from pathlib import Path

def validate_raw_data():
    """Validate raw data files"""
    print("=" * 60)
    print("Validating Raw Data Zone")
    print("=" * 60)
    
    raw_path = Path("datalake/raw")
    required_files = {
        'sales.csv': 'Sales transactions',
        'products.json': 'Product catalog',
        'customers.csv': 'Customer information'
    }
    
    all_valid = True
    
    for filename, description in required_files.items():
        filepath = raw_path / filename
        if filepath.exists():
            size = filepath.stat().st_size
            print(f"✓ {description}: {filename} ({size:,} bytes)")
        else:
            print(f"✗ {description}: {filename} NOT FOUND")
            all_valid = False
    
    return all_valid

def validate_processed_data():
    """Validate processed data"""
    print("\n" + "=" * 60)
    print("Validating Processed Data Zone")
    print("=" * 60)
    
    processed_path = Path("datalake/processed")
    required_dirs = ['sales', 'products', 'customers']
    
    all_valid = True
    
    for dirname in required_dirs:
        dirpath = processed_path / dirname
        if dirpath.exists() and dirpath.is_dir():
            # Count parquet files
            parquet_files = list(dirpath.glob("*.parquet"))
            if parquet_files:
                total_size = sum(f.stat().st_size for f in parquet_files)
                print(f"✓ {dirname}: {len(parquet_files)} parquet files ({total_size:,} bytes)")
            else:
                print(f"⚠ {dirname}: Directory exists but no parquet files found")
                all_valid = False
        else:
            print(f"✗ {dirname}: Directory NOT FOUND")
            all_valid = False
    
    return all_valid

def validate_warehouse_data():
    """Validate warehouse data"""
    print("\n" + "=" * 60)
    print("Validating Warehouse Data Zone")
    print("=" * 60)
    
    warehouse_path = Path("datalake/warehouse")
    required_dirs = [
        'fact_sales',
        'dim_products',
        'dim_customers',
        'agg_monthly_revenue',
        'agg_top_products',
        'agg_regional_sales'
    ]
    
    all_valid = True
    
    for dirname in required_dirs:
        dirpath = warehouse_path / dirname
        if dirpath.exists() and dirpath.is_dir():
            parquet_files = list(dirpath.glob("*.parquet"))
            if parquet_files:
                total_size = sum(f.stat().st_size for f in parquet_files)
                print(f"✓ {dirname}: {len(parquet_files)} parquet files ({total_size:,} bytes)")
            else:
                print(f"⚠ {dirname}: Directory exists but no parquet files found")
                all_valid = False
        else:
            print(f"✗ {dirname}: Directory NOT FOUND")
            all_valid = False
    
    return all_valid

def validate_all():
    """Validate all stages"""
    print("\n" + "=" * 80)
    print("DATA PIPELINE VALIDATION REPORT")
    print("=" * 80)
    
    raw_valid = validate_raw_data()
    processed_valid = validate_processed_data()
    warehouse_valid = validate_warehouse_data()
    
    print("\n" + "=" * 60)
    print("Validation Summary")
    print("=" * 60)
    print(f"Raw Data Zone:       {'✓ PASS' if raw_valid else '✗ FAIL'}")
    print(f"Processed Data Zone: {'✓ PASS' if processed_valid else '✗ FAIL'}")
    print(f"Warehouse Data Zone: {'✓ PASS' if warehouse_valid else '✗ FAIL'}")
    print("=" * 60)
    
    if raw_valid and processed_valid and warehouse_valid:
        print("\n✓ All validation checks passed!")
        return True
    else:
        print("\n✗ Some validation checks failed")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Validate Data Pipeline')
    parser.add_argument(
        '--stage',
        choices=['raw', 'processed', 'warehouse', 'all'],
        default='all',
        help='Which stage to validate'
    )
    
    args = parser.parse_args()
    
    # Change to project root
    os.chdir(Path(__file__).parent.parent)
    
    if args.stage == 'raw':
        success = validate_raw_data()
    elif args.stage == 'processed':
        success = validate_processed_data()
    elif args.stage == 'warehouse':
        success = validate_warehouse_data()
    else:
        success = validate_all()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
