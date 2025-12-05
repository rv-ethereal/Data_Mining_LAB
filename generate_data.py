"""
Data Generator for On-Premise Data Lake
Generates realistic sales and customer data (1000+ rows each)
Run this script to populate the datalake/raw/ directory with test data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration
NUM_CUSTOMERS = 250
NUM_SALES = 300
OUTPUT_DIR = "datalake/raw"

# Data constants
PRODUCTS = [
    ("Mobile", 15000, 25000),
    ("Laptop", 40000, 80000),
    ("Tablet", 15000, 35000),
    ("Headphone", 1000, 5000),
    ("Smartwatch", 3000, 15000),
    ("Speaker", 2000, 10000),
    ("Camera", 20000, 60000),
    ("Monitor", 10000, 30000),
    ("Keyboard", 500, 3000),
    ("Mouse", 300, 2000),
]

REGIONS = ["North", "South", "East", "West", "Central"]

CITIES = {
    "North": ["Delhi", "Lucknow", "Chandigarh", "Jaipur", "Amritsar"],
    "South": ["Bangalore", "Chennai", "Hyderabad", "Kochi", "Mysore"],
    "East": ["Kolkata", "Patna", "Bhubaneswar", "Guwahati", "Ranchi"],
    "West": ["Mumbai", "Pune", "Ahmedabad", "Surat", "Nagpur"],
    "Central": ["Bhopal", "Indore", "Raipur", "Jabalpur", "Gwalior"],
}

FIRST_NAMES = [
    "Rahul", "Priya", "Amit", "Neha", "Vikram", "Anjali", "Rohan", "Pooja",
    "Sanjay", "Divya", "Arjun", "Kavita", "Karan", "Sneha", "Raj", "Meera",
    "Aditya", "Shreya", "Varun", "Riya", "Nikhil", "Isha", "Akash", "Simran",
    "Siddharth", "Tanvi", "Harsh", "Nisha", "Manish", "Kriti", "Gaurav", "Ananya"
]

LAST_NAMES = [
    "Sharma", "Verma", "Singh", "Kumar", "Patel", "Gupta", "Reddy", "Iyer",
    "Nair", "Joshi", "Rao", "Mehta", "Desai", "Pillai", "Agarwal", "Chopra"
]

def generate_customers(num_customers):
    """Generate customer data"""
    print(f"Generating {num_customers} customer records...")
    
    customers = []
    
    for i in range(1, num_customers + 1):
        region = random.choice(REGIONS)
        city = random.choice(CITIES[region])
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        name = f"{first_name} {last_name}"
        
        # Add some variety with email and phone
        email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"
        phone = f"+91-{random.randint(7000000000, 9999999999)}"
        
        customers.append({
            "cust_id": i,
            "name": name,
            "email": email,
            "phone": phone,
            "city": city,
            "region": region,
            "registration_date": (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
        })
    
    df = pd.DataFrame(customers)
    print(f"✓ Generated {len(df)} customers")
    return df

def generate_sales(num_sales, num_customers):
    """Generate sales transaction data"""
    print(f"Generating {num_sales} sales records...")
    
    sales = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_range = (end_date - start_date).days
    
    for i in range(1, num_sales + 1):
        # Select product
        product_name, min_price, max_price = random.choice(PRODUCTS)
        
        # Generate price (with some random variation)
        base_price = random.uniform(min_price, max_price)
        price = round(base_price, 2)
        
        # Generate quantity (weighted towards smaller quantities)
        qty = random.choices(
            [1, 2, 3, 4, 5, 10, 15, 20],
            weights=[40, 25, 15, 10, 5, 3, 1, 1],
            k=1
        )[0]
        
        # Generate date
        random_days = random.randint(0, date_range)
        sale_date = (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")
        
        # Assign customer and region
        cust_id = random.randint(1, num_customers)
        region = random.choice(REGIONS)
        
        # Add discount occasionally
        discount = random.choices([0, 5, 10, 15, 20], weights=[60, 20, 10, 7, 3], k=1)[0]
        
        # Payment method
        payment_method = random.choice(["Credit Card", "Debit Card", "UPI", "Cash", "Net Banking"])
        
        # Status
        status = random.choices(
            ["Completed", "Pending", "Cancelled", "Returned"],
            weights=[85, 8, 5, 2],
            k=1
        )[0]
        
        sales.append({
            "id": i,
            "date": sale_date,
            "cust_id": cust_id,
            "product": product_name,
            "price": price,
            "qty": qty,
            "discount_percent": discount,
            "region": region,
            "payment_method": payment_method,
            "status": status
        })
    
    df = pd.DataFrame(sales)
    print(f"✓ Generated {len(df)} sales transactions")
    return df

def add_data_quality_issues(df, issue_rate=0.02):
    """
    Intentionally add some data quality issues for realistic ETL testing
    - Missing values
    - Duplicate records
    - Outliers
    """
    print(f"Adding data quality issues (rate: {issue_rate*100}%)...")
    
    df_copy = df.copy()
    num_rows = len(df_copy)
    num_issues = int(num_rows * issue_rate)
    
    # Add some NULL values randomly
    for _ in range(num_issues // 2):
        row_idx = random.randint(0, num_rows - 1)
        col_name = random.choice(df_copy.columns)
        df_copy.at[row_idx, col_name] = None
    
    # Add some duplicate rows
    for _ in range(num_issues // 4):
        row_idx = random.randint(0, num_rows - 1)
        df_copy = pd.concat([df_copy, df_copy.iloc[[row_idx]]], ignore_index=True)
    
    print(f"✓ Added ~{num_issues} data quality issues")
    return df_copy

def save_data(df, filename, output_dir):
    """Save dataframe to CSV"""
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath, index=False)
    print(f"✓ Saved to {filepath} ({len(df)} rows)")
    return filepath

def generate_statistics(sales_df, customers_df):
    """Print statistics about generated data"""
    print("\n" + "=" * 80)
    print("DATA GENERATION STATISTICS")
    print("=" * 80)
    
    print(f"\n📊 SALES DATA:")
    print(f"   Total Transactions: {len(sales_df)}")
    print(f"   Date Range: {sales_df['date'].dropna().min()} to {sales_df['date'].dropna().max()}")
    print(f"   Total Revenue: ₹{(sales_df['price'].fillna(0) * sales_df['qty'].fillna(0)).sum():,.2f}")
    print(f"   Average Order Value: ₹{(sales_df['price'].fillna(0) * sales_df['qty'].fillna(0)).mean():,.2f}")
    print(f"   Unique Products: {sales_df['product'].nunique()}")
    print(f"   Products: {', '.join(sales_df['product'].unique())}")
    
    print(f"\n   Sales by Region:")
    for region, count in sales_df['region'].value_counts().items():
        region_data = sales_df[sales_df['region'] == region]
        revenue = (region_data['price'].fillna(0) * region_data['qty'].fillna(0)).sum()
        print(f"      {region}: {count} transactions, ₹{revenue:,.2f} revenue")
    
    print(f"\n   Sales by Status:")
    for status, count in sales_df['status'].value_counts().items():
        print(f"      {status}: {count} ({count/len(sales_df)*100:.1f}%)")
    
    print(f"\n👥 CUSTOMER DATA:")
    print(f"   Total Customers: {len(customers_df)}")
    print(f"   Unique Regions: {customers_df['region'].nunique()}")
    print(f"   Customers by Region:")
    for region, count in customers_df['region'].value_counts().items():
        print(f"      {region}: {count} customers")
    
    print("\n" + "=" * 80)

def main():
    """Main execution function"""
    print("=" * 80)
    print("DATA GENERATOR FOR ON-PREMISE DATA LAKE")
    print("=" * 80)
    print(f"Configuration:")
    print(f"  - Customers to generate: {NUM_CUSTOMERS}")
    print(f"  - Sales to generate: {NUM_SALES}")
    print(f"  - Output directory: {OUTPUT_DIR}")
    print("=" * 80 + "\n")
    
    # Generate customer data
    customers_df = generate_customers(NUM_CUSTOMERS)
    
    # Generate sales data
    sales_df = generate_sales(NUM_SALES, NUM_CUSTOMERS)
    
    # Add realistic data quality issues
    sales_df = add_data_quality_issues(sales_df, issue_rate=0.02)
    customers_df = add_data_quality_issues(customers_df, issue_rate=0.01)
    
    # Save to CSV files
    print("\n" + "=" * 80)
    print("SAVING DATA FILES")
    print("=" * 80)
    
    sales_file = save_data(sales_df, "sales.csv", OUTPUT_DIR)
    customers_file = save_data(customers_df, "customers.csv", OUTPUT_DIR)
    
    # Print statistics
    generate_statistics(sales_df, customers_df)
    
    print("\n✅ DATA GENERATION COMPLETE!")
    print(f"\nGenerated files:")
    print(f"  1. {sales_file}")
    print(f"  2. {customers_file}")
    print(f"\nYou can now run the Spark ETL pipeline:")
    print(f"  spark-submit spark/spark_etl.py")
    print("=" * 80)

if __name__ == "__main__":
    main()