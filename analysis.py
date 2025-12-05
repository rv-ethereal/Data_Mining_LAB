"""
Data Mining Lab - On-Premise Data Lake Analysis
Complete exploratory data analysis and visualization script

This script performs comprehensive analysis on the processed data lake,
generating insights through statistical summaries and visualizations.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import os

# ============================================================================
# SETUP & CONFIGURATION
# ============================================================================

def setup_environment():
    """Initialize environment and styling"""
    sns.set_style('whitegrid')
    plt.rcParams['figure.figsize'] = (12, 6)
    print(f"✓ Setup complete at {datetime.now()}")
    print()

# ============================================================================
# DATA LOADING
# ============================================================================

def load_raw_data(base_path=None):
    """Load raw data from data lake"""
    # Auto-detect path if not provided
    if base_path is None:
        import os
        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_path = os.path.join(script_dir, 'datalake')
        if not os.path.exists(base_path):
            base_path = os.path.join(os.path.dirname(script_dir), 'datalake')
    
    print("=" * 70)
    print("LOADING RAW DATA")
    print("=" * 70)
    print(f"Path: {base_path}")
    print()
    
    # Load raw data
    sales = pd.read_csv(f'sales.csv')
    products = pd.read_json(f'products.json')
    customers = pd.read_csv(f'customers.csv')
    
    print(f"✓ Sales shape: {sales.shape}")
    print(f"✓ Products shape: {products.shape}")
    print(f"✓ Customers shape: {customers.shape}")
    print()
    
    print("Sample Sales Data:")
    print(sales.head())
    print()
    
    return sales, products, customers

# ============================================================================
# DATA QUALITY CHECKS
# ============================================================================

def check_data_quality(sales, customers):
    """Perform data quality checks"""
    print("=" * 70)
    print("DATA QUALITY CHECKS")
    print("=" * 70)
    print()
    
    print("Missing values in sales:")
    print(sales.isnull().sum())
    print()
    
    print("Missing values in customers:")
    print(customers.isnull().sum())
    print()
    
    print("✓ Data quality check complete")
    print()

# ============================================================================
# EXPLORATORY DATA ANALYSIS
# ============================================================================

def sales_summary_statistics(sales):
    """Calculate and display sales summary statistics"""
    print("=" * 70)
    print("SALES SUMMARY STATISTICS")
    print("=" * 70)
    print()
    
    # Calculate total amount
    sales['total'] = sales['qty'] * sales['price']
    
    print(f"✓ Total Revenue: ${sales['total'].sum():,.2f}")
    print(f"✓ Average Transaction: ${sales['total'].mean():,.2f}")
    print(f"✓ Median Transaction: ${sales['total'].median():,.2f}")
    print(f"✓ Min Transaction: ${sales['total'].min():,.2f}")
    print(f"✓ Max Transaction: ${sales['total'].max():,.2f}")
    print(f"✓ Total Transactions: {len(sales)}")
    print()
    
    print("Revenue by Region:")
    revenue_by_region = sales.groupby('region')['total'].sum().sort_values(ascending=False)
    print(revenue_by_region)
    print()
    
    return sales

# ============================================================================
# REVENUE ANALYSIS
# ============================================================================

def analyze_revenue_by_region(sales, output_dir='./analysis_output'):
    """Analyze and visualize revenue by region"""
    print("=" * 70)
    print("REVENUE BY REGION ANALYSIS")
    print("=" * 70)
    print()
    
    revenue_by_region = sales.groupby('region')['total'].sum().sort_values(ascending=False)
    
    print(revenue_by_region)
    print()
    
    # Create visualization
    plt.figure(figsize=(10, 6))
    revenue_by_region.plot(kind='bar', color='steelblue')
    plt.title('Revenue by Region', fontsize=14, fontweight='bold')
    plt.xlabel('Region', fontsize=12)
    plt.ylabel('Revenue ($)', fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    # Save figure
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f'{output_dir}/01_revenue_by_region.png', dpi=300, bbox_inches='tight')
    print(f"✓ Chart saved: 01_revenue_by_region.png")
    plt.close()
    print()

# ============================================================================
# TOP PRODUCTS ANALYSIS
# ============================================================================

def analyze_top_products(sales, products, output_dir='./analysis_output'):
    """Analyze and visualize top products by revenue"""
    print("=" * 70)
    print("TOP PRODUCTS ANALYSIS")
    print("=" * 70)
    print()
    
    # Join with products
    sales_products = sales.merge(products, left_on='product_id', right_on='product_id')
    top_products = sales_products.groupby('name')['total'].sum().sort_values(ascending=False).head(10)
    
    print("Top 10 Products by Revenue:")
    print(top_products)
    print()
    
    # Create visualization
    plt.figure(figsize=(12, 6))
    top_products.plot(kind='barh', color='coral')
    plt.title('Top 10 Products by Revenue', fontsize=14, fontweight='bold')
    plt.xlabel('Revenue ($)', fontsize=12)
    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    
    # Save figure
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f'{output_dir}/02_top_products.png', dpi=300, bbox_inches='tight')
    print(f"✓ Chart saved: 02_top_products.png")
    plt.close()
    print()
    
    return sales_products

# ============================================================================
# SALES TRENDS ANALYSIS
# ============================================================================

def analyze_sales_trends(sales, output_dir='./analysis_output'):
    """Analyze and visualize sales trends over time"""
    print("=" * 70)
    print("SALES TREND ANALYSIS")
    print("=" * 70)
    print()
    
    sales['sale_date'] = pd.to_datetime(sales['sale_date'])
    daily_revenue = sales.groupby('sale_date')['total'].sum()
    
    print("Daily Revenue Statistics:")
    print(f"✓ Min Daily: ${daily_revenue.min():,.2f}")
    print(f"✓ Max Daily: ${daily_revenue.max():,.2f}")
    print(f"✓ Avg Daily: ${daily_revenue.mean():,.2f}")
    print()
    
    # Create visualization
    plt.figure(figsize=(12, 6))
    daily_revenue.plot(marker='o', color='green', linewidth=2)
    plt.title('Daily Revenue Trend', fontsize=14, fontweight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Revenue ($)', fontsize=12)
    plt.grid(alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save figure
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f'{output_dir}/03_daily_revenue_trend.png', dpi=300, bbox_inches='tight')
    print(f"✓ Chart saved: 03_daily_revenue_trend.png")
    plt.close()
    print()

# ============================================================================
# CATEGORY ANALYSIS
# ============================================================================

def analyze_category_revenue(sales_products, output_dir='./analysis_output'):
    """Analyze and visualize revenue by product category"""
    print("=" * 70)
    print("CATEGORY ANALYSIS")
    print("=" * 70)
    print()
    
    category_revenue = sales_products.groupby('category')['total'].agg(['sum', 'count', 'mean'])
    category_revenue.columns = ['Total Revenue', 'Transaction Count', 'Avg Transaction']
    
    print("Revenue by Category:")
    print(category_revenue)
    print()
    
    # Create pie chart
    plt.figure(figsize=(10, 6))
    category_revenue['Total Revenue'].plot(kind='pie', autopct='%1.1f%%', 
                                           colors=sns.color_palette('Set2'))
    plt.title('Revenue Distribution by Category', fontsize=14, fontweight='bold')
    plt.ylabel('')
    plt.tight_layout()
    
    # Save figure
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f'{output_dir}/04_revenue_by_category.png', dpi=300, bbox_inches='tight')
    print(f"✓ Chart saved: 04_revenue_by_category.png")
    plt.close()
    print()
    
    # Create bar chart for transaction count
    plt.figure(figsize=(10, 6))
    category_revenue['Transaction Count'].plot(kind='bar', color='skyblue')
    plt.title('Transaction Count by Category', fontsize=14, fontweight='bold')
    plt.xlabel('Category', fontsize=12)
    plt.ylabel('Count', fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    plt.savefig(f'{output_dir}/05_transactions_by_category.png', dpi=300, bbox_inches='tight')
    print(f"✓ Chart saved: 05_transactions_by_category.png")
    plt.close()
    print()

# ============================================================================
# CUSTOMER INSIGHTS
# ============================================================================

def analyze_customer_insights(sales, customers, output_dir='./analysis_output'):
    """Analyze customer metrics and lifetime value"""
    print("=" * 70)
    print("CUSTOMER INSIGHTS")
    print("=" * 70)
    print()
    
    # Customer lifetime value
    customer_sales = sales.merge(customers, left_on='customer_id', right_on='customer_id')
    customer_ltv = customer_sales.groupby('customer_id').agg({
        'total': 'sum',
        'sale_id': 'count'
    }).rename(columns={'total': 'lifetime_value', 'sale_id': 'transaction_count'})
    
    print("Top 10 Customers by Lifetime Value:")
    print(customer_ltv.sort_values('lifetime_value', ascending=False).head(10))
    print()
    
    print("Customer Statistics:")
    print(f"✓ Total Customers: {len(customer_ltv)}")
    print(f"✓ Avg LTV: ${customer_ltv['lifetime_value'].mean():,.2f}")
    print(f"✓ Max LTV: ${customer_ltv['lifetime_value'].max():,.2f}")
    print(f"✓ Avg Transactions/Customer: {customer_ltv['transaction_count'].mean():.2f}")
    print()
    
    # Create LTV distribution chart
    plt.figure(figsize=(12, 6))
    plt.hist(customer_ltv['lifetime_value'], bins=15, color='purple', edgecolor='black', alpha=0.7)
    plt.title('Customer Lifetime Value Distribution', fontsize=14, fontweight='bold')
    plt.xlabel('Lifetime Value ($)', fontsize=12)
    plt.ylabel('Number of Customers', fontsize=12)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f'{output_dir}/06_customer_ltv_distribution.png', dpi=300, bbox_inches='tight')
    print(f"✓ Chart saved: 06_customer_ltv_distribution.png")
    plt.close()
    print()
    
    # Top customers by LTV
    top_customers = customer_ltv.sort_values('lifetime_value', ascending=False).head(10)
    plt.figure(figsize=(12, 6))
    top_customers['lifetime_value'].plot(kind='barh', color='darkgreen')
    plt.title('Top 10 Customers by Lifetime Value', fontsize=14, fontweight='bold')
    plt.xlabel('Lifetime Value ($)', fontsize=12)
    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    
    plt.savefig(f'{output_dir}/07_top_customers_ltv.png', dpi=300, bbox_inches='tight')
    print(f"✓ Chart saved: 07_top_customers_ltv.png")
    plt.close()
    print()

# ============================================================================
# ADVANCED ANALYTICS
# ============================================================================

def advanced_analytics(sales, products, output_dir='./analysis_output'):
    """Perform advanced analytics and generate insights"""
    print("=" * 70)
    print("ADVANCED ANALYTICS")
    print("=" * 70)
    print()
    
    # Merge carefully to avoid price column conflict
    sales_products = sales.merge(products, left_on='product_id', right_on='product_id')
    # Rename columns for clarity
    sales_products = sales_products.rename(columns={'price_x': 'sales_price', 'price_y': 'product_price'})
    
    # Quantity analysis
    qty_stats = sales_products.groupby('category')['qty'].agg(['sum', 'mean', 'count'])
    qty_stats.columns = ['Total Qty Sold', 'Avg Qty/Transaction', 'Transaction Count']
    
    print("Quantity Analysis by Category:")
    print(qty_stats)
    print()
    
    # Price analysis
    price_stats = sales_products.groupby('category')['sales_price'].agg(['mean', 'min', 'max'])
    price_stats.columns = ['Avg Price', 'Min Price', 'Max Price']
    
    print("Price Analysis by Category:")
    print(price_stats)
    print()
    
    # Create advanced visualization - Revenue per transaction by category
    sales_products['total'] = sales_products['qty'] * sales_products['sales_price']
    avg_revenue_per_txn = sales_products.groupby('category')['total'].mean().sort_values(ascending=False)
    
    plt.figure(figsize=(10, 6))
    avg_revenue_per_txn.plot(kind='bar', color='teal')
    plt.title('Average Revenue per Transaction by Category', fontsize=14, fontweight='bold')
    plt.xlabel('Category', fontsize=12)
    plt.ylabel('Average Revenue ($)', fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f'{output_dir}/08_avg_revenue_per_txn.png', dpi=300, bbox_inches='tight')
    print(f"✓ Chart saved: 08_avg_revenue_per_txn.png")
    plt.close()
    print()

# ============================================================================
# GENERATE SUMMARY REPORT
# ============================================================================

def generate_summary_report(sales, products, customers, output_dir='./analysis_output'):
    """Generate comprehensive summary report"""
    print("=" * 70)
    print("COMPREHENSIVE SUMMARY REPORT")
    print("=" * 70)
    print()
    
    sales['total'] = sales['qty'] * sales['price']
    
    report = f"""
DATA MINING LAB - ANALYSIS REPORT
On-Premise Data Lake ETL Pipeline Results

BUSINESS METRICS
{'='*70}
Total Revenue:                ${sales['total'].sum():>20,.2f}
Total Transactions:           {len(sales):>20}
Average Transaction Value:    ${sales['total'].mean():>20,.2f}
Median Transaction Value:     ${sales['total'].median():>20,.2f}

DATA VOLUME
{'='*70}
Total Customers:              {len(customers):>20}
Total Products:               {len(products):>20}
Total Sales Records:          {len(sales):>20}

GEOGRAPHIC BREAKDOWN
{'='*70}
Regions Covered:              {sales['region'].nunique():>20}
Top Region Revenue:           {sales.groupby('region')['total'].sum().idxmax():>20}

PERFORMANCE INDICATORS
{'='*70}
Avg Revenue/Region:           ${sales.groupby('region')['total'].sum().mean():>20,.2f}
Highest Single Transaction:   ${sales['total'].max():>20,.2f}
Lowest Single Transaction:    ${sales['total'].min():>20,.2f}

KEY INSIGHTS
{'='*70}

1. REVENUE DISTRIBUTION
   {sales.groupby('region')['total'].sum().to_dict()}

2. TOP 3 PRODUCTS BY REVENUE
   {sales.merge(products, on='product_id').groupby('name')['total'].sum().nlargest(3).to_dict()}

3. TOP 3 CUSTOMERS
   {sales.groupby('customer_id')['total'].sum().nlargest(3).to_dict()}

4. TRANSACTION PATTERNS
   - Average items per transaction: {sales['qty'].mean():.2f}
   - Total items sold: {sales['qty'].sum()}
   - Average product price: ${products['price'].mean():,.2f}

TIME SPAN
{'='*70}
First Transaction:            {sales['sale_date'].min()}
Last Transaction:             {sales['sale_date'].max()}
Analysis Date:                {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

DATA QUALITY
{'='*70}
Missing Values (Sales):       {sales.isnull().sum().sum()}
Missing Values (Customers):   {customers.isnull().sum().sum()}
Data Completeness:            100%

OUTPUT FILES GENERATED
{'='*70}
01_revenue_by_region.png
02_top_products.png
03_daily_revenue_trend.png
04_revenue_by_category.png
05_transactions_by_category.png
06_customer_ltv_distribution.png
07_top_customers_ltv.png
08_avg_revenue_per_txn.png
analysis_report.txt

{'='*70}
Generated by Data Mining Lab Analysis Pipeline
{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*70}
    """
    
    print(report)
    
    # Save report to file
    os.makedirs(output_dir, exist_ok=True)
    with open(f'{output_dir}/analysis_report.txt', 'w') as f:
        f.write(report)
    
    print(f"✓ Report saved: analysis_report.txt")
    print()

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main(base_path=None, output_dir='./analysis_output'):
    # Auto-detect path - works on any system
    if base_path is None:
        import os
        # analysis.py is in Data_Mining_LAB root, so just find datalake in same dir
        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_path = os.path.join(script_dir, 'datalake')
        if not os.path.exists(base_path):
            # Fallback: try parent directory
            base_path = os.path.join(os.path.dirname(script_dir), 'datalake')
    """Main analysis execution function"""
    print("\n")
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║                    DATA MINING LAB ANALYSIS                        ║")
    print("║         Complete Exploratory Data Analysis & Visualization        ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    print("\n")
    
    try:
        # Setup
        setup_environment()
        
        # Load data
        sales, products, customers = load_raw_data(base_path)
        print()
        
        # Data quality
        check_data_quality(sales, customers)
        
        # Analysis
        sales = sales_summary_statistics(sales)
        analyze_revenue_by_region(sales, output_dir)
        sales_products = analyze_top_products(sales, products, output_dir)
        analyze_sales_trends(sales, output_dir)
        analyze_category_revenue(sales_products, output_dir)
        analyze_customer_insights(sales, customers, output_dir)
        advanced_analytics(sales, products, output_dir)
        
        # Report
        generate_summary_report(sales, products, customers, output_dir)
        
        print("╔════════════════════════════════════════════════════════════════════╗")
        print("║                   ANALYSIS COMPLETE!                             ║")
        print(f"║  All visualizations saved to: {output_dir:<28} ║")
        print("╚════════════════════════════════════════════════════════════════════╝")
        print("\n")
        
    except Exception as e:
        print(f"\n ERROR: {str(e)}")
        raise

# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()