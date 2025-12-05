"""
Extract data from SQLite warehouse and create JSON for dashboard visualization
"""
import sqlite3
import json
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent.parent / "warehouse" / "datawarehouse.db"
OUTPUT_DIR = Path(__file__).parent

def get_connection():
    """Create database connection"""
    return sqlite3.connect(DB_PATH)

def extract_revenue_by_month():
    """Extract monthly revenue data"""
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        year || '-' || printf('%02d', month) as month_label,
        year,
        month,
        SUM(total_revenue) as revenue,
        COUNT(*) as transactions
    FROM fact_sales
    GROUP BY year, month
    ORDER BY year, month
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    return [{
        'month': row[0],
        'year': row[1],
        'month_num': row[2],
        'revenue': round(row[3], 2),
        'transactions': row[4]
    } for row in rows]

def extract_sales_trend():
    """Extract daily sales trend"""
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        sale_date,
        SUM(total_revenue) as revenue,
        COUNT(*) as transactions
    FROM fact_sales
    GROUP BY sale_date
    ORDER BY sale_date
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    return [{
        'date': row[0],
        'revenue': round(row[1], 2),
        'transactions': row[2]
    } for row in rows]

def extract_sales_by_region():
    """Extract sales by customer region"""
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        c.customer_region,
        SUM(f.total_revenue) as revenue,
        COUNT(DISTINCT f.customer_id) as customers,
        COUNT(*) as transactions
    FROM fact_sales f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    GROUP BY c.customer_region
    ORDER BY revenue DESC
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    return [{
        'region': row[0],
        'revenue': round(row[1], 2),
        'customers': row[2],
        'transactions': row[3]
    } for row in rows]

def extract_top_products():
    """Extract top 10 products by revenue"""
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        p.product_name,
        p.product_category,
        SUM(f.total_revenue) as revenue,
        SUM(f.quantity) as units_sold,
        COUNT(*) as transactions
    FROM fact_sales f
    JOIN dim_products p ON f.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY revenue DESC
    LIMIT 10
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    return [{
        'product': row[0],
        'category': row[1],
        'revenue': round(row[2], 2),
        'units': row[3],
        'transactions': row[4]
    } for row in rows]

def extract_customers_by_city():
    """Extract customer distribution by region (simulating city data)"""
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        customer_region,
        COUNT(DISTINCT customer_id) as customer_count
    FROM dim_customers
    GROUP BY customer_region
    ORDER BY customer_count DESC
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    return [{
        'city': row[0],  # Using region as city
        'customers': row[1]
    } for row in rows]

def calculate_returns_percentage():
    """Calculate returns percentage (simulated based on low-quantity transactions)"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Total transactions
    cursor.execute("SELECT COUNT(*) FROM fact_sales")
    total_transactions = cursor.fetchone()[0]
    
    # Simulated returns: transactions with very low revenue (potentially returns/refunds)
    # In a real scenario, you'd have a returns table
    cursor.execute("""
        SELECT COUNT(*) 
        FROM fact_sales 
        WHERE total_revenue < (SELECT AVG(total_revenue) * 0.3 FROM fact_sales)
    """)
    potential_returns = cursor.fetchone()[0]
    
    # Get monthly breakdown
    cursor.execute("""
        SELECT 
            year || '-' || printf('%02d', month) as month_label,
            COUNT(*) as total,
            SUM(CASE WHEN total_revenue < (SELECT AVG(total_revenue) * 0.3 FROM fact_sales) THEN 1 ELSE 0 END) as returns
        FROM fact_sales
        GROUP BY year, month
        ORDER BY year, month
    """)
    rows = cursor.fetchall()
    conn.close()
    
    monthly_data = [{
        'month': row[0],
        'total': row[1],
        'returns': row[2],
        'percentage': round((row[2] / row[1] * 100), 2) if row[1] > 0 else 0
    } for row in rows]
    
    overall_percentage = round((potential_returns / total_transactions * 100), 2) if total_transactions > 0 else 0
    
    return {
        'overall_percentage': overall_percentage,
        'total_transactions': total_transactions,
        'returns_count': potential_returns,
        'monthly_data': monthly_data
    }

def extract_summary_stats():
    """Extract summary statistics for dashboard"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Total revenue
    cursor.execute("SELECT SUM(total_revenue) FROM fact_sales")
    total_revenue = cursor.fetchone()[0]
    
    # Total transactions
    cursor.execute("SELECT COUNT(*) FROM fact_sales")
    total_transactions = cursor.fetchone()[0]
    
    # Total customers
    cursor.execute("SELECT COUNT(*) FROM dim_customers")
    total_customers = cursor.fetchone()[0]
    
    # Total products
    cursor.execute("SELECT COUNT(*) FROM dim_products")
    total_products = cursor.fetchone()[0]
    
    # Average transaction value
    avg_transaction = total_revenue / total_transactions if total_transactions > 0 else 0
    
    conn.close()
    
    return {
        'total_revenue': round(total_revenue, 2),
        'total_transactions': total_transactions,
        'total_customers': total_customers,
        'total_products': total_products,
        'avg_transaction': round(avg_transaction, 2)
    }

def main():
    """Extract all dashboard data and save to JSON"""
    print("Extracting dashboard data from warehouse...")
    
    # Create output directory if it doesn't exist
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Extract all data
    dashboard_data = {
        'revenue_by_month': extract_revenue_by_month(),
        'sales_trend': extract_sales_trend(),
        'sales_by_region': extract_sales_by_region(),
        'top_products': extract_top_products(),
        'customers_by_city': extract_customers_by_city(),
        'returns_data': calculate_returns_percentage(),
        'summary_stats': extract_summary_stats()
    }
    
    # Save to JSON file
    output_file = OUTPUT_DIR / 'dashboard_data.json'
    with open(output_file, 'w') as f:
        json.dump(dashboard_data, f, indent=2)
    
    print(f"✅ Dashboard data extracted successfully!")
    print(f"📊 Output: {output_file}")
    print(f"\nSummary:")
    print(f"  - Total Revenue: ${dashboard_data['summary_stats']['total_revenue']:,.2f}")
    print(f"  - Total Transactions: {dashboard_data['summary_stats']['total_transactions']:,}")
    print(f"  - Total Customers: {dashboard_data['summary_stats']['total_customers']:,}")
    print(f"  - Total Products: {dashboard_data['summary_stats']['total_products']:,}")
    print(f"  - Returns Percentage: {dashboard_data['returns_data']['overall_percentage']}%")

if __name__ == "__main__":
    main()
