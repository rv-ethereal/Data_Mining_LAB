"""
Sample Data Generator for Data Lake Project
Generates realistic sales, products, and customer datasets
"""

import pandas as pd
import json
from faker import Faker
from datetime import datetime, timedelta
import random
import os

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# Configuration
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 100
NUM_SALES = 10000
OUTPUT_DIR = "datalake/raw"

def generate_customers(num_customers):
    """Generate customer dataset"""
    print(f"Generating {num_customers} customers...")
    
    customers = []
    for i in range(1, num_customers + 1):
        customer = {
            'customer_id': f'CUST{i:05d}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state(),
            'zip_code': fake.zipcode(),
            'country': 'USA',
            'registration_date': fake.date_between(start_date='-3y', end_date='today').strftime('%Y-%m-%d'),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
            'age': random.randint(18, 75),
            'gender': random.choice(['M', 'F', 'Other'])
        }
        customers.append(customer)
    
    return pd.DataFrame(customers)

def generate_products(num_products):
    """Generate product catalog as JSON"""
    print(f"Generating {num_products} products...")
    
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Food & Beverage']
    products = []
    
    for i in range(1, num_products + 1):
        category = random.choice(categories)
        product = {
            'product_id': f'PROD{i:04d}',
            'product_name': f'{fake.word().title()} {category[:-1] if category.endswith("s") else category}',
            'category': category,
            'subcategory': fake.word().title(),
            'brand': fake.company(),
            'unit_price': round(random.uniform(5.99, 999.99), 2),
            'cost_price': round(random.uniform(3.00, 500.00), 2),
            'stock_quantity': random.randint(0, 500),
            'supplier': fake.company(),
            'description': fake.text(max_nb_chars=200),
            'weight_kg': round(random.uniform(0.1, 50.0), 2),
            'dimensions': f'{random.randint(5,50)}x{random.randint(5,50)}x{random.randint(5,50)} cm',
            'launch_date': fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
        }
        products.append(product)
    
    return products

def generate_sales(num_sales, customers_df, products_list):
    """Generate sales transactions"""
    print(f"Generating {num_sales} sales transactions...")
    
    sales = []
    start_date = datetime.now() - timedelta(days=730)  # 2 years of data
    
    for i in range(1, num_sales + 1):
        customer = customers_df.sample(1).iloc[0]
        product = random.choice(products_list)
        
        transaction_date = start_date + timedelta(days=random.randint(0, 730))
        quantity = random.randint(1, 10)
        unit_price = product['unit_price']
        discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20])  # Most sales have no discount
        
        subtotal = quantity * unit_price
        discount_amount = subtotal * (discount_pct / 100)
        total_amount = subtotal - discount_amount
        
        sale = {
            'transaction_id': f'TXN{i:07d}',
            'transaction_date': transaction_date.strftime('%Y-%m-%d'),
            'transaction_time': f'{random.randint(8,21):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}',
            'customer_id': customer['customer_id'],
            'product_id': product['product_id'],
            'quantity': quantity,
            'unit_price': unit_price,
            'discount_percent': discount_pct,
            'discount_amount': round(discount_amount, 2),
            'subtotal': round(subtotal, 2),
            'tax_amount': round(total_amount * 0.08, 2),  # 8% tax
            'total_amount': round(total_amount * 1.08, 2),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Gift Card']),
            'shipping_method': random.choice(['Standard', 'Express', 'Overnight', 'Pickup']),
            'order_status': random.choice(['Completed', 'Completed', 'Completed', 'Pending', 'Cancelled', 'Returned']),
            'store_location': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Online'])
        }
        sales.append(sale)
    
    return pd.DataFrame(sales)

def main():
    """Main function to generate all datasets"""
    print("=" * 60)
    print("Data Lake Sample Data Generator")
    print("=" * 60)
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Generate datasets
    customers_df = generate_customers(NUM_CUSTOMERS)
    products_list = generate_products(NUM_PRODUCTS)
    sales_df = generate_sales(NUM_SALES, customers_df, products_list)
    
    # Save customers as CSV
    customers_file = os.path.join(OUTPUT_DIR, 'customers.csv')
    customers_df.to_csv(customers_file, index=False)
    print(f"\n✓ Saved customers to: {customers_file}")
    print(f"  Rows: {len(customers_df)}, Columns: {len(customers_df.columns)}")
    
    # Save products as JSON
    products_file = os.path.join(OUTPUT_DIR, 'products.json')
    with open(products_file, 'w') as f:
        json.dump(products_list, f, indent=2)
    print(f"\n✓ Saved products to: {products_file}")
    print(f"  Products: {len(products_list)}")
    
    # Save sales as CSV
    sales_file = os.path.join(OUTPUT_DIR, 'sales.csv')
    sales_df.to_csv(sales_file, index=False)
    print(f"\n✓ Saved sales to: {sales_file}")
    print(f"  Rows: {len(sales_df)}, Columns: {len(sales_df.columns)}")
    
    # Print summary statistics
    print("\n" + "=" * 60)
    print("Data Generation Summary")
    print("=" * 60)
    print(f"Total Customers: {len(customers_df)}")
    print(f"Total Products: {len(products_list)}")
    print(f"Total Sales Transactions: {len(sales_df)}")
    print(f"Date Range: {sales_df['transaction_date'].min()} to {sales_df['transaction_date'].max()}")
    print(f"Total Revenue: ${sales_df['total_amount'].sum():,.2f}")
    print(f"Average Order Value: ${sales_df['total_amount'].mean():,.2f}")
    print("=" * 60)
    print("\nData generation completed successfully!")

if __name__ == "__main__":
    main()
