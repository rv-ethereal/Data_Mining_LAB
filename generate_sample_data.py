"""
Generate sample data for the data engineering pipeline
"""
import pandas as pd
import json
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
random.seed(42)

# Get the base path
base_path = os.path.dirname(__file__)
raw_path = os.path.join(base_path, 'datalake', 'raw')

# Generate sample products
products = []
categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports', 'Toys']
suppliers = ['Supplier A', 'Supplier B', 'Supplier C', 'Supplier D']

for i in range(1, 51):  # 50 products
    products.append({
        'product_id': f'P{i:03d}',
        'name': f'Product {i}',
        'category': random.choice(categories),
        'supplier': random.choice(suppliers),
        'cost': round(random.uniform(10, 500), 2)
    })

# Save products as JSON
with open(os.path.join(raw_path, 'products.json'), 'w') as f:
    json.dump(products, f, indent=2)

print(f"✓ Generated {len(products)} products")

# Generate sample customers
customers_data = []
regions = ['North', 'South', 'East', 'West', 'Central']
start_date = datetime(2022, 1, 1)

for i in range(1, 101):  # 100 customers
    join_date = start_date + timedelta(days=random.randint(0, 730))
    customers_data.append({
        'customer_id': f'C{i:04d}',
        'name': f'Customer {i}',
        'email': f'customer{i}@example.com',
        'region': random.choice(regions),
        'join_date': join_date.strftime('%Y-%m-%d')
    })

# Save customers as CSV
customers_df = pd.DataFrame(customers_data)
customers_df.to_csv(os.path.join(raw_path, 'customers.csv'), index=False)

print(f"✓ Generated {len(customers_data)} customers")

# Generate sample sales
sales_data = []
start_sale_date = datetime(2023, 1, 1)
end_sale_date = datetime(2024, 12, 31)

for i in range(1, 1001):  # 1000 sales transactions
    sale_date = start_sale_date + timedelta(
        days=random.randint(0, (end_sale_date - start_sale_date).days)
    )
    product = random.choice(products)
    
    sales_data.append({
        'sale_id': f'S{i:05d}',
        'product_id': product['product_id'],
        'customer_id': random.choice(customers_data)['customer_id'],
        'quantity': random.randint(1, 10),
        'unit_price': round(product['cost'] * random.uniform(1.2, 2.0), 2),  # Markup
        'sale_date': sale_date.strftime('%Y-%m-%d')
    })

# Save sales as CSV
sales_df = pd.DataFrame(sales_data)
sales_df.to_csv(os.path.join(raw_path, 'sales.csv'), index=False)

print(f"✓ Generated {len(sales_data)} sales transactions")
print(f"\n✓ All sample data files created successfully in {raw_path}")
