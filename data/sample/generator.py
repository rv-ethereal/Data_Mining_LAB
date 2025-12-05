"""
Sample E-Commerce Data Generator
Generates realistic sales data for the lakehouse demo
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Set random seed for reproducibility
np.random.seed(42)

# Output directory
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# Generate Customers
print("Generating customers...")
num_customers = 10000

customers = pd.DataFrame({
    'customer_id': range(1, num_customers + 1),
    'customer_name': [f"Customer_{i}" for i in range(1, num_customers + 1)],
    'email': [f"customer{i}@example.com" for i in range(1, num_customers + 1)],
    'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 
                               'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'], num_customers),
    'signup_date': [datetime(2023, 1, 1) + timedelta(days=int(x)) for x in np.random.randint(0, 365, num_customers)]
})

# Generate Products
print("Generating products...")
num_products = 1000

categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Beauty', 'Toys', 'Books', 'Food']
products = pd.DataFrame({
    'product_id': range(1, num_products + 1),
    'product_name': [f"Product_{i}" for i in range(1, num_products + 1)],
   'category': np.random.choice(categories, num_products),
    'price': np.random.uniform(10, 1000, num_products).round(2)
})

# Generate Orders
print("Generating orders...")
num_orders = 50000

orders = pd.DataFrame({
    'order_id': range(1, num_orders + 1),
    'customer_id': np.random.randint(1, num_customers + 1, num_orders),
    'order_date': [datetime(2024, 1, 1) + timedelta(days=int(x)) for x in np.random.randint(0, 300, num_orders)],
    'status': np.random.choice(['completed', 'pending', 'cancelled'], num_orders, p=[0.8, 0.15, 0.05])
})

# Generate Order Items
print("Generating order items...")
order_items_list = []

for order_id in range(1, num_orders + 1):
    num_items = np.random.randint(1, 6)  # 1-5 items per order
    for _ in range(num_items):
        product_id = np.random.randint(1, num_products + 1)
        quantity = np.random.randint(1, 4)
        price = products[products['product_id'] == product_id]['price'].values[0]
        
        order_items_list.append({
            'order_item_id': len(order_items_list) + 1,
            'order_id': order_id,
            'product_id': product_id,
            'quantity': quantity,
            'unit_price': price,
            'total_price': round(price * quantity, 2)
        })

order_items = pd.DataFrame(order_items_list)

# Save to CSV
print("\nSaving CSV files...")
customers.to_csv(os.path.join(OUTPUT_DIR, 'customers.csv'), index=False)
products.to_csv(os.path.join(OUTPUT_DIR, 'products.csv'), index=False)
orders.to_csv(os.path.join(OUTPUT_DIR, 'orders.csv'), index=False)
order_items.to_csv(os.path.join(OUTPUT_DIR, 'order_items.csv'), index=False)

print(f"\n[SUCCESS] Data generation complete!")
print(f"Customers: {len(customers):,}")
print(f"Products: {len(products):,}")
print(f"Orders: {len(orders):,}")
print(f"Order Items: {len(order_items):,}")
print(f"\nFiles created in: {OUTPUT_DIR}")