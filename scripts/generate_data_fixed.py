import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import json
import os
import yaml

with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

fake = Faker()
np.random.seed(42)

def generate_customers(num_customers=1000):
    print(f"Generating {num_customers} customer records...")
    customers = []
    for i in range(1, num_customers + 1):
        customer = {
            'customer_id': f'CUST{i:06d}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state(),
            'country': 'India',
            'postal_code': fake.postcode(),
            'registration_date': fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d'),
            'customer_segment': np.random.choice(['Premium', 'Regular', 'Basic'], p=[0.2, 0.5, 0.3])
        }
        customers.append(customer)
    
    df = pd.DataFrame(customers)
    output_path = os.path.join(config['paths']['raw'], 'customers.csv')
    df.to_csv(output_path, index=False)
    print(f"✓ Customers saved to {output_path}")
    return df

def generate_products(num_products=100):
    print(f"Generating {num_products} product records...")
    categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Sports', 'Beauty']
    
    output_path = os.path.join(config['paths']['raw'], 'products.json')
    
    # Write as newline-delimited JSON (one JSON object per line)
    with open(output_path, 'w') as f:
        for i in range(1, num_products + 1):
            product = {
                'product_id': f'PROD{i:05d}',
                'product_name': fake.catch_phrase(),
                'category': np.random.choice(categories),
                'price': round(np.random.uniform(10, 1000), 2),
                'cost': round(np.random.uniform(5, 500), 2),
                'brand': fake.company(),
                'stock_quantity': int(np.random.randint(0, 500)),
                'rating': round(np.random.uniform(1, 5), 1),
                'description': fake.text(max_nb_chars=100)
            }
            f.write(json.dumps(product) + '\n')
    
    print(f"✓ Products saved to {output_path}")
    return pd.DataFrame([json.loads(line) for line in open(output_path)])

def generate_sales(num_records=10000, customers_df=None, products_df=None):
    print(f"Generating {num_records} sales records...")
    
    if customers_df is None or products_df is None:
        raise ValueError("Customer and Product dataframes required")
    
    sales = []
    start_date = datetime.now() - timedelta(days=365)
    
    for i in range(1, num_records + 1):
        customer_id = np.random.choice(customers_df['customer_id'])
        product_id = np.random.choice(products_df['product_id'])
        product_price = products_df[products_df['product_id'] == product_id]['price'].values[0]
        
        quantity = int(np.random.randint(1, 10))
        discount = round(np.random.choice([0, 0.05, 0.1, 0.15, 0.2], p=[0.6, 0.15, 0.15, 0.05, 0.05]), 2)
        
        transaction_date = start_date + timedelta(days=int(np.random.randint(0, 365)))
        
        sale = {
            'order_id': f'ORD{i:08d}',
            'customer_id': customer_id,
            'product_id': product_id,
            'quantity': quantity,
            'price': product_price,
            'discount': discount,
            'transaction_date': transaction_date.strftime('%Y-%m-%d'),
            'transaction_time': fake.time(),
            'payment_method': np.random.choice(['Credit Card', 'Debit Card', 'UPI', 'Cash'], p=[0.4, 0.3, 0.25, 0.05]),
            'status': np.random.choice(['Completed', 'Pending', 'Cancelled'], p=[0.85, 0.10, 0.05])
        }
        sales.append(sale)
    
    df = pd.DataFrame(sales)
    null_indices = np.random.choice(df.index, size=int(len(df) * 0.02), replace=False)
    df.loc[null_indices, 'discount'] = np.nan
    
    output_path = os.path.join(config['paths']['raw'], 'sales.csv')
    df.to_csv(output_path, index=False)
    print(f"✓ Sales saved to {output_path}")
    return df

def main():
    print("="*60)
    print("Starting Data Generation for Data Lake")
    print("="*60)
    
    customers_df = generate_customers(config['data_generation']['num_customers'])
    products_df = generate_products(config['data_generation']['num_products'])
    sales_df = generate_sales(
        config['data_generation']['num_sales_records'],
        customers_df,
        products_df
    )
    
    print("\n" + "="*60)
    print("Data Generation Complete!")
    print("="*60)
    print(f"\nDatasets created in: {config['paths']['raw']}")
    print(f"  - customers.csv: {len(customers_df)} records")
    print(f"  - products.json: {len(products_df)} records")
    print(f"  - sales.csv: {len(sales_df)} records")
    print("\n✓ Ready for ETL processing!")

if __name__ == "__main__":
    main()
