import pandas as pd
import numpy as np
import os

def generate_student_data(num_records=50000):
    np.random.seed(42)

    customer_ids = [f'STUD_{i:06d}' for i in range(1, num_records + 1)]
    genders = np.random.choice(['Male', 'Female'], num_records, p=[0.45, 0.55])
    ages = np.random.normal(21, 3, num_records)   
    ages = np.clip(ages, 18, 30).astype(int)

    annual_income = np.random.normal(50, 20, num_records)
    annual_income = np.clip(annual_income, 10, 120).astype(int)

    spending_score = np.random.normal(50, 25, num_records)
    spending_score = np.clip(spending_score, 1, 100).astype(int)

    df = pd.DataFrame({
        'CustomerID': customer_ids,
        'Gender': genders,
        'Age': ages,
        'Annual Income': annual_income,
        'Spending Score': spending_score
    })

    return df


def save_data():
    print("Creating 50,000 student customer records...")
    df = generate_student_data(50000)

    os.makedirs('./data', exist_ok=True)
    file_path = './data/students.csv'
    df.to_csv(file_path, index=False)

    print(f"Data saved to {file_path}")
    print(f"Dataset shape: {df.shape}")
    print("\nFirst 5 rows:")
    print(df.head())

    return df


if __name__ == "__main__":
    save_data()