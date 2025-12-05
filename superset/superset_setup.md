# Apache Superset Setup Guide

## Overview
This guide walks through setting up Apache Superset locally to visualize data from the SQLite warehouse.

## Prerequisites
- Python 3.8 or higher
- SQLite database created by running `warehouse/load_data.py`

## Installation Steps

### 1. Install Superset
```bash
# Install Superset (already in requirements.txt)
pip install apache-superset==3.1.0
```

### 2. Initialize Superset
```bash
# Set the FLASK_APP environment variable
export FLASK_APP=superset  # On Windows: set FLASK_APP=superset

# Initialize the database
superset db upgrade

# Create an admin user
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password admin

# Load example data (optional)
# superset load_examples

# Initialize roles and permissions
superset init
```

### 3. Start Superset
```bash
# Start the development web server
superset run -p 8088 --with-threads --reload --debugger
```

Access Superset at: **http://localhost:8088**

Login with:
- **Username**: admin
- **Password**: admin

## Connecting to the Warehouse

### 1. Add Database Connection

1. Click on **Settings** (gear icon) → **Database Connections**
2. Click **+ Database**
3. Select **SQLite**
4. Enter connection details:
   - **Display Name**: `Data Warehouse`
   - **SQLAlchemy URI**: `sqlite:///C:/Users/vives/OneDrive/Desktop/data lake ( data mining )/Data_Mining_LAB/data-engineering-pipeline/warehouse/datawarehouse.db`
   
   *(Adjust the path to match your actual database location)*

5. Click **Test Connection**
6. Click **Connect**

### 2. Add Datasets

After connecting the database, add the three tables as datasets:

1. Go to **Data** → **Datasets**
2. Click **+ Dataset**
3. Select:
   - **Database**: Data Warehouse
   - **Schema**: main
   - **Table**: fact_sales
4. Click **Add**
5. Repeat for `dim_products` and `dim_customers`

## Creating Dashboards

### Dashboard 1: Sales by Product

1. Go to **Charts** → **+ Chart**
2. Select dataset: `fact_sales`
3. Choose visualization: **Bar Chart**
4. Configure:
   - **Dimensions**: Join with `dim_products`, use `product_category`
   - **Metrics**: `SUM(total_revenue)`
   - **Sort by**: `SUM(total_revenue)` DESC
5. **Save** as "Revenue by Product Category"

### Dashboard 2: Sales by Customer Region

1. Create new chart
2. Dataset: `fact_sales`
3. Visualization: **Pie Chart**
4. Configure:
   - Join with `dim_customers`
   - **Dimensions**: `customer_region`
   - **Metrics**: `SUM(total_revenue)`
5. **Save** as "Revenue by Region"

### Dashboard 3: Daily Revenue Trend

1. Create new chart
2. Dataset: `fact_sales`
3. Visualization: **Time Series Line Chart**
4. Configure:
   - **Time Column**: `sale_date`
   - **Metrics**: `SUM(total_revenue)`
   - **Time Grain**: Day
5. **Save** as "Daily Revenue Trend"

### Combine into Dashboard

1. Go to **Dashboards** → **+ Dashboard**
2. Name it "Sales Analytics Dashboard"
3. Drag and drop the three charts onto the dashboard
4. Arrange and resize as needed
5. **Save**

## Sample SQL Queries in Superset

You can also create charts using custom SQL:

### Top 10 Products
```sql
SELECT 
    p.product_name,
    SUM(f.total_revenue) as revenue
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 10
```

### Monthly Revenue Trend
```sql
SELECT 
    year || '-' || printf('%02d', month) as month,
    SUM(total_revenue) as revenue
FROM fact_sales
GROUP BY year, month
ORDER BY year, month
```

## Tips

- **Performance**: For large datasets, use aggregations and filters
- **Caching**: Enable caching in Superset settings for better performance
- **Security**: Change default admin password in production
- **Refresh**: Datasets can be refreshed from the **Data** tab

## Troubleshooting

### Issue: Cannot connect to database
- Verify the database file path is correct
- Check that `datawarehouse.db` exists
- Ensure SQLite is properly installed

### Issue: No data showing
- Run `warehouse/load_data.py` to populate the database
- Refresh the dataset in Superset
- Check SQL Lab to verify data exists

### Issue: Permission errors
- Initialize superset with `superset init`
- Restart Superset server

## Next Steps

1. Explore **SQL Lab** for ad-hoc queries
2. Set up **email reports** for scheduled dashboard delivery
3. Create **custom filters** for interactive dashboards
4. Add **calculated metrics** for advanced analytics
