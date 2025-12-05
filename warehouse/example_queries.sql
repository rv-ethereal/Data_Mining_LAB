-- Example Analytical Queries for the Data Warehouse

-- 1. Top 10 Products by Revenue
SELECT 
    p.product_name,
    p.product_category,
    SUM(f.total_revenue) as total_revenue,
    COUNT(*) as transaction_count,
    SUM(f.quantity) as units_sold
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.product_category
ORDER BY total_revenue DESC
LIMIT 10;

-- 2. Top 10 Customers by Lifetime Value
SELECT 
    c.customer_name,
    c.customer_region,
    c.customer_email,
    SUM(f.total_revenue) as lifetime_value,
    COUNT(*) as purchase_count,
    AVG(f.total_revenue) as avg_purchase_value
FROM fact_sales f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_region, c.customer_email
ORDER BY lifetime_value DESC
LIMIT 10;

-- 3. Daily Revenue Trend
SELECT 
    sale_date,
    SUM(total_revenue) as daily_revenue,
    COUNT(*) as transaction_count,
    AVG(total_revenue) as avg_transaction_value
FROM fact_sales
GROUP BY sale_date
ORDER BY sale_date;

-- 4. Monthly Revenue by Year
SELECT 
    year,
    month,
    SUM(total_revenue) as monthly_revenue,
    COUNT(*) as transaction_count
FROM fact_sales
GROUP BY year, month
ORDER BY year, month;

-- 5. Revenue by Product Category
SELECT 
    p.product_category,
    SUM(f.total_revenue) as category_revenue,
    COUNT(DISTINCT f.product_id) as product_count,
    SUM(f.quantity) as units_sold,
    AVG(f.unit_price) as avg_unit_price
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_category
ORDER BY category_revenue DESC;

-- 6. Revenue by Customer Region
SELECT 
    c.customer_region,
    SUM(f.total_revenue) as regional_revenue,
    COUNT(DISTINCT f.customer_id) as customer_count,
    COUNT(*) as transaction_count,
    AVG(f.total_revenue) as avg_transaction_value
FROM fact_sales f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_region
ORDER BY regional_revenue DESC;

-- 7. Best Selling Products by Quantity
SELECT 
    p.product_name,
    p.product_category,
    SUM(f.quantity) as total_quantity_sold,
    SUM(f.total_revenue) as total_revenue,
    COUNT(*) as transaction_count
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.product_category
ORDER BY total_quantity_sold DESC
LIMIT 10;

-- 8. Customer Purchase Frequency
SELECT 
    c.customer_name,
    c.customer_region,
    COUNT(*) as purchase_count,
    SUM(f.total_revenue) as lifetime_value,
    MIN(f.sale_date) as first_purchase,
    MAX(f.sale_date) as last_purchase,
    JULIANDAY(MAX(f.sale_date)) - JULIANDAY(MIN(f.sale_date)) as days_active
FROM fact_sales f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_region
HAVING purchase_count > 1
ORDER BY purchase_count DESC
LIMIT 10;

-- 9. Product Performance by Supplier
SELECT 
    p.product_supplier,
    COUNT(DISTINCT p.product_id) as product_count,
    SUM(f.total_revenue) as total_revenue,
    SUM(f.quantity) as units_sold,
    AVG(f.total_revenue) as avg_sale_value
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_supplier
ORDER BY total_revenue DESC;

-- 10. Year-over-Year Growth
SELECT 
    current.year,
    current.total_revenue as current_year_revenue,
    previous.total_revenue as previous_year_revenue,
    ROUND(((current.total_revenue - previous.total_revenue) / previous.total_revenue) * 100, 2) as yoy_growth_percent
FROM (
    SELECT year, SUM(total_revenue) as total_revenue
    FROM fact_sales
    GROUP BY year
) current
LEFT JOIN (
    SELECT year, SUM(total_revenue) as total_revenue
    FROM fact_sales
    GROUP BY year
) previous ON current.year = previous.year + 1
ORDER BY current.year;
