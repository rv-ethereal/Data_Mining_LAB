import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style="whitegrid")  # Professional chart styling

# --------------------------------------
#  LOAD DATA
# --------------------------------------
sales_fact = pd.read_parquet("datalake/warehouse/sales_fact")

print("\nSales Fact Loaded:")
print(sales_fact.head())

# Convert total_amount to numeric
sales_fact["total_amount"] = pd.to_numeric(sales_fact["total_amount"], errors="coerce")

# --------------------------------------
# 1. TOTAL REVENUE
# --------------------------------------
total_revenue = sales_fact["total_amount"].sum()
print("\nTotal Revenue:", total_revenue)

# --------------------------------------
# 2. TOP 10 CITIES BY REVENUE
# --------------------------------------
city_sales = (
    sales_fact.groupby("city")["total_amount"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

plt.figure(figsize=(10, 5))
sns.barplot(x=city_sales.index, y=city_sales.values, palette="Blues_d")
plt.title("Top 10 Cities by Revenue")
plt.xticks(rotation=45, ha="right")
plt.ylabel("Revenue")
plt.tight_layout()
plt.show()

# --------------------------------------
# 3. REVENUE BY CATEGORY (if valid)
# --------------------------------------
if "category" in sales_fact.columns and sales_fact["category"].notna().any():

    category_rev = (
        sales_fact.dropna(subset=["category"])
        .groupby("category")["total_amount"]
        .sum()
        .sort_values(ascending=False)
    )

    plt.figure(figsize=(8, 5))
    sns.barplot(
        x=category_rev.index,
        y=category_rev.values,
        palette="Greens_d"
    )
    plt.title("Revenue by Product Category")
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Revenue")
    plt.tight_layout()
    plt.show()

else:
    print("\nSkipping category chart (no category data).")

# --------------------------------------
# 4. AGE DISTRIBUTION
# --------------------------------------
sales_fact["age"] = pd.to_numeric(sales_fact["age"], errors="coerce")

plt.figure(figsize=(8, 4))
sns.histplot(sales_fact["age"].dropna(), bins=10, kde=True, color="purple")
plt.title("Customer Age Distribution")
plt.xlabel("Age")
plt.ylabel("Count")
plt.tight_layout()
plt.show()

# --------------------------------------
# 5. SALES TREND BY DATE (New & Professional)
# --------------------------------------
sales_fact["sale_date"] = pd.to_datetime(sales_fact["sale_date"], errors="coerce")

daily_sales = (
    sales_fact.groupby("sale_date")["total_amount"]
    .sum()
    .reset_index()
)

plt.figure(figsize=(10, 4))
sns.lineplot(data=daily_sales, x="sale_date", y="total_amount", marker="o")
plt.title("Daily Sales Trend")
plt.xlabel("Date")
plt.ylabel("Revenue")
plt.tight_layout()
plt.show()

# --------------------------------------
# 6. SAVE SUMMARY REPORT
# --------------------------------------
summary = {
    "Total Revenue": [total_revenue],
    "Total Sales Count": [len(sales_fact)],
    "Unique Customers": [sales_fact['customer_id'].nunique()],
    "Unique Products": [sales_fact['product_id'].nunique()]
}

pd.DataFrame(summary).to_csv("dashboard_summary.csv", index=False)
print("\nDashboard Summary Saved → dashboard_summary.csv")
