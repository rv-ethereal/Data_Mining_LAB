"""
Dashboard Generator - Creates HTML dashboard from warehouse data
"""

import pandas as pd
import json
import os
from pathlib import Path

print("=" * 80)
print("GENERATING ANALYTICS DASHBOARD")
print("=" * 80)
print()

# Read warehouse data
print("Loading data from warehouse...")
fact_sales = pd.read_parquet('datalake/warehouse/fact_sales/fact_sales.parquet')
agg_monthly = pd.read_parquet('datalake/warehouse/agg_monthly_revenue/agg_monthly.parquet')
agg_products = pd.read_parquet('datalake/warehouse/agg_top_products/agg_products.parquet')
agg_regions = pd.read_parquet('datalake/warehouse/agg_regional_sales/agg_regions.parquet')

print(f"✓ Loaded {len(fact_sales):,} sales records")
print(f"✓ Loaded {len(agg_monthly)} monthly aggregations")
print(f"✓ Loaded {len(agg_products)} product records")
print(f"✓ Loaded {len(agg_regions)} regional records")
print()

# Prepare data for charts
print("Preparing chart data...")

# Monthly revenue data
monthly_data = agg_monthly.sort_values(['year', 'month'])
monthly_labels = [f"{int(row['year'])}-{int(row['month']):02d}" for _, row in monthly_data.iterrows()]
monthly_revenue = monthly_data['total_revenue'].tolist()

# Top 10 products
top_products = agg_products.head(10)
product_names = top_products['product_name'].tolist()
product_revenue = top_products['total_revenue'].tolist()

# Top 10 regions
top_regions = agg_regions.head(10)
region_labels = [f"{row['city']}, {row['state']}" for _, row in top_regions.iterrows()]
region_revenue = top_regions['total_revenue'].tolist()

# Customer segments
completed_sales = fact_sales[fact_sales['order_status'] == 'Completed']
segment_data = completed_sales.groupby('customer_segment')['total_amount'].sum().sort_values(ascending=False)
segment_labels = segment_data.index.tolist()
segment_values = segment_data.values.tolist()

# Category performance
category_data = completed_sales.groupby('category')['total_amount'].sum().sort_values(ascending=False)
category_labels = category_data.index.tolist()
category_values = category_data.values.tolist()

# KPIs
total_revenue = completed_sales['total_amount'].sum()
avg_order_value = completed_sales['total_amount'].mean()
total_orders = len(completed_sales)
total_customers = completed_sales['customer_id'].nunique()

print("✓ Chart data prepared")
print()

# Generate HTML dashboard
print("Generating HTML dashboard...")

html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sales Analytics Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }}
        
        .dashboard {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        .header {{
            text-align: center;
            color: white;
            margin-bottom: 30px;
        }}
        
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}
        
        .header p {{
            font-size: 1.1em;
            opacity: 0.9;
        }}
        
        .kpi-container {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .kpi-card {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            transition: transform 0.3s ease;
        }}
        
        .kpi-card:hover {{
            transform: translateY(-5px);
        }}
        
        .kpi-label {{
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }}
        
        .kpi-value {{
            color: #667eea;
            font-size: 2.5em;
            font-weight: bold;
        }}
        
        .kpi-subtitle {{
            color: #999;
            font-size: 0.85em;
            margin-top: 5px;
        }}
        
        .charts-container {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }}
        
        .chart-card {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }}
        
        .chart-card h3 {{
            color: #333;
            margin-bottom: 20px;
            font-size: 1.3em;
        }}
        
        .chart-wrapper {{
            position: relative;
            height: 300px;
        }}
        
        .full-width {{
            grid-column: 1 / -1;
        }}
        
        .footer {{
            text-align: center;
            color: white;
            margin-top: 30px;
            padding: 20px;
            opacity: 0.8;
        }}
        
        @media (max-width: 768px) {{
            .charts-container {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="dashboard">
        <div class="header">
            <h1>📊 Sales Analytics Dashboard</h1>
            <p>On-Premise Data Lake - Real-time Insights</p>
        </div>
        
        <div class="kpi-container">
            <div class="kpi-card">
                <div class="kpi-label">Total Revenue</div>
                <div class="kpi-value">${total_revenue:,.0f}</div>
                <div class="kpi-subtitle">Completed Orders</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Average Order Value</div>
                <div class="kpi-value">${avg_order_value:,.2f}</div>
                <div class="kpi-subtitle">Per Transaction</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Total Orders</div>
                <div class="kpi-value">{total_orders:,}</div>
                <div class="kpi-subtitle">Completed</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Unique Customers</div>
                <div class="kpi-value">{total_customers:,}</div>
                <div class="kpi-subtitle">Active Buyers</div>
            </div>
        </div>
        
        <div class="charts-container">
            <div class="chart-card full-width">
                <h3>📈 Revenue Trend (Monthly)</h3>
                <div class="chart-wrapper">
                    <canvas id="revenueChart"></canvas>
                </div>
            </div>
            
            <div class="chart-card">
                <h3>🏆 Top 10 Products</h3>
                <div class="chart-wrapper">
                    <canvas id="productsChart"></canvas>
                </div>
            </div>
            
            <div class="chart-card">
                <h3>🗺️ Top 10 Regions</h3>
                <div class="chart-wrapper">
                    <canvas id="regionsChart"></canvas>
                </div>
            </div>
            
            <div class="chart-card">
                <h3>👥 Customer Segments</h3>
                <div class="chart-wrapper">
                    <canvas id="segmentsChart"></canvas>
                </div>
            </div>
            
            <div class="chart-card">
                <h3>📦 Product Categories</h3>
                <div class="chart-wrapper">
                    <canvas id="categoriesChart"></canvas>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>Generated from Data Lake Warehouse | {len(fact_sales):,} total records processed</p>
            <p>Last Updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
    
    <script>
        // Chart.js default configuration
        Chart.defaults.font.family = "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif";
        Chart.defaults.color = '#666';
        
        // Revenue Trend Chart
        new Chart(document.getElementById('revenueChart'), {{
            type: 'line',
            data: {{
                labels: {json.dumps(monthly_labels)},
                datasets: [{{
                    label: 'Monthly Revenue',
                    data: {json.dumps(monthly_revenue)},
                    borderColor: '#667eea',
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        display: false
                    }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return '$' + context.parsed.y.toLocaleString();
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{
                                return '$' + value.toLocaleString();
                            }}
                        }}
                    }}
                }}
            }}
        }});
        
        // Top Products Chart
        new Chart(document.getElementById('productsChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(product_names)},
                datasets: [{{
                    label: 'Revenue',
                    data: {json.dumps(product_revenue)},
                    backgroundColor: 'rgba(102, 126, 234, 0.8)',
                    borderColor: '#667eea',
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {{
                    legend: {{
                        display: false
                    }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return '$' + context.parsed.x.toLocaleString();
                            }}
                        }}
                    }}
                }},
                scales: {{
                    x: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{
                                return '$' + value.toLocaleString();
                            }}
                        }}
                    }}
                }}
            }}
        }});
        
        // Top Regions Chart
        new Chart(document.getElementById('regionsChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(region_labels)},
                datasets: [{{
                    label: 'Revenue',
                    data: {json.dumps(region_revenue)},
                    backgroundColor: 'rgba(118, 75, 162, 0.8)',
                    borderColor: '#764ba2',
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {{
                    legend: {{
                        display: false
                    }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return '$' + context.parsed.x.toLocaleString();
                            }}
                        }}
                    }}
                }},
                scales: {{
                    x: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{
                                return '$' + value.toLocaleString();
                            }}
                        }}
                    }}
                }}
            }}
        }});
        
        // Customer Segments Chart
        new Chart(document.getElementById('segmentsChart'), {{
            type: 'doughnut',
            data: {{
                labels: {json.dumps(segment_labels)},
                datasets: [{{
                    data: {json.dumps(segment_values)},
                    backgroundColor: [
                        'rgba(102, 126, 234, 0.8)',
                        'rgba(118, 75, 162, 0.8)',
                        'rgba(237, 100, 166, 0.8)'
                    ],
                    borderWidth: 2,
                    borderColor: '#fff'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        position: 'bottom'
                    }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return context.label + ': $' + context.parsed.toLocaleString();
                            }}
                        }}
                    }}
                }}
            }}
        }});
        
        // Categories Chart
        new Chart(document.getElementById('categoriesChart'), {{
            type: 'pie',
            data: {{
                labels: {json.dumps(category_labels)},
                datasets: [{{
                    data: {json.dumps(category_values)},
                    backgroundColor: [
                        'rgba(102, 126, 234, 0.8)',
                        'rgba(118, 75, 162, 0.8)',
                        'rgba(237, 100, 166, 0.8)',
                        'rgba(52, 211, 153, 0.8)',
                        'rgba(251, 191, 36, 0.8)',
                        'rgba(239, 68, 68, 0.8)',
                        'rgba(59, 130, 246, 0.8)'
                    ],
                    borderWidth: 2,
                    borderColor: '#fff'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        position: 'bottom'
                    }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return context.label + ': $' + context.parsed.toLocaleString();
                            }}
                        }}
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
"""

# Save dashboard
dashboard_path = 'dashboard.html'
with open(dashboard_path, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"✓ Dashboard generated: {dashboard_path}")
print()
print("=" * 80)
print("DASHBOARD GENERATION COMPLETE")
print("=" * 80)
print()
print(f"Open the dashboard: {os.path.abspath(dashboard_path)}")
print("Or double-click the file to open in your browser")
print()
