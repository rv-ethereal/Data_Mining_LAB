from superset import db
from superset.models.slice import Slice
from superset.models.dashboard import Dashboard
from superset.connectors.sqla.models import SqlaTable
import json

def create_charts():
    # Fetch tables
    t1 = db.session.query(SqlaTable).filter_by(table_name='revenue_by_product').first()
    t2 = db.session.query(SqlaTable).filter_by(table_name='revenue_by_region').first()
    t3 = db.session.query(SqlaTable).filter_by(table_name='monthly_sales').first()

    if not t1 or not t2 or not t3:
        print("Tables not found!")
        return

    # Charts
    # 1. Product
    params_product = {
        "viz_type": "echarts_area", 
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_revenue"}, "aggregate": "SUM", "label": "Revenue"}],
        "groupby": ["product"],
        "x_axis": "product"
    }
    s1 = Slice(slice_name="Revenue by Product", viz_type="echarts_area", datasource_type='table', datasource_id=t1.id, params=json.dumps(params_product))
    
    # 2. Region
    params_region = {
        "viz_type": "pie",
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_revenue"}, "aggregate": "SUM", "label": "Revenue"}],
        "groupby": ["region"]
    }
    s2 = Slice(slice_name="Revenue by Region", viz_type="pie", datasource_type='table', datasource_id=t2.id, params=json.dumps(params_region))

    # 3. Trend
    params_trend = {
        "viz_type": "echarts_timeseries",
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "monthly_revenue"}, "aggregate": "SUM", "label": "Revenue"}],
        "groupby": [],
        "x_axis": "year_month"
    }
    s3 = Slice(slice_name="Monthly Sales Trend", viz_type="echarts_timeseries", datasource_type='table', datasource_id=t3.id, params=json.dumps(params_trend))

    db.session.add(s1)
    db.session.add(s2)
    db.session.add(s3)
    db.session.commit()

    # Dashboard
    dash = Dashboard(dashboard_title="Sales Analytics Dashboard", slug="sales_analytics", slices=[s1, s2, s3], published=True)
    db.session.add(dash)
    db.session.commit()
    print("Dashboard Created")

create_charts()
