import logging
import traceback
import sys
import json
from superset import db
from superset.models.core import Database
from superset.models.slice import Slice
from superset.models.dashboard import Dashboard
from superset.connectors.sqla.models import SqlaTable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_or_create_db(name, uri):
    database = db.session.query(Database).filter_by(database_name=name).first()
    if not database:
        logger.info(f"Creating database {name}...")
        database = Database(database_name=name, sqlalchemy_uri=uri)
        db.session.add(database)
        db.session.commit()
    return database

def get_or_create_table(name, db_id, schema=None):
    table = db.session.query(SqlaTable).filter_by(table_name=name, database_id=db_id).first()
    if not table:
        logger.info(f"Creating dataset {name}...")
        table = SqlaTable(table_name=name, database_id=db_id, schema=schema)
        try:
            table.fetch_metadata()
            db.session.add(table)
            db.session.commit()
        except Exception as e:
            logger.error(f"Error fetching metadata for {name}: {e}")
    return table

def create_slice(title, viz_type, datasource, params):
    slc = db.session.query(Slice).filter_by(slice_name=title).first()
    if not slc:
        logger.info(f"Creating chart {title}...")
        slc = Slice(
            slice_name=title,
            viz_type=viz_type,
            datasource_type='table',
            datasource_id=datasource.id,
            params=json.dumps(params)
        )
        db.session.add(slc)
        db.session.commit()
    return slc

def create_dashboard(slug, title, slices):
    dash = db.session.query(Dashboard).filter_by(slug=slug).first()
    if not dash:
        logger.info(f"Creating dashboard {title}...")
        dash = Dashboard(
            dashboard_title=title,
            slug=slug,
            slices=slices,
            published=True
        )
        db.session.add(dash)
        db.session.commit()
    return dash

def run_setup():
    try:
        logger.info("Starting Superset setup...")
        
        # 1. Connect to SQLite
        db_uri = 'sqlite:////app/datalake/warehouse.db'
        database = get_or_create_db('OnPrem Data Lake', db_uri)

        # 2. Register Datasets
        revenue_by_product = get_or_create_table('revenue_by_product', database.id)
        revenue_by_region = get_or_create_table('revenue_by_region', database.id)
        monthly_sales = get_or_create_table('monthly_sales', database.id)

        if not revenue_by_product or not revenue_by_region or not monthly_sales:
            logger.error("Failed to create datasets. Aborting chart creation.")
            return

        # 3. Create Charts

        # 3.1 Revenue by Product
        params_product = {
            "viz_type": "echarts_area", 
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_revenue"}, "aggregate": "SUM", "label": "Revenue"}],
            "groupby": ["product"],
            "x_axis": "product"
        }
        chart_product = create_slice("Revenue by Product", "echarts_area", revenue_by_product, params_product)


        # 3.2 Revenue by Region (Pie)
        params_region = {
            "viz_type": "pie",
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_revenue"}, "aggregate": "SUM", "label": "Revenue"}],
            "groupby": ["region"]
        }
        chart_region = create_slice("Revenue by Region", "pie", revenue_by_region, params_region)


        # 3.3 Monthly Sales Trend (Line)
        params_trend = {
            "viz_type": "echarts_timeseries",
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "monthly_revenue"}, "aggregate": "SUM", "label": "Revenue"}],
            "groupby": [],
            "x_axis": "year_month"
        }
        chart_trend = create_slice("Monthly Sales Trend", "echarts_timeseries", monthly_sales, params_trend)

        # 4. Create Dashboard
        create_dashboard('sales_analytics', 'Sales Analytics Dashboard', [chart_product, chart_region, chart_trend])

        logger.info("Setup Complete!")
    except Exception:
        logger.error("Error during setup:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_setup()
