import pandas as pd
from sqlalchemy import create_engine

# SQLite database
sqlite_db = r"C:/Users/91874/Downloads/onprem-datalake/datalake/warehouse.db"
sqlite_engine = create_engine(f"sqlite:///{sqlite_db}")

# PostgreSQL database
pg_engine = create_engine("postgresql+psycopg2://rohit:rohit@localhost:5432/warehouse")

# Get all tables in SQLite
tables = sqlite_engine.table_names()

for table in tables:
    df = pd.read_sql_table(table, sqlite_engine)
    df.to_sql(table, pg_engine, if_exists='replace', index=False)
    print(f"Table {table} migrated successfully!")
