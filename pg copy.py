import pandas as pd
import psycopg2
from sqlalchemy import create_engine

data = {
    "products": pd.read_csv("products.csv"),
    "orders": pd.read_csv("orders.csv"),
}
for table, df in data.items():
    conn = psycopg2.connect(
        host="localhost", dbname="postgres", user="postgres", password="", port=5432
    )
    cur = conn.cursor()
    print(f"Drop table {table}")
    cur.execute(f"DROP TABLE IF EXISTS {table};")
    print(f"Create table {table}")
    with open(f"problem-0/{table}.sql") as f:
        sql = "".join(f.readlines())
        assert sql!="", "Found empty SQL, please make sure that you fill in table schema in products.sql or orders.sql files."
        print(sql)
        cur.execute(sql)
    conn.commit()
    conn.close()
    print(f"Import data to table {table}")
    conn_str = "postgresql://postgres@localhost:5432/postgres"
    eng = create_engine(conn_str)
    df.to_sql(name=table, con=eng, if_exists="append", chunksize=1000, index=False)
    df = pd.read_sql(f"select count(1) as cnt from {table}", con=eng)
    print(f"Imported {df['cnt'][0]} rows")
