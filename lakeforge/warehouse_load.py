"""
Loads the processed Parquet zone into a star-schema analytical warehouse.
DuckDB stands in locally for Amazon Redshift here -- it is a real
columnar SQL engine with the same dimensional-modeling and SQL semantics
a Redshift cluster would use, so the schema design, the COPY-equivalent
bulk load, and the queries built on top of it transfer directly: in
production this file's DuckDB connection is swapped for a Redshift
connection string (psycopg2/redshift_connector), and the SQL does not
change.
"""
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from lakeforge import s3_client
from lakeforge.raw_events import PRODUCTS


def _load_all_processed_parquet(s3, bucket: str, processed_prefix: str = "processed/") -> pa.Table:
    keys = [k for k in s3_client.list_keys(s3, bucket, processed_prefix) if k.endswith(".parquet")]
    if not keys:
        raise ValueError(f"No processed Parquet files found under s3://{bucket}/{processed_prefix}")

    tables = []
    for key in keys:
        buf = s3_client.get_parquet_bytes(s3, bucket, key)
        tables.append(pq.read_table(buf))
    return pa.concat_tables(tables)


def load_warehouse(s3, bucket: str, db_path: str = ":memory:", processed_prefix: str = "processed/") -> dict:
    """
    Builds a star schema in DuckDB from everything currently in the
    processed zone: fact_orders (the bulk-loaded fact table) plus
    dim_customers and dim_products. Returns row counts for each table,
    the way a load job would report back to an orchestrator.
    """
    fact_table = _load_all_processed_parquet(s3, bucket, processed_prefix)

    con = duckdb.connect(db_path)
    con.register("fact_orders_stage", fact_table)
    con.execute("CREATE OR REPLACE TABLE fact_orders AS SELECT * FROM fact_orders_stage")

    con.execute("""
        CREATE OR REPLACE TABLE dim_customers AS
        SELECT DISTINCT customer_id FROM fact_orders ORDER BY customer_id
    """)

    products_table = pa.table({
        "product_id": [p["product_id"] for p in PRODUCTS],
        "name": [p["name"] for p in PRODUCTS],
        "category": [p["category"] for p in PRODUCTS],
        "catalog_price": [p["price"] for p in PRODUCTS],
    })
    con.register("dim_products_stage", products_table)
    con.execute("CREATE OR REPLACE TABLE dim_products AS SELECT * FROM dim_products_stage")

    counts = {
        "fact_orders": con.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0],
        "dim_customers": con.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0],
        "dim_products": con.execute("SELECT COUNT(*) FROM dim_products").fetchone()[0],
    }
    con.close()
    return counts


def query(db_path: str, sql: str):
    """Run an analytical query against the warehouse -- the local
    equivalent of a Redshift query from a BI tool."""
    con = duckdb.connect(db_path, read_only=True)
    result = con.execute(sql).fetchdf()
    con.close()
    return result
