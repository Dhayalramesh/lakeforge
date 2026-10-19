"""
End-to-end pipeline run: generate raw order events, land them in S3,
run the Glue-style ingestion job (validate + transform + partition),
load the result into a Redshift-style star-schema warehouse, then run a
couple of analytical queries against it -- the same shape of pipeline
a data engineer intern would be asked to build and operate.

This script runs against a moto-mocked S3 bucket so it works anywhere
with no AWS account or credentials required -- exactly what you want
for local development and CI. To point it at real AWS: remove the
`mock_aws()` context manager, make sure AWS credentials/region are
configured in the environment, and nothing else in the pipeline code
changes -- s3_client.py, glue_job.py and warehouse_load.py are already
written against the real boto3/DuckDB APIs.
"""
from moto import mock_aws

from lakeforge import s3_client
from lakeforge.raw_events import generate_raw_batches
from lakeforge.glue_job import run_glue_job
from lakeforge.warehouse_load import load_warehouse, query

BUCKET = "lakeforge-data-lake"
DB_PATH = "warehouse.duckdb"


def main():
    with mock_aws():
        s3 = s3_client.get_client()
        s3_client.ensure_bucket(s3, BUCKET)

        print("=" * 70)
        print("1. LANDING RAW ORDER EVENTS IN S3 (raw/ zone)")
        print("=" * 70)
        batches = generate_raw_batches(n_days=7, orders_per_day=80)
        for key, body in batches.items():
            s3_client.put_object(s3, BUCKET, key, body)
        print(f"Landed {len(batches)} daily batches "
              f"({sum(len(b.splitlines()) for b in batches.values())} raw events) "
              f"under s3://{BUCKET}/raw/")

        print()
        print("=" * 70)
        print("2. RUNNING GLUE-STYLE INGESTION JOB (validate + transform + partition)")
        print("=" * 70)
        summary = run_glue_job(s3, BUCKET)
        print(f"Partitions processed: {len(summary['partitions_processed'])}")
        print(f"Partitions skipped (already processed): {len(summary['partitions_skipped'])}")
        for partition, dq in summary["dq_results"].items():
            print(f"  {partition}: {dq.valid_records}/{dq.total_records} valid "
                  f"({dq.rejection_rate*100:.1f}% rejected) -- reasons: {dq.rejection_reasons}")

        print()
        print("=" * 70)
        print("3. LOADING PROCESSED ZONE INTO STAR-SCHEMA WAREHOUSE (DuckDB, Redshift-style)")
        print("=" * 70)
        counts = load_warehouse(s3, BUCKET, db_path=DB_PATH)
        for table, n in counts.items():
            print(f"  {table}: {n} rows")

        print()
        print("=" * 70)
        print("4. RUNNING IDEMPOTENCY CHECK (re-run the Glue job on the same data)")
        print("=" * 70)
        rerun_summary = run_glue_job(s3, BUCKET)
        print(f"Partitions reprocessed: {len(rerun_summary['partitions_processed'])} "
              f"(expected 0 -- idempotent)")
        print(f"Partitions skipped: {len(rerun_summary['partitions_skipped'])} "
              f"(expected {len(summary['partitions_processed'])})")

        print()
        print("=" * 70)
        print("5. SAMPLE ANALYTICAL QUERIES AGAINST THE WAREHOUSE")
        print("=" * 70)
        revenue_by_category = query(DB_PATH, """
            SELECT p.category, ROUND(SUM(f.order_total), 2) AS revenue, COUNT(*) AS orders
            FROM fact_orders f
            JOIN dim_products p ON f.product_id = p.product_id
            WHERE f.status = 'completed'
            GROUP BY p.category
            ORDER BY revenue DESC
        """)
        print("Revenue by category (completed orders):")
        print(revenue_by_category.to_string(index=False))

        top_customers = query(DB_PATH, """
            SELECT customer_id, ROUND(SUM(order_total), 2) AS total_spend, COUNT(*) AS orders
            FROM fact_orders
            WHERE status = 'completed'
            GROUP BY customer_id
            ORDER BY total_spend DESC
            LIMIT 5
        """)
        print("\nTop 5 customers by spend:")
        print(top_customers.to_string(index=False))


if __name__ == "__main__":
    main()
