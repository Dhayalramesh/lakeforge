# LakeForge

**Live demo:** https://lakeforge-ojez2dypajaozkidu4cylz.streamlit.app

An AWS-pattern data lake pipeline: raw order events land in an S3 data
lake, a Glue-style job validates and transforms them into partitioned
Parquet, and a Redshift-style star-schema warehouse is bulk-loaded on
top for analytics — with a Streamlit dashboard to run and explore it.

Built to close a specific gap: hands-on experience with the AWS
big-data stack (S3, Glue, Redshift, partitioned Parquet data lakes) a
data-engineering role actually screens for, beyond general SQL/ETL
familiarity.

## Why this runs with no AWS account

The pipeline runs against a **moto-mocked S3 bucket** — a real in-memory
implementation of the S3 API, not a stub. Every line of `s3_client.py`,
`glue_job.py` and `run_pipeline.py` uses the real `boto3` client; the
only thing `moto` does is intercept the AWS calls so they're served
locally instead of hitting a real account. To point this at real AWS:
drop the `mock_aws()` context manager in `run_pipeline.py`, set
`AWS_PROFILE`/region in the environment, and nothing else changes.

The warehouse side uses **DuckDB** as a local stand-in for Redshift —
a real columnar SQL engine with the same dimensional-modeling and SQL
semantics. `warehouse_load.py`'s `load_warehouse()` builds the same
star schema a Redshift `COPY` + dimensional model would; swapping the
DuckDB connection for `redshift_connector`/`psycopg2` is a config
change, not a rewrite.

## Architecture
raw/dt=YYYY-MM-DD/orders.json (S3 data lake, raw zone)
|
v Glue-style ETL job (glue_job.py)
| - validates every record through a DQ gate (dq_checks.py)
| - derives order_total, writes partitioned Parquet
| - idempotent: skips partitions already processed
v
processed/dt=YYYY-MM-DD/orders.parquet (S3 data lake, processed zone)
|
v Warehouse load (warehouse_load.py)
v
fact_orders / dim_customers / dim_products (DuckDB, Redshift-style star schema)
|
v
Streamlit dashboard (app.py) + analytical SQL queries

## What it does

- **S3 data lake with raw/processed zones**, partitioned by date (`dt=`)
  the same way a Glue crawler or Athena table would expect.
- **Glue-style ingestion job**: reads raw newline-delimited JSON,
  validates every record through a data-quality gate, derives
  `order_total`, and writes clean partitioned Parquet.
- **Data-quality gate**: rejects records with missing customer IDs,
  invalid quantities, or non-numeric prices, and quarantines an entire
  partition if more than 10% of it is malformed — a few bad rows get
  dropped, a mostly-broken batch stops the pipeline instead of silently
  loading garbage.
- **Idempotent by design**: re-running the ingestion job on data
  already processed skips those partitions rather than duplicating
  rows — verified by both `run_pipeline.py`'s step 4 and a dedicated
  test.
- **Redshift-style star-schema warehouse**: `fact_orders` plus
  `dim_customers` and `dim_products`, bulk-loaded from the processed
  Parquet zone, queryable with standard analytical SQL (joins,
  aggregates, window-style rollups).
- **Streamlit dashboard**: run the pipeline, inspect DQ results per
  partition, explore the warehouse with preset or custom SQL, and see
  the idempotency guarantee explained.

## Running it

```bash
pip install -r requirements.txt
PYTHONPATH=. python run_pipeline.py     # full pipeline, printed to the terminal
streamlit run app.py                    # interactive dashboard
PYTHONPATH=. pytest tests/ -v           # 14 tests, including moto-backed integration tests
```

## Design notes

- **The pipeline code is AWS-shaped, not AWS-flavored.** `s3_client.py`
  calls the real `boto3` S3 API; `glue_job.py` reads/writes the same
  raw/processed Parquet layout a real Glue job would. The only thing
  standing between this and production is swapping `moto`'s mock for
  real credentials.
- **Idempotency is tested, not just claimed.**
  `test_glue_job_is_idempotent` runs the ingestion job twice against
  the same bucket and asserts the second run reprocesses zero
  partitions.
- **The DQ gate is tested against every defect type it's meant to
  catch** (missing customer ID, invalid quantity, non-numeric price,
  missing fields) plus its pass/fail threshold behavior at exactly the
  10% boundary.
- **End-to-end row-count reconciliation**: one test generates raw data
  with a known malformed-record rate, runs the full pipeline, and
  asserts the warehouse's final row count equals raw events minus
  rejected records — catching any silent data loss between zones.

  
