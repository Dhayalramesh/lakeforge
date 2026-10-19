"""
Integration tests against a moto-mocked S3 bucket: the whole pipeline
(land raw data -> Glue-style ingest -> warehouse load) is exercised the
same way it would run against real AWS, including the properties that
matter most in a production data pipeline -- idempotency, a working DQ
gate, and a warehouse that loads correctly.
"""
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from lakeforge import s3_client
from lakeforge.raw_events import generate_raw_batches
from lakeforge.glue_job import run_glue_job
from lakeforge.warehouse_load import load_warehouse, query

BUCKET = "test-lakeforge-bucket"


@pytest.fixture
def s3_bucket():
    with mock_aws():
        s3 = s3_client.get_client()
        s3_client.ensure_bucket(s3, BUCKET)
        yield s3


def test_raw_batches_land_in_s3(s3_bucket):
    batches = generate_raw_batches(n_days=3, orders_per_day=20)
    for key, body in batches.items():
        s3_client.put_object(s3_bucket, BUCKET, key, body)

    keys = s3_client.list_keys(s3_bucket, BUCKET, "raw/")
    assert len(keys) == 3
    assert all(k.endswith("orders.json") for k in keys)


def test_glue_job_produces_valid_partitioned_parquet(s3_bucket):
    batches = generate_raw_batches(n_days=2, orders_per_day=30, malformed_rate=0.0)
    for key, body in batches.items():
        s3_client.put_object(s3_bucket, BUCKET, key, body)

    summary = run_glue_job(s3_bucket, BUCKET)
    assert len(summary["partitions_processed"]) == 2

    processed_keys = s3_client.list_keys(s3_bucket, BUCKET, "processed/")
    assert len(processed_keys) == 2
    for key in processed_keys:
        buf = s3_client.get_parquet_bytes(s3_bucket, BUCKET, key)
        table = pq.read_table(buf)
        assert table.num_rows == 30
        assert "order_total" in table.column_names


def test_glue_job_quarantines_records_via_dq_gate(s3_bucket):
    batches = generate_raw_batches(n_days=1, orders_per_day=50, malformed_rate=0.05)
    for key, body in batches.items():
        s3_client.put_object(s3_bucket, BUCKET, key, body)

    summary = run_glue_job(s3_bucket, BUCKET)
    dq = list(summary["dq_results"].values())[0]
    assert dq.rejected_records > 0
    assert dq.passed  # 5% malformed rate stays under the 10% gate


def test_glue_job_is_idempotent(s3_bucket):
    batches = generate_raw_batches(n_days=3, orders_per_day=20)
    for key, body in batches.items():
        s3_client.put_object(s3_bucket, BUCKET, key, body)

    first_run = run_glue_job(s3_bucket, BUCKET)
    assert len(first_run["partitions_processed"]) == 3

    second_run = run_glue_job(s3_bucket, BUCKET)
    assert len(second_run["partitions_processed"]) == 0
    assert len(second_run["partitions_skipped"]) == 3


def test_warehouse_loads_star_schema(tmp_path, s3_bucket):
    batches = generate_raw_batches(n_days=4, orders_per_day=40, malformed_rate=0.0)
    for key, body in batches.items():
        s3_client.put_object(s3_bucket, BUCKET, key, body)
    run_glue_job(s3_bucket, BUCKET)

    db_path = str(tmp_path / "warehouse.duckdb")
    counts = load_warehouse(s3_bucket, BUCKET, db_path=db_path)

    assert counts["fact_orders"] == 160
    assert counts["dim_products"] == 8
    assert counts["dim_customers"] > 0


def test_warehouse_analytical_query_returns_sane_results(tmp_path, s3_bucket):
    batches = generate_raw_batches(n_days=5, orders_per_day=40, malformed_rate=0.0)
    for key, body in batches.items():
        s3_client.put_object(s3_bucket, BUCKET, key, body)
    run_glue_job(s3_bucket, BUCKET)

    db_path = str(tmp_path / "warehouse.duckdb")
    load_warehouse(s3_bucket, BUCKET, db_path=db_path)

    df = query(db_path, """
        SELECT category, SUM(order_total) AS revenue
        FROM fact_orders f JOIN dim_products p ON f.product_id = p.product_id
        GROUP BY category
    """)
    assert len(df) > 0
    assert (df["revenue"] > 0).all()


def test_end_to_end_row_count_matches_valid_records(tmp_path, s3_bucket):
    batches = generate_raw_batches(n_days=7, orders_per_day=80, malformed_rate=0.03)
    total_raw = 0
    for key, body in batches.items():
        s3_client.put_object(s3_bucket, BUCKET, key, body)
        total_raw += len(body.splitlines())

    summary = run_glue_job(s3_bucket, BUCKET)
    total_rejected = sum(dq.rejected_records for dq in summary["dq_results"].values())

    db_path = str(tmp_path / "warehouse.duckdb")
    counts = load_warehouse(s3_bucket, BUCKET, db_path=db_path)

    assert counts["fact_orders"] == total_raw - total_rejected
