"""
The Glue-style ETL job: reads raw newline-delimited JSON from the S3
"raw/" prefix, runs it through the data-quality gate, derives
order_total, and writes the clean result as partitioned Parquet to the
"processed/" prefix -- the same layout an AWS Glue job or EMR Spark job
would produce for a downstream Redshift COPY or Athena query.

Idempotent by design: a partition (one day) that has already been
written to processed/ is skipped on re-run, the way a real incremental
Glue job avoids reprocessing a day it already landed.
"""
import io

import pyarrow as pa
import pyarrow.parquet as pq

from lakeforge import s3_client
from lakeforge.dq_checks import validate_records


def _records_to_parquet_bytes(records: list[dict]) -> bytes:
    table = pa.table({
        "order_id": [r["order_id"] for r in records],
        "customer_id": [r["customer_id"] for r in records],
        "product_id": [r["product_id"] for r in records],
        "quantity": [r["quantity"] for r in records],
        "unit_price": [float(r["unit_price"]) for r in records],
        "order_total": [r["quantity"] * float(r["unit_price"]) for r in records],
        "order_ts": [r["order_ts"] for r in records],
        "status": [r["status"] for r in records],
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _partition_from_raw_key(raw_key: str) -> str:
    # raw/dt=2026-09-01/orders.json -> dt=2026-09-01
    parts = raw_key.split("/")
    return next(p for p in parts if p.startswith("dt="))


def run_glue_job(s3, bucket: str, raw_prefix: str = "raw/", processed_prefix: str = "processed/") -> dict:
    """
    Processes every raw partition not already present under processed/.
    Returns a summary dict: partitions processed, records in/out, and the
    DQ result for each partition -- the kind of run report a real Glue
    job would emit to CloudWatch.
    """
    raw_keys = [k for k in s3_client.list_keys(s3, bucket, raw_prefix) if k.endswith(".json")]
    existing_processed = set(s3_client.list_keys(s3, bucket, processed_prefix))

    summary = {"partitions_processed": [], "partitions_skipped": [], "dq_results": {}}

    for raw_key in raw_keys:
        partition = _partition_from_raw_key(raw_key)
        out_key = f"{processed_prefix}{partition}/orders.parquet"

        if out_key in existing_processed:
            summary["partitions_skipped"].append(partition)
            continue

        records = s3_client.get_object_json_lines(s3, bucket, raw_key)
        valid_records, dq_result = validate_records(records)
        summary["dq_results"][partition] = dq_result

        if not dq_result.passed:
            # Quarantine: don't write a partition that failed the DQ gate.
            summary.setdefault("partitions_quarantined", []).append(partition)
            continue

        if valid_records:
            parquet_bytes = _records_to_parquet_bytes(valid_records)
            s3_client.put_parquet_bytes(s3, bucket, out_key, parquet_bytes)
            summary["partitions_processed"].append(partition)

    return summary
