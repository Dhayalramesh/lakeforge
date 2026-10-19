"""
Thin S3 wrapper so the rest of the pipeline never talks to boto3 directly.
In tests (and in this portfolio build) it talks to a moto-mocked S3
endpoint; in production it is the exact same code against real AWS --
only the environment's credentials and region change, nothing in the
pipeline code does. This is the seam that makes the project deployable
to real AWS without a rewrite.
"""
import io
import json

import boto3


def get_client(region: str = "us-east-1"):
    return boto3.client("s3", region_name=region)


def ensure_bucket(s3, bucket: str, region: str = "us-east-1"):
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )


def put_object(s3, bucket: str, key: str, body: bytes):
    s3.put_object(Bucket=bucket, Key=key, Body=body)


def list_keys(s3, bucket: str, prefix: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def get_object_bytes(s3, bucket: str, key: str) -> bytes:
    resp = s3.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()


def get_object_json_lines(s3, bucket: str, key: str) -> list[dict]:
    body = get_object_bytes(s3, bucket, key).decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def put_parquet_bytes(s3, bucket: str, key: str, parquet_bytes: bytes):
    put_object(s3, bucket, key, parquet_bytes)


def get_parquet_bytes(s3, bucket: str, key: str) -> io.BytesIO:
    return io.BytesIO(get_object_bytes(s3, bucket, key))
