"""Acceso S3 (Moto local) con la misma API que Amazon S3 (boto3)."""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from src import config


def client():
    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT_URL,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        region_name=config.S3_REGION,
    )


def ensure_bucket() -> None:
    s3 = client()
    try:
        s3.head_bucket(Bucket=config.S3_BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=config.S3_BUCKET)


def put_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    client().put_object(
        Bucket=config.S3_BUCKET,
        Key=key,
        Body=data,
        ContentType=content_type,
        ServerSideEncryption="AES256",
    )


def put_text(key: str, text: str, content_type: str = "text/plain") -> None:
    put_bytes(key, text.encode("utf-8"), content_type)


def get_bytes(key: str) -> bytes:
    response = client().get_object(Bucket=config.S3_BUCKET, Key=key)
    return response["Body"].read()


def read_text(key: str) -> str:
    return get_bytes(key).decode("utf-8")


def write_json(key: str, payload: dict) -> None:
    put_text(
        key,
        json.dumps(payload, indent=2, ensure_ascii=False),
        "application/json",
    )


def read_json(key: str) -> dict:
    return json.loads(read_text(key))


def upload_file(local_path: Path, key: str, content_type: str = "application/octet-stream") -> None:
    client().upload_file(
        Filename=str(local_path),
        Bucket=config.S3_BUCKET,
        Key=key,
        ExtraArgs={
            "ContentType": content_type,
            "ServerSideEncryption": "AES256",
        },
    )


def put_parquet(key: str, dataframe: pd.DataFrame) -> str:
    buffer = io.BytesIO()
    dataframe.to_parquet(buffer, index=False, engine="pyarrow")
    payload = buffer.getvalue()
    put_bytes(key, payload, "application/octet-stream")
    return hashlib.sha256(payload).hexdigest()


def get_parquet(key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(get_bytes(key)), engine="pyarrow")


def object_exists(key: str) -> bool:
    try:
        client().head_object(Bucket=config.S3_BUCKET, Key=key)
        return True
    except ClientError:
        return False


def list_keys(prefix: str) -> list[str]:
    paginator = client().get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    return sorted(keys)


def raw_historical_key(ingest_date: str, batch_id: str) -> str:
    return (
        f"raw/source={config.HISTORICAL_SOURCE}/ingest_date={ingest_date}/"
        f"batch_id={batch_id}/orders.csv"
    )


def raw_events_key(ingest_date: str, batch_id: str) -> str:
    return (
        f"raw/source={config.EVENTS_SOURCE}/ingest_date={ingest_date}/"
        f"batch_id={batch_id}/events.jsonl"
    )


def raw_catalog_key(ingest_date: str, batch_id: str, version: int) -> str:
    return (
        f"raw/source={config.CATALOG_SOURCE}/ingest_date={ingest_date}/"
        f"batch_id={batch_id}/catalog_v{version}.json"
    )


def layer_key(layer: str, source: str, ingest_date: str, batch_id: str, filename: str) -> str:
    return (
        f"{layer}/source={source}/ingest_date={ingest_date}/"
        f"batch_id={batch_id}/{filename}"
    )


def curated_key(table: str, ingest_date: str, batch_id: str) -> str:
    return (
        f"curated/table={table}/ingest_date={ingest_date}/"
        f"batch_id={batch_id}/{table}.parquet"
    )
