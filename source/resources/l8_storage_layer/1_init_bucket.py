# Initialize MinIO bucket with mock data
# Supports two formats:  parquet  |  delta
#
# Usage:
#   python initialize_bucket.py parquet
#   python initialize_bucket.py delta

import io
import argparse
import pandas as pd
from minio import Minio

# --- Format choice ---
parser = argparse.ArgumentParser(description="Initialize MinIO bucket with mock data.")
parser.add_argument("--format", choices=["parquet", "delta"], default="parquet")
args = parser.parse_args()

FORMAT = args.format
BUCKET = "datalake" if FORMAT == "parquet" else "lakehouse"
print(f"Format: {FORMAT}  |  Bucket: {BUCKET}")

# --- MinIO connection ---
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

if not client.bucket_exists(BUCKET):
    client.make_bucket(BUCKET)
    print(f"Bucket '{BUCKET}' created.")
else:
    print(f"Bucket '{BUCKET}' already exists.")


def upload_parquet(df: pd.DataFrame, topic: str):
    """Upload a single Parquet file to MinIO."""
    object_path = f"topics/{topic}/data.parquet"
    data = df.to_parquet(index=False)
    client.put_object(BUCKET, object_path, io.BytesIO(data), len(data))
    print(f"  Uploaded  s3://{BUCKET}/{object_path}  ({len(df)} rows)")


def upload_delta(df: pd.DataFrame, topic: str):
    """Write a Delta table directly to MinIO using S3-compatible storage options.

    deltalake writes a folder structure:
        <topic>/
            part-0.parquet      ← actual data
            _delta_log/
                00000.json      ← transaction log (what makes it Delta)
    """
    from deltalake.writer import write_deltalake

    write_deltalake(
        f"s3://{BUCKET}/topics/{topic}",
        df,
        storage_options={
            "endpoint_url": f"http://{MINIO_ENDPOINT}",
            "access_key_id": MINIO_ACCESS_KEY,
            "secret_access_key": MINIO_SECRET_KEY,
            "allow_http": "true",
        },
    )
    print(f"  Uploaded  s3://{BUCKET}/topics/{topic}/  ({len(df)} rows)")


def upload(df: pd.DataFrame, topic: str):
    if FORMAT == "parquet":
        upload_parquet(df, topic)
    else:
        upload_delta(df, topic)


# ── Topic 1: customers ────────────────────────────────────────────────────────
customers = pd.DataFrame({
    "customer_id": [1001, 1002, 1003, 1004, 1005],
    "name":        ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "email":       [
        "alice@example.com",
        "bob@example.com",
        "charlie@example.com",
        "diana@example.com",
        "eve@example.com",
    ],
    "country": ["US", "UK", "US", "DE", "FR"],
})

upload(customers, "customers")

# ── Topic 2: orders ───────────────────────────────────────────────────────────
orders = pd.DataFrame({
    "order_id":    [3001, 3002, 3003, 3004, 3005, 3006],
    "customer_id": [1001, 1002, 1001, 1003, 1005, 1002],
    "product":     ["Laptop", "Phone", "Mouse", "Keyboard", "Monitor", "Tablet"],
    "amount":      [999.99, 599.99, 29.99, 79.99, 349.99, 449.99],
    "order_date":  pd.to_datetime([
        "2024-01-10", "2024-01-11", "2024-01-12",
        "2024-01-13", "2024-01-14", "2024-01-15",
    ]),
})

upload(orders, "orders")

# ── Topic 3: events (clickstream) ────────────────────────────────────────────
events = pd.DataFrame({
    "event_id":    list(range(1, 8)),
    "customer_id": [1001, 1003, 1002, 1001, 1004, 1005, 1003],
    "event_type":  ["page_view", "add_to_cart", "purchase", "page_view",
                    "add_to_cart", "purchase", "page_view"],
    "page":        ["/home", "/product/1", "/checkout", "/product/2",
                    "/product/3", "/checkout", "/home"],
    "timestamp":   pd.to_datetime([
        "2024-01-10 10:00", "2024-01-11 11:30", "2024-01-11 12:00",
        "2024-01-12 09:00", "2024-01-13 14:00", "2024-01-14 16:00",
        "2024-01-15 08:00",
    ]),
})

upload(events, "events")

print("\nDone. Objects in bucket:")
for obj in client.list_objects(BUCKET, prefix="topics/", recursive=True):
    print(f"  {obj.object_name}")
