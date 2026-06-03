"""
EHR Bronze Ingestion Script

Ingests synthetic EHR data from the local generation output folder into:
1. MinIO: Writes Delta Lake tables as Delta Lake format under s3://lakehouse/topics/
2. Trino: Registers Delta tables under the delta.bronze schema
"""

import os
import pandas as pd
from minio import Minio
from loguru import logger
from deltalake.writer import write_deltalake
from dotenv import load_dotenv
from utils import register_tables_in_trino


# Load workspace environment variables
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env"))

# MINIO Connection
MINIO_PORT       = os.getenv("MINIO_PORT",          "9000")
MINIO_HOST       = os.getenv("MINIO_HOST",           "localhost")
MINIO_ENDPOINT   = f"{MINIO_HOST}:{MINIO_PORT}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER",     "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD",  "minioadmin")
BUCKET           = "lakehouse"

# TRINO Connection
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8090"))
TRINO_USER = os.getenv("TRINO_USER", "trino")


def get_data_paths():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
    data_dir = os.path.join(project_root, "data", "synthetic")
    return {
        "patients":             os.path.join(data_dir, "patients.parquet"),
        "wards":                os.path.join(data_dir, "wards.parquet"),
        "event_metadata":       os.path.join(data_dir, "event_metadata.parquet"),
        "visits":               os.path.join(data_dir, "visits.parquet"),
        "events":               os.path.join(data_dir, "events.parquet"),
    }



def ingest_to_minio(paths):
    logger.info("Initializing MinIO connection...")
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
        logger.info(f"Created MinIO bucket: '{BUCKET}'")
    else:
        logger.info(f"MinIO bucket '{BUCKET}' already exists.")
        
    storage_options = {
        "endpoint_url": f"http://{MINIO_ENDPOINT}",
        "access_key_id": MINIO_ACCESS_KEY,
        "secret_access_key": MINIO_SECRET_KEY,
        "allow_http": "true",
    }
    
    write_opts = dict(
        storage_options=storage_options,
        mode="overwrite",
        schema_mode="overwrite",
        target_file_size=16 * 1024 * 1024,  # 16 MB per file — avoids IncompleteBody on large single-PUT uploads
    )

    for table_key, path in paths.items():
        table_name = f"raw_{table_key}"
        logger.info(f"Writing {table_name} table to S3/Delta...")
        df = pd.read_parquet(path)
        df["bronze_ingest_ts"] = pd.Timestamp.now(tz="UTC")
        write_deltalake(f"s3://{BUCKET}/topics/{table_name}", df, **write_opts)
    
    logger.info("Successfully wrote all historical EHR Delta tables to MinIO.")


def main():
    paths = get_data_paths()
    
    # 1. Ingest to MinIO
    try:
        ingest_to_minio(paths)
    except Exception as e:
        logger.error(f"Failed MinIO Ingestion step: {e}")
        
    # 2. Register in Trino
    try:
        tables = [f"raw_{key}" for key in paths.keys()]
        register_tables_in_trino(schema_name="bronze", tables=tables)
    except Exception as e:
        logger.error(f"Failed Trino Registration step: {e}")
        
    logger.info("Historical Batch Ingestion process finished.")

if __name__ == "__main__":
    main()
