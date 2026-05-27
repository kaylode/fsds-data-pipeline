"""
EHR Bronze Ingestion Script

Ingests synthetic EHR data from the local generation output folder into:
1. MinIO: Writes Delta Lake tables as Delta Lake format under s3://lakehouse/topics/
2. Trino: Registers Delta tables under the delta.bronze schema
"""

import os
import pandas as pd
import trino
from minio import Minio
from loguru import logger
from deltalake.writer import write_deltalake
from dotenv import load_dotenv


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
        "clinical_event_stream": os.path.join(data_dir, "clinical_event_stream.json")
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
    
    # 1. Patients
    logger.info("Writing raw_patients table to S3/Delta...")
    df_patients = pd.read_parquet(paths["patients"])
    df_patients["bronze_ingest_ts"] = pd.Timestamp.now(tz="UTC")
    write_deltalake(
        f"s3://{BUCKET}/topics/raw_patients",
        df_patients,
        storage_options=storage_options,
        mode="overwrite"
    )
    
    # 2. Wards
    logger.info("Writing raw_wards table to S3/Delta...")
    df_wards = pd.read_parquet(paths["wards"])
    df_wards["bronze_ingest_ts"] = pd.Timestamp.now(tz="UTC")
    write_deltalake(
        f"s3://{BUCKET}/topics/raw_wards",
        df_wards,
        storage_options=storage_options,
        mode="overwrite"
    )
    
    # 3. Event Metadata
    logger.info("Writing raw_event_metadata table to S3/Delta...")
    df_meta = pd.read_parquet(paths["event_metadata"])
    df_meta["bronze_ingest_ts"] = pd.Timestamp.now(tz="UTC")
    write_deltalake(
        f"s3://{BUCKET}/topics/raw_event_metadata",
        df_meta,
        storage_options=storage_options,
        mode="overwrite"
    )
    
    # 4. Visits
    logger.info("Writing raw_visits table to S3/Delta...")
    df_visits = pd.read_parquet(paths["visits"])
    df_visits["bronze_ingest_ts"] = pd.Timestamp.now(tz="UTC")
    write_deltalake(
        f"s3://{BUCKET}/topics/raw_visits",
        df_visits,
        storage_options=storage_options,
        mode="overwrite"
    )

    # 5. Events
    logger.info("Writing raw_events table to S3/Delta...")
    df_events = pd.read_parquet(paths["events"])
    df_events["bronze_ingest_ts"] = pd.Timestamp.now(tz="UTC")
    write_deltalake(
        f"s3://{BUCKET}/topics/raw_events",
        df_events,
        storage_options=storage_options,
        mode="overwrite"
    )
    
    logger.info("Successfully wrote all historical EHR Delta tables to MinIO.")


def register_in_trino():
    logger.info("Connecting to Trino to register tables...")
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER
    )
    cursor = conn.cursor()
    
    # Step 1: Create Schema
    logger.info("Creating delta.bronze schema in Trino...")
    cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS delta.bronze 
        WITH (location = 's3://lakehouse/')
    """)
    
    tables = ["raw_patients", "raw_wards", "raw_event_metadata", "raw_visits", "raw_events"]
    
    # Step 2: Register Tables
    for table in tables:
        logger.info(f"Registering Delta table 'delta.bronze.{table}' with Trino...")
        # Drop table projection registry in Trino catalog first if it exists to allow clean re-registration
        cursor.execute(f"DROP TABLE IF EXISTS delta.bronze.{table}")
        
        cursor.execute(f"""
            CALL delta.system.register_table(
                schema_name => 'bronze',
                table_name => '{table}',
                table_location => 's3://{BUCKET}/topics/{table}/'
            )
        """)
        
    cursor.close()
    conn.close()
    logger.info("Successfully registered all historical Delta tables in Trino catalog.")


def main():
    paths = get_data_paths()
    
    # 1. Ingest to MinIO
    try:
        ingest_to_minio(paths)
    except Exception as e:
        logger.error(f"Failed MinIO Ingestion step: {e}")
        
    # 2. Register in Trino
    try:
        register_in_trino()
    except Exception as e:
        logger.error(f"Failed Trino Registration step: {e}")
        
    logger.info("Historical Batch Ingestion process finished.")

if __name__ == "__main__":
    main()
