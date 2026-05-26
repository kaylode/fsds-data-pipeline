#!/usr/bin/env python3
"""
EHR Live Event Stream Consumer
Consumes records from the 'patient-events' Kafka topic and writes them in micro-batches
directly to the 'raw_streams' Delta table in MinIO (Bronze Layer).
"""

import os
import json
import time
import pandas as pd
import trino
import pyarrow as pa
from confluent_kafka import Consumer, KafkaError
from deltalake import write_deltalake
from loguru import logger
from dotenv import load_dotenv

# Load environment variables
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

KAFKA_PORT        = os.getenv("KAFKA_PORT", "9092")
BOOTSTRAP_SERVERS = f"localhost:{KAFKA_PORT}"
TOPIC_NAME        = "patient-events"

MINIO_PORT       = os.getenv("MINIO_PORT", "9000")
MINIO_ENDPOINT   = f"localhost:{MINIO_PORT}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER",    "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET           = "lakehouse"
TABLE_NAME       = "raw_streams"
TABLE_PATH       = f"s3://{BUCKET}/topics/{TABLE_NAME}"

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8090"))
TRINO_USER = os.getenv("TRINO_USER", "trino")

storage_options = {
    "endpoint_url": f"http://{MINIO_ENDPOINT}",
    "access_key_id": MINIO_ACCESS_KEY,
    "secret_access_key": MINIO_SECRET_KEY,
    "allow_http": "true",
}

# Define explicit PyArrow schema to prevent "Invalid data type for Delta Lake: Null"
SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("visit_id", pa.string()),
    ("patient_id", pa.string()),
    ("ward_id", pa.string()),
    ("event_type", pa.string()),
    ("event_type_id", pa.string()),
    ("event_name", pa.string()),
    ("event_timestamp", pa.string()),
    ("created_ts", pa.string()),
    ("text_value", pa.string()),
    ("num_value", pa.float64()),
    ("severity_level", pa.string()),
    ("device_type", pa.string()),
    ("source", pa.string()),
])


def register_table_in_trino():
    """Registers the raw_streams table with Trino if not already done."""
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER
        )
        cursor = conn.cursor()

        cursor.execute("SHOW TABLES FROM delta.bronze")
        tables = [row[0] for row in cursor.fetchall()]

        if TABLE_NAME not in tables:
            logger.info(f"Registering '{TABLE_NAME}' Delta table with Trino...")
            cursor.execute(f"""
                CALL delta.system.register_table(
                    schema_name => 'bronze',
                    table_name => '{TABLE_NAME}',
                    table_location => '{TABLE_PATH}/'
                )
            """)
            logger.info("Table registered in Trino successfully.")

        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to register table in Trino: {e}")


def write_batch_to_delta(batch):
    """Converts batch list to PyArrow Table with strict schema and writes to Delta Lake."""
    df = pd.DataFrame(batch)
    
    # Ensure all schema columns exist in the DataFrame
    for col in SCHEMA.names:
        if col not in df.columns:
            df[col] = None
            
    # Reorder columns to match schema exactly
    df = df[SCHEMA.names]
    
    # Convert to Arrow Table using the schema
    table = pa.Table.from_pandas(df, schema=SCHEMA)
    
    write_deltalake(
        TABLE_PATH,
        table,
        storage_options=storage_options,
        mode="append"
    )


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "ehr-lakehouse-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([TOPIC_NAME])
    logger.info(f"Subscribed to topic '{TOPIC_NAME}'. Starting consumption loop...")

    batch = []
    batch_size_limit = 2000
    last_flush_time = time.time()
    flush_interval_seconds = 2.0

    try:
        while True:
            msg = consumer.poll(timeout=0.5)

            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break

                # Decode and store record
                record = json.loads(msg.value().decode("utf-8"))
                batch.append(record)

            # Check if we should flush the batch to Delta Lake
            current_time = time.time()
            if len(batch) >= batch_size_limit or (len(batch) > 0 and (current_time - last_flush_time) >= flush_interval_seconds):
                logger.info(f"Flushing micro-batch of {len(batch):,} records to Delta table...")

                # Write batch using PyArrow schema
                write_batch_to_delta(batch)

                # Commit Kafka offsets after successful write
                consumer.commit(asynchronous=False)

                # Register table in Trino
                register_table_in_trino()

                logger.info(f"Flushed and committed offsets for {len(batch):,} events.")

                # Reset batch state
                batch = []
                last_flush_time = time.time()

    except KeyboardInterrupt:
        logger.info("Consumption loop interrupted by user.")
    finally:
        # Flush any remaining messages
        if batch:
            logger.info(f"Flushing final batch of {len(batch):,} records...")
            write_batch_to_delta(batch)
            consumer.commit(asynchronous=False)
            register_table_in_trino()
            logger.info("Final batch flushed successfully.")

        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()
