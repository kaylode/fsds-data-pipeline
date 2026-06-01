#!/usr/bin/env python3
"""
EHR Live Feature Stream → Feast Online Store

Reads pre-aggregated feature records from the 'patient-features-24h' Kafka
topic (produced by 6_flink_stream_processor.py) and pushes them directly
into the Feast Online Store via store.push().

The Flink processor already computed all 24h rolling mean/min/max/std stats
per patient, so this consumer has no aggregation logic — it is a thin bridge
between the Flink output and the Feast online store.

Pipeline:
    Flink → Kafka (patient-features-24h) → [this script] → Feast Online Store

Run:
    uv run python source/scripts/course-work/6_stream_to_online_store.py
"""

import json
import os
import re
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from loguru import logger

try:
    from deltalake import DeltaTable
except ImportError:
    logger.error("deltalake not installed. Run: uv add deltalake")
    raise

try:
    from feast import FeatureStore
except ImportError:
    logger.error("Feast is not installed. Run: uv add feast")
    import sys
    sys.exit(1)

# ── Environment ────────────────────────────────────────────────────────────────
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

KAFKA_PORT        = os.getenv("KAFKA_PORT", "9092")
BOOTSTRAP_SERVERS = f"localhost:{KAFKA_PORT}"
INPUT_TOPIC       = "patient-features-24h"

FEATURE_STORE_DIR = os.path.join(project_root, "config", "feature_store")

MINIO_PORT       = os.getenv("MINIO_PORT",        "9000")
MINIO_HOST       = os.getenv("MINIO_HOST",         "localhost")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER",    "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

os.environ["AWS_ACCESS_KEY_ID"]     = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["AWS_ENDPOINT_URL"]      = f"http://localhost:{MINIO_PORT}"

_ICD10_CHAPTERS = [
    "I","II","III","IV","V","VI","VII","VIII","IX","X","XI",
    "XII","XIII","XIV","XV","XVI","XVII","XVIII","XIX","XX","XXI","XXII",
]

_STORAGE_OPTIONS = {
    "endpoint_url":      f"http://{MINIO_HOST}:{MINIO_PORT}",
    "access_key_id":     MINIO_ACCESS_KEY,
    "secret_access_key": MINIO_SECRET_KEY,
    "allow_http":        "true",
}


def _derive_feature_columns() -> list[str]:
    """
    Build FEATURE_COLUMNS from gold dim tables at startup — mirrors the Flink sink schema.
    Falls back to an empty list if dims aren't built yet (lets the process start anyway).
    """
    def _read(table):
        return DeltaTable(f"s3://lakehouse/topics/{table}", storage_options=_STORAGE_OPTIONS).to_pandas()

    def _to_col(name):
        return name.lower().replace(" ", "_").replace("-", "_")

    try:
        cols = []
        for table in ("dim_vital", "dim_lab"):
            for _, row in _read(table).iterrows():
                prefix = _to_col(row["event_name"])
                cols.extend([f"{prefix}_{s}" for s in ("mean", "min", "max", "std")])
        for _, row in _read("dim_medication").iterrows():
            cols.append(f"{_to_col(row['event_name'])}_count")
        for roman in _ICD10_CHAPTERS:
            cols.append(f"icd_chap_{roman}")
        logger.info(f"Derived {len(cols)} feature columns from gold dim tables.")
        return cols
    except Exception as e:
        logger.warning(f"Could not load dim tables ({e}) — FEATURE_COLUMNS will be empty.")
        return []


FEATURE_COLUMNS = _derive_feature_columns()


def record_to_feast_df(record: dict) -> pd.DataFrame | None:
    """
    Convert a Flink feature record to the DataFrame format expected by store.push().
    Returns None if the record is missing required fields.
    """
    patient_id = record.get("patient_id")
    window_ts  = record.get("window_end_ts")

    if not patient_id or not window_ts:
        logger.warning(f"Skipping record with missing patient_id or window_end_ts: {record}")
        return None

    try:
        event_timestamp = pd.to_datetime(window_ts, utc=True)
    except Exception:
        event_timestamp = pd.Timestamp.now(tz="UTC")

    row = {"patient_id": patient_id, "feature_timestamp": event_timestamp}
    for col in FEATURE_COLUMNS:
        row[col] = record.get(col)   # None if metric absent in window — Feast stores as null

    return pd.DataFrame([row])


def create_topic_if_not_exists():
    from confluent_kafka.admin import AdminClient, NewTopic
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        topics = admin_client.list_topics(timeout=5).topics
        if INPUT_TOPIC not in topics:
            logger.info(f"Creating Kafka topic '{INPUT_TOPIC}'...")
            new_topic = NewTopic(INPUT_TOPIC, num_partitions=1, replication_factor=1)
            fs = admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                f.result()  # Wait for creation
            logger.info(f"Kafka topic '{INPUT_TOPIC}' created successfully.")
        else:
            logger.info(f"Kafka topic '{INPUT_TOPIC}' already exists.")
    except Exception as e:
        logger.error(f"Failed to check/create Kafka topic: {e}")


def main() -> None:
    logger.info("Initialising Feast feature store ...")
    store = FeatureStore(repo_path=FEATURE_STORE_DIR)

    create_topic_if_not_exists()

    consumer = Consumer({
        "bootstrap.servers":  BOOTSTRAP_SERVERS,
        "group.id":           "ehr-feast-online-writer",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([INPUT_TOPIC])
    logger.info(f"Subscribed to '{INPUT_TOPIC}'. Waiting for Flink feature records ...")

    pushed = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse message: {e}")
                continue

            df = record_to_feast_df(record)
            if df is None:
                continue

            # Vitals + labs in one push (single FeatureView with both groups).
            # Feast routes columns to the matching FeatureView by column name.
            try:
                store.push("patient_vitals_stream", df)
                pushed += 1
                if pushed % 100 == 0:
                    logger.info(f"Pushed {pushed} feature records to Feast online store.")
            except Exception as e:
                logger.error(f"Feast push failed for patient {record.get('patient_id')}: {e}")

    except KeyboardInterrupt:
        logger.info(f"Stream consumer stopped. Total records pushed: {pushed}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
