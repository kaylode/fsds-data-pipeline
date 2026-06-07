#!/usr/bin/env python3
"""
EHR Feature Store Pipeline — Unified Orchestrator

Keeps the Feast feature store current by running two parallel processes:

  1. feast apply + full materialize      (once at startup)
  2. Kafka → Feast online push           (background thread, continuous)
  3. Every REFRESH_INTERVAL_S:
       feast materialize_incremental     (picks up new batch features)

This script is a pure producer — it only writes to the online store (Redis).
To query features or export ML datasets use query_featurestore.py:
    uv run scripts/main/query_featurestore.py --online
    uv run scripts/main/query_featurestore.py --export

Run:
    uv run scripts/main/7_feature_pipeline.py
    uv run scripts/main/7_feature_pipeline.py --skip-full-materialize

Stop with Ctrl-C — the Kafka consumer thread exits cleanly.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import threading
from datetime import datetime, timedelta, timezone

import pandas as pd
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from loguru import logger

try:
    from feast import FeatureStore
except ImportError:
    logger.error("Feast is not installed. Run: uv add feast")
    sys.exit(1)

from utils import ICD10_CHAPTERS, to_col, read_delta_table_as_pandas

# ── Environment ────────────────────────────────────────────────────────────────
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

FEATURE_STORE_DIR = os.getenv("FEAST_REPO_DIR", os.path.join(project_root, "config", "feature_store"))

KAFKA_PORT        = os.getenv("KAFKA_PORT", "9092")
BOOTSTRAP_SERVERS = f"localhost:{KAFKA_PORT}"
INPUT_TOPIC       = "patient-features-24h"

MINIO_PORT       = os.getenv("MINIO_PORT",        "9000")
MINIO_HOST       = os.getenv("MINIO_HOST",         "localhost")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER",    "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

os.environ["AWS_ACCESS_KEY_ID"]     = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["AWS_ENDPOINT_URL"]      = f"http://{MINIO_HOST}:{MINIO_PORT}"
os.environ["AWS_S3_ENDPOINT_URL"]   = f"http://{MINIO_HOST}:{MINIO_PORT}"
os.environ["PROJECT_ROOT"]          = project_root

REFRESH_INTERVAL_S = 60

_STORAGE_OPTIONS = {
    "endpoint_url":      f"http://{MINIO_HOST}:{MINIO_PORT}",
    "access_key_id":     MINIO_ACCESS_KEY,
    "secret_access_key": MINIO_SECRET_KEY,
    "allow_http":        "true",
}
_ICD10_CHAPTERS = [roman for roman, _, _ in ICD10_CHAPTERS]


# ── Step 1: Feature store initialisation ──────────────────────────────────────

def feast_apply() -> None:
    logger.info("Running `feast apply` ...")
    res = subprocess.run(
        ["feast", "apply", "--skip-source-validation"],
        cwd=FEATURE_STORE_DIR,
    )
    if res.returncode != 0:
        logger.error(f"feast apply failed (exit {res.returncode})")
        sys.exit(res.returncode)
    logger.info("feast apply complete.")


def feast_materialize_full(store: FeatureStore) -> None:
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end   = datetime.now(tz=timezone.utc) + timedelta(days=1)
    logger.info(f"Full materialization: {start.date()} → {end.date()} ...")
    store.materialize(start_date=start, end_date=end)
    logger.info("Full materialization complete.")


def feast_materialize_incremental(store: FeatureStore) -> None:
    end = datetime.now(tz=timezone.utc)
    logger.info(f"Incremental materialization up to {end.isoformat()} ...")
    store.materialize_incremental(end_date=end)
    logger.info("Incremental materialization complete.")


# ── Step 2: Kafka → Feast online push (background thread) ─────────────────────

def _derive_feature_columns() -> list[str]:
    try:
        cols = []
        for table in ("dim_vital", "dim_lab"):
            for _, row in read_delta_table_as_pandas(table, _STORAGE_OPTIONS).iterrows():
                prefix = to_col(row["event_name"])
                cols.extend([f"{prefix}_{s}" for s in ("mean", "min", "max", "std")])
        for _, row in read_delta_table_as_pandas("dim_medication", _STORAGE_OPTIONS).iterrows():
            cols.append(f"{to_col(row['event_name'])}_count")
        for roman in _ICD10_CHAPTERS:
            cols.append(f"icd_chap_{roman}")
        logger.info(f"Derived {len(cols)} feature columns from gold dim tables.")
        return cols
    except Exception as e:
        logger.warning(f"Could not load dim tables ({e}) — FEATURE_COLUMNS will be empty.")
        return []


def _ensure_topic() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        existing = admin.list_topics(timeout=5).topics
        if INPUT_TOPIC not in existing:
            logger.info(f"Creating Kafka topic '{INPUT_TOPIC}' ...")
            fs = admin.create_topics([NewTopic(INPUT_TOPIC, num_partitions=1, replication_factor=1)])
            for _, f in fs.items():
                f.result()
            logger.info(f"Topic '{INPUT_TOPIC}' created.")
        else:
            logger.info(f"Kafka topic '{INPUT_TOPIC}' already exists.")
    except Exception as e:
        logger.error(f"Topic check/create failed: {e}")


def _record_to_df(record: dict, feature_columns: list[str]) -> pd.DataFrame | None:
    patient_id = record.get("patient_id")
    window_ts  = record.get("window_end_ts")
    if not patient_id or not window_ts:
        logger.warning(f"Skipping record missing patient_id or window_end_ts: {record}")
        return None
    try:
        ts = pd.to_datetime(window_ts, utc=True)
    except Exception:
        ts = pd.Timestamp.now(tz="UTC")
    row = {"patient_id": patient_id, "feature_timestamp": ts}
    for col in feature_columns:
        row[col] = record.get(col)
    return pd.DataFrame([row])


def _kafka_push_loop(stop_event: threading.Event) -> None:
    """Runs in a daemon thread: consumes patient-features-24h → Feast online store."""
    # Own FeatureStore instance — FeatureStore is not thread-safe, don't share with main thread
    store = FeatureStore(repo_path=FEATURE_STORE_DIR)
    _ensure_topic()
    feature_columns = _derive_feature_columns()

    consumer = Consumer({
        "bootstrap.servers":  BOOTSTRAP_SERVERS,
        "group.id":           "ehr-feast-online-writer",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([INPUT_TOPIC])
    logger.info(f"Stream consumer subscribed to '{INPUT_TOPIC}'.")

    pushed = 0
    try:
        while not stop_event.is_set():
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
                logger.warning(f"Failed to parse Kafka message: {e}")
                continue

            df = _record_to_df(record, feature_columns)
            if df is None:
                continue
            try:
                store.push("patient_vitals_stream", df)
                pushed += 1
                if pushed % 100 == 0:
                    logger.info(f"Pushed {pushed} feature records to Feast online store.")
            except Exception as e:
                logger.error(f"Feast push failed for patient {record.get('patient_id')}: {e}")
    finally:
        consumer.close()
        logger.info(f"Stream consumer stopped. Total records pushed: {pushed}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main(skip_full_materialize: bool = False) -> None:
    logger.info("=" * 60)
    logger.info("EHR Feature Pipeline starting")
    logger.info("=" * 60)

    stop_event = threading.Event()

    # SIGINT (Ctrl-C) and SIGTERM handler to force immediate shutdown
    def _on_shutdown(signum, frame):
        logger.info("Shutdown requested — exiting immediately.")
        os._exit(0)
    signal.signal(signal.SIGINT, _on_shutdown)
    signal.signal(signal.SIGTERM, _on_shutdown)

    # 1. One-time initialisation
    feast_apply()
    store = FeatureStore(repo_path=FEATURE_STORE_DIR)
    store.refresh_registry()
    if skip_full_materialize:
        logger.info("Skipping full materialization (--skip-full-materialize).")
    else:
        feast_materialize_full(store)

    # 2. Start Kafka → Feast push in background thread
    push_thread = threading.Thread(
        target=_kafka_push_loop,
        args=(stop_event,),
        daemon=True,
        name="kafka-feast-push",
    )
    push_thread.start()
    logger.info("Stream consumer thread started.")

    # 3. Periodic incremental materialize loop
    logger.info(f"Entering refresh loop (every {REFRESH_INTERVAL_S}s). Ctrl-C to stop.")
    try:
        while True:
            cycle_start = time.time()
            try:
                store.refresh_registry()
                feast_materialize_incremental(store)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.exception(f"Materialization error: {e}")

            elapsed  = time.time() - cycle_start
            sleep_s  = max(0, REFRESH_INTERVAL_S - elapsed)
            logger.info(f"Cycle done in {elapsed:.1f}s. Next refresh in {sleep_s:.0f}s.")

            # Sleep in 1-second chunks — time.sleep() raises KeyboardInterrupt immediately
            # on Ctrl-C. A single long sleep() or Event.wait() is silently retried by
            # CPython (PEP 475) when the signal handler doesn't raise an exception.
            deadline = time.monotonic() + sleep_s
            while time.monotonic() < deadline:
                time.sleep(min(1.0, deadline - time.monotonic()))

    except KeyboardInterrupt:
        logger.info("Shutdown requested — stopping cleanly.")
    finally:
        stop_event.set()
        push_thread.join(timeout=5)
        logger.info("Feature pipeline stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EHR Feature Store Pipeline")
    parser.add_argument(
        "--skip-full-materialize", action="store_true",
        help="Skip the full backfill on startup (use after the first run)",
    )
    args = parser.parse_args()
    main(skip_full_materialize=args.skip_full_materialize)
