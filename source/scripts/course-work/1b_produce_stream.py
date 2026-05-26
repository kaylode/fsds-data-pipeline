#!/usr/bin/env python3
"""
EHR Live Event Stream Producer
Reads flat JSON clinical event records and publishes them to the 'patient-events' Kafka topic.
"""

import os
import json
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from loguru import logger
from dotenv import load_dotenv

# Load environment variables
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
BOOTSTRAP_SERVERS = f"localhost:{KAFKA_PORT}"
TOPIC_NAME = "patient-events"


def create_topic_if_not_exists():
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        topics = admin_client.list_topics(timeout=5).topics
        if TOPIC_NAME not in topics:
            logger.info(f"Creating Kafka topic '{TOPIC_NAME}'...")
            new_topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
            fs = admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                f.result()  # Wait for creation
            logger.info(f"Kafka topic '{TOPIC_NAME}' created successfully.")
        else:
            logger.info(f"Kafka topic '{TOPIC_NAME}' already exists.")
    except Exception as e:
        logger.error(f"Failed to check/create Kafka topic: {e}")


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")


def main():
    create_topic_if_not_exists()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "ehr-producer"
    })

    stream_file = os.path.join(project_root, "data", "synthetic", "clinical_event_stream.json")

    if not os.path.exists(stream_file):
        logger.error(f"Streaming data file not found at {stream_file}. Run generation first.")
        return

    logger.info(f"Reading streaming events from {stream_file}...")
    with open(stream_file, "r") as f:
        records = json.load(f)

    logger.info(f"Loaded {len(records):,} records. Starting publishing stream to Kafka...")

    count = 0
    start_time = time.time()
    for rec in records:
        producer.produce(
            topic=TOPIC_NAME,
            key=rec["patient_id"],
            value=json.dumps(rec),
            on_delivery=delivery_report
        )
        count += 1

        # Periodically poll to serve delivery callbacks
        if count % 2000 == 0:
            producer.poll(0)
            logger.info(f"Published {count:,} / {len(records):,} events...")
            time.sleep(0.02)  # Simulate continuous streaming pacing

    logger.info("Flushing final producer messages...")
    producer.flush()

    elapsed = time.time() - start_time
    logger.info(f"Successfully published {count:,} events to topic '{TOPIC_NAME}' in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    main()
