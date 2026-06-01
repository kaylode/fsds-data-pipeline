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


def delete_topics_if_exist(topics_to_delete: list[str]):
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        existing_topics = admin_client.list_topics(timeout=5).topics
        to_delete = [t for t in topics_to_delete if t in existing_topics]
        if to_delete:
            logger.info(f"Deleting existing Kafka topics: {to_delete}...")
            fs = admin_client.delete_topics(to_delete)
            for topic, f in fs.items():
                f.result()  # Wait for deletion
            logger.info("Kafka topics deleted successfully.")
            # Give Kafka a brief moment to process the deletion
            time.sleep(1.0)
        else:
            logger.info("No matching topics found to delete.")
    except Exception as e:
        logger.error(f"Failed to delete Kafka topics: {e}")


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
            # Give Kafka a brief moment to stabilize partition leader election
            time.sleep(2.0)
        else:
            logger.info(f"Kafka topic '{TOPIC_NAME}' already exists.")
    except Exception as e:
        logger.error(f"Failed to check/create Kafka topic: {e}")


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")


def main():
    import random
    from datetime import datetime, timezone

    # Clear out patient-events and patient-features-24h on startup
    delete_topics_if_exist([TOPIC_NAME, "patient-features-24h"])

    create_topic_if_not_exists()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "ehr-producer"
    })

    stream_file = os.path.join(project_root, "data", "synthetic", "clinical_event_stream.json")

    if not os.path.exists(stream_file):
        logger.error(f"Streaming data file not found at {stream_file}. Run generation first.")
        return

    logger.info(f"Reading template streaming events from {stream_file}...")
    with open(stream_file, "r") as f:
        records = json.load(f)

    logger.info(f"Loaded {len(records):,} template records. Starting indefinite streaming loop to Kafka...")

    count = 0
    start_time = time.time()
    
    try:
        while True:
            # Shuffle or cycle through template records to keep the stream dynamic
            random.shuffle(records)
            
            for rec in records:
                # 1. Update timestamp to current real-time UTC
                now = datetime.now(timezone.utc)
                
                # Simulate latency/out-of-orderness:
                # - 85% normal (0-2s lag)
                # - 10% late arrival (5-30s lag)
                # - 5% severe lag (1-5 minutes lag)
                rand = random.random()
                if rand < 0.85:
                    lag_seconds = random.uniform(0, 2)
                elif rand < 0.95:
                    lag_seconds = random.uniform(5, 30)
                else:
                    lag_seconds = random.uniform(60, 300)
                
                event_time = now - timedelta_val(seconds=lag_seconds)
                # Format to match original stream timestamp format (ISO with 'Z')
                rec["event_timestamp"] = event_time.isoformat().replace("+00:00", "Z")

                # Produce record
                producer.produce(
                    topic=TOPIC_NAME,
                    key=rec["patient_id"],
                    value=json.dumps(rec),
                    on_delivery=delivery_report
                )
                count += 1

                # 2. Simulate traffic patterns (Bursting vs Lags)
                # - Periodically poll to serve delivery callbacks
                if count % 100 == 0:
                    producer.poll(0)
                
                # Determine pacing/delay before next message:
                pacing_rand = random.random()
                if pacing_rand < 0.05:
                    # 5% chance of a burst: send next 50-200 records instantly without sleep
                    burst_size = random.randint(50, 200)
                    logger.info(f"🔥 Burst active! Publishing {burst_size} messages instantly...")
                    # We continue the loop and will skip sleeping for the next 'burst_size' iterations
                    continue
                elif pacing_rand < 0.08:
                    # 3% chance of network lag: pause the stream for a few seconds
                    lag_duration = random.uniform(1.5, 4.0)
                    logger.info(f"⏳ Simulated network lag: Pausing stream for {lag_duration:.2f} seconds...")
                    producer.flush()  # Flush any outstanding messages before pausing
                    time.sleep(lag_duration)
                else:
                    # Normal pacing: sleep between messages
                    # Sleep between 5ms and 30ms to maintain a steady stream of ~30-100 events/sec
                    time.sleep(random.uniform(0.005, 0.03))

                if count % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = count / elapsed
                    logger.info(f"Published {count:,} events total. Current rate: {rate:.1f} msg/sec.")

    except KeyboardInterrupt:
        logger.info("Stream producer interrupted by user.")
    finally:
        logger.info("Flushing final producer messages...")
        producer.flush()
        elapsed = time.time() - start_time
        logger.info(f"Stopped. Successfully published {count:,} events to topic '{TOPIC_NAME}' in {elapsed:.2f} seconds.")


# Helper to construct datetime offsets without importing timedelta at module level
def timedelta_val(seconds):
    from datetime import timedelta
    return timedelta(seconds=seconds)


if __name__ == "__main__":
    main()
