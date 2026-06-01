#!/usr/bin/env python3
"""
Test script to consume clinical feature records from a Kafka topic (e.g., patient-features-24h or patient-features)
to verify if data is successfully published.
"""

import os
import json
import argparse
import time
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
BOOTSTRAP_SERVERS = f"localhost:{KAFKA_PORT}"
DEFAULT_TOPIC = "patient-features-24h"

def main():
    parser = argparse.ArgumentParser(description="Consume and verify Kafka messages.")
    parser.add_argument(
        "--topic",
        type=str,
        default=DEFAULT_TOPIC,
        help=f"Kafka topic to consume from (default: {DEFAULT_TOPIC})"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of messages to consume before exiting (default: 5)"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Timeout in seconds to wait for new messages before giving up (default: 15.0)"
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Consume messages starting from the earliest offset instead of latest"
    )
    args = parser.parse_args()

    # Generate a unique group id if consuming from beginning, or use a default one
    group_id = f"test-consumer-{args.topic}-{time.time()}" if args.from_beginning else f"test-consumer-{args.topic}"

    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": False
    }

    logger.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    logger.info(f"Configured options: Topic={args.topic}, Limit={args.limit}, Timeout={args.timeout}s, GroupID={group_id}")

    try:
        consumer = Consumer(conf)
    except Exception as e:
        logger.error(f"Failed to create consumer: {e}")
        return

    try:
        # Check if the topic exists
        metadata = consumer.list_topics(timeout=5)
        if args.topic not in metadata.topics:
            logger.warning(f"Topic '{args.topic}' does not currently exist. Available topics: {list(metadata.topics.keys())}")
            logger.info("Will attempt to subscribe anyway in case it gets created...")
        
        consumer.subscribe([args.topic])
        logger.info(f"Subscribed to topic '{args.topic}'. Polling for messages...")

        count = 0
        start_time = time.time()
        last_msg_time = time.time()

        while count < args.limit:
            # Check for overall timeout or timeout since last message
            elapsed_total = time.time() - start_time
            elapsed_since_last = time.time() - last_msg_time

            if count == 0 and elapsed_total > args.timeout:
                logger.warning(f"No messages received within the overall timeout of {args.timeout} seconds.")
                break
            elif count > 0 and elapsed_since_last > args.timeout:
                logger.warning(f"No new messages received within {args.timeout} seconds since the last message.")
                break

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            # Message received successfully
            last_msg_time = time.time()
            count += 1
            
            key = msg.key().decode("utf-8") if msg.key() else "None"
            raw_value = msg.value()
            
            logger.info(f"--- [Message #{count}] ---")
            logger.info(f"Topic: {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}")
            logger.info(f"Key: {key}")
            
            try:
                value_json = json.loads(raw_value.decode("utf-8"))
                # Print formatted JSON
                logger.info(f"Value:\n{json.dumps(value_json, indent=2)}")
            except Exception as e:
                # Fallback to printing decoded raw string or binary representation
                try:
                    logger.info(f"Value: {raw_value.decode('utf-8')}")
                except Exception:
                    logger.info(f"Value (binary): {raw_value}")

        logger.info(f"Done. Successfully consumed {count} messages from '{args.topic}'.")

    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
