#!/usr/bin/env python3
"""
EHR Live Stream CDC Consumer

Consumes real-time Change Data Capture (CDC) events captured by Debezium
from the PostgreSQL patient_events table, and prints the patient clinical
events in real-time.
"""

import os
import sys
import json
from loguru import logger
from confluent_kafka import Consumer, KafkaError

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_PATIENT_EVENTS = "ehrserver.public.patient_events"
TOPIC_SUBJECTS = "ehrserver.public.subjects"

def main():
    logger.info("Initializing EHR Live Stream CDC Consumer...")
    
    # Configure Kafka Consumer
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "ehr-cdc-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    }
    
    try:
        consumer = Consumer(conf)
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        sys.exit(1)
        
    # Subscribe to clinical topics
    consumer.subscribe([TOPIC_PATIENT_EVENTS, TOPIC_SUBJECTS])
    
    logger.info(f"Connected to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Subscribed to topics: {TOPIC_PATIENT_EVENTS}, {TOPIC_SUBJECTS}")
    logger.info("Waiting for real-time patient streams... Press Ctrl+C to stop.")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    logger.error(f"Kafka consumer error: {msg.error()}")
                    continue
                    
            # Process received message
            try:
                # Decode message metadata
                topic = msg.topic()
                value_bytes = msg.value()
                if not value_bytes:
                    continue
                    
                event = json.loads(value_bytes.decode("utf-8"))
                
                # Debezium wraps the CDC payload inside a "payload" key
                payload = event.get("payload", event)
                if not payload:
                    continue
                    
                op = payload.get("op")  # c=create (insert), u=update, d=delete, r=snapshot
                after = payload.get("after")
                before = payload.get("before")
                
                if topic == TOPIC_SUBJECTS:
                    if op in ("c", "r") and after:
                        subj_id = after.get("subject_id")
                        logger.info(f"🆕 [NEW SUBJECT REGISTERED] ID: {subj_id}")
                elif topic == TOPIC_PATIENT_EVENTS:
                    if op in ("c", "r") and after:
                        subj_id = after.get("subject_id")
                        adm_id = after.get("admission_id")
                        hist_time = after.get("historical_time")
                        code = after.get("code")
                        num_val = after.get("numeric_value")
                        text_val = after.get("text_value")
                        
                        # Format timestamp (historical clinical time)
                        if isinstance(hist_time, int):
                            # Debezium converts timestamp/datetime to microseconds/milliseconds since epoch
                            # depending on configuration, let's format nicely if possible
                            from datetime import datetime
                            # Debezium timestamp is typically in microseconds since epoch for TIMESTAMP without timezone
                            try:
                                dt = datetime.fromtimestamp(hist_time / 1000000.0)
                                hist_time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            except Exception:
                                hist_time_str = str(hist_time)
                        else:
                            hist_time_str = str(hist_time)
                            
                        val_str = ""
                        if num_val is not None:
                            val_str = f"Value: {num_val}"
                        elif text_val is not None:
                            val_str = f"Value: '{text_val}'"
                            
                        # Log structured clinical event
                        logger.info(
                            f"🏥 [CLINICAL EVENT] "
                            f"Patient: {subj_id:<5} | "
                            f"Time: {hist_time_str} | "
                            f"Code: {code:<15} | "
                            f"{val_str}"
                        )
                    elif op == "u":
                        logger.debug(f"✏️ [EVENT UPDATED] before={before} after={after}")
                    elif op == "d":
                        logger.warning(f"❌ [EVENT DELETED] before={before}")
                        
            except json.JSONDecodeError:
                logger.error(f"Failed to decode message JSON on topic {msg.topic()}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        logger.info("CDC Consumer stopped by user.")
    finally:
        consumer.close()
        logger.info("Kafka consumer connection closed.")

if __name__ == "__main__":
    main()
