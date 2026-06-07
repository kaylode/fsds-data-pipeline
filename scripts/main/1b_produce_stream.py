#!/usr/bin/env python3
"""
EHR Live Event Stream Producer

Generates a synthetic clinical event stream in-memory and publishes records
to the 'patient-events' Kafka topic indefinitely.

The in-memory template pool is generated using the same parameters as the
offline generator (1a_generate_offline_ehr.py) but requires no disk files.
Real-time UTC timestamps are applied to each record before publishing.

Streaming challenges simulated:
  - Late arrivals (85% normal, 10% late 5-30s, 5% severe 1-5 min lag)
  - Traffic bursts (5% chance of burst publishing 50-200 records instantly)
  - Network lag simulation (3% chance of 1.5-4s pause)
"""

import os
import json
import random
import time
import numpy as np
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from loguru import logger

# ── Environment ────────────────────────────────────────────────────────────────
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

KAFKA_PORT        = os.getenv("KAFKA_PORT", "9092")
BOOTSTRAP_SERVERS = f"localhost:{KAFKA_PORT}"
TOPIC_NAME        = "patient-events"

# ── Stream Generation Config ───────────────────────────────────────────────────
STREAM_CONFIG = {
    "random_seed":            42,
    "start_date":             "2023-01-01",
    "end_date":               "2024-01-01",
    "n_patients":             10_000,
    "n_visits":               50_000,
    "n_events":               100_000,
    "base_events_per_min":    100,
    "burst_multiplier":       30.0,
    "burst_windows": [
        {"start": "12:00", "end": "12:20"},
        {"start": "20:00", "end": "20:20"},
    ],
    "late_arrival_rate":        0.12,    # 12% of events have ingestion lag
    "late_delay_min_max_mins":  [5, 45],
    "duplicate_rate_stream":    0.015,   # 1.5% duplicate events in stream
}

# ── Reference Pools (shared with 1a_generate_offline_ehr.py) ──────────────────
WARDS = {
    "General_Ward":   "General Medicine",
    "Pediatric_Ward": "Pediatrics",
    "ICU_Ward":       "Intensive Care Unit",
    "ER_Ward":        "Emergency Department",
}

EVENT_METADATA_DEFS = [
    # Vitals
    {"event_type_id": "V01", "event_name": "Heart Rate",    "event_description": "Resting heart rate in beats per minute", "unit_of_measurement": "bpm",     "event_type": "vital"},
    {"event_type_id": "V02", "event_name": "Systolic BP",   "event_description": "Systolic blood pressure",                "unit_of_measurement": "mmHg",    "event_type": "vital"},
    {"event_type_id": "V03", "event_name": "Diastolic BP",  "event_description": "Diastolic blood pressure",               "unit_of_measurement": "mmHg",    "event_type": "vital"},
    {"event_type_id": "V04", "event_name": "Temperature",   "event_description": "Body temperature in Celsius",            "unit_of_measurement": "°C",      "event_type": "vital"},
    # Labs
    {"event_type_id": "L01", "event_name": "Glucose",       "event_description": "Fasting blood glucose level",           "unit_of_measurement": "mg/dL",   "event_type": "lab"},
    {"event_type_id": "L02", "event_name": "Creatinine",    "event_description": "Serum creatinine level",                "unit_of_measurement": "mg/dL",   "event_type": "lab"},
    {"event_type_id": "L03", "event_name": "WBC",           "event_description": "White blood cell count",                "unit_of_measurement": "10^3/µL", "event_type": "lab"},
    {"event_type_id": "L04", "event_name": "Hemoglobin",    "event_description": "Hemoglobin level",                      "unit_of_measurement": "g/dL",    "event_type": "lab"},
    # Medications
    {"event_type_id": "M01", "event_name": "Lisinopril",    "event_description": "Lisinopril 10mg administration",   "unit_of_measurement": None, "event_type": "medication"},
    {"event_type_id": "M02", "event_name": "Metformin",     "event_description": "Metformin 500mg administration",   "unit_of_measurement": None, "event_type": "medication"},
    {"event_type_id": "M03", "event_name": "Amoxicillin",   "event_description": "Amoxicillin 500mg administration", "unit_of_measurement": None, "event_type": "medication"},
    {"event_type_id": "M04", "event_name": "Atorvastatin",  "event_description": "Atorvastatin 20mg administration", "unit_of_measurement": None, "event_type": "medication"},
    # Diagnoses
    {"event_type_id": "D01", "event_name": "Hypertension",  "event_description": "Essential primary hypertension (ICD-10 I10)",                "unit_of_measurement": None, "event_type": "diagnosis"},
    {"event_type_id": "D02", "event_name": "Diabetes",      "event_description": "Type 2 diabetes mellitus (ICD-10 E11.9)",                    "unit_of_measurement": None, "event_type": "diagnosis"},
    {"event_type_id": "D03", "event_name": "URI",           "event_description": "Acute upper respiratory infection, unspecified (ICD-10 J06.9)", "unit_of_measurement": None, "event_type": "diagnosis"},
    {"event_type_id": "D04", "event_name": "Hyperlipidemia","event_description": "Hyperlipidemia, unspecified (ICD-10 E78.5)",                 "unit_of_measurement": None, "event_type": "diagnosis"},
]


# ── Template Pool Generation ───────────────────────────────────────────────────

def generate_stream_template() -> list[dict]:
    """
    Generates a pool of synthetic streaming event records in-memory.
    Records are used as templates — real-time UTC timestamps are applied
    by the producer loop before each message is published to Kafka.

    Returns a list of dicts representing clinical event records.
    """
    logger.info("Generating in-memory streaming event template pool...")
    cfg = STREAM_CONFIG
    np.random.seed(cfg["random_seed"])

    start_date       = datetime.strptime(cfg["start_date"], "%Y-%m-%d")
    end_date         = datetime.strptime(cfg["end_date"],   "%Y-%m-%d")
    schema_evol_date = datetime(2023, 8, 1)  # matches offline generator cutoff

    n_patients  = cfg["n_patients"]
    n_visits    = cfg["n_visits"]
    n_events    = cfg["n_events"]
    ward_keys   = list(WARDS.keys())

    # ── Time distribution with burst windows ──────────────────────────────
    total_minutes      = int((end_date - start_date).total_seconds() / 60)
    start_minute_of_day = start_date.hour * 60 + start_date.minute
    minute_offsets     = np.arange(total_minutes)
    minute_indices     = (minute_offsets + start_minute_of_day) % 1440

    day_weights = np.ones(1440)
    for window in cfg["burst_windows"]:
        start_h, start_m = map(int, window["start"].split(":"))
        end_h,   end_m   = map(int, window["end"].split(":"))
        start_idx = start_h * 60 + start_m
        end_idx   = end_h   * 60 + end_m
        if start_idx <= end_idx:
            day_weights[start_idx:end_idx] = cfg["burst_multiplier"]
        else:
            day_weights[start_idx:] = cfg["burst_multiplier"]
            day_weights[:end_idx]   = cfg["burst_multiplier"]

    all_weights    = day_weights[minute_indices]
    probs          = all_weights / all_weights.sum()
    sampled_offsets = np.random.choice(total_minutes, size=n_events, p=probs)
    event_timestamps = sorted([start_date + timedelta(minutes=int(m)) for m in sampled_offsets])

    # ── Late arrivals ─────────────────────────────────────────────────────
    min_lag_min, max_lag_min = cfg["late_delay_min_max_mins"]
    created_timestamps = []
    for ts in event_timestamps:
        if np.random.rand() < cfg["late_arrival_rate"]:
            lag_mins = np.random.randint(min_lag_min, max_lag_min)
            created_timestamps.append(ts + timedelta(minutes=lag_mins))
        else:
            created_timestamps.append(ts + timedelta(seconds=np.random.randint(0, 5)))

    # ── Build flat records ────────────────────────────────────────────────
    meta_choices = np.random.choice(len(EVENT_METADATA_DEFS), n_events)
    stream_records = []

    for i in range(n_events):
        meta   = EVENT_METADATA_DEFS[meta_choices[i]]
        e_type = meta["event_type"]
        e_name = meta["event_name"]
        ts     = event_timestamps[i]

        num_val  = None
        text_val = None
        if e_type == "vital":
            lookup = {"Heart Rate": (75, 12), "Systolic BP": (120, 15), "Diastolic BP": (80, 8), "Temperature": (36.8, 0.4)}
            mu, sigma = lookup.get(e_name, (0, 1))
            num_val = float(np.round(np.random.normal(mu, sigma), 1))
        elif e_type == "lab":
            lookup = {"Glucose": (100, 20), "Creatinine": (0.8, 0.2), "WBC": (7.0, 2.0), "Hemoglobin": (14.5, 1.5)}
            mu, sigma = lookup.get(e_name, (0, 1))
            num_val = float(np.round(np.random.normal(mu, sigma), 2))
        elif e_type == "medication":
            text_val = f"Administered {e_name} per physician prescription"
        elif e_type == "diagnosis":
            text_val = f"Confirmed diagnosis: {meta['event_description']}"

        severity = None
        if ts >= schema_evol_date:
            severity = np.random.choice(["mild", "moderate", "severe"], p=[0.60, 0.30, 0.10])

        stream_records.append({
            "event_id":        f"EVT_STR{i+1:08d}",
            "visit_id":        f"V{np.random.randint(1, n_visits   + 1):07d}",
            "patient_id":      f"P{np.random.randint(1, n_patients + 1):06d}",
            "ward_id":         np.random.choice(ward_keys, p=[0.85, 0.10, 0.03, 0.02]),
            "event_type":      e_type,
            "event_type_id":   meta["event_type_id"],
            "event_name":      e_name,
            "event_timestamp": ts.isoformat(),
            "created_ts":      created_timestamps[i].isoformat(),
            "text_value":      text_val,
            "num_value":       num_val,
            "severity_level":  severity,
            "device_type":     np.random.choice(["monitor", "handheld", "desktop"], p=[0.70, 0.20, 0.10]),
            "source":          np.random.choice(["ward_monitor", "hand_entry"],     p=[0.70, 0.30]),
        })

    # ── Inject 1.5% duplicates ────────────────────────────────────────────
    n_dupes = int(n_events * cfg["duplicate_rate_stream"])
    logger.info(f"Injecting {n_dupes:,} duplicate events ({cfg['duplicate_rate_stream']*100:.1f}% rate) into template pool...")
    for idx in np.random.choice(n_events, n_dupes):
        base_rec  = stream_records[idx].copy()
        dupe_delay = timedelta(minutes=np.random.randint(1, 4))
        base_rec["event_timestamp"] = (datetime.fromisoformat(base_rec["event_timestamp"]) + dupe_delay).isoformat()
        base_rec["created_ts"]      = (datetime.fromisoformat(base_rec["created_ts"])      + dupe_delay).isoformat()
        stream_records.append(base_rec)

    stream_records.sort(key=lambda x: x["event_timestamp"])
    logger.info(f"Template pool ready: {len(stream_records):,} records ({n_events:,} base + {n_dupes:,} dupes)")
    return stream_records


# ── Kafka Helpers ──────────────────────────────────────────────────────────────

def delete_topics_if_exist(topics_to_delete: list[str]) -> None:
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        existing_topics = admin_client.list_topics(timeout=5).topics
        to_delete = [t for t in topics_to_delete if t in existing_topics]
        if to_delete:
            logger.info(f"Deleting existing Kafka topics: {to_delete}...")
            fs = admin_client.delete_topics(to_delete)
            for topic, f in fs.items():
                f.result()
            logger.info("Kafka topics deleted successfully.")
            time.sleep(1.0)
        else:
            logger.info("No matching topics found to delete.")
    except Exception as e:
        logger.error(f"Failed to delete Kafka topics: {e}")


def create_topic_if_not_exists() -> None:
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        topics = admin_client.list_topics(timeout=5).topics
        if TOPIC_NAME not in topics:
            logger.info(f"Creating Kafka topic '{TOPIC_NAME}'...")
            new_topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
            fs = admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                f.result()
            logger.info(f"Kafka topic '{TOPIC_NAME}' created successfully.")
            time.sleep(2.0)
        else:
            logger.info(f"Kafka topic '{TOPIC_NAME}' already exists.")
    except Exception as e:
        logger.error(f"Failed to check/create Kafka topic: {e}")


def delivery_report(err, msg) -> None:
    if err is not None:
        logger.error(f"Message delivery failed: {err}")


# ── Producer Loop ──────────────────────────────────────────────────────────────

def main():
    # Reset topics on startup for a clean run
    delete_topics_if_exist([TOPIC_NAME, "patient-features-24h"])
    create_topic_if_not_exists()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "ehr-producer",
    })

    # Generate template pool in-memory (no disk I/O needed)
    records = generate_stream_template()

    logger.info(f"Loaded {len(records):,} template records. Starting indefinite streaming loop to Kafka...")

    count      = 0
    start_time = time.time()

    try:
        while True:
            # Shuffle records each cycle to keep the stream dynamic
            random.shuffle(records)

            for rec in records:
                # Apply real-time UTC timestamp with simulated latency
                now = datetime.now(timezone.utc)
                rand = random.random()
                if rand < 0.85:
                    lag_seconds = random.uniform(0, 2)       # normal: 0-2s
                elif rand < 0.95:
                    lag_seconds = random.uniform(5, 30)      # late: 5-30s
                else:
                    lag_seconds = random.uniform(60, 300)    # severe: 1-5 min
                event_time = now - timedelta(seconds=lag_seconds)
                rec["event_timestamp"] = event_time.isoformat().replace("+00:00", "Z")

                producer.produce(
                    topic=TOPIC_NAME,
                    key=rec["patient_id"],
                    value=json.dumps(rec),
                    on_delivery=delivery_report,
                )
                count += 1

                # Poll periodically to serve delivery callbacks
                if count % 100 == 0:
                    producer.poll(0)

                # Pacing / traffic pattern simulation
                pacing_rand = random.random()
                if pacing_rand < 0.05:
                    # 5% chance: burst — skip sleep for this record
                    burst_size = random.randint(50, 200)
                    logger.info(f"🔥 Burst active! Publishing {burst_size} messages instantly...")
                    continue
                elif pacing_rand < 0.08:
                    # 3% chance: network lag pause
                    lag_duration = random.uniform(1.5, 4.0)
                    logger.info(f"⏳ Simulated network lag: pausing {lag_duration:.2f}s...")
                    producer.flush()
                    time.sleep(lag_duration)
                else:
                    # Normal pacing: ~30-100 events/sec
                    time.sleep(random.uniform(0.005, 0.03))

                if count % 1000 == 0:
                    elapsed = time.time() - start_time
                    logger.info(f"Published {count:,} events | rate: {count/elapsed:.1f} msg/sec")

    except KeyboardInterrupt:
        logger.info("Stream producer interrupted by user.")
    finally:
        logger.info("Flushing remaining producer messages...")
        producer.flush()
        elapsed = time.time() - start_time
        logger.info(f"Stopped. Published {count:,} events to '{TOPIC_NAME}' in {elapsed:.2f}s.")


if __name__ == "__main__":
    main()
