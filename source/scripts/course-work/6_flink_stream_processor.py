#!/usr/bin/env python3
"""
EHR Patient Events — Apache Flink Unified Stream Processor (Datastream API)

Consumes raw events from the 'patient-events' Kafka topic, computes 24h rolling
window aggregates per patient, prints results to stdout, and writes to Kafka topic.
"""

import argparse
import json
import math
import os
import re
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

try:
    from deltalake import DeltaTable
except ImportError:
    logger.error("deltalake not installed. Run: uv add deltalake")
    raise

try:
    from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.watermark_strategy import TimestampAssigner
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource, KafkaSink, KafkaRecordSerializationSchema
    from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
    from pyflink.datastream.window import SlidingEventTimeWindows, Time
except ImportError:
    logger.error("PyFlink not installed. Run: uv add apache-flink")
    raise

# ── Environment ────────────────────────────────────────────────────────────────
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

MINIO_PORT       = os.getenv("MINIO_PORT",        "9000")
MINIO_HOST       = os.getenv("MINIO_HOST",         "localhost")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER",    "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

KAFKA_PORT      = os.getenv("KAFKA_PORT", "9092")
KAFKA_BOOTSTRAP = f"localhost:{KAFKA_PORT}"
INPUT_TOPIC     = "patient-events"
OUTPUT_TOPIC    = "patient-features-24h"

WINDOW_SIZE_MINS   = int(os.getenv("WINDOW_SIZE_MINS", "5"))
WINDOW_SLIDE_SECS  = int(os.getenv("WINDOW_SLIDE_SECS", "10"))
WATERMARK_LAG_SECS = int(os.getenv("WATERMARK_LAG_SECS", "5"))

_FLINK_LIB = os.path.join(project_root, "..", ".tmp", "flink-lib")

_STORAGE_OPTIONS = {
    "endpoint_url":      f"http://{MINIO_HOST}:{MINIO_PORT}",
    "access_key_id":     MINIO_ACCESS_KEY,
    "secret_access_key": MINIO_SECRET_KEY,
    "allow_http":        "true",
}

_ICD10_CHAPTERS = [
    ("I",    "A",  "B"),   ("II",   "C",  "D4"),  ("III",  "D5", "D8"),
    ("IV",   "E",  "E"),   ("V",    "F",  "F"),   ("VI",   "G",  "G"),
    ("VII",  "H0", "H5"),  ("VIII", "H6", "H9"),  ("IX",   "I",  "I"),
    ("X",    "J",  "J"),   ("XI",   "K",  "K"),   ("XII",  "L",  "L"),
    ("XIII", "M",  "M"),   ("XIV",  "N",  "N"),   ("XV",   "O",  "O"),
    ("XVI",  "P",  "P"),   ("XVII", "Q",  "Q"),   ("XVIII","R",  "R"),
    ("XIX",  "S",  "T"),   ("XX",   "V",  "Y"),   ("XXI",  "Z",  "Z"),
    ("XXII", "U",  "U"),
]


# ── Schema discovery from gold dim tables ──────────────────────────────────────
def _to_col(event_name: str) -> str:
    return event_name.lower().replace(" ", "_").replace("-", "_")


def _icd_code_to_chapter(code: str) -> str | None:
    if not isinstance(code, str) or not code:
        return None
    code = code.strip().upper().replace(".", "")
    c0, c2 = code[0], code[:2] if len(code) >= 2 else code
    for roman, lo, hi in _ICD10_CHAPTERS:
        if lo == hi and len(lo) == 1:
            if c0 == lo:
                return roman
        else:
            if lo <= c2 <= hi or lo <= c0 <= hi:
                return roman
    return None


def _read_dim(table_name: str) -> pd.DataFrame:
    return (
        DeltaTable(f"s3://lakehouse/topics/{table_name}", storage_options=_STORAGE_OPTIONS)
        .to_pandas()
    )


def _build_icd_chapter_map(df_diag: pd.DataFrame) -> dict[str, list[str]]:
    chapter_to_names: dict[str, list[str]] = defaultdict(list)
    for _, row in df_diag.iterrows():
        match = re.search(r'ICD-10\s+([A-Z][0-9A-Z.]+)', str(row.get("event_description", "")))
        if not match:
            continue
        chapter = _icd_code_to_chapter(match.group(1))
        if chapter:
            chapter_to_names[chapter].append(row["event_name"])
    return dict(chapter_to_names)


def load_metrics_from_gold() -> dict:
    logger.info("Loading feature schema from gold dimension tables ...")
    df_vital = _read_dim("dim_vital")
    df_lab   = _read_dim("dim_lab")
    df_med   = _read_dim("dim_medication")
    df_diag  = _read_dim("dim_diagnosis")

    vital_metrics    = [(_to_col(r.event_name), r.event_name) for _, r in df_vital.iterrows()]
    lab_metrics      = [(_to_col(r.event_name), r.event_name) for _, r in df_lab.iterrows()]
    med_metrics      = [(_to_col(r.event_name), r.event_name) for _, r in df_med.iterrows()]
    chapter_to_names = _build_icd_chapter_map(df_diag)

    return {
        "vital_metrics":    vital_metrics,
        "lab_metrics":      lab_metrics,
        "med_metrics":      med_metrics,
        "chapter_to_names": chapter_to_names,
    }


# ── Watermark Strategy & Timestamp Extractor ───────────────────────────────────
class PatientEventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        event = json.loads(value)
        ts_str = event["event_timestamp"]
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)


# ── Flink Datastream Aggregation ───────────────────────────────────────────────
class PatientFeatureAccumulator:
    def __init__(self):
        # Maps metric_col -> {count, sum, min, max, sum_sq}
        self.numeric_stats = {}
        # Maps med_col -> count
        self.med_counts = {}
        # Maps chapter_roman -> count
        self.chap_counts = {}


class PatientFeatureAggregator(AggregateFunction):
    def __init__(self, metrics: dict):
        self.metrics = metrics
        self.vital_names = {name: col for col, name in metrics["vital_metrics"]}
        self.lab_names = {name: col for col, name in metrics["lab_metrics"]}
        self.med_names = {name: col for col, name in metrics["med_metrics"]}
        
        self.icd_map = {}
        for roman, names in metrics["chapter_to_names"].items():
            for name in names:
                self.icd_map[name] = roman

    def create_accumulator(self):
        return PatientFeatureAccumulator()

    def add(self, value_str, accumulator):
        event = json.loads(value_str)
        event_type = event.get("event_type")
        event_name = event.get("event_name")
        num_val = event.get("num_value")

        if event_type == "vital" and event_name in self.vital_names:
            col = self.vital_names[event_name]
            if num_val is not None:
                val = float(num_val)
                if col not in accumulator.numeric_stats:
                    accumulator.numeric_stats[col] = {"count": 0, "sum": 0.0, "min": float("inf"), "max": float("-inf"), "sum_sq": 0.0}
                stats = accumulator.numeric_stats[col]
                stats["count"] += 1
                stats["sum"] += val
                stats["sum_sq"] += val * val
                stats["min"] = min(stats["min"], val)
                stats["max"] = max(stats["max"], val)

        elif event_type == "lab" and event_name in self.lab_names:
            col = self.lab_names[event_name]
            if num_val is not None:
                val = float(num_val)
                if col not in accumulator.numeric_stats:
                    accumulator.numeric_stats[col] = {"count": 0, "sum": 0.0, "min": float("inf"), "max": float("-inf"), "sum_sq": 0.0}
                stats = accumulator.numeric_stats[col]
                stats["count"] += 1
                stats["sum"] += val
                stats["sum_sq"] += val * val
                stats["min"] = min(stats["min"], val)
                stats["max"] = max(stats["max"], val)

        elif event_type == "medication" and event_name in self.med_names:
            col = self.med_names[event_name]
            accumulator.med_counts[col] = accumulator.med_counts.get(col, 0) + 1

        elif event_type == "diagnosis" and event_name in self.icd_map:
            roman = self.icd_map[event_name]
            accumulator.chap_counts[roman] = accumulator.chap_counts.get(roman, 0) + 1

        return accumulator

    def get_result(self, accumulator):
        features = {}

        # 1. Calculate stats for numeric features
        for col in list(self.vital_names.values()) + list(self.lab_names.values()):
            stats = accumulator.numeric_stats.get(col)
            if stats and stats["count"] > 0:
                count = stats["count"]
                mean = stats["sum"] / count
                variance = max(0.0, (stats["sum_sq"] / count) - (mean * mean))
                std = math.sqrt(variance)

                features[f"{col}_mean"] = mean
                features[f"{col}_min"] = stats["min"]
                features[f"{col}_max"] = stats["max"]
                features[f"{col}_std"] = std
            else:
                features[f"{col}_mean"] = None
                features[f"{col}_min"] = None
                features[f"{col}_max"] = None
                features[f"{col}_std"] = None

        # 2. Medication counts
        for col in self.med_names.values():
            features[f"{col}_count"] = accumulator.med_counts.get(col, 0)

        # 3. Diagnosis ICD Chapters
        for roman, _, _ in _ICD10_CHAPTERS:
            features[f"icd_chap_{roman}"] = accumulator.chap_counts.get(roman, 0)

        return features

    def merge(self, a, b):
        # Merge numeric stats
        for col, b_stats in b.numeric_stats.items():
            if col not in a.numeric_stats:
                a.numeric_stats[col] = {"count": 0, "sum": 0.0, "min": float("inf"), "max": float("-inf"), "sum_sq": 0.0}
            a_stats = a.numeric_stats[col]
            if b_stats["count"] > 0:
                a_stats["count"] += b_stats["count"]
                a_stats["sum"] += b_stats["sum"]
                a_stats["sum_sq"] += b_stats["sum_sq"]
                a_stats["min"] = min(a_stats["min"], b_stats["min"])
                a_stats["max"] = max(a_stats["max"], b_stats["max"])

        # Merge medications
        for col, count in b.med_counts.items():
            a.med_counts[col] = a.med_counts.get(col, 0) + count

        # Merge diagnosis chapters
        for roman, count in b.chap_counts.items():
            a.chap_counts[roman] = a.chap_counts.get(roman, 0) + count

        return a


class PatientWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        patient_id = key[0]
        features = list(elements)[0]
        window = context.window()
        window_end_ts = datetime.fromtimestamp(window.end / 1000, tz=timezone.utc).isoformat()
        
        result = {
            "patient_id": patient_id,
            "window_end_ts": window_end_ts
        }
        result.update(features)
        yield json.dumps(result)


# ── Flink Environment Setup ───────────────────────────────────────────────────
def build_execution_env(local: bool) -> StreamExecutionEnvironment:
    """
    Build the Flink StreamExecutionEnvironment.

    For BOTH local and remote modes we use get_execution_environment() — the
    standard PyFlink entry point. Using the Java createRemoteEnvironment() API
    for Python jobs causes the "Could not deserialize stream node" error because
    it serializes the graph as a Java job rather than a Python one.

    The Kafka connector JAR is registered via pipeline.jars so it is sent to the
    cluster on job submission, and also via add_jars so it is on the local JVM
    classpath during graph construction.
    """
    # Prefer the Flink 2.x Kafka connector (4.x series); fall back to 1.x
    kafka_jar = os.path.abspath(os.path.join(_FLINK_LIB, "flink-sql-connector-kafka-4.0.0-2.0.jar"))
    if not os.path.exists(kafka_jar):
        kafka_jar = os.path.abspath(os.path.join(_FLINK_LIB, "flink-sql-connector-kafka-3.3.0-1.19.jar"))

    config = Configuration()
    config.set_string("pipeline.jars", f"file://{kafka_jar}")

    if local:
        logger.info(f"Starting Flink local mini-cluster with JAR: {kafka_jar}")
        # Let local environment bind to any free port for its REST service
        config.set_string("rest.bind-port", "0")
    else:
        rest_port = int(os.getenv("FLINK_REST_PORT", "8088"))
        logger.info(f"Connecting to remote Flink cluster at localhost:{rest_port} with JAR: {kafka_jar}")
        # Target the remote cluster without trying to spin up a local Web UI on port 8088
        config.set_string("execution.target", "remote")
        config.set_string("jobmanager.rpc.address", "127.0.0.1")
        config.set_string("rest.address", "127.0.0.1")
        config.set_string("rest.port", str(rest_port))

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    # Register the JAR on the local JVM classpath
    env.add_jars(f"file://{kafka_jar}")

    return env


def get_patient_id(value_str):
    event = json.loads(value_str)
    return event.get("patient_id", "unknown")


# ── Main Pipeline ──────────────────────────────────────────────────────────────
def main(local: bool = False) -> None:
    metrics = load_metrics_from_gold()
    env = build_execution_env(local=local)

    # Create Kafka Source
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(INPUT_TOPIC)
        .set_group_id("flink-console-datastream-processor")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Watermark Strategy
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_LAG_SECS))
        .with_timestamp_assigner(PatientEventTimestampAssigner())
    )

    # Build Stream Pipeline
    logger.info("Starting Flink Datastream patient aggregation job...")
    raw_stream = env.from_source(source, watermark_strategy, "Kafka patient-events")

    result_stream = (
        raw_stream
        .key_by(get_patient_id)
        .window(SlidingEventTimeWindows.of(
            Time.minutes(WINDOW_SIZE_MINS),
            Time.seconds(WINDOW_SLIDE_SECS)
        ))
        .aggregate(
            PatientFeatureAggregator(metrics),
            PatientWindowFunction(),
            output_type=Types.STRING()
        )
    )

    # Print results to stdout
    result_stream.print()

    # Write to Kafka topic
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    result_stream.sink_to(kafka_sink)

    env.execute("EHR Patient Rolling Features Datastream Job")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EHR Flink Stream Processor (Datastream API)")
    parser.add_argument("--local", action="store_true",
                        help="Run in local mini-cluster mode (no Flink cluster needed)")
    args = parser.parse_args()
    main(local=args.local)
