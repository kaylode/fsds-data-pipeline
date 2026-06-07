"""
EHR Silver Transformation Job (PySpark)

Reads raw Bronze tables from MinIO (s3a://lakehouse/topics/raw_*),
applies cleaning, deduplication, and standardisation, then writes
cleaned Silver Delta tables back to MinIO (s3a://lakehouse/topics/stg_*)
and registers them under the delta.silver schema in Trino.

Cluster: apache/spark:3.5.3-scala2.12-java11-python3-ubuntu (Standalone)

Submit from the host machine with:
    uv run spark-submit \\
        --master spark://127.0.0.1:7077 \\
        --packages io.delta:delta-spark_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
        source/scripts/main/4a_silver_transform.py

Prerequisites:
    - MinIO + Trino + Hive Metastore running (make up)
    - spark-master + spark-worker running (see docker-compose.yaml)
    - Bronze tables already ingested (run 2a_ingest_to_bronze.py first)
    - pyspark==3.5.6 installed: uv add pyspark==3.5.6
"""

import os
import time
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from pyspark.sql import SparkSession
from utils import build_spark_session, register_tables_in_trino, run_with_spark_submit, run_data_assertions

# ── Environment ────────────────────────────────────────────────────────────────
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

MINIO_PORT        = os.getenv("MINIO_PORT",        "9000")
MINIO_HOST        = os.getenv("MINIO_HOST",         "localhost")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ROOT_USER",    "minioadmin")
MINIO_SECRET_KEY  = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET            = "lakehouse"
BRONZE_BASE       = f"s3a://{BUCKET}/topics"
SILVER_BASE       = f"s3a://{BUCKET}/topics"

TRINO_HOST        = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT        = int(os.getenv("TRINO_PORT", "8090"))
TRINO_USER        = os.getenv("TRINO_USER", "trino")

# Prefer explicit SPARK_MASTER_URL; fall back to composing from SPARK_MASTER_PORT
_spark_master_port = os.getenv("SPARK_MASTER_PORT", "7077")
SPARK_MASTER       = os.getenv("SPARK_MASTER_URL", f"spark://127.0.0.1:{_spark_master_port}")

# Silver table name mapping: bronze → silver
TABLES = {
    "raw_patients":       "stg_patients",
    "raw_wards":          "stg_wards",
    "raw_event_metadata": "stg_event_metadata",
    "raw_visits":         "stg_visits",
    "raw_events":         "stg_events",
}


# ── Spark Session and Trino Registration imported from utils ────────────────────


# ── Transformation Functions ───────────────────────────────────────────────────

def transform_patients(spark: SparkSession) -> int:
    """
    stg_patients: deduplicate on patient_id, cast date columns.
    No schema evolution issue here — all columns were always present.
    """
    logger.info("Transforming raw_patients → stg_patients ...")
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/raw_patients")

    before_count = df.count()

    df_clean = (
        df
        .dropDuplicates(["patient_id"])
        .withColumn("dob",           F.col("dob").cast(DateType()))
        .withColumn("date_of_death", F.col("date_of_death").cast(DateType()))
        # Standardise country code to uppercase
        .withColumn("country", F.upper(F.col("country")))
        # Add Silver metadata
        .withColumn("silver_ingest_ts", F.current_timestamp())
    )

    after_count = df_clean.count()
    dupes_removed = before_count - after_count
    logger.info(f"  stg_patients: {before_count:,} → {after_count:,} rows (removed {dupes_removed:,} dupes)")

    # Run validations
    run_data_assertions("stg_patients", df_clean)

    (
        df_clean.write
        .format("delta")
        .mode("overwrite")
        .save(f"{SILVER_BASE}/stg_patients")
    )
    return after_count


def transform_wards(spark: SparkSession) -> int:
    """
    stg_wards: deduplicate on ward_id, clean ward_name formatting.
    Small dimension table — no partitioning needed.
    """
    logger.info("Transforming raw_wards → stg_wards ...")
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/raw_wards")

    before_count = df.count()

    df_clean = (
        df
        .dropDuplicates(["ward_id"])
        # Normalise ward_name: replace underscores with spaces
        .withColumn("ward_name", F.regexp_replace(F.col("ward_id"), "_", " "))
        .withColumn("silver_ingest_ts", F.current_timestamp())
    )

    after_count = df_clean.count()
    logger.info(f"  stg_wards: {before_count:,} → {after_count:,} rows")

    # Run validations
    run_data_assertions("stg_wards", df_clean)

    (
        df_clean.write
        .format("delta")
        .mode("overwrite")
        .save(f"{SILVER_BASE}/stg_wards")
    )
    return after_count


def transform_event_metadata(spark: SparkSession) -> int:
    """
    stg_event_metadata: deduplicate lookup table on event_type_id.
    Small reference table — no partitioning needed.
    """
    logger.info("Transforming raw_event_metadata → stg_event_metadata ...")
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/raw_event_metadata")

    before_count = df.count()

    df_clean = (
        df
        .dropDuplicates(["event_type_id"])
        .withColumn("silver_ingest_ts", F.current_timestamp())
    )

    after_count = df_clean.count()
    logger.info(f"  stg_event_metadata: {before_count:,} → {after_count:,} rows")

    # Run validations
    run_data_assertions("stg_event_metadata", df_clean)

    (
        df_clean.write
        .format("delta")
        .mode("overwrite")
        .save(f"{SILVER_BASE}/stg_event_metadata")
    )
    return after_count


def transform_visits(spark: SparkSession) -> int:
    """
    stg_visits: handle schema evolution (severity_level NULL for older visits),
    validate timestamps, add derived visit_duration_mins.

    Schema evolution issue (from generator):
      - Visits with admission_timestamp < 2023-08-01 have NULL severity_level
      - Silver layer coalesces these to 'UNKNOWN' so downstream models
        don't encounter unexpected NULLs in this column.

    Partitioned by visit_date (NOT by ward_id — 85% skew in General_Ward
    would create massively unbalanced partitions).
    """
    logger.info("Transforming raw_visits → stg_visits ...")
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/raw_visits")

    before_count = df.count()

    df_clean = (
        df
        .dropDuplicates(["visit_id"])
        # Handle schema evolution: fill NULLs with 'UNKNOWN'
        .withColumn(
            "severity_level",
            F.coalesce(F.col("severity_level"), F.lit("UNKNOWN"))
        )
        # Derived column: visit duration in minutes (NULL if no discharge yet)
        .withColumn(
            "visit_duration_mins",
            F.when(
                F.col("discharge_timestamp").isNotNull(),
                (
                    F.unix_timestamp("discharge_timestamp") -
                    F.unix_timestamp("admission_timestamp")
                ) / 60
            ).otherwise(F.lit(None))
        )
        # Filter out impossible records: discharge before admission
        .filter(
            F.col("discharge_timestamp").isNull() |
            (F.col("discharge_timestamp") >= F.col("admission_timestamp"))
        )
        .withColumn("silver_ingest_ts", F.current_timestamp())
    )

    after_count = df_clean.count()
    logger.info(f"  stg_visits: {before_count:,} → {after_count:,} rows")

    # Run validations
    run_data_assertions("stg_visits", df_clean)

    (
        df_clean.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("visit_date")   # date-based, avoids ward_id skew
        .save(f"{SILVER_BASE}/stg_visits")
    )
    return after_count


def transform_events(spark: SparkSession) -> int:
    """
    stg_events: the most important transformation — removes the 2% duplicate
    rate injected by the generator using event_id as the dedup key.

    Also:
    - Validates event_type is in known set {vital, lab, medication, diagnosis}
    - Adds silver_ingest_ts watermark for lineage tracking
    - Partitioned by event_date (avoids skew from event_type or ward_id)

    Dedup strategy mirrors 2_process_skew_data.py approach but uses
    dropDuplicates on the business key rather than salting (dedup is
    partition-friendly since event_ids are unique per partition).
    """
    logger.info("Transforming raw_events → stg_events ...")
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/raw_events")

    before_count = df.count()

    # Known valid event types from the generator's EVENT_METADATA_DEFS
    valid_event_types = ["vital", "lab", "medication", "diagnosis"]

    df_clean = (
        df
        # Deduplicate: keep one row per event_id (removes 2% injected dupes)
        # Sort by event_timestamp descending so dropDuplicates keeps the latest
        .orderBy(F.col("event_timestamp").desc())
        .dropDuplicates(["event_id"])
        # Quarantine records with unknown event_type (log but don't drop)
        .withColumn(
            "is_valid_event_type",
            F.col("event_type").isin(valid_event_types)
        )
        .withColumn("silver_ingest_ts", F.current_timestamp())
    )

    after_count = df_clean.count()
    dupes_removed = before_count - after_count
    dupe_pct = (dupes_removed / before_count * 100) if before_count > 0 else 0

    invalid_count = df_clean.filter(~F.col("is_valid_event_type")).count()

    logger.info(f"  stg_events: {before_count:,} → {after_count:,} rows")
    logger.info(f"  Duplicates removed: {dupes_removed:,} ({dupe_pct:.2f}%)")
    logger.info(f"  Unknown event_type rows flagged: {invalid_count:,}")

    # Run validations
    run_data_assertions("stg_events", df_clean)

    (
        df_clean.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_date")   # date-based partitioning, avoids event_type/ward skew
        .save(f"{SILVER_BASE}/stg_events")
    )
    return after_count


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    run_start = time.time()
    run_id    = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 60)
    logger.info(f"EHR SILVER TRANSFORMATION — run_id={run_id}")
    logger.info("=" * 60)

    spark = build_spark_session("EHR-Silver-Transformation")
    row_counts = {}

    try:
        # Run transformations in dependency order (dim tables first, then facts)
        row_counts["stg_patients"]       = transform_patients(spark)
        row_counts["stg_wards"]          = transform_wards(spark)
        row_counts["stg_event_metadata"] = transform_event_metadata(spark)
        row_counts["stg_visits"]         = transform_visits(spark)
        row_counts["stg_events"]         = transform_events(spark)

        logger.info("=" * 60)
        logger.info("Silver table row counts:")
        for table, count in row_counts.items():
            logger.info(f"  {table:<25}: {count:>10,} rows")

        # Register all tables in Trino
        register_tables_in_trino("silver", list(TABLES.values()))

    finally:
        spark.stop()

    elapsed = time.time() - run_start
    logger.info("=" * 60)
    logger.info(f"Silver transformation complete. run_id={run_id}, elapsed={elapsed:.1f}s")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_with_spark_submit(__file__)
    main()
