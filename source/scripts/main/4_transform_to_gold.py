"""
EHR Gold Transformation Job (PySpark)

Reads Silver tables from MinIO (s3a://lakehouse/topics/stg_*),
models them into conformed dimensions, facts, and an OBT (One Big Table),
writes them to Gold Delta tables (s3a://lakehouse/topics/{dim_*, fact_*, obt_*}),
and registers them under the delta.gold schema in Trino.

Submit from the host machine with:
    uv run spark-submit \
        --master spark://127.0.0.1:7077 \
        --packages io.delta:delta-spark_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        source/scripts/main/4_transform_to_gold.py
"""

import os
import time
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import functions as F

from pyspark.sql import SparkSession
from utils import build_spark_session, register_tables_in_trino, run_with_spark_submit

# ── Environment ────────────────────────────────────────────────────────────────
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

MINIO_PORT        = os.getenv("MINIO_PORT",        "9000")
MINIO_HOST        = os.getenv("MINIO_HOST",         "localhost")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ROOT_USER",    "minioadmin")
MINIO_SECRET_KEY  = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET            = "lakehouse"
SILVER_BASE       = f"s3a://{BUCKET}/topics"
GOLD_BASE         = f"s3a://{BUCKET}/topics"

TRINO_HOST        = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT        = int(os.getenv("TRINO_PORT", "8090"))
TRINO_USER        = os.getenv("TRINO_USER", "trino")

# Prefer explicit SPARK_MASTER_URL; fall back to composing from SPARK_MASTER_PORT
_spark_master_port = os.getenv("SPARK_MASTER_PORT", "7077")
SPARK_MASTER       = os.getenv("SPARK_MASTER_URL", f"spark://127.0.0.1:{_spark_master_port}")

GOLD_TABLES = [
    "dim_patient",
    "dim_ward",
    "dim_event_type",
    "dim_vital",
    "dim_lab",
    "dim_medication",
    "dim_diagnosis",
    "fact_visit",
    "fact_clinical_event",
    "obt_clinical_events",
]

# ── Spark Session and Trino Registration imported from utils ────────────────────

# ── Dimension Transformers ─────────────────────────────────────────────────────

def transform_dim_patient(spark: SparkSession) -> int:
    logger.info("Building dim_patient...")
    df_stg = spark.read.format("delta").load(f"{SILVER_BASE}/stg_patients")
    
    df_dim = (
        df_stg
        .withColumn("patient_key", F.sha2(F.col("patient_id"), 256))
        .select(
            "patient_key",
            "patient_id",
            "gender",
            "ethnic",
            "country",
            "dob",
            "date_of_death"
        )
    )
    
    df_dim.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_patient")
    return df_dim.count()


def transform_dim_ward(spark: SparkSession) -> int:
    logger.info("Building dim_ward...")
    df_stg = spark.read.format("delta").load(f"{SILVER_BASE}/stg_wards")
    
    df_dim = (
        df_stg
        .withColumn("ward_key", F.sha2(F.col("ward_id"), 256))
        .select(
            "ward_key",
            "ward_id",
            "ward_name",
            "department"
        )
    )
    
    df_dim.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_ward")
    return df_dim.count()


def transform_dim_event_type(spark: SparkSession) -> int:
    logger.info("Building dim_event_type...")
    df_stg = spark.read.format("delta").load(f"{SILVER_BASE}/stg_event_metadata")
    
    df_dim = (
        df_stg
        .withColumn("event_type_key", F.sha2(F.col("event_type_id"), 256))
        .select(
            "event_type_key",
            "event_type_id",
            "event_name",
            "event_description",
            "unit_of_measurement",
            "event_type"
        )
    )
    
    df_dim.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_event_type")
    return df_dim.count()


def _build_event_type_dim(spark: SparkSession, event_type: str) -> "DataFrame":
    """Filter stg_event_metadata to a single event_type and add surrogate key."""
    df_stg = spark.read.format("delta").load(f"{SILVER_BASE}/stg_event_metadata")
    return (
        df_stg
        .filter(F.col("event_type") == event_type)
        .withColumn("event_type_key", F.sha2(F.col("event_type_id"), 256))
        .select(
            "event_type_key",
            "event_type_id",
            "event_name",
            "event_description",
            "unit_of_measurement",
        )
    )


def transform_dim_vital(spark: SparkSession) -> int:
    logger.info("Building dim_vital...")
    df_dim = _build_event_type_dim(spark, "vital")
    df_dim.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_vital")
    return df_dim.count()


def transform_dim_lab(spark: SparkSession) -> int:
    logger.info("Building dim_lab...")
    df_dim = _build_event_type_dim(spark, "lab")
    df_dim.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_lab")
    return df_dim.count()


def transform_dim_medication(spark: SparkSession) -> int:
    logger.info("Building dim_medication...")
    df_dim = (
        _build_event_type_dim(spark, "medication")
        # Drop unit_of_measurement — not meaningful for medications
        .drop("unit_of_measurement")
    )
    df_dim.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_medication")
    return df_dim.count()


def transform_dim_diagnosis(spark: SparkSession) -> int:
    logger.info("Building dim_diagnosis...")
    df_dim = (
        _build_event_type_dim(spark, "diagnosis")
        # Drop unit_of_measurement — not meaningful for diagnoses
        .drop("unit_of_measurement")
    )
    df_dim.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_diagnosis")
    return df_dim.count()

# ── Fact Transformers ──────────────────────────────────────────────────────────

def transform_fact_visit(spark: SparkSession) -> int:
    logger.info("Building fact_visit...")
    df_stg = spark.read.format("delta").load(f"{SILVER_BASE}/stg_visits")
    
    df_fact = (
        df_stg
        .withColumn("patient_key", F.sha2(F.col("patient_id"), 256))
        .withColumn("ward_key", F.sha2(F.col("ward_id"), 256))
        .withColumn("is_active", F.when(F.col("discharge_timestamp").isNull(), 1).otherwise(0))
        .select(
            "visit_id",
            "patient_key",
            "ward_key",
            "admission_timestamp",
            "discharge_timestamp",
            "severity_level",
            "visit_duration_mins",
            "is_active"
        )
    )
    
    df_fact.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/fact_visit")
    return df_fact.count()


def transform_fact_clinical_event(spark: SparkSession) -> int:
    logger.info("Building fact_clinical_event...")
    df_events = spark.read.format("delta").load(f"{SILVER_BASE}/stg_events")
    df_visits = spark.read.format("delta").load(f"{SILVER_BASE}/stg_visits").select("visit_id", "patient_id", "ward_id")
    
    # Resolve keys by joining events with visit to get ward_id and patient_id
    df_resolved = df_events.join(df_visits, on="visit_id", how="inner")
    
    df_fact = (
        df_resolved
        .withColumn("patient_key", F.sha2(F.col("patient_id"), 256))
        .withColumn("ward_key", F.sha2(F.col("ward_id"), 256))
        .withColumn("event_type_key", F.sha2(F.col("event_type_id"), 256))
        .withColumn("event_year", F.year(F.col("event_timestamp")))
        .withColumn("event_month", F.month(F.col("event_timestamp")))
        .select(
            "event_id",
            "visit_id",
            "patient_key",
            "ward_key",
            "event_type_key",
            "event_timestamp",
            "text_value",
            "num_value",
            "event_year",
            "event_month"
        )
    )
    
    (
        df_fact.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_year", "event_month")
        .save(f"{GOLD_BASE}/fact_clinical_event")
    )
    return df_fact.count()

# ── OBT Transformer ────────────────────────────────────────────────────────────

def transform_obt(spark: SparkSession) -> int:
    logger.info("Building obt_clinical_events...")
    df_fact = spark.read.format("delta").load(f"{GOLD_BASE}/fact_clinical_event")
    df_pat = spark.read.format("delta").load(f"{GOLD_BASE}/dim_patient")
    df_ward = spark.read.format("delta").load(f"{GOLD_BASE}/dim_ward")
    df_evt_type = spark.read.format("delta").load(f"{GOLD_BASE}/dim_event_type")
    df_visit = spark.read.format("delta").load(f"{GOLD_BASE}/fact_visit").select(
        "visit_id", "admission_timestamp", "discharge_timestamp", "visit_duration_mins", "severity_level"
    )
    
    df_obt = (
        df_fact
        .join(df_pat, on="patient_key", how="inner")
        .join(df_ward, on="ward_key", how="inner")
        .join(df_evt_type, on="event_type_key", how="inner")
        .join(df_visit, on="visit_id", how="inner")
        .select(
            "event_id",
            "visit_id",
            "event_timestamp",
            "text_value",
            "num_value",
            "patient_id",
            "gender",
            "ethnic",
            "country",
            "dob",
            "date_of_death",
            "ward_id",
            "ward_name",
            "department",
            "admission_timestamp",
            "discharge_timestamp",
            "visit_duration_mins",
            "severity_level",
            "event_type",
            "event_type_id",
            "event_name",
            "unit_of_measurement",
            "event_year",
            "event_month"
        )
    )
    
    (
        df_obt.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_year", "event_month")
        .save(f"{GOLD_BASE}/obt_clinical_events")
    )
    return df_obt.count()

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    run_start = time.time()
    run_id    = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 60)
    logger.info(f"EHR GOLD TRANSFORMATION — run_id={run_id}")
    logger.info("=" * 60)

    spark = build_spark_session("EHR-Gold-Transformation")
    row_counts = {}

    try:
        # Build dimensions first, then facts, then OBT
        row_counts["dim_patient"]          = transform_dim_patient(spark)
        row_counts["dim_ward"]             = transform_dim_ward(spark)
        row_counts["dim_event_type"]       = transform_dim_event_type(spark)
        row_counts["dim_vital"]            = transform_dim_vital(spark)
        row_counts["dim_lab"]             = transform_dim_lab(spark)
        row_counts["dim_medication"]       = transform_dim_medication(spark)
        row_counts["dim_diagnosis"]        = transform_dim_diagnosis(spark)
        row_counts["fact_visit"]           = transform_fact_visit(spark)
        row_counts["fact_clinical_event"]  = transform_fact_clinical_event(spark)
        row_counts["obt_clinical_events"]  = transform_obt(spark)

        logger.info("=" * 60)
        logger.info("Gold table row counts:")
        for table, count in row_counts.items():
            logger.info(f"  {table:<25}: {count:>10,} rows")

        # Register all tables in Trino
        register_tables_in_trino("gold", GOLD_TABLES)

    finally:
        spark.stop()

    elapsed = time.time() - run_start
    logger.info("=" * 60)
    logger.info(f"Gold transformation complete. run_id={run_id}, elapsed={elapsed:.1f}s")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_with_spark_submit(__file__)
    main()
