"""
EHR Feature Engineering Job (PySpark)

Reads Gold Delta tables from MinIO and computes pre-aggregated patient-level
feature tables over a 6-month lookback window for the Feast offline store.

Output tables written as Delta and registered in delta.gold:
  feat_patient_vitals_6m      avg/min/max/std for 4 vital measurements
  feat_patient_labs_6m        avg/min/max/std for 4 lab tests
  feat_patient_icd_6m         occurrence count per ICD-10 chapter (22 cols)
  feat_patient_medication_6m  administration count per medication (4 cols)
  feat_patient_demographics   gender, ethnicity, country, age, is_deceased

The `as_of_ts` is derived from max(event_timestamp) in the OBT by default,
which makes the job correct for historical synthetic datasets. Set env var
FEATURE_AS_OF_DATE (ISO format: 2024-01-01) to override.

Submit with:
    uv run spark-submit \\
        --master spark://127.0.0.1:7077 \\
        --packages io.delta:delta-spark_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
        source/scripts/main/5_compute_features.py
"""

import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from utils import build_spark_session, load_event_metadata, ICD10_CHAPTERS, register_tables_in_trino, run_with_spark_submit

# ── Environment ────────────────────────────────────────────────────────────────
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

MINIO_PORT       = os.getenv("MINIO_PORT",        "9000")
MINIO_HOST       = os.getenv("MINIO_HOST",         "localhost")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER",    "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET           = "lakehouse"
GOLD_BASE        = f"s3a://{BUCKET}/topics"

TRINO_HOST       = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT       = int(os.getenv("TRINO_PORT", "8090"))
TRINO_USER       = os.getenv("TRINO_USER", "trino")

# Prefer explicit SPARK_MASTER_URL; fall back to composing from SPARK_MASTER_PORT
_spark_master_port = os.getenv("SPARK_MASTER_PORT", "7077")
SPARK_MASTER       = os.getenv("SPARK_MASTER_URL", f"spark://127.0.0.1:{_spark_master_port}")

LOOKBACK_MONTHS  = 6

FEAT_TABLES = [
    "feat_patient_vitals_6m",
    "feat_patient_labs_6m",
    "feat_patient_icd_6m",
    "feat_patient_medication_6m",
    "feat_patient_demographics",
]

LABEL_TABLES = [
    "gold_visit_labels",
]

# ICD10_CHAPTERS is imported from utils

def _snake(name: str) -> str:
    """Convert an event_name to a safe column prefix, e.g. 'Heart Rate' -> 'heart_rate'."""
    return name.lower().replace(" ", "_").replace("-", "_")


@udf(returnType=StringType())
def extract_icd_chapter_udf(event_description: str):
    """
    Parses an ICD-10 code from strings like
    'Essential primary hypertension (ICD-10 I10)'
    and maps it to its ICD-10 chapter roman numeral.
    Self-contained so Spark can serialise it to workers.
    """
    import re
    from utils import ICD10_CHAPTERS as _CHAPTERS
    if not event_description:
        return None
    match = re.search(r'ICD-10\s+([A-Z][0-9A-Z.]+)', event_description)
    if not match:
        return None
    code = match.group(1).strip().upper().replace(".", "")
    c0 = code[0]
    c2 = code[:2] if len(code) >= 2 else code
    for roman, lo, hi in _CHAPTERS:
        if lo == hi and len(lo) == 1:
            if c0 == lo:
                return roman
        else:
            if lo <= c2 <= hi or lo <= c0 <= hi:
                return roman
    return None


# build_spark_session is imported from utils


def resolve_as_of_ts(spark: SparkSession) -> str:
    """
    Determine the as-of timestamp for feature computation.
    Uses FEATURE_AS_OF_DATE env var if set, otherwise falls back to
    max(event_timestamp) from the OBT so the 6-month window captures
    real data even when running against a historical synthetic dataset.
    """
    override = os.getenv("FEATURE_AS_OF_DATE")
    if override:
        logger.info(f"Using FEATURE_AS_OF_DATE override: {override}")
        return override

    logger.info("FEATURE_AS_OF_DATE not set — deriving as_of_ts from max(event_timestamp) in OBT ...")
    row = (
        spark.read.format("delta")
        .load(f"{GOLD_BASE}/obt_clinical_events")
        .agg(F.max("event_timestamp").alias("max_ts"))
        .collect()[0]
    )
    as_of_ts = row["max_ts"].strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Derived as_of_ts: {as_of_ts}")
    return as_of_ts


def load_obt_window(spark: SparkSession, as_of_ts: str):
    """Load OBT filtered to the 6-month lookback window from as_of_ts."""
    cutoff = F.add_months(F.lit(as_of_ts).cast("timestamp"), -LOOKBACK_MONTHS)
    df = (
        spark.read.format("delta")
        .load(f"{GOLD_BASE}/obt_clinical_events")
        .filter(F.col("event_timestamp").between(cutoff, F.lit(as_of_ts).cast("timestamp")))
    )
    logger.info(f"OBT window [{as_of_ts} - {LOOKBACK_MONTHS}m]: {df.count():,} events")
    return df


# load_event_metadata is imported from utils


# ── Feature Computations ───────────────────────────────────────────────────────

def compute_vitals_features(df_obt, vital_names: list[str], as_of_ts: str) -> int:
    """
    For each vital name in vital_names (loaded from dim_event_type), computes
    mean / min / max / std of num_value per patient over the lookback window.
    Column names are derived automatically: 'Heart Rate' -> heart_rate_mean, etc.
    """
    logger.info(f"Computing feat_patient_vitals_6m for {len(vital_names)} vitals: {vital_names}")
    df = df_obt.filter(F.col("event_type") == "vital")

    agg_exprs = [
        expr
        for name in vital_names
        for expr in [
            F.avg  (F.when(F.col("event_name") == name, F.col("num_value"))).alias(f"{_snake(name)}_mean"),
            F.min  (F.when(F.col("event_name") == name, F.col("num_value"))).alias(f"{_snake(name)}_min"),
            F.max  (F.when(F.col("event_name") == name, F.col("num_value"))).alias(f"{_snake(name)}_max"),
            F.stddev(F.when(F.col("event_name") == name, F.col("num_value"))).alias(f"{_snake(name)}_std"),
        ]
    ]

    df_feat = (
        df.groupBy("patient_id").agg(*agg_exprs)
        .withColumn("feature_timestamp", F.lit(as_of_ts).cast("timestamp"))
    )

    df_feat.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/feat_patient_vitals_6m")
    count = df_feat.count()
    logger.info(f"  feat_patient_vitals_6m: {count:,} patients")
    return count


def compute_labs_features(df_obt, lab_names: list[str], as_of_ts: str) -> int:
    """
    For each lab name in lab_names (loaded from dim_event_type), computes
    mean / min / max / std of num_value per patient over the lookback window.
    """
    logger.info(f"Computing feat_patient_labs_6m for {len(lab_names)} labs: {lab_names}")
    df = df_obt.filter(F.col("event_type") == "lab")

    agg_exprs = [
        expr
        for name in lab_names
        for expr in [
            F.avg  (F.when(F.col("event_name") == name, F.col("num_value"))).alias(f"{_snake(name)}_mean"),
            F.min  (F.when(F.col("event_name") == name, F.col("num_value"))).alias(f"{_snake(name)}_min"),
            F.max  (F.when(F.col("event_name") == name, F.col("num_value"))).alias(f"{_snake(name)}_max"),
            F.stddev(F.when(F.col("event_name") == name, F.col("num_value"))).alias(f"{_snake(name)}_std"),
        ]
    ]

    df_feat = (
        df.groupBy("patient_id").agg(*agg_exprs)
        .withColumn("feature_timestamp", F.lit(as_of_ts).cast("timestamp"))
    )

    df_feat.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/feat_patient_labs_6m")
    count = df_feat.count()
    logger.info(f"  feat_patient_labs_6m: {count:,} patients")
    return count


def compute_icd_features(spark: SparkSession, df_obt, as_of_ts: str) -> int:
    """
    Maps each diagnosis event's ICD-10 code (embedded in event_description
    from dim_event_type) to one of 22 ICD-10 chapters and counts occurrences
    per patient. event_description is NOT in the OBT, so we join dim_event_type.
    """
    logger.info("Computing feat_patient_icd_6m ...")
    df_dim = (
        spark.read.format("delta")
        .load(f"{GOLD_BASE}/dim_event_type")
        .select("event_type_id", "event_description")
    )

    df_diag = (
        df_obt
        .filter(F.col("event_type") == "diagnosis")
        .join(df_dim, on="event_type_id", how="inner")
        .withColumn("icd_chapter", extract_icd_chapter_udf(F.col("event_description")))
        .filter(F.col("icd_chapter").isNotNull())
    )

    df_feat = (
        df_diag.groupBy("patient_id").agg(
            *[
                F.sum(F.when(F.col("icd_chapter") == roman, 1).otherwise(0))
                 .cast("long")
                 .alias(f"icd_chap_{roman}")
                for roman, _, _ in ICD10_CHAPTERS
            ]
        )
        .withColumn("feature_timestamp", F.lit(as_of_ts).cast("timestamp"))
    )

    df_feat.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/feat_patient_icd_6m")
    count = df_feat.count()
    logger.info(f"  feat_patient_icd_6m: {count:,} patients")
    return count


def compute_medication_features(df_obt, medication_names: list[str], as_of_ts: str) -> int:
    """
    For each medication name in medication_names (loaded from dim_event_type),
    counts the number of administrations per patient over the lookback window.
    """
    logger.info(f"Computing feat_patient_medication_6m for {len(medication_names)} medications: {medication_names}")
    df = df_obt.filter(F.col("event_type") == "medication")

    agg_exprs = [
        F.sum(F.when(F.col("event_name") == name, 1).otherwise(0))
         .cast("long")
         .alias(f"{_snake(name)}_count")
        for name in medication_names
    ]

    df_feat = (
        df.groupBy("patient_id").agg(*agg_exprs)
        .withColumn("feature_timestamp", F.lit(as_of_ts).cast("timestamp"))
    )

    df_feat.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/feat_patient_medication_6m")
    count = df_feat.count()
    logger.info(f"  feat_patient_medication_6m: {count:,} patients")
    return count


def compute_demographics_features(spark: SparkSession, as_of_ts: str) -> int:
    """
    Static patient-level features derived from dim_patient.
    Age is computed relative to as_of_ts so it stays meaningful
    when the feature snapshot is used for historical training.
    """
    logger.info("Computing feat_patient_demographics ...")
    df_pat = spark.read.format("delta").load(f"{GOLD_BASE}/dim_patient")

    df_feat = (
        df_pat
        .withColumn(
            "age",
            F.round(
                F.datediff(F.lit(as_of_ts).cast("date"), F.col("dob")) / 365.25,
                1
            )
        )
        .withColumn(
            "is_deceased",
            F.when(F.col("date_of_death").isNotNull(), 1).otherwise(0).cast("long")
        )
        .withColumn("feature_timestamp", F.lit(as_of_ts).cast("timestamp"))
        .select("patient_id", "feature_timestamp", "gender", "ethnic", "country", "age", "is_deceased")
    )

    df_feat.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/feat_patient_demographics")
    count = df_feat.count()
    logger.info(f"  feat_patient_demographics: {count:,} patients")
    return count


# ── Label Computation ─────────────────────────────────────────────────────────

def compute_visit_labels(spark: SparkSession, as_of_ts: str) -> int:
    """
    Computes per-visit training labels from gold tables using Spark window functions.
    Only completed visits (discharge_timestamp IS NOT NULL) are labelled.

    Labels:
      is_readmitted_7d       — patient had a new admission ≤ 7 days after this discharge
      has_inpatient_mortality — patient died during this visit
      has_30day_mortality    — patient died within 30 days after discharge

    No data generation changes needed:
      - Readmission uses LEAD() over visits ordered by admission_timestamp per patient.
      - Mortality uses date_of_death already present in dim_patient (~1% of patients).
      Class imbalance (low positive rate) is expected; handle at training time via
      class weights or oversampling.

    Output: gold_visit_labels — join with feat_* tables on patient_id to build
    training datasets for 8_build_training_dataset.py.
    """
    logger.info("Computing gold_visit_labels ...")

    df_visits = spark.read.format("delta").load(f"{GOLD_BASE}/fact_visit")
    df_patients = (
        spark.read.format("delta")
        .load(f"{GOLD_BASE}/dim_patient")
        .select("patient_key", "patient_id", "date_of_death")
    )

    # Window to find the next admission per patient (for readmission label)
    w_next = Window.partitionBy("patient_key").orderBy("admission_timestamp")

    df = (
        df_visits
        .join(df_patients, on="patient_key", how="left")
        .withColumn("_next_admission_ts", F.lead("admission_timestamp").over(w_next))

        # Label 1: readmitted within 7 days of this discharge
        .withColumn(
            "is_readmitted_7d",
            F.when(
                F.col("discharge_timestamp").isNotNull() &
                F.col("_next_admission_ts").isNotNull() &
                (F.datediff(F.col("_next_admission_ts"), F.col("discharge_timestamp")) <= 7),
                1
            ).otherwise(0).cast("int")
        )

        # Label 2: patient died during this visit
        .withColumn(
            "has_inpatient_mortality",
            F.when(
                F.col("date_of_death").isNotNull() &
                F.col("discharge_timestamp").isNotNull() &
                F.col("date_of_death").cast("date").between(
                    F.col("admission_timestamp").cast("date"),
                    F.col("discharge_timestamp").cast("date")
                ),
                1
            ).otherwise(0).cast("int")
        )

        # Label 3: patient died within 30 days post-discharge
        .withColumn(
            "has_30day_mortality",
            F.when(
                F.col("date_of_death").isNotNull() &
                F.col("discharge_timestamp").isNotNull() &
                (
                    F.col("date_of_death").cast("date") <=
                    F.date_add(F.col("discharge_timestamp").cast("date"), 30)
                ),
                1
            ).otherwise(0).cast("int")
        )
        .drop("_next_admission_ts", "date_of_death")
        # Only completed visits are labelable as training targets
        .filter(F.col("discharge_timestamp").isNotNull())
        .select(
            "visit_id",
            "patient_id",
            "patient_key",
            "admission_timestamp",
            "discharge_timestamp",
            "severity_level",
            "is_readmitted_7d",
            "has_inpatient_mortality",
            "has_30day_mortality",
            F.lit(as_of_ts).cast("timestamp").alias("feature_timestamp"),
        )
    )

    df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/gold_visit_labels")
    count = df.count()

    n_readmit  = df.filter(F.col("is_readmitted_7d") == 1).count()
    n_inhosp   = df.filter(F.col("has_inpatient_mortality") == 1).count()
    n_30day    = df.filter(F.col("has_30day_mortality") == 1).count()
    logger.info(f"  gold_visit_labels: {count:,} visits")
    logger.info(f"    is_readmitted_7d:       {n_readmit:,} ({n_readmit / count * 100:.1f}%)")
    logger.info(f"    has_inpatient_mortality: {n_inhosp:,}  ({n_inhosp  / count * 100:.1f}%)")
    logger.info(f"    has_30day_mortality:     {n_30day:,}  ({n_30day   / count * 100:.1f}%)")
    return count


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    run_start = time.time()
    run_id    = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 60)
    logger.info(f"EHR FEATURE ENGINEERING — run_id={run_id}")
    logger.info(f"  lookback={LOOKBACK_MONTHS} months")
    logger.info("=" * 60)

    spark = build_spark_session("EHR-Feature-Engineering")
    spark.sparkContext.addPyFile(os.path.join(script_dir, "utils.py"))
    row_counts = {}

    try:
        as_of_ts = resolve_as_of_ts(spark)
        logger.info(f"as_of_ts = {as_of_ts}")

        # Load event names from dim_event_type (Gold) — single source of truth.
        # Adding a new vital/lab/medication to the data pipeline automatically
        # extends the feature set here without any code changes.
        event_meta = load_event_metadata(spark)
        vital_names      = event_meta.get("vital",      [])
        lab_names        = event_meta.get("lab",        [])
        medication_names = event_meta.get("medication", [])

        df_obt = load_obt_window(spark, as_of_ts)
        df_obt.cache()

        row_counts["feat_patient_vitals_6m"]    = compute_vitals_features(df_obt, vital_names, as_of_ts)
        row_counts["feat_patient_labs_6m"]      = compute_labs_features(df_obt, lab_names, as_of_ts)
        row_counts["feat_patient_icd_6m"]       = compute_icd_features(spark, df_obt, as_of_ts)
        row_counts["feat_patient_medication_6m"]= compute_medication_features(df_obt, medication_names, as_of_ts)
        row_counts["feat_patient_demographics"] = compute_demographics_features(spark, as_of_ts)
        row_counts["gold_visit_labels"]         = compute_visit_labels(spark, as_of_ts)

        logger.info("=" * 60)
        logger.info("Row counts:")
        for table, count in row_counts.items():
            logger.info(f"  {table:<35}: {count:>10,} rows")

        register_tables_in_trino("gold", FEAT_TABLES + LABEL_TABLES)

    finally:
        spark.stop()

    elapsed = time.time() - run_start
    logger.info("=" * 60)
    logger.info(f"Feature engineering complete. run_id={run_id}, elapsed={elapsed:.1f}s")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_with_spark_submit(__file__)
    main()
