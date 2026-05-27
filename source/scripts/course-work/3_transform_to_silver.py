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
        source/scripts/course-work/4a_silver_transform.py

Prerequisites:
    - MinIO + Trino + Hive Metastore running (make up)
    - spark-master + spark-worker running (see docker-compose.yaml)
    - Bronze tables already ingested (run 2a_ingest_to_bronze.py first)
    - pyspark==3.5.6 installed: uv add pyspark==3.5.6
"""

import os
import time
from datetime import datetime

import trino
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

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

SPARK_MASTER      = os.getenv("SPARK_MASTER_URL", "spark://127.0.0.1:7077")

# Silver table name mapping: bronze → silver
TABLES = {
    "raw_patients":       "stg_patients",
    "raw_wards":          "stg_wards",
    "raw_event_metadata": "stg_event_metadata",
    "raw_visits":         "stg_visits",
    "raw_events":         "stg_events",
}


# ── Spark Session ──────────────────────────────────────────────────────────────

def build_spark_session() -> SparkSession:
    """
    Builds a SparkSession connected to the Spark Standalone cluster.
    JARs and S3A config are loaded from spark-defaults.conf which is
    mounted at /opt/spark/conf/spark-defaults.conf inside the containers.
    On the host side, spark-submit picks up the --packages flag from the conf.
    """
    logger.info(f"Connecting to Spark cluster: {SPARK_MASTER}")
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName("EHR-Silver-Transformation")
        # S3A credentials (also set in spark-defaults.conf, but explicit here
        # ensures the host-submitted driver can reach MinIO directly too)
        .config("spark.hadoop.fs.s3a.endpoint",              f"http://{MINIO_HOST}:{MINIO_PORT}")
        .config("spark.hadoop.fs.s3a.access.key",            MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",            MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.impl",                  "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Delta Lake extension (Spark 4.x, Scala 2.13 artifacts)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # AQE — auto-handles the 85% ward skew in visits/events
        .config("spark.sql.adaptive.enabled",          "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions",        "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark version: {spark.version}")
    return spark


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

    (
        df_clean.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_date")   # date-based partitioning, avoids event_type/ward skew
        .save(f"{SILVER_BASE}/stg_events")
    )
    return after_count


# ── Trino Registration ─────────────────────────────────────────────────────────

def register_silver_in_trino():
    """
    Creates delta.silver schema and registers all stg_* Silver tables in Trino.
    Follows the same pattern as 2a_ingest_to_bronze.py.
    """
    logger.info("Registering Silver tables in Trino under delta.silver ...")
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
    )
    cursor = conn.cursor()

    # 1. Create schema
    cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS delta.silver
        WITH (location = 's3://lakehouse/')
    """)
    logger.info("  delta.silver schema ensured.")

    # 2. Register each Silver table
    silver_tables = list(TABLES.values())  # stg_patients, stg_wards, etc.

    for table in silver_tables:
        logger.info(f"  Registering delta.silver.{table} ...")
        # Drop existing registration to allow clean re-registration on reruns
        cursor.execute(f"DROP TABLE IF EXISTS delta.silver.{table}")
        cursor.execute(f"""
            CALL delta.system.register_table(
                schema_name  => 'silver',
                table_name   => '{table}',
                table_location => 's3://lakehouse/topics/{table}/'
            )
        """)

    cursor.close()
    conn.close()
    logger.info("  All Silver tables registered in Trino.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    run_start = time.time()
    run_id    = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 60)
    logger.info(f"EHR SILVER TRANSFORMATION — run_id={run_id}")
    logger.info("=" * 60)

    spark = build_spark_session()
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
        register_silver_in_trino()

    finally:
        spark.stop()

    elapsed = time.time() - run_start
    logger.info("=" * 60)
    logger.info(f"Silver transformation complete. run_id={run_id}, elapsed={elapsed:.1f}s")
    logger.info("=" * 60)


if __name__ == "__main__":
    import sys
    import subprocess

    # When spark-submit runs, it sets env variables such as SPARK_ENV_LOADED or SPARK_HOME.
    # We check these to know if we are inside a spark-submit wrapper process.
    is_submitted = (
        "spark-submit" in sys.argv[0] or 
        os.environ.get("SPARK_ENV_LOADED") == "1" or
        "SPARK_HOME" in os.environ
    )
    
    if not is_submitted:
        logger.info("Script was not started with spark-submit. Submitting job to cluster via subprocess...")
        
        # Load Spark master URL and default port configuration
        spark_master = os.getenv("SPARK_MASTER_URL", "spark://127.0.0.1:7077")
        
        # Build the spark-submit command
        cmd = [
            "spark-submit",
            "--master", spark_master,
            "--packages", "io.delta:delta-spark_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            __file__
        ]
        
        logger.info(f"Running command: {' '.join(cmd)}")
        try:
            # Run spark-submit and pipe stderr to stdout to stream execution progress in real-time
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            
            # Stream logs in real-time
            for line in process.stdout:
                print(line, end="")
            
            process.wait()
            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, cmd)
                
            sys.exit(0)
        except subprocess.CalledProcessError as e:
            logger.error(f"Spark job submission failed with exit code: {e.returncode}")
            sys.exit(e.returncode)
        except Exception as e:
            logger.error(f"Failed to execute spark-submit: {e}")
            sys.exit(1)
    else:
        main()
