import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from loguru import logger
import trino

# Load environment
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

MINIO_PORT        = os.getenv("MINIO_PORT",        "9000")
MINIO_HOST        = os.getenv("MINIO_HOST",         "localhost")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ROOT_USER",    "minioadmin")
MINIO_SECRET_KEY  = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET            = "lakehouse"

TRINO_HOST        = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT        = int(os.getenv("TRINO_PORT", "8090"))
TRINO_USER        = os.getenv("TRINO_USER", "trino")

_spark_master_port = os.getenv("SPARK_MASTER_PORT", "7077")
SPARK_MASTER       = os.getenv("SPARK_MASTER_URL", "local[*]")

SPARK_PACKAGES                = "io.delta:delta-spark_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
SPARK_EXECUTOR_CORES          = "2"
SPARK_EXECUTOR_MEMORY         = "16g"
SPARK_DRIVER_MEMORY           = "16g"
SPARK_SHUFFLE_PARTITIONS      = "8"
SPARK_S3A_MULTIPART_SIZE      = str(64 * 1024 * 1024)
SPARK_S3A_MULTIPART_THRESHOLD = str(64 * 1024 * 1024)
# bytebuffer pre-declares Content-Length=blocksize; the last chunk is smaller → IncompleteBody 400 from MinIO
SPARK_S3A_FAST_UPLOAD_BUFFER  = "disk"
SPARK_EVENT_LOG_ENABLED       = "true"
_spark_log_path               = os.path.abspath(os.path.join(project_root, "..", ".tmp", "spark-logs"))
try:
    os.makedirs(_spark_log_path, exist_ok=True)
except:
    pass

SPARK_EVENT_LOG_DIR           = f"file://{_spark_log_path}"

ICD10_CHAPTERS = [
    ("I",    "A",  "B"),
    ("II",   "C",  "D4"),
    ("III",  "D5", "D8"),
    ("IV",   "E",  "E"),
    ("V",    "F",  "F"),
    ("VI",   "G",  "G"),
    ("VII",  "H0", "H5"),
    ("VIII", "H6", "H9"),
    ("IX",   "I",  "I"),
    ("X",    "J",  "J"),
    ("XI",   "K",  "K"),
    ("XII",  "L",  "L"),
    ("XIII", "M",  "M"),
    ("XIV",  "N",  "N"),
    ("XV",   "O",  "O"),
    ("XVI",  "P",  "P"),
    ("XVII", "Q",  "Q"),
    ("XVIII","R",  "R"),
    ("XIX",  "S",  "T"),
    ("XX",   "V",  "Y"),
    ("XXI",  "Z",  "Z"),
    ("XXII", "U",  "U"),
]

def build_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Connecting to Spark cluster: {SPARK_MASTER} for {app_name}")
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(app_name)
        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint",               f"http://{MINIO_HOST}:{MINIO_PORT}")
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.upload",            "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer",     SPARK_S3A_FAST_UPLOAD_BUFFER)
        .config("spark.hadoop.fs.s3a.multipart.size",         SPARK_S3A_MULTIPART_SIZE)
        .config("spark.hadoop.fs.s3a.multipart.threshold",    SPARK_S3A_MULTIPART_THRESHOLD)
        # Delta Lake
        .config("spark.sql.extensions",          "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Performance
        .config("spark.executor.cores",               SPARK_EXECUTOR_CORES)
        .config("spark.executor.memory",              SPARK_EXECUTOR_MEMORY)
        .config("spark.driver.memory",                SPARK_DRIVER_MEMORY)
        .config("spark.sql.adaptive.enabled",         "true")
        .config("spark.sql.adaptive.skewJoin.enabled","true")
        .config("spark.sql.shuffle.partitions",       SPARK_SHUFFLE_PARTITIONS)
        # Event logging
        .config("spark.eventLog.enabled", SPARK_EVENT_LOG_ENABLED)
        .config("spark.eventLog.dir",     SPARK_EVENT_LOG_DIR)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark version: {spark.version}")
    return spark

def load_event_metadata(spark: SparkSession, gold_base: str = None) -> dict[str, list[str]]:
    """
    Reads dim_event_type from Gold and returns event names grouped by event_type.
    """
    if not gold_base:
        gold_base = f"s3a://{BUCKET}/topics"
    logger.info("Loading event metadata from dim_event_type (Gold)...")
    rows = (
        spark.read.format("delta")
        .load(f"{gold_base}/dim_event_type")
        .select("event_type", "event_name")
        .distinct()
        .orderBy("event_type", "event_name")
        .collect()
    )
    metadata: dict[str, list[str]] = {}
    for row in rows:
        metadata.setdefault(row["event_type"], []).append(row["event_name"])

    for event_type, names in metadata.items():
        logger.info(f"  {event_type}: {names}")
    return metadata

def register_tables_in_trino(schema_name: str, tables: list[str], bucket: str = None):
    """
    Creates delta.{schema_name} schema and registers all tables in Trino.
    """
    if not bucket:
        bucket = BUCKET
    logger.info(f"Registering tables in Trino under delta.{schema_name} ...")
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
    )
    cursor = conn.cursor()

    # Create schema
    cursor.execute(f"""
        CREATE SCHEMA IF NOT EXISTS delta.{schema_name}
        WITH (location = 's3://{bucket}/')
    """)
    logger.info(f"  delta.{schema_name} schema ensured.")

    for table in tables:
        logger.info(f"  Registering delta.{schema_name}.{table} ...")
        cursor.execute(f"DROP TABLE IF EXISTS delta.{schema_name}.{table}")
        cursor.execute(f"""
            CALL delta.system.register_table(
                schema_name  => '{schema_name}',
                table_name   => '{table}',
                table_location => 's3://{bucket}/topics/{table}/'
            )
        """)

    cursor.close()
    conn.close()
    logger.info(f"  All tables registered in Trino under delta.{schema_name}.")


def run_with_spark_submit(file_path: str):
    """
    Submits the script via spark-submit if it's not already running in a Spark environment.
    Exits the calling process if it delegates to spark-submit.
    """
    import sys
    import subprocess

    is_submitted = (
        "spark-submit" in sys.argv[0] or 
        os.environ.get("SPARK_ENV_LOADED") == "1" or
        "SPARK_HOME" in os.environ
    )
    
    if not is_submitted:
        logger.info("Script was not started with spark-submit. Submitting job to cluster via subprocess...")
        spark_master = os.getenv("SPARK_MASTER_URL", "local[*]")
        
        cmd = [
            "spark-submit",
            "--master", spark_master,
            "--packages", SPARK_PACKAGES,
            file_path
        ]
        
        logger.info(f"Running command: {' '.join(cmd)}")
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
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


def to_col(event_name: str) -> str:
    """Standardize event name to column format."""
    return event_name.lower().replace(" ", "_").replace("-", "_")


def icd_code_to_chapter(code: str) -> str | None:
    """Map ICD-10 code to its corresponding chapter Roman numeral."""
    if not isinstance(code, str) or not code:
        return None
    code = code.strip().upper().replace(".", "")
    c0, c2 = code[0], code[:2] if len(code) >= 2 else code
    for roman, lo, hi in ICD10_CHAPTERS:
        if lo == hi and len(lo) == 1:
            if c0 == lo:
                return roman
        else:
            if lo <= c2 <= hi or lo <= c0 <= hi:
                return roman
    return None


def build_icd_chapter_map(df_diag) -> dict[str, list[str]]:
    """Build a mapping of ICD-10 chapter to list of event names."""
    import re
    from collections import defaultdict
    chapter_to_names = defaultdict(list)
    for _, row in df_diag.iterrows():
        match = re.search(r'ICD-10\s+([A-Z][0-9A-Z.]+)', str(row.get("event_description", "")))
        if not match:
            continue
        chapter = icd_code_to_chapter(match.group(1))
        if chapter:
            chapter_to_names[chapter].append(row["event_name"])
    return dict(chapter_to_names)


def read_delta_table_as_pandas(table_name: str, storage_options: dict):
    """Read a Delta table from S3/MinIO into a pandas DataFrame."""
    from deltalake import DeltaTable
    return (
        DeltaTable(f"s3://{BUCKET}/topics/{table_name}", storage_options=storage_options)
        .to_pandas()
    )


def get_trino_connection():
    """Returns a connection to Trino."""
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
    )
