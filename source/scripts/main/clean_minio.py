import os
import trino
from minio import Minio
from dotenv import load_dotenv

# Load workspace environment variables
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

MINIO_PORT       = os.getenv("MINIO_PORT",          "9000")
MINIO_HOST       = os.getenv("MINIO_HOST",           "localhost")
MINIO_ENDPOINT   = f"{MINIO_HOST}:{MINIO_PORT}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER",     "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD",  "minioadmin")
BUCKET           = "lakehouse"

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8090"))
TRINO_USER = os.getenv("TRINO_USER", "trino")

# 1. Drop Tables in Trino (Hive Metastore catalog)
print("Connecting to Trino to drop metadata tables...")
try:
    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES FROM delta.bronze")
    tables = [row[0] for row in cursor.fetchall()]
    for table in tables:
        print(f"Dropping Trino table delta.bronze.{table}...")
        cursor.execute(f"DROP TABLE IF EXISTS delta.bronze.{table}")
    cursor.close()
    conn.close()
    print("All Trino metadata tables dropped successfully.")
except Exception as e:
    print(f"Failed to drop tables in Trino: {e}")

# 2. Delete data files in MinIO
print(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)
if client.bucket_exists(BUCKET):
    print(f"Bucket '{BUCKET}' exists. Deleting all objects...")
    # List and delete all objects (including versions/delete markers if any, or just recursively)
    objects_to_delete = client.list_objects(BUCKET, recursive=True)
    for obj in objects_to_delete:
        print(f"Deleting object: {obj.object_name}")
        client.remove_object(BUCKET, obj.object_name)
    
    # Delete the bucket itself
    client.remove_bucket(BUCKET)
    print(f"Bucket '{BUCKET}' deleted successfully.")
    
    # Recreate the bucket so it is clean
    client.make_bucket(BUCKET)
    print(f"Bucket '{BUCKET}' recreated.")
else:
    print(f"Bucket '{BUCKET}' does not exist.")