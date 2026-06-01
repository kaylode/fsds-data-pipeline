#!/usr/bin/env python3
"""
EHR Feature Store Management Script
Consolidates Feast apply, materialization, and verification tasks in a single script.
"""

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from feast import FeatureStore

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CUR_DIR, "..", ".."))
os.environ["PROJECT_ROOT"] = PROJECT_ROOT
FEATURE_STORE_DIR = os.path.join(PROJECT_ROOT, "config", "feature_store")
# Load environment variables
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# Configure AWS environment variables for PyArrow/Feast S3 FileSource resolving to local MinIO
os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ROOT_USER", "minioadmin")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
os.environ["AWS_ENDPOINT_URL"] = f"http://localhost:{os.getenv('MINIO_PORT', '9000')}"
os.environ["AWS_S3_ENDPOINT_URL"] = f"http://localhost:{os.getenv('MINIO_PORT', '9000')}"



def run_apply():
    logger.info("Running `feast apply` in the feature store repository...")
    # Run the feast apply command via subprocess in the correct directory
    res = subprocess.run(["feast", "apply", "--skip-source-validation"], cwd=FEATURE_STORE_DIR)
    if res.returncode == 0:
        logger.info("Feast registry and tables successfully applied.")
    else:
        logger.error(f"Feast apply failed with exit code: {res.returncode}")
        sys.exit(res.returncode)


def run_materialize():
    logger.info("Initializing Feast Feature Store...")
    store = FeatureStore(repo_path=FEATURE_STORE_DIR)
    
    start_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end_date = datetime.now(tz=timezone.utc) + timedelta(days=1)
    
    logger.info(f"Materializing batch features from {start_date} to {end_date}...")
    store.materialize(start_date=start_date, end_date=end_date)
    logger.info("Materialization complete. Gold batch tables loaded into Redis online store.")


def run_testview():
    logger.info("Testing feature view schema...")
    store = FeatureStore(repo_path=FEATURE_STORE_DIR)
    for fv in store.list_feature_views():
        logger.info(f"Feature View: {fv.name}")
        for field in fv.schema:
            logger.info(f"  - {field.name}: {field.dtype}")


def main():
    logger.info("Starting Feast Feature Store initialization and materialization...")
    run_apply()
    run_materialize()
    run_testview()


if __name__ == "__main__":
    main()
