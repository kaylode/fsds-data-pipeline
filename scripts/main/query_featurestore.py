#!/usr/bin/env python3
"""
EHR Feature Store Query & ML Dataset Builder

Two modes:

  --export   Build train/test Parquet for ML training.
             Uses get_historical_features() with patient_training_v1 —
             returns features + labels in one point-in-time correct call.
             Writes:
               data/ml/train.parquet  (oldest 80% of visits)
               data/ml/test.parquet   (most recent 20% of visits)

  --online   Test real-time feature serving from the Feast online store (Redis).
             Uses get_online_features() with patient_ml_features_v1 —
             returns the latest feature snapshot per patient.

  --all      Run both modes.

Run:
    uv run scripts/main/query_featurestore.py --export
    uv run scripts/main/query_featurestore.py --online
    uv run scripts/main/query_featurestore.py --all

Prerequisites:
    - Gold tables built        (make golden)
    - Feature tables + labels  (make features)
    - Feature pipeline running (make feature-pipeline)
    - [--online only] Flink stream running (make flink-stream)
"""

import os

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

try:
    from feast import FeatureStore
except ImportError:
    logger.error("Feast not installed. Run: uv add feast")
    raise

from utils import get_trino_connection

# ── Environment ────────────────────────────────────────────────────────────────
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

os.environ["AWS_ACCESS_KEY_ID"]     = os.getenv("MINIO_ROOT_USER",    "minioadmin")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
os.environ["AWS_ENDPOINT_URL"]      = f"http://localhost:{os.getenv('MINIO_PORT', '9000')}"

FEATURE_STORE_DIR  = os.path.join(project_root, "config", "feature_store")
ML_OUTPUT_DIR      = os.path.join(project_root, "data", "ml")
TRAIN_SPLIT_RATIO  = 0.80
ONLINE_SAMPLE_SIZE = 10


def _trino_fetch(sql: str) -> pd.DataFrame:
    conn   = get_trino_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        cursor.close()
        conn.close()


# ── Mode 1: Training export ────────────────────────────────────────────────────

def run_export() -> None:
    """
    Features + labels via a single get_historical_features() call.
    Labels come from the patient_labels FeatureView (offline store),
    not from a manual Trino query.
    """
    logger.info("── Export: get_historical_features (training dataset) ────────")

    # Minimal entity_df: just visit keys + timestamps to drive the point-in-time join
    entity_df = _trino_fetch(
        "SELECT patient_id, feature_timestamp FROM delta.gold.gold_visit_labels"
    )
    if entity_df.empty:
        logger.warning("gold_visit_labels is empty. Run make features first.")
        return
    entity_df = entity_df.rename(columns={"feature_timestamp": "event_timestamp"})
    entity_df["event_timestamp"] = pd.to_datetime(entity_df["event_timestamp"], utc=True)
    logger.info(f"Entity df: {len(entity_df):,} visits.")

    store = FeatureStore(repo_path=FEATURE_STORE_DIR)

    # patient_training_v1 = features + labels — one call returns everything
    logger.info("Calling get_historical_features (features + labels) ...")
    df = store.get_historical_features(
        entity_df=entity_df,
        features=store.get_feature_service("patient_training_v1"),
    ).to_df().drop(columns=["event_timestamp"], errors="ignore")

    logger.info(f"Retrieved: {len(df):,} visits × {len(df.columns)} columns")

    # Temporal split — oldest → train, most recent → test (prevents data leakage)
    df = df.sort_values("admission_timestamp").reset_index(drop=True)
    split_idx = int(len(df) * TRAIN_SPLIT_RATIO)
    train_df  = df.iloc[:split_idx].reset_index(drop=True)
    test_df   = df.iloc[split_idx:].reset_index(drop=True)
    logger.info(f"Split: {len(train_df):,} train / {len(test_df):,} test")

    os.makedirs(ML_OUTPUT_DIR, exist_ok=True)
    train_df.to_parquet(os.path.join(ML_OUTPUT_DIR, "train.parquet"), index=False)
    test_df.to_parquet(os.path.join(ML_OUTPUT_DIR,  "test.parquet"),  index=False)
    logger.info(f"Saved train.parquet and test.parquet to {ML_OUTPUT_DIR}")


# ── Mode 2: Online serving ─────────────────────────────────────────────────────

def run_online() -> None:
    """
    Latest feature snapshot per patient from Redis.
    Mirrors what a real-time prediction service would call at inference time.
    """
    logger.info("── Online: get_online_features (serving) ─────────────────────")

    patient_ids = _trino_fetch(
        f"SELECT patient_id FROM delta.gold.gold_visit_labels ORDER BY rand() LIMIT {ONLINE_SAMPLE_SIZE}"
    )["patient_id"].tolist()

    if not patient_ids:
        logger.warning("No patients found. Run make features first.")
        return

    store = FeatureStore(repo_path=FEATURE_STORE_DIR)

    logger.info(f"Calling get_online_features for {len(patient_ids)} patients ...")
    result_df = store.get_online_features(
        features=store.get_feature_service("patient_ml_features_v1"),
        entity_rows=[{"patient_id": pid} for pid in patient_ids],
    ).to_df()

    total    = result_df.shape[0] * (result_df.shape[1] - 1)
    filled   = result_df.drop(columns=["patient_id"]).notna().sum().sum()
    coverage = filled / total * 100 if total else 0

    logger.info(f"Result: {len(result_df)} patients × {result_df.shape[1]} features")
    logger.info(f"Feature coverage: {coverage:.1f}% non-null")
    print("\n─── ONLINE FEATURE VECTORS ──────────────────────────────────────────")
    with pd.option_context("display.max_columns", 10, "display.width", 120):
        print(result_df.set_index("patient_id").T.to_string())
    print("─" * 68)


# ── Entry Point ────────────────────────────────────────────────────────────────

def main() -> None:
    run_export()
    run_online()


if __name__ == "__main__":
    main()
