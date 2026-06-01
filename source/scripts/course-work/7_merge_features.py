#!/usr/bin/env python3
"""
EHR Feature Merger & ML Dataset Builder

This script retrieves historical and backfilled streaming features for patients from the
Feast offline store (using Trino) with point-in-time correctness, merges them with clinical
labels, and splits the data into train/test Parquet files for machine learning.

Pipeline:
  1. Fetch labels (visits) from delta.gold.gold_visit_labels.
  2. Create entity dataframe (patient_id, timestamp) for Feast.
  3. Query Feast get_historical_features to perform point-in-time joins of all feature views.
  4. Rename columns to prefix vitals with 'stream_24h_' and other features with 'hist_'.
  5. Apply an 80/20 temporal split (oldest visits -> train, most recent -> test).
  6. Save output datasets:
       - data/ml/train.parquet
       - data/ml/test.parquet
"""

import os
import time
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from utils import get_trino_connection

try:
    from feast import FeatureStore
except ImportError:
    logger.error("Feast is not installed. Run: uv add feast")
    import sys
    sys.exit(1)

# ── Environment ────────────────────────────────────────────────────────────────
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8090"))
TRINO_USER = os.getenv("TRINO_USER", "trino")

FEATURE_STORE_DIR = os.path.join(project_root, "config", "feature_store")
ML_OUTPUT_DIR     = os.path.join(project_root, "data", "ml")

os.environ["AWS_ACCESS_KEY_ID"]     = os.getenv("MINIO_ROOT_USER",    "minioadmin")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
os.environ["AWS_ENDPOINT_URL"]      = f"http://localhost:{os.getenv('MINIO_PORT', '9000')}"

TRAIN_SPLIT_RATIO  = 0.80  # 80% train, 20% test



def _fetch_df(cursor, sql: str) -> pd.DataFrame:
    cursor.execute(sql)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    return pd.DataFrame(rows, columns=cols)

def main() -> None:
    logger.info("Initializing Feast feature store ...")
    store = FeatureStore(repo_path=FEATURE_STORE_DIR)

    logger.info("Connecting to Trino to fetch labels...")
    conn = get_trino_connection()
    cursor = conn.cursor()
    
    # Read labels (one row per completed visit)
    logger.info("Reading gold_visit_labels from delta.gold...")
    try:
        labels_df = _fetch_df(cursor, "SELECT * FROM delta.gold.gold_visit_labels")
    except Exception as e:
        logger.error(f"Failed to query gold_visit_labels: {e}")
        logger.error("Please ensure 5_compute_features.py has been run successfully.")
        return
    finally:
        cursor.close()
        conn.close()

    if labels_df.empty:
        logger.warning("No visits or labels found in gold_visit_labels. Exiting.")
        return

    logger.info(f"Loaded {len(labels_df):,} labeled visits. Preparing entity dataframe for Feast...")

    # Feast requires a column named 'timestamp' to perform point-in-time correct joins
    entity_df = labels_df.copy()
    entity_df["timestamp"] = pd.to_datetime(entity_df["feature_timestamp"], utc=True)

    # Fetch point-in-time features from Feast offline store (Trino)
    logger.info("Retrieving historical features from Feast offline store (Trino/MinIO)...")
    feature_service = store.get_feature_service("patient_ml_features_v1")
    
    start_time = time.time()
    merged_df = store.get_historical_features(
        entity_df=entity_df,
        features=feature_service
    ).to_df()
    elapsed = time.time() - start_time
    logger.info(f"Historical features retrieved in {elapsed:.1f} seconds.")

    # Drop Feast system columns and duplicate/temporary timestamps
    cols_to_drop = ["timestamp", "feature_timestamp_y", "feature_timestamp_x"]
    merged_df = merged_df.drop(columns=[c for c in cols_to_drop if c in merged_df.columns])

    # Rename features to reflect their source
    vitals_names = ["heart_rate", "systolic_bp", "diastolic_bp", "temperature"]
    stats = ["mean", "min", "max", "std"]
    stream_features = [f"{v}_{s}" for v in vitals_names for s in stats]

    rename_dict = {}
    for col in merged_df.columns:
        if col in ("patient_id", "visit_id", "patient_key", "admission_timestamp", 
                   "discharge_timestamp", "severity_level", "is_readmitted_7d", 
                   "has_inpatient_mortality", "has_30day_mortality", "feature_timestamp"):
            continue
        if col in stream_features:
            rename_dict[col] = f"stream_24h_{col}"
        else:
            rename_dict[col] = f"hist_{col}"

    merged_df = merged_df.rename(columns=rename_dict)
    logger.info(f"Unified features merged: {len(merged_df):,} visits × {len(merged_df.columns)} columns total")

    # Perform temporal split based on admission_timestamp (prevents future data leaking into train)
    merged_df = merged_df.sort_values("admission_timestamp").reset_index(drop=True)
    split_idx = int(len(merged_df) * TRAIN_SPLIT_RATIO)
    train_df  = merged_df.iloc[:split_idx].reset_index(drop=True)
    test_df   = merged_df.iloc[split_idx:].reset_index(drop=True)

    split_ts = merged_df["admission_timestamp"].iloc[split_idx] if split_idx < len(merged_df) else "N/A"
    logger.info(f"Temporal split at row {split_idx} / admission >= {split_ts}")
    logger.info(f"  Train dataset: {len(train_df):,} visits")
    logger.info(f"  Test dataset:  {len(test_df):,} visits")

    # Display label distribution
    label_cols = ["is_readmitted_7d", "has_inpatient_mortality", "has_30day_mortality"]
    print("\n─── LABEL DISTRIBUTIONS ─────────────────────────────────────────────")
    for col in label_cols:
        if col in train_df.columns:
            tr_pos  = int(train_df[col].sum())
            te_pos  = int(test_df[col].sum())
            tr_rate = tr_pos / len(train_df) * 100
            te_rate = te_pos / len(test_df)  * 100
            print(
                f"  {col:<30} "
                f"train {tr_pos:>5}/{len(train_df):>6} ({tr_rate:4.1f}%)  "
                f"test {te_pos:>4}/{len(test_df):>5} ({te_rate:4.1f}%)"
            )

    # Feature coverage per group
    print("\n─── FEATURE COVERAGE IN TRAIN ───────────────────────────────────────")
    groups = {
        "vitals (stream)": [c for c in train_df if c.startswith("stream_24h_")],
        "labs (hist)":     [c for c in train_df if c.startswith("hist_glucose_") or c.startswith("hist_creatinine_") or c.startswith("hist_wbc_") or c.startswith("hist_hemoglobin_")],
        "icd_chapters":    [c for c in train_df if c.startswith("hist_icd_chap_")],
        "medications":     [c for c in train_df if c.endswith("_count")],
        "demographics":    [c for c in train_df if c in ("hist_gender", "hist_ethnic", "hist_country", "hist_age", "hist_is_deceased")],
    }
    for group, cols in groups.items():
        if cols:
            pct = train_df[cols].notna().mean().mean() * 100
            print(f"  {group:<18}: {len(cols):>3} features,  {pct:5.1f}% non-null")
    print("─" * 68)

    # SaveParquet
    os.makedirs(ML_OUTPUT_DIR, exist_ok=True)
    train_path = os.path.join(ML_OUTPUT_DIR, "train.parquet")
    test_path  = os.path.join(ML_OUTPUT_DIR, "test.parquet")

    train_df.to_parquet(train_path, index=False)
    test_df.to_parquet(test_path,   index=False)

    logger.info(f"Saved training dataset to: {train_path}")
    logger.info(f"Saved testing dataset to:  {test_path}")

if __name__ == "__main__":
    main()
