#!/usr/bin/env python3
"""
EHR Feature Store Query & ML Dataset Builder

Two modes:

  --online   Test real-time feature retrieval from the Feast online store.
             Fetches feature vectors for a sample of active patients and
             prints a summary table.

  --export   Build train/test Parquet datasets for ML model training.
             Reads gold_visit_labels + all feat_* tables from Trino, merges
             them on patient_id, applies a temporal 80/20 split (oldest
             visits → train, most recent → test), and writes:
               data/ml/train.parquet
               data/ml/test.parquet

             Labels available in both files:
               is_readmitted_7d        — binary (0/1)
               has_inpatient_mortality — binary (0/1)
               has_30day_mortality     — binary (0/1)

  --all      Run both modes.

Run:
    uv run python source/scripts/course-work/query_featurestore.py --online
    uv run python source/scripts/course-work/query_featurestore.py --export
    uv run python source/scripts/course-work/query_featurestore.py --all

Prerequisites:
    - Gold tables built      (4_transform_to_gold.py)
    - Feature tables + labels built (5_compute_features.py)
    - Feast store applied    (5_manage_feature_store.py --apply)
    - Feast materialized     (5_manage_feature_store.py --materialize)
    - [--online only] stream running (6_flink + 6_stream_to_online_store)
"""

import argparse
import os

import pandas as pd
import trino
from dotenv import load_dotenv
from loguru import logger

try:
    from feast import FeatureStore
except ImportError:
    logger.error("Feast not installed. Run: uv add feast")
    raise

# ── Environment ────────────────────────────────────────────────────────────────
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

os.environ["AWS_ACCESS_KEY_ID"]     = os.getenv("MINIO_ROOT_USER",    "minioadmin")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
os.environ["AWS_ENDPOINT_URL"]      = f"http://localhost:{os.getenv('MINIO_PORT', '9000')}"

TRINO_HOST        = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT        = int(os.getenv("TRINO_PORT", "8090"))
TRINO_USER        = os.getenv("TRINO_USER",  "trino")
FEATURE_STORE_DIR = os.path.join(project_root, "config", "feature_store")
ML_OUTPUT_DIR     = os.path.join(project_root, "data", "ml")

ONLINE_SAMPLE_SIZE = 10    # patients to fetch for the --online test
TRAIN_SPLIT_RATIO  = 0.80  # temporal split: first 80% → train, last 20% → test


# ── Helpers ───────────────────────────────────────────────────────────────────

def _trino_conn():
    return trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)


def _fetch_df(cursor, sql: str) -> pd.DataFrame:
    cursor.execute(sql)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    return pd.DataFrame(rows, columns=cols)


def _read_gold_table(cursor, table: str, drop_cols: list[str] | None = None) -> pd.DataFrame:
    df = _fetch_df(cursor, f"SELECT * FROM delta.gold.{table}")
    if drop_cols:
        df = df.drop(columns=[c for c in drop_cols if c in df.columns])
    return df


# ── Mode 1: Online Feature Test ───────────────────────────────────────────────

def run_online_test() -> None:
    """
    Fetches online feature vectors for a sample of patients from the Feast
    online store (populated by 5_manage_feature_store --materialize and
    6_stream_to_online_store.py).
    """
    logger.info("── Online Feature Serving Test ──────────────────────────────")

    conn   = _trino_conn()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT patient_id
        FROM (
            SELECT DISTINCT patient_id
            FROM delta.gold.gold_visit_labels
        )
        ORDER BY rand()
        LIMIT {ONLINE_SAMPLE_SIZE}
    """)
    patient_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    if not patient_ids:
        logger.warning("No patients found. Run 5_compute_features.py first.")
        return

    logger.info(f"Querying online features for {len(patient_ids)} patients ...")
    store = FeatureStore(repo_path=FEATURE_STORE_DIR)

    result_df = store.get_online_features(
        features=store.get_feature_service("patient_ml_features_v1"),
        entity_rows=[{"patient_id": pid} for pid in patient_ids],
    ).to_df()

    total   = result_df.shape[0] * (result_df.shape[1] - 1)
    filled  = result_df.drop(columns=["patient_id"]).notna().sum().sum()
    coverage = filled / total * 100 if total else 0

    logger.info(f"Result: {len(result_df)} patients × {result_df.shape[1]} features")
    logger.info(f"Feature coverage: {coverage:.1f}% non-null")

    print("\n─── ONLINE FEATURE VECTORS ─────────────────────────────────────────")
    with pd.option_context("display.max_columns", 10, "display.width", 120):
        print(result_df.set_index("patient_id").T.to_string())
    print("─" * 68)


# ── Mode 2: Export train/test Parquet ─────────────────────────────────────────

def run_export() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Joins gold_visit_labels with all feat_* tables and saves train/test Parquet.

    Schema is read dynamically from Trino — no hardcoded column lists.
    Adding new feat_ columns (e.g., new vital/lab from dim tables) is automatic.

    Temporal split:
      Sort all visits by admission_timestamp (oldest first).
      First TRAIN_SPLIT_RATIO → train.parquet  (historical data for fitting)
      Remaining               → test.parquet   (most recent data for evaluation)
      This prevents future data leaking into training (random split would not).

    Label columns (binary 0/1):
      is_readmitted_7d        — new visit within 7 days of discharge
      has_inpatient_mortality — patient died during this visit
      has_30day_mortality     — patient died within 30 days post-discharge
    """
    logger.info("── Building ML Datasets ─────────────────────────────────────")

    conn   = _trino_conn()
    cursor = conn.cursor()

    # Read labels (one row per completed visit)
    logger.info("Reading gold_visit_labels ...")
    labels_df = _read_gold_table(
        cursor, "gold_visit_labels",
        drop_cols=["feature_timestamp", "patient_key"]
    )
    logger.info(f"  {len(labels_df):,} labelled visits")

    # Read feat tables — schema discovered at runtime from Trino
    feat_tables = [
        "feat_patient_vitals_6m",
        "feat_patient_labs_6m",
        "feat_patient_icd_6m",
        "feat_patient_medication_6m",
        "feat_patient_demographics",
    ]

    feat_dfs: dict[str, pd.DataFrame] = {}
    for table in feat_tables:
        df = _read_gold_table(cursor, table, drop_cols=["feature_timestamp"])
        feat_dfs[table] = df
        logger.info(f"  {table}: {len(df):,} patients, {len(df.columns)} cols")

    cursor.close()
    conn.close()

    # Merge: each visit row gets its patient's feature snapshot (LEFT JOIN on patient_id)
    df = labels_df.copy()
    for table, feat_df in feat_dfs.items():
        df = df.merge(feat_df, on="patient_id", how="left")

    logger.info(f"Merged: {len(df):,} visits × {len(df.columns)} columns total")

    # Temporal split
    df = df.sort_values("admission_timestamp").reset_index(drop=True)
    split_idx  = int(len(df) * TRAIN_SPLIT_RATIO)
    train_df   = df.iloc[:split_idx].reset_index(drop=True)
    test_df    = df.iloc[split_idx:].reset_index(drop=True)

    split_ts = df["admission_timestamp"].iloc[split_idx] if split_idx < len(df) else "N/A"
    logger.info(f"Temporal split at row {split_idx} / admission >= {split_ts}")
    logger.info(f"  Train: {len(train_df):,} visits  |  Test: {len(test_df):,} visits")

    # Label distribution
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

    # Feature coverage per group (auto-detected from column names)
    print("\n─── FEATURE COVERAGE IN TRAIN ───────────────────────────────────────")
    _VITAL_PREFIXES = ("heart_rate", "systolic_bp", "diastolic_bp", "temperature")
    _LAB_PREFIXES   = ("glucose", "creatinine", "wbc", "hemoglobin")
    groups = {
        "vitals":       [c for c in train_df if any(c.startswith(p) for p in _VITAL_PREFIXES)],
        "labs":         [c for c in train_df if any(c.startswith(p) for p in _LAB_PREFIXES)],
        "icd_chapters": [c for c in train_df if c.startswith("icd_chap_")],
        "medications":  [c for c in train_df if c.endswith("_count")],
        "demographics": [c for c in train_df if c in ("gender", "ethnic", "country", "age", "is_deceased")],
    }
    for group, cols in groups.items():
        if cols:
            pct = train_df[cols].notna().mean().mean() * 100
            print(f"  {group:<15}: {len(cols):>3} features,  {pct:5.1f}% non-null")
    print("─" * 68)

    # Save
    os.makedirs(ML_OUTPUT_DIR, exist_ok=True)
    train_path = os.path.join(ML_OUTPUT_DIR, "train.parquet")
    test_path  = os.path.join(ML_OUTPUT_DIR, "test.parquet")

    train_df.to_parquet(train_path, index=False)
    test_df.to_parquet(test_path,   index=False)

    logger.info(f"Saved {train_path}")
    logger.info(f"Saved {test_path}")
    return train_df, test_df


# ── Entry Point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="EHR Feature Store Query & ML Dataset Builder")
    parser.add_argument("--online", action="store_true", help="Test online feature retrieval from Feast")
    parser.add_argument("--export", action="store_true", help="Build and save train/test Parquet for ML")
    parser.add_argument("--all",    action="store_true", help="Run both --online and --export")
    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        return

    if args.online or args.all:
        run_online_test()

    if args.export or args.all:
        run_export()


if __name__ == "__main__":
    main()
