"""
Electronic Health Record (EHR) Offline Dataset Generator

Generates structured offline Parquet dimension and fact tables representing
relational EHR data. These tables are ingested into the Bronze layer by
2a_ingest_to_bronze.py.

Output tables (written to source/data/synthetic/):
  - patients.parquet       : patient registry (demographics)
  - wards.parquet          : ward dimension table
  - event_metadata.parquet : event type lookup table
  - visits.parquet         : patient hospital admissions
  - events.parquet         : historical clinical event observations

Injected data quality challenges:
  - High cardinality patient IDs
  - 85% ward skewness toward 'General_Ward'
  - 2% duplicate rate in the events table
  - Schema evolution: visits before 2023-08-01 have NULL severity_level
"""

import os
import argparse
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from loguru import logger

# ── Configuration ──────────────────────────────────────────────────────────────
CONFIG = {
    "random_seed": 42,
    "start_date":  "2023-01-01",
    "end_date":    "2024-01-01",
    "n_patients":  10_000,
    "n_visits":    50_000,
    "n_events":    150_000,
    "skew_ratio_ward":        0.85,        # 85% of visits occur in 'General_Ward'
    "duplicate_rate_events":  0.02,        # 2% duplicate rate in events table
    "schema_evolution_date":  "2023-08-01",  # visits before this date have NULL severity_level
}

# ── Reference Pools ────────────────────────────────────────────────────────────
COUNTRIES    = ["VN", "US", "SG", "MY", "IN"]
GENDERS      = ["Male", "Female", "Other"]
GENDER_PROBS = [0.49, 0.49, 0.02]
ETHNICITIES  = ["Kinh", "Tay", "Thai", "Caucasian", "Asian", "African American", "Hispanic"]
ETHNIC_PROBS = [0.75, 0.05, 0.03, 0.07, 0.05, 0.03, 0.02]

WARDS = {
    "General_Ward":   "General Medicine",
    "Pediatric_Ward": "Pediatrics",
    "ICU_Ward":       "Intensive Care Unit",
    "ER_Ward":        "Emergency Department",
}

EVENT_METADATA_DEFS = [
    # Vitals
    {"event_type_id": "V01", "event_name": "Heart Rate",    "event_description": "Resting heart rate in beats per minute", "unit_of_measurement": "bpm",     "event_type": "vital"},
    {"event_type_id": "V02", "event_name": "Systolic BP",   "event_description": "Systolic blood pressure",                "unit_of_measurement": "mmHg",    "event_type": "vital"},
    {"event_type_id": "V03", "event_name": "Diastolic BP",  "event_description": "Diastolic blood pressure",               "unit_of_measurement": "mmHg",    "event_type": "vital"},
    {"event_type_id": "V04", "event_name": "Temperature",   "event_description": "Body temperature in Celsius",            "unit_of_measurement": "°C",      "event_type": "vital"},
    # Labs
    {"event_type_id": "L01", "event_name": "Glucose",       "event_description": "Fasting blood glucose level",           "unit_of_measurement": "mg/dL",   "event_type": "lab"},
    {"event_type_id": "L02", "event_name": "Creatinine",    "event_description": "Serum creatinine level",                "unit_of_measurement": "mg/dL",   "event_type": "lab"},
    {"event_type_id": "L03", "event_name": "WBC",           "event_description": "White blood cell count",                "unit_of_measurement": "10^3/µL", "event_type": "lab"},
    {"event_type_id": "L04", "event_name": "Hemoglobin",    "event_description": "Hemoglobin level",                      "unit_of_measurement": "g/dL",    "event_type": "lab"},
    # Medications
    {"event_type_id": "M01", "event_name": "Lisinopril",    "event_description": "Lisinopril 10mg administration",   "unit_of_measurement": None, "event_type": "medication"},
    {"event_type_id": "M02", "event_name": "Metformin",     "event_description": "Metformin 500mg administration",   "unit_of_measurement": None, "event_type": "medication"},
    {"event_type_id": "M03", "event_name": "Amoxicillin",   "event_description": "Amoxicillin 500mg administration", "unit_of_measurement": None, "event_type": "medication"},
    {"event_type_id": "M04", "event_name": "Atorvastatin",  "event_description": "Atorvastatin 20mg administration", "unit_of_measurement": None, "event_type": "medication"},
    # Diagnoses
    {"event_type_id": "D01", "event_name": "Hypertension",  "event_description": "Essential primary hypertension (ICD-10 I10)",           "unit_of_measurement": None, "event_type": "diagnosis"},
    {"event_type_id": "D02", "event_name": "Diabetes",      "event_description": "Type 2 diabetes mellitus (ICD-10 E11.9)",               "unit_of_measurement": None, "event_type": "diagnosis"},
    {"event_type_id": "D03", "event_name": "URI",           "event_description": "Acute upper respiratory infection, unspecified (ICD-10 J06.9)", "unit_of_measurement": None, "event_type": "diagnosis"},
    {"event_type_id": "D04", "event_name": "Hyperlipidemia","event_description": "Hyperlipidemia, unspecified (ICD-10 E78.5)",            "unit_of_measurement": None, "event_type": "diagnosis"},
]


# ── Generation ─────────────────────────────────────────────────────────────────

def generate_offline_data(output_dir: str) -> None:
    """
    Generates structured offline Parquet tables for the Bronze layer.
    """
    logger.info("Initializing offline relational data generation...")
    cfg = CONFIG
    np.random.seed(cfg["random_seed"])

    start_date       = datetime.strptime(cfg["start_date"], "%Y-%m-%d")
    end_date         = datetime.strptime(cfg["end_date"],   "%Y-%m-%d")
    schema_evol_date = datetime.strptime(cfg["schema_evolution_date"], "%Y-%m-%d")
    total_days       = (end_date - start_date).days

    # ── 1. Patients ──────────────────────────────────────────────────────────
    logger.info(f"Generating {cfg['n_patients']:,} patient registry rows...")
    patient_ids   = [f"P{i:06d}" for i in range(1, cfg["n_patients"] + 1)]
    dob_start     = datetime(1940, 1, 1)
    dob_end       = datetime(2020, 1, 1)
    dob_days_diff = (dob_end - dob_start).days
    dobs = [dob_start + timedelta(days=int(np.random.randint(0, dob_days_diff)))
            for _ in range(cfg["n_patients"])]

    date_of_deaths = []
    for dob in dobs:
        if np.random.rand() < 0.01:
            death_date = dob + timedelta(days=int(np.random.randint(365 * 10, 365 * 85)))
            date_of_deaths.append(death_date if death_date < end_date else None)
        else:
            date_of_deaths.append(None)

    df_patients = pd.DataFrame({
        "patient_id":    patient_ids,
        "country":       np.random.choice(COUNTRIES,   cfg["n_patients"], p=[0.70, 0.10, 0.08, 0.06, 0.06]),
        "phone_number":  [f"+84-9{np.random.randint(10000000, 99999999)}" if np.random.rand() >= 0.05 else None for _ in range(cfg["n_patients"])],
        "address":       [f"District {np.random.randint(1, 12)}, HCMC, VN" if np.random.rand() >= 0.05 else None for _ in range(cfg["n_patients"])],
        "dob":           dobs,
        "date_of_death": date_of_deaths,
        "gender":        np.random.choice(GENDERS,     cfg["n_patients"], p=GENDER_PROBS),
        "ethnic":        np.random.choice(ETHNICITIES, cfg["n_patients"], p=ETHNIC_PROBS),
    })
    patients_file = os.path.join(output_dir, "patients.parquet")
    pq.write_table(pa.Table.from_pandas(df_patients), patients_file)
    logger.info(f"Saved patients table → {patients_file}")

    # ── 2. Wards ─────────────────────────────────────────────────────────────
    logger.info("Generating wards dimension table...")
    df_wards = pd.DataFrame([
        {"ward_id": k, "ward_name": k.replace("_", " "), "department": v}
        for k, v in WARDS.items()
    ])
    wards_file = os.path.join(output_dir, "wards.parquet")
    pq.write_table(pa.Table.from_pandas(df_wards), wards_file)
    logger.info(f"Saved wards table → {wards_file}")

    # ── 3. Event Metadata ────────────────────────────────────────────────────
    logger.info("Generating event_metadata lookup table...")
    df_meta = pd.DataFrame(EVENT_METADATA_DEFS)
    meta_file = os.path.join(output_dir, "event_metadata.parquet")
    pq.write_table(pa.Table.from_pandas(df_meta), meta_file)
    logger.info(f"Saved event_metadata table → {meta_file}")

    # ── 4. Visits ────────────────────────────────────────────────────────────
    logger.info(f"Generating {cfg['n_visits']:,} visits...")
    visit_ids = [f"V{i:07d}" for i in range(1, cfg["n_visits"] + 1)]
    visit_patients = np.random.choice(patient_ids, cfg["n_visits"])

    admission_timestamps = [
        start_date + timedelta(
            days=int(np.random.randint(0, total_days)),
            hours=int(np.random.randint(0, 24)),
            minutes=int(np.random.randint(0, 60))
        )
        for _ in range(cfg["n_visits"])
    ]

    discharge_timestamps = []
    for adm in admission_timestamps:
        if np.random.rand() < 0.05:
            discharge_timestamps.append(None)
        else:
            stay_minutes = np.random.randint(60, 14 * 24 * 60)
            discharge_timestamps.append(adm + timedelta(minutes=stay_minutes))

    ward_keys  = list(WARDS.keys())
    ward_probs = [cfg["skew_ratio_ward"]] + [(1 - cfg["skew_ratio_ward"]) / (len(ward_keys) - 1)] * (len(ward_keys) - 1)
    visit_wards = np.random.choice(ward_keys, cfg["n_visits"], p=ward_probs)

    severities = []
    for adm in admission_timestamps:
        if adm < schema_evol_date:
            severities.append(None)
        else:
            severities.append(np.random.choice(["mild", "moderate", "severe"], p=[0.60, 0.30, 0.10]))

    df_visits = pd.DataFrame({
        "visit_id":             visit_ids,
        "patient_id":           visit_patients,
        "admission_timestamp":  admission_timestamps,
        "discharge_timestamp":  discharge_timestamps,
        "ward_id":              visit_wards,
        "severity_level":       severities,
    })
    df_visits["visit_date"] = df_visits["admission_timestamp"].dt.strftime("%Y-%m-%d")

    visits_file = os.path.join(output_dir, "visits.parquet")
    pq.write_table(pa.Table.from_pandas(df_visits), visits_file)
    logger.info(f"Saved visits table → {visits_file}")

    # ── 5. Events ────────────────────────────────────────────────────────────
    logger.info(f"Generating {cfg['n_events']:,} clinical event observations...")
    event_ids    = [f"EVT{i:08d}" for i in range(1, cfg["n_events"] + 1)]
    event_visits = np.random.choice(visit_ids, cfg["n_events"])

    visit_time_map = {
        row["visit_id"]: (row["admission_timestamp"], row["discharge_timestamp"])
        for _, row in df_visits.iterrows()
    }

    event_timestamps = []
    for v_id in event_visits:
        adm_ts, dis_ts = visit_time_map[v_id]
        max_ts   = end_date if (dis_ts is None or pd.isnull(dis_ts)) else dis_ts
        diff_sec = int((max_ts - adm_ts).total_seconds())
        event_timestamps.append(adm_ts + timedelta(seconds=np.random.randint(0, max(1, diff_sec))))

    meta_indices = np.random.choice(len(EVENT_METADATA_DEFS), cfg["n_events"])

    num_values, text_values = [], []
    for idx in meta_indices:
        meta   = EVENT_METADATA_DEFS[idx]
        e_type = meta["event_type"]
        e_name = meta["event_name"]
        if e_type == "vital":
            lookup = {"Heart Rate": (75, 12), "Systolic BP": (120, 15), "Diastolic BP": (80, 8), "Temperature": (36.8, 0.4)}
            mu, sigma = lookup.get(e_name, (0, 1))
            num_values.append(np.round(np.random.normal(mu, sigma), 1))
            text_values.append(None)
        elif e_type == "lab":
            lookup = {"Glucose": (100, 20), "Creatinine": (0.8, 0.2), "WBC": (7.0, 2.0), "Hemoglobin": (14.5, 1.5)}
            mu, sigma = lookup.get(e_name, (0, 1))
            num_values.append(np.round(np.random.normal(mu, sigma), 2))
            text_values.append(None)
        elif e_type == "medication":
            num_values.append(None)
            text_values.append(f"Administered {e_name} per physician prescription")
        else:
            num_values.append(None)
            text_values.append(f"Confirmed diagnosis: {meta['event_description']}")

    df_events = pd.DataFrame({
        "event_id":        event_ids,
        "visit_id":        event_visits,
        "event_timestamp": event_timestamps,
        "event_type":      [EVENT_METADATA_DEFS[i]["event_type"]    for i in meta_indices],
        "event_type_id":   [EVENT_METADATA_DEFS[i]["event_type_id"] for i in meta_indices],
        "event_name":      [EVENT_METADATA_DEFS[i]["event_name"]    for i in meta_indices],
        "text_value":      text_values,
        "num_value":       num_values,
    })

    # Inject 2% duplicates
    n_dupes   = int(cfg["n_events"] * cfg["duplicate_rate_events"])
    df_events = pd.concat([df_events, df_events.iloc[np.random.choice(cfg["n_events"], n_dupes)]], ignore_index=True)
    df_events = df_events.sort_values("event_timestamp").reset_index(drop=True)
    df_events["event_date"] = df_events["event_timestamp"].dt.strftime("%Y-%m-%d")

    events_file = os.path.join(output_dir, "events.parquet")
    pq.write_table(pa.Table.from_pandas(df_events), events_file)
    logger.info(f"Saved events table → {events_file}")

    # ── Verification Logs ────────────────────────────────────────────────────
    logger.info("=== Offline Dataset Characteristics ===")
    logger.info(f"- Patients:       {df_patients['patient_id'].nunique():,} unique patients")
    logger.info(f"- Ward Skewness:  {(df_visits['ward_id'] == 'General_Ward').mean()*100:.1f}% visits in General_Ward")
    v1 = (df_visits["admission_timestamp"] < schema_evol_date).sum()
    v2 = (df_visits["admission_timestamp"] >= schema_evol_date).sum()
    logger.info(f"- Schema Evol:    {v1:,} older visits (NULL severity_level), {v2:,} newer visits")
    n_unique = df_events["event_id"].nunique()
    total    = len(df_events)
    logger.info(f"- Duplication:    {(total - n_unique) / n_unique * 100:.2f}% duplicates injected in events")


# ── Entry Point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="EHR Offline Dataset Generator")
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Target output directory (default: source/data/synthetic/)"
    )
    args = parser.parse_args()

    if args.output_dir:
        output_dir = args.output_dir
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.abspath(os.path.join(script_dir, "..", "..", "data", "synthetic"))

    os.makedirs(output_dir, exist_ok=True)
    np.random.seed(CONFIG["random_seed"])

    generate_offline_data(output_dir)

    logger.info("Offline EHR data generation completed successfully.")


if __name__ == "__main__":
    main()
