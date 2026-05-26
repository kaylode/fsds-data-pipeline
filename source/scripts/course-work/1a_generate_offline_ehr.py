"""
Electronic Health Record (EHR) Event Log Generator

This script simulates a synthetic clinical event log dataset containing high-fidelity
EHR data. It is designed to model realistic data quality and engineering challenges
encountered in production streaming and analytics pipelines.

Supports two modes of generation:
1. --mode offline: Generates relational dimension and fact tables as partitioned Parquet files:
   - patients (registry demographics)
   - wards (healthcare facility units - formerly facilities)
   - visits (patient hospital admissions - formerly encounters)
   - event_metadata (lookup description of event types)
   - events (granular clinical event observations - formerly historical_clinical_records)
   It injects offline challenges: high cardinality, 85% ward skewness, 2% event duplicates,
   and schema evolution (null severity_level on older visits).

2. --mode stream: Generates a single flat event log of JSON records simulating new daily
   incoming events with streaming challenges:
   - Bursts (high event density in configured peak hours)
   - Late arrivals (12 % lag between event time and system ingestion time)
   - Stream duplicates (1.5 % duplicate events with minor delays)
"""

import os
import sys
import argparse
import json
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from loguru import logger

# Centralized Generator Configuration
CONFIG = {
    "random_seed": 42,
    "start_date": "2023-01-01",
    "end_date": "2024-01-01",

    # Offline Generation Parameters
    "offline": {
        "n_patients": 10_000,
        "n_visits": 50_000,
        "n_events": 150_000,
        "skew_ratio_ward": 0.85,          # 85% of visits occur in 'General_Ward'
        "duplicate_rate_events": 0.02,    # 2% duplicate rate in events table
        "schema_evolution_date": "2023-08-01",  # Cutoff date: older visits missing severity_level
    },
    
    # Stream Generation Parameters
    "stream": {
        "n_events": 100_000,
        "base_events_per_min": 100,
        "burst_multiplier": 30.0,
        "burst_windows": [
            {"start": "12:00", "end": "12:20"},
            {"start": "20:00", "end": "20:20"}
        ],
        "late_arrival_rate": 0.12,        # 12 % of streaming events are late arrivals
        "late_delay_min_max_mins": [5, 45],
        "duplicate_rate_stream": 0.015,   # 1.5 % duplicate events in stream
    }
}

# Pools for Generating Patient Demographics
COUNTRIES   = ["VN", "US", "SG", "MY", "IN"]
GENDERS     = ["Male", "Female", "Other"]
GENDER_PROBS = [0.49, 0.49, 0.02]
ETHNICITIES  = ["Kinh", "Tay", "Thai", "Caucasian", "Asian", "African American", "Hispanic"]
ETHNIC_PROBS = [0.75, 0.05, 0.03, 0.07, 0.05, 0.03, 0.02]

# Wards Pool
WARDS = {
    "General_Ward":   "General Medicine",
    "Pediatric_Ward": "Pediatrics",
    "ICU_Ward":       "Intensive Care Unit",
    "ER_Ward":        "Emergency Department",
}

# Event Metadata definitions (Lookup Table)
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
    {"event_type_id": "D04", "event_name": "Hyperlipidemia","event_description": "Hyperlipidemia, unspecified (ICD-10 E78.5)",            "unit_of_measurement": None, "event_type": "diagnosis"}
]


def generate_offline_data(output_dir):
    """
    Generates structured offline Parquet tables for the Bronze layer:
      - patients.parquet       : patient registry (demographics)
      - wards.parquet          : ward dimension table
      - event_metadata.parquet : event type lookup table
      - visits/                : patient hospital admissions
      - events/                : historical clinical events
    """
    logger.info("Initializing offline relational data generation...")
    cfg = CONFIG["offline"]
    np.random.seed(CONFIG["random_seed"])

    start_date = datetime.strptime(CONFIG["start_date"], "%Y-%m-%d")
    end_date   = datetime.strptime(CONFIG["end_date"],   "%Y-%m-%d")
    schema_evol_date = datetime.strptime(cfg["schema_evolution_date"], "%Y-%m-%d")
    total_days = (end_date - start_date).days

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
        "country":       np.random.choice(COUNTRIES,    cfg["n_patients"], p=[0.70, 0.10, 0.08, 0.06, 0.06]),
        "phone_number":  [f"+84-9{np.random.randint(10000000, 99999999)}" if np.random.rand() >= 0.05 else None for _ in range(cfg["n_patients"])],
        "address":       [f"District {np.random.randint(1, 12)}, HCMC, VN" if np.random.rand() >= 0.05 else None for _ in range(cfg["n_patients"])],
        "dob":           dobs,
        "date_of_death": date_of_deaths,
        "gender":        np.random.choice(GENDERS,      cfg["n_patients"], p=GENDER_PROBS),
        "ethnic":        np.random.choice(ETHNICITIES,  cfg["n_patients"], p=ETHNIC_PROBS),
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

    # ── 4. Visits ────────────────────────────────────────────────────────────
    logger.info(f"Generating {cfg['n_visits']:,} visits...")
    visit_ids = [f"V{i:07d}" for i in range(1, cfg["n_visits"] + 1)]
    visit_patients = np.random.choice(patient_ids, cfg["n_visits"])
    
    admission_timestamps = [start_date + timedelta(days=int(np.random.randint(0, total_days)), 
                                                   hours=int(np.random.randint(0, 24)),
                                                   minutes=int(np.random.randint(0, 60))) 
                            for _ in range(cfg["n_visits"])]
    
    discharge_timestamps = []
    for adm in admission_timestamps:
        if np.random.rand() < 0.05:
            discharge_timestamps.append(None)
        else:
            stay_duration_minutes = np.random.randint(60, 14 * 24 * 60)
            discharge_timestamps.append(adm + timedelta(minutes=stay_duration_minutes))
            
    ward_keys = list(WARDS.keys())
    ward_probs = [cfg["skew_ratio_ward"]] + [(1 - cfg["skew_ratio_ward"]) / (len(ward_keys) - 1)] * (len(ward_keys) - 1)
    visit_wards = np.random.choice(ward_keys, cfg["n_visits"], p=ward_probs)
    
    severities = []
    for adm in admission_timestamps:
        if adm < schema_evol_date:
            severities.append(None)
        else:
            severities.append(np.random.choice(["mild", "moderate", "severe"], p=[0.60, 0.30, 0.10]))
            
    df_visits = pd.DataFrame({
        "visit_id": visit_ids,
        "patient_id": visit_patients,
        "admission_timestamp": admission_timestamps,
        "discharge_timestamp": discharge_timestamps,
        "ward_id": visit_wards,
        "severity_level": severities
    })
    
    df_visits["visit_date"] = df_visits["admission_timestamp"].dt.strftime("%Y-%m-%d")
    
    visits_file = os.path.join(output_dir, "visits.parquet")
    pq.write_table(pa.Table.from_pandas(df_visits), visits_file)
    logger.info(f"Saved visits table → {visits_file}")
    
    # 5. Events Table
    logger.info(f"Generating {cfg['n_events']:,} clinical event observations...")
    event_ids = [f"EVT{i:08d}" for i in range(1, cfg["n_events"] + 1)]
    event_visits = np.random.choice(visit_ids, cfg["n_events"])
    
    visit_time_map = {row["visit_id"]: (row["admission_timestamp"], row["discharge_timestamp"]) 
                      for _, row in df_visits.iterrows()}
    
    event_timestamps = []
    for v_id in event_visits:
        adm_ts, dis_ts = visit_time_map[v_id]
        max_ts = end_date if (dis_ts is None or pd.isnull(dis_ts)) else dis_ts
        diff_sec = int((max_ts - adm_ts).total_seconds())
        event_timestamps.append(adm_ts + timedelta(seconds=np.random.randint(0, max(1, diff_sec))))
            
    meta_indices = np.random.choice(len(EVENT_METADATA_DEFS), cfg["n_events"])
    
    num_values, text_values = [], []
    for idx in meta_indices:
        meta = EVENT_METADATA_DEFS[idx]
        e_type, e_name = meta["event_type"], meta["event_name"]
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
        "event_id": event_ids,
        "visit_id": event_visits,
        "event_timestamp": event_timestamps,
        "event_type": [EVENT_METADATA_DEFS[i]["event_type"] for i in meta_indices],
        "event_type_id": [EVENT_METADATA_DEFS[i]["event_type_id"] for i in meta_indices],
        "event_name": [EVENT_METADATA_DEFS[i]["event_name"] for i in meta_indices],
        "text_value": text_values,
        "num_value": num_values
    })
    
    # Inject 2% duplicates
    n_dupes = int(cfg["n_events"] * cfg["duplicate_rate_events"])
    df_events = pd.concat([df_events, df_events.iloc[np.random.choice(cfg["n_events"], n_dupes)]], ignore_index=True)
    df_events = df_events.sort_values("event_timestamp").reset_index(drop=True)
    df_events["event_date"] = df_events["event_timestamp"].dt.strftime("%Y-%m-%d")
    
    events_file = os.path.join(output_dir, "events.parquet")
    pq.write_table(pa.Table.from_pandas(df_events), events_file)
    logger.info(f"Saved events table → {events_file}")
    
    # Verification Logs
    logger.info("=== Offline Dataset Characteristics ===")
    logger.info(f"- Patients Segment Size: {df_patients['patient_id'].nunique():,} unique patients")
    logger.info(f"- Ward Skewness: {(df_visits['ward_id'] == 'General_Ward').mean()*100:.1f}% visits in General_Ward")
    v1_visits = (df_visits["admission_timestamp"] < schema_evol_date).sum()
    v2_visits = (df_visits["admission_timestamp"] >= schema_evol_date).sum()
    logger.info(f"- Schema Evolution: {v1_visits:,} older visits missing severity_level, {v2_visits:,} newer visits with severity_level")
    n_unique_events = df_events["event_id"].nunique()
    total_events = len(df_events)
    logger.info(f"- Duplication Rate: {(total_events - n_unique_events)/n_unique_events * 100:.2f}% duplicates injected in events table")


def generate_streaming_data(output_dir):
    """
    Generates a continuous flat event stream representing Kafka payloads, written as JSON.
    """
    logger.info("Initializing streaming EHR event generation...")
    cfg = CONFIG["stream"]
    
    start_date = datetime.strptime(CONFIG["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(CONFIG["end_date"], "%Y-%m-%d")
    schema_evol_date = datetime.strptime(CONFIG["offline"]["schema_evolution_date"], "%Y-%m-%d")
    
    # Base entities for mapping
    # Since this is a self-contained simulator, generate lookup lists of active patient_ids & visit_ids
    n_patients = CONFIG["offline"]["n_patients"]
    n_visits = CONFIG["offline"]["n_visits"]
    patient_ids = [f"P{i:06d}" for i in range(1, n_patients + 1)]
    visit_ids = [f"V{i:07d}" for i in range(1, n_visits + 1)]
    
    # Time ranges & Burst Injection
    total_minutes = int((end_date - start_date).total_seconds() / 60)
    
    start_minute_of_day = start_date.hour * 60 + start_date.minute
    minute_offsets = np.arange(total_minutes)
    minute_indices = (minute_offsets + start_minute_of_day) % 1440
    
    day_weights = np.ones(1440)
    for window in cfg["burst_windows"]:
        start_h, start_m = map(int, window["start"].split(":"))
        end_h, end_m = map(int, window["end"].split(":"))
        start_idx = start_h * 60 + start_m
        end_idx = end_h * 60 + end_m
        if start_idx <= end_idx:
            day_weights[start_idx:end_idx] = cfg["burst_multiplier"]
        else:
            day_weights[start_idx:] = cfg["burst_multiplier"]
            day_weights[:end_idx] = cfg["burst_multiplier"]
            
    all_weights = day_weights[minute_indices]
    probs = all_weights / all_weights.sum()
    
    # Sample event times with bursts
    n_events = cfg["n_events"]
    sampled_offsets = np.random.choice(total_minutes, size=n_events, p=probs)
    event_timestamps = [start_date + timedelta(minutes=int(m)) for m in sampled_offsets]
    
    # Sort events chronologically to simulate chronological stream
    event_timestamps.sort()
    
    # Generate metadata details
    meta_choices = np.random.choice(len(EVENT_METADATA_DEFS), n_events)
    ward_keys = list(WARDS.keys())
    
    # Late Arrivals Logic (12% of events have created_ts delayed)
    created_timestamps = []
    is_late_list = []
    min_lag_min, max_lag_min = cfg["late_delay_min_max_mins"]
    
    for ts in event_timestamps:
        if np.random.rand() < cfg["late_arrival_rate"]:
            lag_mins = np.random.randint(min_lag_min, max_lag_min)
            created_timestamps.append(ts + timedelta(minutes=lag_mins))
            is_late_list.append(True)
        else:
            # Immediate processing lag of 0 to 5 seconds
            lag_secs = np.random.randint(0, 5)
            created_timestamps.append(ts + timedelta(seconds=lag_secs))
            is_late_list.append(False)
            
    # Generate flat event records
    stream_records = []
    for i in range(n_events):
        idx = meta_choices[i]
        meta = EVENT_METADATA_DEFS[idx]
        e_type = meta["event_type"]
        e_name = meta["event_name"]
        ts = event_timestamps[i]
        
        # Populate values
        num_val = None
        text_val = None
        if e_type == "vital":
            if e_name == "Heart Rate":
                num_val = np.round(np.random.normal(75, 12), 1)
            elif e_name == "Systolic BP":
                num_val = np.round(np.random.normal(120, 15), 1)
            elif e_name == "Diastolic BP":
                num_val = np.round(np.random.normal(80, 8), 1)
            elif e_name == "Temperature":
                num_val = np.round(np.random.normal(36.8, 0.4), 1)
        elif e_type == "lab":
            if e_name == "Glucose":
                num_val = np.round(np.random.normal(100, 20), 1)
            elif e_name == "Creatinine":
                num_val = np.round(np.random.normal(0.8, 0.2), 2)
            elif e_name == "WBC":
                num_val = np.round(np.random.normal(7.0, 2.0), 1)
            elif e_name == "Hemoglobin":
                num_val = np.round(np.random.normal(14.5, 1.5), 1)
        elif e_type == "medication":
            text_val = f"Administered {e_name} per physician prescription"
        elif e_type == "diagnosis":
            text_val = f"Confirmed diagnosis: {meta['event_description']}"
            
        # Schema evolution severity level (only v2.0 after cutoff date)
        severity = None
        if ts >= schema_evol_date:
            severity = np.random.choice(["mild", "moderate", "severe"], p=[0.60, 0.30, 0.10])
            
        record = {
            "event_id": f"EVT_STR{i+1:08d}",
            "visit_id": f"V{np.random.randint(1, n_visits + 1):07d}",
            "patient_id": f"P{np.random.randint(1, n_patients + 1):06d}",
            "ward_id": np.random.choice(ward_keys, p=[0.85, 0.10, 0.03, 0.02]), # Ward skew simulated on stream as well
            "event_type": e_type,
            "event_type_id": meta["event_type_id"],
            "event_name": e_name,
            "event_timestamp": ts.isoformat(),
            "created_ts": created_timestamps[i].isoformat(),
            "text_value": text_val,
            "num_value": num_val,
            "severity_level": severity,
            "device_type": np.random.choice(["monitor", "handheld", "desktop"], p=[0.70, 0.20, 0.10]),
            "source": np.random.choice(["ward_monitor", "hand_entry"], p=[0.70, 0.30])
        }
        stream_records.append(record)
        
    # Inject 1.5% duplicate streaming events (with 1 to 3 minute delay)
    n_stream_dupes = int(n_events * cfg["duplicate_rate_stream"])
    logger.info(f"Injecting {n_stream_dupes:,} duplicate events (1.5% duplicate rate) into stream...")
    
    dupe_source_indices = np.random.choice(n_events, n_stream_dupes)
    dupe_records = []
    
    for idx in dupe_source_indices:
        base_rec = stream_records[idx].copy()
        
        # Parse timestamp to add 1 to 3 minute delay
        base_ts = datetime.fromisoformat(base_rec["event_timestamp"])
        dupe_delay = timedelta(minutes=np.random.randint(1, 4))
        
        # Keep same event_id but update timestamp & created_ts slightly
        base_rec["event_timestamp"] = (base_ts + dupe_delay).isoformat()
        
        base_created = datetime.fromisoformat(base_rec["created_ts"])
        base_rec["created_ts"] = (base_created + dupe_delay).isoformat()
        
        dupe_records.append(base_rec)
        
    stream_records.extend(dupe_records)
    
    # Sort entire stream record set by event_timestamp
    stream_records.sort(key=lambda x: x["event_timestamp"])
    
    # Write output to clinical_event_stream.json
    output_file = os.path.join(output_dir, "clinical_event_stream.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(stream_records, f, indent=2)
        
    logger.info(f"Saved streaming data json file to: {output_file}")
    
    # Verification Logs
    logger.info("=== Streaming Dataset Characteristics ===")
    logger.info(f"- Stream size: {len(stream_records):,} records generated")
    
    actual_lates = sum(is_late_list)
    logger.info(f"- Late Arrival rate: {actual_lates/n_events * 100:.2f}% of baseline events had ingestion lag")
    
    # Average lab late lag
    lab_lags = []
    for r in stream_records:
        if r["event_type"] == "lab":
            t_evt = datetime.fromisoformat(r["event_timestamp"])
            t_cre = datetime.fromisoformat(r["created_ts"])
            lag_sec = (t_cre - t_evt).total_seconds()
            if lag_sec > 5: # Only count true late arrivals
                lab_lags.append(lag_sec / 60)
    if lab_lags:
        logger.info(f"- Avg late lab lag: {np.mean(lab_lags):.1f} minutes")
    else:
        logger.info("- Avg late lab lag: N/A")


def main():
    parser = argparse.ArgumentParser(description="EHR Synthetic Data Generator")
    parser.add_argument("--mode", type=str, choices=["offline", "stream", "both"], default="both",
                        help="Mode of data generation: offline relational tables, flat stream JSON, or both.")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Target output directory (default: source/data)")
    
    args = parser.parse_args()
    
    # Determine output folder
    if args.output_dir:
        output_dir = args.output_dir
    else:
        # Default output directory relative to script path
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.abspath(os.path.join(script_dir, "..", "..", "data", "synthetic"))
        
    os.makedirs(output_dir, exist_ok=True)
    
    # Seed numpy random
    np.random.seed(CONFIG["random_seed"])
    
    if args.mode in ["offline", "both"]:
        generate_offline_data(output_dir)
        
    if args.mode in ["stream", "both"]:
        generate_streaming_data(output_dir)
        
    logger.info("EHR event generation process completed successfully.")

if __name__ == "__main__":
    main()
