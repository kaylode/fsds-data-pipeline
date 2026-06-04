# Feature Store Design

## Overview

The feature store is built on **Feast** and provides two complementary serving paths:

| Path  | Source | Use case |
|---|---|---|---|
| **Offline** | Trino / Delta Lake (`feat_*` tables) | Training data retrieval, point-in-time joins |
| **Online** | Redis | Real-time inference, prediction serving |

```
Batch pipeline (5_compute_features.py)
    │
    ├──▶ feat_* Delta tables (MinIO / Trino)       ← Feast Offline Store
    │         │
    │         ▼
    │    feast apply + feast materialize
    │         │
    │         ▼
    │    Redis online store                         ← Feast Online Store (batch path)
    │
Flink stream (6_flink_stream_processor.py)
    │
    └──▶ Kafka: patient-features-24h
              │
              ▼
         7_feature_pipeline.py (store.push())
              │
              ▼
         Redis online store                         ← Feast Online Store (stream path, overrides batch)
```

---

## Offline Store

### Backend

- **Engine:** Trino (`delta` catalog)
- **Storage:** MinIO → Delta Lake tables under `delta.gold.feat_*`
- **Point-in-time join:** Feast uses `feature_timestamp` column on each table to support temporal joins for training data retrieval

### Feature Tables

All feature tables have grain: **one row per patient**, with a `feature_timestamp` representing the as-of date of the snapshot.

#### `feat_patient_vitals_6m`

6-month rolling aggregate statistics for vital signs.

| Column | Type | Description |
|---|---|---|
| `patient_id` | `VARCHAR` | Entity key |
| `feature_timestamp` | `TIMESTAMP` | Snapshot date |
| `heart_rate_mean` | `DOUBLE` | Mean heart rate over 6 months |
| `heart_rate_min` | `DOUBLE` | Min heart rate |
| `heart_rate_max` | `DOUBLE` | Max heart rate |
| `heart_rate_std` | `DOUBLE` | Std dev of heart rate |
| `systolic_bp_mean` | `DOUBLE` | Mean systolic blood pressure |
| `systolic_bp_min` | `DOUBLE` | — |
| `systolic_bp_max` | `DOUBLE` | — |
| `systolic_bp_std` | `DOUBLE` | — |
| `diastolic_bp_mean` | `DOUBLE` | Mean diastolic blood pressure |
| `diastolic_bp_min` | `DOUBLE` | — |
| `diastolic_bp_max` | `DOUBLE` | — |
| `diastolic_bp_std` | `DOUBLE` | — |
| `temperature_mean` | `DOUBLE` | Mean temperature |
| `temperature_min` | `DOUBLE` | — |
| `temperature_max` | `DOUBLE` | — |
| `temperature_std` | `DOUBLE` | — |

**Total: 16 feature columns** (4 vitals × 4 stats)

---

#### `feat_patient_labs_6m`

6-month rolling aggregate statistics for lab results.

| Column | Type | Description |
|---|---|---|
| `patient_id` | `VARCHAR` | Entity key |
| `feature_timestamp` | `TIMESTAMP` | Snapshot date |
| `glucose_mean/min/max/std` | `DOUBLE` | Glucose stats |
| `creatinine_mean/min/max/std` | `DOUBLE` | Creatinine stats |
| `wbc_mean/min/max/std` | `DOUBLE` | White blood cell count stats |
| `hemoglobin_mean/min/max/std` | `DOUBLE` | Hemoglobin stats |

**Total: 16 feature columns** (4 labs × 4 stats)

---

#### `feat_patient_icd_6m`

6-month ICD-10 chapter diagnosis counts per patient.

| Column | Type | Description |
|---|---|---|
| `patient_id` | `VARCHAR` | Entity key |
| `feature_timestamp` | `TIMESTAMP` | Snapshot date |
| `icd_chap_I` … `icd_chap_XXII` | `INT` | Count of diagnoses in each ICD-10 chapter |

**Total: 22 feature columns** (one per ICD-10 chapter)

---

#### `feat_patient_medication_6m`

6-month medication administration counts per patient.

| Column | Type | Description |
|---|---|---|
| `patient_id` | `VARCHAR` | Entity key |
| `feature_timestamp` | `TIMESTAMP` | Snapshot date |
| `lisinopril_count` | `INT` | Number of administrations |
| `metformin_count` | `INT` | — |
| `amoxicillin_count` | `INT` | — |
| `atorvastatin_count` | `INT` | — |

**Total: 4 feature columns**

---

#### `feat_patient_demographics`

Static patient demographics. TTL: 10 years (effectively permanent).

| Column | Type | Description |
|---|---|---|
| `patient_id` | `VARCHAR` | Entity key |
| `feature_timestamp` | `TIMESTAMP` | Snapshot date |
| `gender` | `VARCHAR` | Standardised gender |
| `ethnic` | `VARCHAR` | Ethnicity |
| `country` | `VARCHAR` | Country |
| `age` | `DOUBLE` | Age at snapshot date |
| `is_deceased` | `INT` | 1 if deceased, 0 otherwise |

**Total: 5 feature columns**

---

#### `gold_visit_labels` (Training Labels — Offline Only)

Not served online. Used exclusively for supervised learning dataset construction.

| Column | Type | Description |
|---|---|---|
| `patient_id` | `VARCHAR` | Entity key |
| `visit_id` | `VARCHAR` | Visit context |
| `feature_timestamp` | `TIMESTAMP` | Discharge date (label as-of time) |
| `admission_timestamp` | `VARCHAR` | Admission time |
| `discharge_timestamp` | `VARCHAR` | Discharge time |
| `severity_level` | `VARCHAR` | Visit severity |
| `is_readmitted_7d` | `INT` | 1 if patient readmitted within 7 days |
| `has_inpatient_mortality` | `INT` | 1 if patient died during the visit |
| `has_30day_mortality` | `INT` | 1 if patient died within 30 days of discharge |

---

## Online Store

### Backend

- **Engine:** Redis (`localhost:6379`)
- **Serialisation:** Feast default (protobuf)
- **TTL:** Controlled per FeatureView (vitals/labs: 1 year, demographics: 10 years)

---


### FeatureViews

| FeatureView | Source | Online | TTL |
|---|---|---|---|
| `patient_vitals` | Kafka stream (+ batch fallback) | ✅ | 1 year |
| `patient_labs` | `feat_patient_labs_6m` (batch) | ✅ | 1 year |
| `patient_icd` | `feat_patient_icd_6m` (batch) | ✅ | 1 year |
| `patient_medication` | `feat_patient_medication_6m` (batch) | ✅ | 1 year |
| `patient_demographics` | `feat_patient_demographics` (batch) | ✅ | 10 years |
| `patient_labels` | `gold_visit_labels` (batch) | ❌ (offline only) | 10 years |

### FeatureServices

| FeatureService | Views included | Purpose |
|---|---|---|
| `patient_ml_features_v1` | All 5 online views | Real-time inference |
| `patient_training_v1` | All 5 online + labels | Training dataset retrieval |

---

## Feature Summary

| Source table | Features | Total columns |
|---|---|---|
| `feat_patient_vitals_6m` | 4 vitals × 4 stats | 16 |
| `feat_patient_labs_6m` | 4 labs × 4 stats | 16 |
| `feat_patient_icd_6m` | 22 ICD chapters | 22 |
| `feat_patient_medication_6m` | 4 medications | 4 |
| `feat_patient_demographics` | 5 demographics | 5 |
| **Total ML features** | | **63** |
| `gold_visit_labels` | 3 label columns | 3 |
