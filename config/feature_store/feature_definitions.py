import os
from datetime import timedelta

from dotenv import load_dotenv
from feast import Entity, FeatureService, FeatureView, Field, PushSource
from feast.data_format import JsonFormat
from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import TrinoSource
from feast.types import Float64, Int64, String

CUR_DIR      = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CUR_DIR, "..", ".."))
os.environ["PROJECT_ROOT"] = PROJECT_ROOT
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# ── Entity ────────────────────────────────────────────────────────────────────
patient = Entity(name="patient", join_keys=["patient_id"])

# ── ICD-10 chapter column names (must match 5_compute_features.py output) ─────
_ICD_CHAPTERS = [
    "I", "II", "III", "IV", "V", "VI", "VII", "VIII",
    "IX", "X", "XI", "XII", "XIII", "XIV", "XV", "XVI",
    "XVII", "XVIII", "XIX", "XX", "XXI", "XXII",
]

# ── Offline Batch Sources ──────────────────────────────────────────────────────
# Each reads from a pre-aggregated feat_* Delta table produced by
# 5_compute_features.py. `feature_timestamp` is the as-of date of the
# snapshot; Feast uses it for point-in-time joins during training retrieval.

_vitals_batch_source = TrinoSource(
    name="feat_patient_vitals_6m",
    timestamp_field="feature_timestamp",
    query="(SELECT * FROM delta.gold.feat_patient_vitals_6m)",
)

_labs_batch_source = TrinoSource(
    name="feat_patient_labs_6m",
    timestamp_field="feature_timestamp",
    query="(SELECT * FROM delta.gold.feat_patient_labs_6m)",
)

_icd_batch_source = TrinoSource(
    name="feat_patient_icd_6m",
    timestamp_field="feature_timestamp",
    query="(SELECT * FROM delta.gold.feat_patient_icd_6m)",
)

_medication_batch_source = TrinoSource(
    name="feat_patient_medication_6m",
    timestamp_field="feature_timestamp",
    query="(SELECT * FROM delta.gold.feat_patient_medication_6m)",
)

_demographics_source = TrinoSource(
    name="feat_patient_demographics",
    timestamp_field="feature_timestamp",
    query="(SELECT * FROM delta.gold.feat_patient_demographics)",
)

_labels_source = TrinoSource(
    name="gold_visit_labels",
    timestamp_field="feature_timestamp",
    query="(SELECT * FROM delta.gold.gold_visit_labels)",
)

# ── Streaming Source (vitals — real-time push from 6_stream_to_online_store.py)
# Schema matches the DataFrame pushed via store.push() in the stream consumer.
patient_vitals_stream_source = PushSource(
    name="patient_vitals_stream",
    batch_source=_vitals_batch_source,
)

# ── Feature Views ─────────────────────────────────────────────────────────────

_vitals_names = ["heart_rate", "systolic_bp", "diastolic_bp", "temperature"]
_labs_names = ["glucose", "creatinine", "wbc", "hemoglobin"]
_stats = ["mean", "min", "max", "std"]
_meds_names = ["lisinopril", "metformin", "amoxicillin", "atorvastatin"]

patient_vitals_fv = FeatureView(
    name="patient_vitals",
    entities=[patient],
    ttl=timedelta(days=365),
    schema=[
        Field(name=f"{v}_{s}", dtype=Float64)
        for v in _vitals_names
        for s in _stats
    ],
    online=True,
    source=patient_vitals_stream_source,
)

patient_labs_fv = FeatureView(
    name="patient_labs",
    entities=[patient],
    ttl=timedelta(days=365),
    schema=[
        Field(name=f"{l}_{s}", dtype=Float64)
        for l in _labs_names
        for s in _stats
    ],
    online=True,
    source=_labs_batch_source,
)

patient_icd_fv = FeatureView(
    name="patient_icd",
    entities=[patient],
    ttl=timedelta(days=365),
    schema=[
        Field(name=f"icd_chap_{r}", dtype=Int64)
        for r in _ICD_CHAPTERS
    ],
    online=True,
    source=_icd_batch_source,
)

patient_medication_fv = FeatureView(
    name="patient_medication",
    entities=[patient],
    ttl=timedelta(days=365),
    schema=[
        Field(name=f"{m}_count", dtype=Int64)
        for m in _meds_names
    ],
    online=True,
    source=_medication_batch_source,
)

patient_demographics_fv = FeatureView(
    name="patient_demographics",
    entities=[patient],
    ttl=timedelta(days=3650),  # effectively permanent — demographics rarely change
    schema=[
        Field(name="gender",      dtype=String),
        Field(name="ethnic",      dtype=String),
        Field(name="country",     dtype=String),
        Field(name="age",         dtype=Float64),
        Field(name="is_deceased", dtype=Int64),
    ],
    online=True,
    source=_demographics_source,
)

patient_labels_fv = FeatureView(
    name="patient_labels",
    entities=[patient],
    ttl=timedelta(days=3650),
    schema=[
        Field(name="visit_id",                dtype=String),
        Field(name="admission_timestamp",     dtype=String),
        Field(name="discharge_timestamp",     dtype=String),
        Field(name="severity_level",          dtype=String),
        Field(name="is_readmitted_7d",        dtype=Int64),
        Field(name="has_inpatient_mortality", dtype=Int64),
        Field(name="has_30day_mortality",     dtype=Int64),
    ],
    online=False,   # labels are not needed in real-time serving
    source=_labels_source,
)

# ── Feature Services ───────────────────────────────────────────────────────────

# Online serving — features only, no labels
#   store.get_online_features(entity_rows, features=patient_ml_features_v1)
patient_ml_features_v1 = FeatureService(
    name="patient_ml_features_v1",
    features=[
        patient_vitals_fv,
        patient_labs_fv,
        patient_icd_fv,
        patient_medication_fv,
        patient_demographics_fv,
    ],
)

# Training — features + labels, offline only
#   store.get_historical_features(entity_df, features=patient_training_v1)
patient_training_v1 = FeatureService(
    name="patient_training_v1",
    features=[
        patient_vitals_fv,
        patient_labs_fv,
        patient_icd_fv,
        patient_medication_fv,
        patient_demographics_fv,
        patient_labels_fv,
    ],
)
