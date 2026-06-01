# EHR Lakehouse & Feature Store

A production-grade, rootless **Medallion Data Lakehouse** with a **Real-Time ML Feature Store**, built on synthetic Electronic Health Record (EHR) data. Demonstrates the complete journey from raw clinical events to ML-ready training datasets and live prediction serving.

---

## Architecture

```
┌─────────────────────────────── DATA SOURCES ──────────────────────────────────┐
│  1a. EHR Generator → patients / wards / visits / events (Parquet)             │
│  1b. Stream Producer → clinical_event_stream.json → Kafka: patient-events     │
└───────────────────────┬───────────────────────────────────────────────────────┘
                        │
          ┌─────────────┴──────────────────────────────────────────┐
          │ BATCH PATH                      STREAMING PATH          │
          │                                                         │
          ▼                                 ▼                       │
  2a. Bronze Ingest               6_flink_stream_processor          │
  (Pandas → Delta Lake)           ┌──────────────────────────┐      │
  raw_patients                    │  Reads Kafka ONCE        │      │
  raw_wards                       │  Fan-out to 2 sinks:     │      │
  raw_event_metadata              │                          │      │
  raw_visits                      │  Sink A: raw_streams     │      │
  raw_events                      │  (Delta Lake, Bronze)    │      │
          │                       │                          │      │
          │                       │  Sink B: SLIDE(24h/15m)  │      │
          ▼                       │  → patient-features-24h  │      │
  3. Silver Transform             │    (Kafka)               │      │
  (PySpark)                       └────────────┬─────────────┘      │
  stg_patients                                 │                    │
  stg_wards                                    ▼                    │
  stg_event_metadata               6_stream_to_online_store         │
  stg_visits                       Feast push → Online Store        │
  stg_events                                   │                    │
          │                                    ▼                    │
          ▼                          ┌──────────────────┐           │
  4. Gold Transform                  │  Feast           │           │
  (PySpark)                          │  Online Store    │           │
  ┌─ Conformed Dims ─────────────┐   │  (PostgreSQL)    │           │
  │  dim_patient                 │   └────────┬─────────┘           │
  │  dim_ward                    │            │                     │
  │  dim_event_type              │            ▼                     │
  ├─ Typed Dims ─────────────────┤   7_merge_features               │
  │  dim_vital                   │   Every 15 min:                  │
  │  dim_lab                     │   active patients +              │
  │  dim_medication              │   get_online_features()          │
  │  dim_diagnosis               │   → prediction_features          │
  ├─ Facts ──────────────────────┤     (PostgreSQL)                 │
  │  fact_visit                  │            │                     │
  │  fact_clinical_event         │            ▼                     │
  ├─ OBT ────────────────────────┤   Model Serving API              │
  │  obt_clinical_events         │                                  │
  └──────────────────────────────┘                                  │
          │                                                         │
          ▼                                                         │
  5. Feature Engineering                                            │
  (PySpark)                                                         │
  ┌─ Feature Tables (Feast Offline) ─┐                              │
  │  feat_patient_vitals_6m          │                              │
  │  feat_patient_labs_6m            │                              │
  │  feat_patient_icd_6m             │                              │
  │  feat_patient_medication_6m      │                              │
  │  feat_patient_demographics       │                              │
  ├─ Training Labels ────────────────┤                              │
  │  gold_visit_labels               │                              │
  │  (readmission_7d, mortality)     │                              │
  └──────────────────────────────────┘                              │
          │                                                         │
          ▼                                                         │
  5. Feast Apply + Materialize                                      │
  (Offline → Online Store)                                          │
          │                                                         │
          ▼                                                         │
  query_featurestore.py --export                                    │
  JOIN labels + feat_* → temporal split → train.parquet / test.parquet
```

---

## Technology Stack

| Component | Technology | Version | Role |
|---|---|---|---|
| **Batch Compute** | Apache Spark (PySpark) | 3.5.6 | Bronze→Silver→Gold transformations, feature engineering |
| **Stream Compute** | Apache Flink (PyFlink) | 1.19 | 24h sliding window features + raw stream archival |
| **SQL Query Engine** | Trino | 480 | Ad-hoc queries across Delta Lake; Feast offline store |
| **Table Format** | Delta Lake | 3.3.0 | ACID-compliant storage for all lakehouse layers |
| **Object Storage** | MinIO | latest | S3-compatible store for all Delta files |
| **Metastore** | Hive Metastore | 3.1.2 | Catalog for Trino → Delta mapping |
| **Message Broker** | Apache Kafka (KRaft) | latest | Real-time event streaming, no ZooKeeper |
| **Feature Store** | Feast | latest | Offline (Trino) + Online (PostgreSQL) feature serving |
| **Relational DB** | PostgreSQL | 15 | Feast online store + Hive metastore backend |
| **Cache** | Redis | 7 | Optional online store (alternative to PostgreSQL) |
| **Package Manager** | uv | latest | Python environment and script runner |

---

## Gold Layer Tables

### Conformed Dimensions

| Table | Description | Key columns |
|---|---|---|
| `dim_patient` | Patient registry | `patient_key`, `patient_id`, `gender`, `ethnic`, `country`, `dob`, `date_of_death` |
| `dim_ward` | Hospital wards | `ward_key`, `ward_id`, `ward_name`, `department` |
| `dim_event_type` | All clinical event types | `event_type_key`, `event_type_id`, `event_name`, `event_type`, `unit_of_measurement` |

### Typed Event Dimensions (derived from `dim_event_type`)

| Table | Event types | Extra |
|---|---|---|
| `dim_vital` | Heart Rate, Systolic BP, Diastolic BP, Temperature | `unit_of_measurement` |
| `dim_lab` | Glucose, Creatinine, WBC, Hemoglobin | `unit_of_measurement` |
| `dim_medication` | Lisinopril, Metformin, Amoxicillin, Atorvastatin | — |
| `dim_diagnosis` | Hypertension (I10), Diabetes (E11.9), URI (J06.9), Hyperlipidemia (E78.5) | `event_description` with ICD code |

### Facts & OBT

| Table | Grain | Key columns |
|---|---|---|
| `fact_visit` | One row per hospital visit | `visit_id`, `patient_key`, `ward_key`, `admission_timestamp`, `discharge_timestamp`, `severity_level`, `is_active` |
| `fact_clinical_event` | One row per clinical event | `event_id`, `visit_id`, `patient_key`, `ward_key`, `event_type_key`, `num_value`, `text_value` |
| `obt_clinical_events` | Denormalised — all dims joined to fact_clinical_event | All columns from all dims + facts |

### Feature Tables (Feast Offline Store)

| Table | Description | Columns |
|---|---|---|
| `feat_patient_vitals_6m` | 6-month vital stats per patient | `{vital}_mean/min/max/std` × 4 vitals = 16 cols |
| `feat_patient_labs_6m` | 6-month lab stats per patient | `{lab}_mean/min/max/std` × 4 labs = 16 cols |
| `feat_patient_icd_6m` | 6-month ICD chapter counts | `icd_chap_{I…XXII}` = 22 cols |
| `feat_patient_medication_6m` | 6-month administration counts | `{drug}_count` × 4 drugs = 4 cols |
| `feat_patient_demographics` | Static demographics + derived age | `gender`, `ethnic`, `country`, `age`, `is_deceased` |

### Training Labels

| Table | Grain | Labels |
|---|---|---|
| `gold_visit_labels` | One row per completed visit | `is_readmitted_7d`, `has_inpatient_mortality`, `has_30day_mortality` |

---

## Feature Store Design

### Offline → Online flow

```
5_compute_features.py    →  feat_* Delta tables (MinIO)
                         →  registered in Trino (delta.gold)
                         →  feast apply (feature_definitions.py)
                         →  feast materialize → Feast Online Store (PostgreSQL)
```

### Streaming → Online flow (24h rolling)

```
Kafka: patient-events
  → Flink SLIDE(24h, 15min) per patient, all 4 event types
  → Kafka: patient-features-24h
  → 6_stream_to_online_store.py
  → store.push() → Feast Online Store
```

### Feature schema at prediction time

The Flink processor reads `dim_vital`, `dim_lab`, `dim_medication`, `dim_diagnosis` **at job startup** to derive the output schema. Adding a new vital/lab/medication to the data pipeline automatically extends the rolling features without code changes.

### Feast FeatureViews

| FeatureView | Source | Features |
|---|---|---|
| `patient_vitals` | Kafka stream + batch | 16 rolling stats (stream overrides batch) |
| `patient_labs` | Batch (feat_patient_labs_6m) | 16 stats |
| `patient_icd` | Batch (feat_patient_icd_6m) | 22 ICD chapter counts |
| `patient_medication` | Batch (feat_patient_medication_6m) | 4 drug counts |
| `patient_demographics` | Batch (feat_patient_demographics) | 5 demographics |

All views share the `patient` entity (join key: `patient_id`) and are served together via the `patient_ml_features_v1` FeatureService.

---

## Pipeline — Ordered Run Guide

### Batch pipeline (run once, in order)

```bash
make generate           # generate synthetic Parquet + stream JSON
make ingest             # Bronze: raw_patients / raw_wards / raw_visits / raw_events
make transform          # Silver: stg_* (clean, deduplicate, derive columns)
make golden             # Gold: dim_* / fact_* / obt_* / typed dims
make features   # feat_* tables + gold_visit_labels (training labels)
make feast-apply        # register Feast feature definitions
make feast-materialize  # push batch features to Feast online store
```

### Streaming (run continuously, each in its own terminal)

```bash
make stream             # publish clinical events to Kafka: patient-events
make flink-stream       # Flink: Bronze archival + 24h rolling features → patient-features-24h
make feast-stream       # push Flink output to Feast online store
```

### Prediction serving (run continuously)

```bash
make merge-features     # every 15 min: active patients → get_online_features → prediction_features
```

### Validation and ML export

```bash
make query-lakehouse      # validate all Bronze / Silver / Gold tables in Trino
make query-featurestore   # test online feature retrieval from Feast
make export-ml-dataset    # build data/ml/train.parquet + test.parquet
make datahub-ingest       # push metadata lineage to DataHub
```

### Utilities

```bash
make check-ports   # check for port conflicts before starting
make help          # list all available make targets
make clean         # wipe .tmp/ and reset all container state
```

---

## UI Consoles

| UI | URL | Credentials | What to check |
|---|---|---|---|
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` | Browse `lakehouse/topics/` to see Delta table files; inspect `_delta_log/` for transaction history |
| **Trino Web UI** | http://localhost:8090 | — | Run SQL across all layers; monitor query plans and worker usage |
| **Spark Master UI** | http://localhost:8089 | — | Track running/completed Spark jobs; inspect worker memory and CPU |
| **Spark Worker UI** | http://localhost:8091 | — | Per-worker task execution and logs |
| **Flink Dashboard** | http://localhost:8088 | — | Monitor the unified stream job; inspect both sink operators (raw_streams + features); check watermark lag |
| **Redpanda Console** | http://localhost:8086 | — | Browse `patient-events` and `patient-features-24h` topics; inspect message offsets and consumer group lag |
| **pgweb** | http://localhost:8085 | — | Browse PostgreSQL: Feast online store (`feast_*` tables) and `prediction_features` |

---

## Port Directory

All services run under `network_mode: host` (rootless Podman). Every port is on `localhost`.

| Port | Service | Protocol | Notes |
|---:|---|---|---|
| `5432` | PostgreSQL | SQL | Feast online store + Hive metastore backend |
| `6123` | Flink JobManager RPC | TCP | Internal Flink cluster communication |
| `6379` | Redis | TCP | Optional Feast online store alternative |
| `7077` | Spark Master | TCP | `spark-submit --master spark://127.0.0.1:7077` |
| `8080` | DataHub GMS | HTTP | Metadata service API |
| `8081` | Schema Registry | HTTP REST | Confluent Schema Registry |
| `8083` | Debezium Connect | HTTP REST | Kafka Connect CDC engine |
| `8085` | pgweb | HTTP | PostgreSQL visual browser |
| `8086` | Redpanda Console | HTTP | Kafka topic and consumer UI |
| `8088` | Flink REST / Dashboard | HTTP | Job submission + monitoring UI |
| `8089` | Spark Master Web UI | HTTP | Spark cluster monitoring |
| `8090` | Trino | HTTP | SQL query engine + Web UI |
| `8091` | Spark Worker Web UI | HTTP | Per-worker status |
| `9000` | MinIO S3 API | HTTP | S3-compatible endpoint for all Delta writes |
| `9001` | MinIO Console | HTTP | Object storage browser |
| `9083` | Hive Metastore | Thrift | Table catalog for Trino |
| `9092` | Kafka Broker | TCP | `PLAINTEXT://localhost:9092` |
| `9093` | Kafka Controller | TCP | KRaft quorum listener |

---

## Quick Start

```bash
# 0. Configure environment
cp source/.env.example source/.env

# 1. Check ports and start all services
make check-ports
make up && make ps

# 2. Run the full batch pipeline
make generate && make ingest && make transform && make golden
make features
make feast-apply && make feast-materialize

# 3. Open 3 terminals for the streaming pipeline
make stream            # terminal 1 — publish events to Kafka
make flink-stream      # terminal 2 — Flink unified processor
make feast-stream      # terminal 3 — Feast online store writer

# 4. Start prediction serving (terminal 4)
make merge-features

# 5. Validate everything and export ML datasets
make query-lakehouse
make export-ml-dataset
```

---

## Storage Layout

All runtime data is isolated under `.tmp/` (never committed):

```
.tmp/
├── kafka-data/          Kafka KRaft log storage
├── postgres-data/       PostgreSQL data directory
├── redis-data/          Redis persistence
├── minio-data/          MinIO object storage
│   └── lakehouse/
│       └── topics/      All Delta Lake tables
│           ├── raw_*/   Bronze batch tables
│           ├── raw_streams/  Bronze stream archive
│           ├── stg_*/   Silver tables
│           ├── dim_*/   Gold dimension tables
│           ├── fact_*/  Gold fact tables
│           ├── obt_*/   Gold one-big-table
│           ├── feat_*/  Feature tables (Feast offline)
│           └── gold_visit_labels/  Training labels
├── spark-logs/          Spark application logs
├── flink-logs/          Flink job logs
└── flink-lib/           Flink connector JARs
```

Run `make clean` to wipe all `.tmp/` state and start fresh.
