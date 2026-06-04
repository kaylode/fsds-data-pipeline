# EHR Lakehouse & Feature Store

> A production-grade **Medallion Data Lakehouse** with a **Real-Time ML Feature Store**, built entirely on synthetic Electronic Health Records (EHR). The system ingests raw clinical data from both batch (Parquet files) and stream (Kafka) sources, applies a three-layer medallion transformation (Bronze → Silver → Gold) using Apache Spark and PyFlink, and materialises ML-ready patient features into an offline (Trino/Delta Lake) and online (Redis) Feast feature store. Metadata governance is handled end-to-end by DataHub, with pipeline orchestration via Apache Airflow. The dataset represents ~1,000 synthetic patients across 5 clinical event types (vitals, labs, medications, diagnoses, ward events), covering admissions, discharges, and longitudinal clinical measurements.

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
  stg_event_metadata               7_feature_pipeline               │
  stg_visits                       Feast push → Online Store        │
  stg_events                                   │                    │
          │                                    ▼                    │
          ▼                          ┌──────────────────┐           │
  4. Gold Transform                  │  Feast           │           │
  (PySpark)                          │  Online Store    │           │
  dim_* / fact_* / obt_*             │  (Redis)         │           │
          │                          └──────────────────┘           │
          ▼                                                         │
  5. Feature Engineering                                            │
  (PySpark)                                                         │
  feat_patient_vitals_6m                                            │
  feat_patient_labs_6m                                              │
  feat_patient_icd_6m                                               │
  feat_patient_medication_6m                                        │
  feat_patient_demographics                                         │
  gold_visit_labels (training labels)                               │
          │                                                         │
          ▼                                                         │
  Feast Apply + Materialize                                         │
  (Offline → Online Store)                                          │
└─────────────────────────────────────────────────────────────────-─┘

Metadata Governance: DataHub (lineage + catalogue)
Orchestration:       Apache Airflow (DAG scheduling)
```

See [`docs/`](docs/) for detailed layer designs, schemas, and feature store documentation.

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
| **Feature Store** | Feast | latest | Offline (Trino/Delta) + Online (Redis) feature serving |
| **Relational DB** | PostgreSQL | 15 | Airflow backend + Hive metastore backend |
| **Cache / Online Store** | Redis | 7 | Feast online feature serving |
| **Metadata Platform** | DataHub | v1.5.0.6 | Data catalogue, lineage, governance |
| **Orchestration** | Apache Airflow | 2.10.2 | DAG scheduling for batch pipeline |
| **Container Runtime** | Podman (rootless) | latest | All services run without root |
| **Package Manager** | uv | latest | Python environment and script runner |

---

## Run Guide

### 0. Prerequisites

```bash
cp source/.env.example source/.env   # configure environment
make check-ports                      # verify no port conflicts
make build-airflow                    # build custom Airflow image (first time only)
make build-spark                      # build custom Spark image (first time only)
```

### 1. Start infrastructure

```bash
make up            # core stack: Kafka, Spark, Flink, Trino, MinIO, Postgres, Redis
make datahub-up    # DataHub (auto-runs ingestion + metadata enrichment)
make airflow-up    # Airflow webserver + scheduler
```

### 2. Batch pipeline (run in order)

```bash
make generate       # generate synthetic Parquet + stream JSON
make ingest         # Bronze: ingest Parquet → Delta Lake
make transform      # Silver: clean, deduplicate, normalise
make golden         # Gold: dims, facts, OBT
make features       # Feature tables + training labels
```

### 3. Streaming (each in a separate terminal)

```bash
make stream         # publish events → Kafka: patient-events
make flink-stream   # Flink: Bronze archival + 24h rolling features
```

### 4. Validate

```bash
make query-lakehouse      # validate all Bronze / Silver / Gold tables in Trino
make query-featurestore   # test online feature retrieval from Feast
```

### UI consoles

| UI | URL | Credentials |
|---|---|---|
| **Airflow** | http://localhost:8082 | `airflow` / `airflow` |
| **DataHub** | http://localhost:9002 | — |
| **Trino** | http://localhost:8090 | — |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **Flink Dashboard** | http://localhost:8095 | — |
| **Spark Master UI** | http://localhost:8089 | — |
| **Redpanda Console** | http://localhost:8086 | — |
| **pgweb** | http://localhost:8085 | — |

---

## Storage Layout

All runtime data is isolated under `.tmp/` (never committed to git):

```
.tmp/
├── kafka-data/          Kafka KRaft log storage
├── postgres-data/       PostgreSQL data directory
├── redis-data/          Redis persistence
├── minio-data/
│   └── lakehouse/
│       └── topics/      All Delta Lake tables
│           ├── raw_*/           Bronze batch tables
│           ├── raw_streams/     Bronze stream archive (Flink)
│           ├── stg_*/           Silver tables
│           ├── dim_*/           Gold dimension tables
│           ├── fact_*/          Gold fact tables
│           ├── obt_*/           Gold one-big-table
│           ├── feat_*/          Feature tables (Feast offline)
│           └── gold_visit_labels/  Training labels
├── spark-logs/          Spark application logs
├── flink-logs/          Flink job logs
└── flink-lib/           Flink connector JARs (auto-downloaded)
```

Run `make clean` to wipe all `.tmp/` state and start fresh.

---

## Port Directory

All services run under `network_mode: host` (rootless Podman). Every port is on `localhost`.

| Port | Service | Protocol | Notes |
|---:|---|---|---|
| `5432` | PostgreSQL | SQL | Airflow DB + Hive metastore backend |
| `5433` | PostgreSQL (Metastore) | SQL | Hive Metastore dedicated DB |
| `6379` | Redis | TCP | Feast online store |
| `7077` | Spark Master | TCP | `spark-submit --master spark://127.0.0.1:7077` |
| `8081` | Schema Registry | HTTP | Confluent Schema Registry |
| `8082` | Airflow Webserver | HTTP | Pipeline orchestration UI |
| `8083` | Debezium Connect | HTTP | Kafka Connect CDC engine |
| `8085` | pgweb | HTTP | PostgreSQL visual browser |
| `8086` | Redpanda Console | HTTP | Kafka topic and consumer UI |
| `8088` | DataHub GMS | HTTP | Metadata service REST API |
| `8089` | Spark Master Web UI | HTTP | Spark cluster monitoring |
| `8090` | Trino | HTTP | SQL query engine + Web UI |
| `8091` | Spark Worker Web UI | HTTP | Per-worker status |
| `8095` | Flink REST / Dashboard | HTTP | Job submission + monitoring UI |
| `9000` | MinIO S3 API | HTTP | S3-compatible endpoint for Delta writes |
| `9001` | MinIO Console | HTTP | Object storage browser |
| `9002` | DataHub Frontend | HTTP | DataHub UI |
| `9083` | Hive Metastore | Thrift | Table catalog for Trino |
| `9092` | Kafka Broker | TCP | `PLAINTEXT://localhost:9092` |
| `9093` | Kafka Controller | TCP | KRaft quorum listener |
| `9200` | OpenSearch | HTTP | DataHub search backend |
