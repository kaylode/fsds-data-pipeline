# EHR Lakehouse & Feature Store

> A production-grade **Medallion Data Lakehouse** with a **Real-Time ML Feature Store**, built entirely on synthetic Electronic Health Records (EHR). The system ingests raw clinical data from both batch (Parquet files) and stream (Kafka) sources, applies a three-layer medallion transformation (Bronze → Silver → Gold) using Apache Spark and PyFlink, and materialises ML-ready patient features into an offline (Trino/Delta Lake) and online (Redis) Feast feature store. Metadata governance is handled end-to-end by DataHub, with pipeline orchestration via Apache Airflow. The dataset represents ~1,000 synthetic patients across 5 clinical event types (vitals, labs, medications, diagnoses, ward events), covering admissions, discharges, and longitudinal clinical measurements.
>
> All services run **rootless** via **Podman** (no Docker daemon, no root required) using `network_mode: host` for maximum compatibility on shared HPC / university clusters.

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
  stg_visits                       feast apply + materialize        │
  stg_events                       + Kafka → Feast push             │
          │                        → Online Store (Redis)           │
          ▼                                    │                    │
  4. Gold Transform                            ▼                    │
  (PySpark)                          ┌──────────────────┐           │
  dim_* / fact_* / obt_*             │  Feast           │           │
          │                          │  Online Store    │           │
          ▼                          │  (Redis)         │           │
  5. Feature Engineering             └──────────────────┘           │
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
Orchestration:       Apache Airflow (2 DAGs: ehr_data_pipeline + datahub_metadata_ingestion)
```

See [`docs/`](docs/) for detailed layer designs, schemas, and feature store documentation.
See [`docs/00_installation.md`](docs/00_installation.md) for full installation and setup instructions.

---

## Technology Stack

| Component | Technology | Version | Role |
|---|---|---|---|
| **Batch Compute** | Apache Spark (PySpark) | 3.5.6 | Bronze→Silver→Gold transformations, feature engineering |
| **Stream Compute** | Apache Flink (PyFlink) | 2.2.0 | 24h sliding window features + raw stream archival |
| **SQL Query Engine** | Trino | 480 | Ad-hoc queries across Delta Lake; Feast offline store |
| **Table Format** | Delta Lake | 3.3.0 | ACID-compliant storage for all lakehouse layers |
| **Object Storage** | MinIO | latest | S3-compatible store for all Delta files |
| **Metastore** | Hive Metastore | 3.1.2 | Catalog for Trino → Delta mapping |
| **Message Broker** | Apache Kafka (KRaft) | latest | Real-time event streaming, no ZooKeeper |
| **Feature Store** | Feast | latest | Offline (Trino/Delta) + Online (Redis) feature serving |
| **Relational DB** | PostgreSQL | 15 | Airflow backend + Hive metastore backend |
| **Cache / Online Store** | Redis | 7 | Feast online feature serving |
| **Metadata Platform** | DataHub | v1.5.0.6 | Data catalogue, lineage, governance |
| **Orchestration** | Apache Airflow | 2.10.2 | DAG scheduling for batch + streaming pipeline |
| **Container Runtime** | Podman (rootless) | latest | All services run without root, no daemon required |
| **Package Manager** | uv | latest | Python environment and script runner |

---

## Run Guide

> **First time?** See [`docs/00_installation.md`](docs/00_installation.md) for Podman installation, JAR download, and image build instructions.

### 0. Prerequisites

```bash
cp .env.example .env            # configure environment
make check-ports                # verify no port conflicts
make build-airflow              # build custom Airflow image (first time only)
make build-spark                # build custom Spark image (first time only)
```

### 1. Generate data

```bash
make generate       # generate synthetic Parquet + stream JSON
```

### 2. Start infrastructure

```bash
make up            # core stack: Kafka, Spark, Flink, Trino, MinIO, Postgres, Redis
make datahub-up    # DataHub (auto-runs ingestion + metadata enrichment)
make airflow-up    # Airflow webserver + scheduler
```

**Verify all containers are running:**

```bash
make ps
```

> _[Screenshot placeholder: output of `make ps` showing all containers healthy]_

### 3. Batch pipeline (run in order)

```bash
make ingest         # Bronze: ingest Parquet → Delta Lake
make transform      # Silver: clean, deduplicate, normalise
make golden         # Gold: dims, facts, OBT
make features       # Feature tables + training labels
```

### 4. Streaming (each in a separate terminal)

```bash
make stream            # publish events → Kafka: patient-events
make flink-stream      # Flink: Bronze archival + 24h sliding window → Kafka: patient-features-24h
make feature-pipeline  # Feast apply + materialize + continuous Kafka → Redis push
```

**Streaming data flow:**
```
1b_produce_stream.py
        │
        ▼  (Kafka: patient-events)
6_flink_stream_processor.py
        │
        ├──▶  Sink A: Delta Lake (raw_streams, Bronze)
        │
        └──▶  Sink B: Kafka: patient-features-24h
                        │
                        ▼
              7_feature_pipeline.py
                        │
                        ▼  (store.push())
                  Redis online store
```

### 5. Validate

```bash
make query-lakehouse      # validate all Bronze / Silver / Gold tables in Trino
make query-featurestore   # test online feature retrieval from Feast
```

### UI Consoles

| UI | URL | Credentials |
|---|---|---|
| **Airflow** | http://localhost:8082 | `airflow` / `airflow` |
| **DataHub** | http://localhost:9002 | `datahub` / `datahub` |
| **Trino** | http://localhost:8090 | — |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **Flink Dashboard** | http://localhost:8087 | — |
| **Spark Master UI** | http://localhost:8089 | — |
| **Redpanda Console** | http://localhost:8086 | — |
| **pgweb** | http://localhost:8085 | — |

---

## Airflow DAGs

Two DAGs are registered:

| DAG | Schedule | Purpose |
|---|---|---|
| `ehr_data_pipeline` | `@daily` | Full batch + streaming pipeline: ingest → validate → silver → gold → features → feast materialize, plus Flink stream processor + stream feature push |
| `datahub_metadata_ingestion` | `*/5 * * * *` | Crawls all sources (postgres, kafka, hive, trino, minio) and refreshes DataHub catalogue |

### `ehr_data_pipeline` task graph

```
bronze_ingest
    │
    ▼
validate_bronze        ← quality gates: row count, null PK, duplicate PK
    │                    results emitted as AssertionRunEvents to DataHub
    ▼
silver_transform
    │
    ▼
gold_transform
    │
    ▼
compute_features
    │
    ▼
feast_materialize       ← feast apply + feast materialize → Redis

flink_stream_processor  ─── (independent branch, runs in parallel)
    │
    ▼
push_stream_features    ← drains patient-features-24h → Redis
```

Each task emits **DataHub lineage events** (DataFlow, DataJob, UpstreamLineage MCPs) and records execution metadata to the `ehr_pipeline_runs` PostgreSQL table.

---

## DataHub Governance

DataHub is automatically populated when running `make datahub-up`. It tracks:

- **Dataset catalogue** — all Bronze, Silver, Gold, Feature, Kafka, and Redis datasets
- **Column-level lineage** — from raw Parquet files through to the online feature store
- **Quality assertions** — from the `validate_bronze` task (row count, null PK, duplicate PK)
- **Ownership, Tags, Glossary Terms, Domains** — via `make datahub-enrich`

Registered sources (via `datahub_metadata_ingestion` DAG):

| Source | Type | What is crawled |
|---|---|---|
| `ingest_postgres` | `postgres` | Airflow DB + Hive metastore schemas |
| `ingest_kafka` | `kafka` | Topics + Schema Registry |
| `ingest_hive_metastore` | `hive-metastore` | Hive catalog (Delta table mappings) |
| `ingest_trino` | `trino` | All `delta.*` tables via Trino |
| `ingest_minio_lakehouse` | `s3` | S3 paths under `s3://lakehouse/` |

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
└── flink-lib/           Flink connector JARs (auto-downloaded by make up)
```

Run `make clean` to wipe all `.tmp/` state and start fresh.

---

## Port Directory

All services run under `network_mode: host` (rootless Podman). Every port is on `localhost`.
Ports are configurable via `.env` — see `.env.example` for all variable names.

| Port | Service | Protocol | Notes |
|---:|---|---|---|
| `5432` | PostgreSQL | SQL | Airflow DB + Hive metastore backend |
| `5433` | PostgreSQL (Metastore) | SQL | Hive Metastore dedicated DB |
| `6123` | Flink JobManager RPC | TCP | Internal Flink cluster communication |
| `6379` | Redis | TCP | Feast online store |
| `7077` | Spark Master | TCP | `spark-submit --master spark://127.0.0.1:7077` |
| `8081` | Schema Registry | HTTP | Confluent Schema Registry |
| `8082` | Airflow Webserver | HTTP | Pipeline orchestration UI |
| `8083` | Debezium Connect | HTTP | Kafka Connect CDC engine |
| `8085` | pgweb | HTTP | PostgreSQL visual browser |
| `8086` | Redpanda Console | HTTP | Kafka topic and consumer UI |
| `8087` | Flink REST / Dashboard | HTTP | Job submission + monitoring UI (`FLINK_REST_PORT`) |
| `8088` | DataHub GMS | HTTP | Metadata service REST API |
| `8089` | Spark Master Web UI | HTTP | Spark cluster monitoring |
| `8090` | Trino | HTTP | SQL query engine + Web UI |
| `8091` | Spark Worker Web UI | HTTP | Per-worker status |
| `9000` | MinIO S3 API | HTTP | S3-compatible endpoint for Delta writes |
| `9001` | MinIO Console | HTTP | Object storage browser |
| `9002` | DataHub Frontend | HTTP | DataHub UI |
| `9083` | Hive Metastore | Thrift | Table catalog for Trino |
| `9092` | Kafka Broker | TCP | `PLAINTEXT://localhost:9092` |
| `9093` | Kafka Controller | TCP | KRaft quorum listener |
| `9200` | OpenSearch | HTTP | DataHub search backend |
