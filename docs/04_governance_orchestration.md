# Data Governance, Orchestration & Lineage

## Overview

Data governance and orchestration are handled by two complementary tools:

| Tool | Role |
|---|---|
| **Apache Airflow** | Pipeline scheduling, task dependency management, execution history |
| **DataHub** | Data catalogue, column-level lineage, quality assertions, metadata enrichment |

Together they provide full observability over the EHR pipeline — from raw data ingestion to ML-ready feature serving.

---

## Orchestration: Apache Airflow

**UI:** http://localhost:8082 (user: `airflow` / pass: `airflow`)

### DAGs

#### `ehr_data_pipeline` (daily)

The main batch pipeline DAG. Runs the full Bronze → Silver → Gold → Feature → Feast materialisation chain.

```
bronze_ingest
    │
    ▼
validate_bronze        ← quality gates: row count, null PK, duplicate PK
    │
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

Each task emits **DataHub lineage events** (input/output dataset URNs) and records execution metadata to the `ehr_pipeline_runs` PostgreSQL table.

---

#### `datahub_metadata_ingestion` (every 5 min, configurable)

Crawls all data sources and pushes catalogue metadata to DataHub GMS.

| Task | Source crawled |
|---|---|
| `ingest_postgres` | Postgres schemas and tables |
| `ingest_kafka` | Kafka topics and Avro schemas |
| `ingest_trino` | All Delta Lake tables via Trino catalog |
| `ingest_minio_lakehouse` | S3 paths in `s3://lakehouse/` |
| `ingest_feast` | Feast feature views and entities |

> **Note:** The schedule can be changed in `config/orchestration/dags/datahub_ingestion.py` (`schedule_interval`). Default is `*/5 * * * *` (every 5 min) for development; change to `0 */6 * * *` for production.

---

### Pipeline Run Metadata

Each task in `ehr_data_pipeline` records a row to `ehr_pipeline_runs` (PostgreSQL, Airflow DB):

| Column | Description |
|---|---|
| `run_id` | UUID for this specific task run |
| `dag_id` | DAG identifier |
| `task_id` | Task identifier |
| `airflow_run_id` | Airflow run ID |
| `start_ts` | Task start timestamp (UTC) |
| `end_ts` | Task end timestamp |
| `status` | `RUNNING` / `SUCCESS` / `FAILED` |
| `input_row_count` | Rows read (where applicable) |
| `output_row_count` | Rows written (where applicable) |
| `error_msg` | First 1000 chars of error if failed |

Browse via pgweb at http://localhost:8085.

---

### Airflow Screenshots

> _Demo screenshots of the Airflow UI will be added here._

<!-- PLACEHOLDER: Insert screenshot of DAG graph view (ehr_data_pipeline) -->
<!-- PLACEHOLDER: Insert screenshot of task run history / Gantt view -->
<!-- PLACEHOLDER: Insert screenshot of datahub_metadata_ingestion DAG -->

---

## Data Governance: DataHub

**UI:** http://localhost:9002

DataHub provides the central data catalogue, automated lineage tracking, and manual metadata enrichment for the EHR pipeline.

### What DataHub tracks

| Aspect | How populated |
|---|---|
| **Dataset catalogue** | `datahub_metadata_ingestion` DAG (auto-crawl) |
| **Column schemas** | Trino/Kafka/Postgres source crawl |
| **Lineage** | Emitted by each Airflow task via DataHub REST emitter |
| **Quality assertions** | Emitted by `validate_bronze` task |
| **Ownership** | `scripts/misc/datahub_enrich_metadata.py` |
| **Tags** | Enrichment script (e.g. `bronze`, `pii`, `ml-ready`) |
| **Glossary Terms** | Enrichment script (e.g. `PatientIdentifier`, `ClinicalEvent`) |
| **Domain** | Enrichment script (`healthcare`, `machine-learning`) |
| **Documentation** | Enrichment script (per-dataset descriptions) |

---

### Lineage Graph

The lineage graph is automatically built by the `ehr_data_pipeline` DAG. Each task emits `DataFlow`, `DataJob`, and `UpstreamLineage` metadata change proposals (MCPs) via the DataHub REST emitter.

**Batch lineage chain:**
```
Parquet files (file platform)
    │
    ▼ (bronze_ingest)
delta.bronze.raw_*
    │
    ▼ (silver_transform)
delta.silver.stg_*
    │
    ▼ (gold_transform)
delta.gold.dim_* / fact_* / obt_*
    │
    ▼ (compute_features)
delta.gold.feat_* / gold_visit_labels
    │
    ▼ (feast_materialize)
redis.ehr_feature_store.online
```

**Streaming lineage chain:**
```
kafka.patient-events
    │
    ▼ (flink_stream_processor)
kafka.patient-features-24h
    │
    ▼ (push_stream_features)
redis.ehr_feature_store.online
```

---

### Quality Assertions

The `validate_bronze` Airflow task runs three checks per Bronze table and emits results as DataHub **AssertionRunEvents**:

| Check | Description | Tables |
|---|---|---|
| `row_count_check` | Table has ≥ expected minimum rows | All 5 Bronze tables |
| `null_pk_check` | Primary key column has 0 null values | All 5 Bronze tables |
| `unique_pk_check` | Primary key column has 0 duplicate values | All except `raw_events` (duplicates expected) |

Assertion results are visible in DataHub under each dataset's **Assertions** tab.

---

### Metadata Enrichment

Run `make datahub-enrich` to push rich metadata to DataHub for all 22 datasets. This is also run automatically as part of `make datahub-up`.

The enrichment script (`scripts/misc/datahub_enrich_metadata.py`) sets:

- **Documentation:** Human-readable description of each dataset's purpose, source, and layer
- **Ownership:** Assigned to `airflow` user (configurable)
- **Tags:** Layer (`bronze`, `silver`, `gold`, `feature`), domain (`ehr`, `pii`, `ml-ready`, `streaming`)
- **Glossary Terms:** Clinical vocabulary (`PatientIdentifier`, `ClinicalEvent`, `HospitalVisit`, `FeatureView`)
- **Domain:** `healthcare` or `machine-learning`
- **Custom Properties:** `layer`, `grain`, `feast_view`, `window`, `sla`

---

### DataHub Screenshots

> _Demo screenshots of the DataHub UI will be added here._

<!-- PLACEHOLDER: Insert screenshot of dataset catalogue (bronze layer) -->
<!-- PLACEHOLDER: Insert screenshot of lineage graph (full pipeline) -->
<!-- PLACEHOLDER: Insert screenshot of quality assertion results -->
<!-- PLACEHOLDER: Insert screenshot of dataset metadata page (docs, owners, tags) -->
<!-- PLACEHOLDER: Insert screenshot of domain view (healthcare / machine-learning) -->

---

## Architecture Summary

```
                    ┌─────────────────────┐
                    │    Apache Airflow    │
                    │  (Scheduling / DAGs) │
                    └──────────┬──────────┘
                               │ triggers & monitors
                    ┌──────────▼──────────┐
                    │   EHR Pipeline      │
                    │  (Spark / Flink /   │
                    │   Feast / Kafka)    │
                    └──────────┬──────────┘
                               │ emits lineage MCPs
                    ┌──────────▼──────────┐
                    │      DataHub        │
                    │  (Catalogue /       │
                    │   Lineage / QA)     │
                    └─────────────────────┘
```

Both tools are integrated: Airflow tasks emit DataHub lineage events in real time, and the `datahub_metadata_ingestion` DAG runs on a schedule to keep the catalogue fresh.
