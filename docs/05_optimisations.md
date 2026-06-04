# Custom Optimisations

## Overview

This document captures the non-trivial engineering decisions, performance challenges, and custom optimisations applied across the EHR pipeline. Each section covers the problem encountered, the solution chosen, the design rationale, and (where applicable) measured execution time improvements.

---

## 1. Data Challenges & Solutions

### 1.1 Duplicate Event IDs in Bronze

**Problem:** The raw `events` table contains ~2% duplicate `event_id` values — a realistic artifact of CDC-style ingestion where retry logic can produce duplicate records.

**Challenge:** A naïve deduplication at ingest time would modify Bronze, breaking the audit-trail contract. Deduplicating too late (at Gold) propagates bad data through Silver aggregations.

**Solution:** Deduplication is applied at the **Silver layer** only, using a window function:
```python
Window.partitionBy("event_id").orderBy(col("event_timestamp").desc())
# Keep row_number == 1 (latest event per event_id)
```

**Tradeoff:** Bronze retains duplicates intentionally. Silver is the first "trusted" layer. The Silver schema enforces `event_id` uniqueness via the dedup transform, not a table constraint (Delta Lake on Trino doesn't enforce PKs).

---

### 1.2 Heterogeneous Timestamp Formats

**Problem:** Raw timestamps arrive as a mix of:
- `BIGINT` Unix microseconds (events)
- ISO 8601 strings (visits, patients)
- NULL (discharge_timestamp for active visits)

**Challenge:** Trino's type system is strict; mixing timestamp representations causes silent cast failures or query errors downstream.

**Solution:**
- Bronze: preserve raw format (no cast)
- Silver: explicit cast per column with known format:
  ```python
  # Microseconds → TIMESTAMP
  (col("event_timestamp") / 1_000_000).cast("timestamp")
  
  # ISO string → DATE
  to_date(col("dob"), "yyyy-MM-dd")
  ```
- Gold: all timestamps are `TIMESTAMP WITH TIME ZONE`

---

### 1.3 Sparse Measurements (Vitals / Labs)

**Problem:** Not all patients have all vital/lab measurements. Patients may have measurements for 2 vitals but not 4, or have labs only from the most recent visit.

**Challenge:** Feature aggregation (6-month rolling stats) over a sparse time series produces many `NULL` values in feature tables, which can silently break downstream ML models.

**Solution:**
- Feature tables retain `NULL` values rather than imputing at storage time (imputation is a model preprocessing concern, not a data engineering concern)
- DataHub documentation explicitly flags each feature column as potentially sparse
- The Feast feature service does not filter on nulls — the ML pipeline is responsible for handling missing values

---

### 1.4 Schema Evolution in the Stream

**Problem:** The Flink streaming processor derives its output schema dynamically from `dim_vital`, `dim_lab`, `dim_medication`, and `dim_diagnosis` at job startup. If a new vital type is added to the event metadata, the Flink job needs to be restarted to pick up the new column.

**Solution:** The Flink job reads the typed dimension tables at startup and builds a dynamic output schema. Adding a new vital to `event_metadata` only requires:
1. Restarting the Flink job (or a rolling restart)
2. Re-running `feast apply` to register the new feature

No code changes required.

---

## 2. Warehouse Optimisations

### 2.1 Delta Lake Partitioning

**Problem:** Large `raw_events` and `stg_events` tables are frequently filtered by `event_type` (vitals, labs, etc.) for feature aggregation queries. Full table scans over the unpartitioned table are expensive.

**Solution:** The Silver and Gold event tables are written with **partitioning by `event_type`**:
```python
df.write.format("delta").partitionBy("event_type").save(path)
```

This reduces data scanned by 75% for single-category queries (e.g., vitals-only aggregation).

**Measured improvement:** _(to be filled — see §4)_

---

### 2.2 Delta Lake Z-Ordering

**Problem:** Feature aggregation queries frequently filter by `patient_id` (or `patient_key`) and scan a date range. Without data clustering, random patient row placement means every Parquet file must be read.

**Solution:** After each Gold-layer write, Z-ordering is applied on `patient_key`:
```python
DeltaTable.forPath(spark, path).optimize().executeZOrderBy("patient_key")
```

Z-ordering co-locates data for the same patient across the fewest possible Parquet files, reducing I/O for per-patient aggregations.

**Measured improvement:** _(to be filled — see §4)_

---

### 2.3 Trino Predicate Pushdown

**Problem:** Trino queries over MinIO/Delta Lake can perform poorly if filter predicates are not pushed down to the file level (i.e., Trino reads all files and filters in memory).

**Solution:**
- Columns used in `WHERE` clauses (`event_type`, `patient_id`, `event_timestamp`) are listed as partition or Z-order columns so Trino's Delta connector can prune files before reading
- `spark-defaults.conf` enables Delta statistics collection so the connector can use column min/max for skip-level pruning

---

### 2.4 OBT vs. Star Schema

**Problem:** ML feature engineering requires joining 5+ tables (facts + dims) per patient. Repeated multi-table joins in feature queries are expensive and verbose.

**Solution:** Maintain both:
- **Star schema** (`fact_*` + `dim_*`) for BI/Trino ad-hoc queries where selective joins are preferred
- **OBT** (`obt_clinical_events`) as a pre-joined, denormalised table used as the sole source for feature computation

The OBT is regenerated on each Gold run and is ~3× the size of `fact_clinical_event`. The storage cost is acceptable given the query time savings in `5_compute_features.py`.

**Measured improvement:** _(to be filled — see §4)_

---

### 2.5 Feast Online Store: Redis over PostgreSQL

**Problem:** The original Feast online store used PostgreSQL. Under high-frequency inference (many concurrent `get_online_features` calls), PostgreSQL connection overhead became the bottleneck.

**Solution:** Switched to **Redis** as the online store backend:
- Sub-millisecond key-value lookups
- No connection pool pressure
- TTL-based automatic expiry per feature view
- Redis persistence (`appendfsync everysec`) to survive restarts

**Measured improvement:** _(to be filled — see §4)_

---

## 3. Motivation & Design Choices

### 3.1 Why Rootless Podman?

The target deployment environment does not have root access. Docker requires a root-level daemon; Podman is fully rootless and daemonless, making it suitable for shared HPC/university clusters.

**Tradeoff:** Rootless Podman has stricter UID mapping (`--userns=keep-id`), which required explicit UID/GID configuration in all compose files.

---

### 3.2 Why PyFlink over Spark Structured Streaming?

| Criterion | PyFlink | Spark Streaming |
|---|---|---|
| Event-time windowing | Native, fine-grained | Supported but heavier |
| Watermark control | Per-operator | Global |
| Kafka fan-out (one source, two sinks) | First-class | Requires workarounds |
| Resource overhead | Lower (JVM only) | Higher (JVM + Python worker) |
| Ecosystem maturity | Younger | More battle-tested |

Flink was chosen for the streaming path specifically because the 24h sliding window with 15-minute steps and event-time watermarking is a natural fit for Flink's DataStream API. Spark was retained for batch transforms where its DataFrame API and Delta Lake integration are more mature.

---

### 3.3 Why Delta Lake over Iceberg/Hudi?

- **Delta Lake + Trino** has first-class support via the `delta` connector in Trino 480+
- **Delta Lake + Spark** is the most mature write path
- Iceberg requires additional catalog configuration complexity
- Hudi's upsert model is not needed here (Silver dedup is handled in Spark, not via table-level upserts)

---

### 3.4 Why Feast over a Custom Feature Store?

Feast provides:
- Point-in-time correct joins (critical for training data to avoid label leakage)
- Offline → Online synchronisation with a single `feast materialize` call
- Stream push support (`store.push()`) for real-time feature updates
- Declarative feature view definitions that double as documentation

A custom Redis/PostgreSQL solution would require re-implementing point-in-time joins and online/offline consistency management.

---

## 4. Execution Time Measurements

> **Note:** Formal benchmarks were not collected for this project. The table below reflects observed order-of-magnitude wall-clock times from actual pipeline runs. Precise baseline vs. optimised comparisons are left as future work.

| Step | Script | Rows processed | Observed wall-clock |
|---|---|---|---|
| Bronze ingest | `2a_ingest_to_bronze.py` | ~57,000 rows | < 1 min |
| Silver transform | `3_transform_to_silver.py` | ~57,000 rows | 2–4 min |
| Gold transform | `4_transform_to_gold.py` | ~57,000 rows | 3–5 min |
| Feature engineering | `5_compute_features.py` | ~1,000 patients | 4–6 min |
| Feast materialize | `feast materialize` | ~1,000 patients × 63 features | 1–2 min |
| Flink window (live stream) | `6_flink_stream_processor.py` | continuous | continuous |

See the Airflow task run history chart (in `docs/04_governance_orchestration.md`) for a visual breakdown of per-task durations from actual DAG runs.
