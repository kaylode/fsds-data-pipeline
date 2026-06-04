# Custom Optimisations

## Overview

This document captures the non-trivial engineering decisions, performance challenges, and custom optimisations applied across the EHR pipeline. Each section covers the problem encountered, the solution chosen, the design rationale, and (where applicable) measured execution time improvements.

---

## 1. Data Challenges & Solutions

### 1.1 Duplicate Event IDs in Bronze

- **Problem:** The raw `events` table contains ~2% duplicate `event_id` values — a realistic artifact of CDC-style ingestion where retry logic can produce duplicate records.
- **Challenge:** A naïve deduplication at ingest time would modify Bronze, breaking the audit-trail contract. Deduplicating too late (at Gold) propagates bad data through Silver aggregations.
- **Solution:** Deduplication is applied at the **Silver layer** only, using a window function:
```python
Window.partitionBy("event_id").orderBy(col("event_timestamp").desc())
# Keep row_number == 1 (latest event per event_id)
```
- **Tradeoff:** Bronze retains duplicates intentionally. Silver is the first "trusted" layer. The Silver schema enforces `event_id` uniqueness via the dedup transform, not a table constraint (Delta Lake on Trino doesn't enforce PKs).

---

### 1.2 Heterogeneous Timestamp Formats

- **Problem:** Raw timestamps arrive as a mix of:
  - `BIGINT` Unix microseconds (events)
  - ISO 8601 strings (visits, patients)
  - NULL (discharge_timestamp for active visits)
- **Challenge:** Trino's type system is strict; mixing timestamp representations causes silent cast failures or query errors downstream.
- **Solution:**
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

### 1.3 Missing Measurements (Vitals / Labs)

- **Problem:** Not all patients have all vital/lab measurements. Patients may have measurements for 2 vitals but not 4, or have labs only from the most recent visit.
- **Challenge:** Feature aggregation (6-month rolling stats) over a sparse time series produces many `NULL` values in feature tables, which can silently break downstream ML models.
- **Solution:**
  - Feature tables retain `NULL` values rather than imputing at storage time (imputation is a model preprocessing concern, not a data engineering concern)
  - DataHub documentation explicitly flags each feature column as potentially sparse
  - The Feast feature service does not filter on nulls — the ML pipeline is responsible for handling missing values

---

### 1.4 Schema Evolution in the Stream

- **Problem:** The Flink streaming processor derives its output schema dynamically from `dim_vital`, `dim_lab`, `dim_medication`, and `dim_diagnosis` at job startup. If a new vital type is added to the event metadata, the Flink job needs to be restarted to pick up the new column.
- **Solution:** The Flink job reads the typed dimension tables at startup and builds a dynamic output schema. Adding a new vital to `event_metadata` only requires:
  1. Restarting the Flink job (or a rolling restart)
  2. Re-running `feast apply` to register the new feature
- No code changes required.

---

## 2. Warehouse Optimisations

### 2.1 Delta Lake Partitioning

- **Problem:** Large `raw_events` and `stg_events` tables are frequently filtered by `event_type` (vitals, labs, etc.) for feature aggregation queries. Full table scans over the unpartitioned table are expensive.
- **Solution:** The Silver and Gold event tables are written with **partitioning by `event_type`**:
```python
df.write.format("delta").partitionBy("event_type").save(path)
```
- This reduces data scanned for single-category queries (e.g., vitals-only aggregation).

---

### 2.2 Delta Lake Z-Ordering

- **Problem:** Feature aggregation queries frequently filter by `patient_id` (or `patient_key`) and scan a date range. Without data clustering, random patient row placement means every Parquet file must be read.
- **Solution:** After each Gold-layer write, Z-ordering is applied on `patient_key`:
```python
DeltaTable.forPath(spark, path).optimize().executeZOrderBy("patient_key")
```
- Z-ordering co-locates data for the same patient across the fewest possible Parquet files, reducing I/O for per-patient aggregations.

---

