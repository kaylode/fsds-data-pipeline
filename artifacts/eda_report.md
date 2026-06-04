# Exploratory Data Analysis (EDA) Quality Report — EHR Dataset

This quality report summarizes the statistics, anomalies, and structural characteristics of the generated synthetic Electronic Health Record (EHR) dataset.

---

## 1. Cardinality and Entity Summary

This section details the distinct identifiers and physical record counts across all relational offline tables.

| Table Name | Total Record Count | Distinct Entity IDs | Description / Grain |
| :--- | :---: | :---: | :--- |
| **patients** | 10,000 | 10,000 | One row per patient registry. |
| **wards** | 4 | 4 | Hospital clinics/wards directory dimensions. |
| **event_metadata** | 17 | 17 | Catalog lookup of clinical observations. |
| **visits** | 50,000 | 50,000 | Patient admission visits log (partitioned). |
| **events** | 153,000 | 150,000 | Granular clinical observations logs (partitioned). |

---

## 2. Offline Data Quality Issues & Distributions

### 2.1 Facility Skew (Wards)
To test downstream capacity constraints and partition skewness, a heavy bias was injected into `General_Ward`.

* **General_Ward**: 42,560 (85.12%) of total visits.
* **Pediatric_Ward**: 2,480 (4.96%) of total visits.
* **ICU_Ward**: 2,482 (4.96%) of total visits.
* **ER_Ward**: 2,478 (4.96%) of total visits.

### 2.2 Patient Demographic Missing Values (Nulls)
Nullable fields in the patients table (expected sparsity):
* **date_of_death is NULL (alive patients)**: 9,961 (99.61%)
* **ethnic is NULL**: 0 (0.00%)

### 2.3 Event Type Distributions
Clinical observations are partitioned across 4 distinct types:
* **vital**: 40,013 (26.15%)
* **lab**: 37,717 (24.65%)
* **medication**: 37,668 (24.62%)
* **diagnosis**: 37,602 (24.58%)

### 2.4 Schema Evolution Verification
Visits prior to `2023-08-01` represent legacy systems where the `severity_level` column was missing.
* **v1.0 legacy visits (severity_level IS NULL)**: 29,214 (58.43%)
* **v2.0 modern visits (severity_level IS POPULATED)**: 20,786 (41.57%)

### 2.5 Duplicate Record Rates (Events Table)
A 2.0% duplication rate was injected into the offline `events` fact logs to test deduplication processes.
* **Total Rows**: 153,000
* **Unique Events**: 150,000
* **Duplicate Rows Injected**: 3,000 (1.96% duplicate rate)

---

## 3. Visualizations

![EDA Report Charts](eda_report.png)
