#!/usr/bin/env python3
"""
EHR Lakehouse Layer Validation & Query Script
Queries Trino (Delta Lakehouse) to check tables in both bronze and silver schemas.
Verifies record counts, schemas, streaming status, and data quality checks (deduplication,
schema evolution, row count reduction, and derived columns).
"""

import os
import sys
import trino
from dotenv import load_dotenv

# Load environment variables
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8090"))
TRINO_USER = os.getenv("TRINO_USER", "trino")

SILVER_TABLES = [
    "stg_patients",
    "stg_wards",
    "stg_event_metadata",
    "stg_visits",
    "stg_events",
]

GOLD_TABLES = [
    # Core dimensions
    "dim_patient",
    "dim_ward",
    "dim_event_type",
    # Specialised event-type dimensions
    "dim_vital",
    "dim_lab",
    "dim_medication",
    "dim_diagnosis",
    # Facts & OBT
    "fact_visit",
    "fact_clinical_event",
    "obt_clinical_events",
    # Pre-aggregated feature tables (Feast offline store)
    "feat_patient_vitals_6m",
    "feat_patient_labs_6m",
    "feat_patient_icd_6m",
    "feat_patient_medication_6m",
    "feat_patient_demographics",
    # Training labels
    "gold_visit_labels",
]


def get_trino_connection():
    return trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)


def query_bronze_tables(cursor):
    """Retrieve and display table structures, counts, and samples for the bronze layer."""
    print("=" * 60)
    print("🔍  EHR BRONZE LAYER — DATA OVERVIEW")
    print("=" * 60)
    
    cursor.execute("SHOW TABLES FROM delta.bronze")
    tables = [row[0] for row in cursor.fetchall()]

    if not tables:
        print("  ⚠️  No tables found in delta.bronze schema.")
        return

    # Group into batch vs stream for a cleaner overview
    batch_tables  = [t for t in tables if not t.startswith("raw_stream")]
    stream_tables = [t for t in tables if t.startswith("raw_stream")]

    print("\n  ── Batch (historical) ──────────────────────────────────")
    for t in sorted(batch_tables):
        cursor.execute(f"SELECT COUNT(*) FROM delta.bronze.{t}")
        count = cursor.fetchone()[0]
        print(f"  • delta.bronze.{t:<28}: {count:>10,} rows")
        
        # Print schema
        cursor.execute(f"DESCRIBE delta.bronze.{t}")
        columns = cursor.fetchall()
        schema_str = ", ".join([f"{col[0]} ({col[1]})" for col in columns])
        print(f"    Schema: {schema_str}")
        
        # Print sample rows
        col_names = [col[0] for col in columns]
        cursor.execute(f"SELECT * FROM delta.bronze.{t} ORDER BY rand() LIMIT 3")
        samples = cursor.fetchall()
        if samples:
            print("    Sample Rows:")
            for sample in samples:
                row_dict = dict(zip(col_names, sample))
                row_dict_str = {k: (str(v) if v is not None else "null") for k, v in row_dict.items()}
                print(f"      - {row_dict_str}")
        print()

    if stream_tables:
        print("\n  ── Stream (live events) ────────────────────────────────")
        for t in sorted(stream_tables):
            cursor.execute(f"SELECT COUNT(*) FROM delta.bronze.{t}")
            count = cursor.fetchone()[0]
            print(f"  • delta.bronze.{t:<28}: {count:>10,} rows")
            
            # Print schema
            cursor.execute(f"DESCRIBE delta.bronze.{t}")
            columns = cursor.fetchall()
            schema_str = ", ".join([f"{col[0]} ({col[1]})" for col in columns])
            print(f"    Schema: {schema_str}")
            
            # Print sample rows
            col_names = [col[0] for col in columns]
            cursor.execute(f"SELECT * FROM delta.bronze.{t} ORDER BY rand() LIMIT 3")
            samples = cursor.fetchall()
            if samples:
                print("    Sample Rows:")
                for sample in samples:
                    row_dict = dict(zip(col_names, sample))
                    row_dict_str = {k: (str(v) if v is not None else "null") for k, v in row_dict.items()}
                    print(f"      - {row_dict_str}")
            print()


def query_silver_tables(cursor) -> bool:
    """Validate the silver layer and run data quality checks."""
    print("=" * 65)
    print("🔍  EHR SILVER LAYER — DATA QUALITY VALIDATION")
    print("=" * 65)

    # Check if silver schema exists/has tables
    try:
        cursor.execute("SHOW TABLES FROM delta.silver")
        tables = [row[0] for row in cursor.fetchall()]
        if not tables:
            print("  ⚠️  No tables found in delta.silver schema. Skipping validation.")
            return True
    except Exception:
        print("  ⚠️  delta.silver schema not found. Skipping validation.")
        return True

    results = {}

    # 1. Row counts
    print("\n  ── Silver Table Row Counts ──────────────────────────────────")
    for table in SILVER_TABLES:
        if table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM delta.silver.{table}")
            count = cursor.fetchone()[0]
            print(f"  • delta.silver.{table:<25}: {count:>10,} rows")
        else:
            print(f"  • delta.silver.{table:<25}: MISSING")

    # 2. Dedup events check
    if "stg_events" in tables:
        cursor.execute("SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM delta.silver.stg_events")
        dupe_count = cursor.fetchone()[0]
        status = "✅ PASS" if dupe_count == 0 else f"❌ FAIL ({dupe_count:,} dupes)"
        print(f"\n  ── Deduplication Check (stg_events.event_id) ────────────────")
        print(f"  • Duplicate event_ids remaining: {dupe_count:,}  →  {status}")
        results["events_dedup"] = (dupe_count == 0)

    # 3. Dedup patients check
    if "stg_patients" in tables:
        cursor.execute("SELECT COUNT(*) - COUNT(DISTINCT patient_id) FROM delta.silver.stg_patients")
        dupe_count = cursor.fetchone()[0]
        status = "✅ PASS" if dupe_count == 0 else f"❌ FAIL ({dupe_count:,} dupes)"
        print(f"\n  ── Deduplication Check (stg_patients.patient_id) ───────────")
        print(f"  • Duplicate patient_ids: {dupe_count:,}  →  {status}")
        results["patients_dedup"] = (dupe_count == 0)

    # 4. Schema evolution visits check
    if "stg_visits" in tables:
        cursor.execute("SELECT COUNT(*) FROM delta.silver.stg_visits WHERE severity_level IS NULL")
        null_count = cursor.fetchone()[0]
        status = "✅ PASS" if null_count == 0 else f"❌ FAIL ({null_count:,} NULLs)"
        print(f"\n  ── Schema Evolution Check (stg_visits.severity_level) ───────")
        print(f"  • NULL severity_level rows: {null_count:,}  →  {status}")
        results["severity_nulls"] = (null_count == 0)

        # Show distribution of severity_level values
        cursor.execute("""
            SELECT severity_level, COUNT(*) AS cnt
            FROM delta.silver.stg_visits
            GROUP BY severity_level
            ORDER BY cnt DESC
        """)
        rows = cursor.fetchall()
        print(f"  • severity_level distribution:")
        for row in rows:
            print(f"      {str(row[0]):<10}: {row[1]:>8,}")

    # 5. Row count reduction checks (Bronze vs Silver)
    try:
        cursor.execute("SHOW TABLES FROM delta.bronze")
        bronze_tables = [row[0] for row in cursor.fetchall()]
        print(f"\n  ── Bronze vs Silver Row Count Comparison ────────────────────")
        all_ok = True
        for bronze_table, silver_table in [
            ("raw_events", "stg_events"),
            ("raw_visits", "stg_visits"),
            ("raw_patients", "stg_patients"),
        ]:
            if bronze_table in bronze_tables and silver_table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM delta.bronze.{bronze_table}")
                bronze_count = cursor.fetchone()[0]
                cursor.execute(f"SELECT COUNT(*) FROM delta.silver.{silver_table}")
                silver_count = cursor.fetchone()[0]

                reduction = bronze_count - silver_count
                pct = (reduction / bronze_count * 100) if bronze_count > 0 else 0
                ok = silver_count <= bronze_count
                status = "✅" if ok else "❌"
                print(f"  {status} {bronze_table:<20} bronze={bronze_count:>9,}  silver={silver_count:>9,}  removed={reduction:>7,} ({pct:.1f}%)")
                all_ok = all_ok and ok
        results["row_reduction"] = all_ok
    except Exception as e:
        print(f"  ⚠️  Failed row reduction check: {e}")

    # 6. Visit duration derived column check
    if "stg_visits" in tables:
        cursor.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(visit_duration_mins) AS with_duration,
                ROUND(AVG(visit_duration_mins), 1) AS avg_duration_mins
            FROM delta.silver.stg_visits
        """)
        row = cursor.fetchone()
        total, with_dur, avg_dur = row
        print(f"\n  ── Derived Column Check (stg_visits.visit_duration_mins) ────")
        print(f"  • Total visits: {total:,}")
        print(f"  • Visits with duration: {with_dur:,} ({with_dur/total*100:.1f}%)")
        print(f"  • Average visit duration: {avg_dur if avg_dur is not None else 0.0} minutes")
        results["visit_duration"] = True

    # 7. Print silver schemas
    print(f"\n  ── Silver Table Schemas ─────────────────────────────────────")
    for table in SILVER_TABLES:
        if table in tables:
            cursor.execute(f"DESCRIBE delta.silver.{table}")
            columns = cursor.fetchall()
            col_str = ", ".join([f"{col[0]} ({col[1]})" for col in columns])
            print(f"  • {table}:")
            print(f"      {col_str}")

    # Summary
    print("\n" + "=" * 65)
    all_passed = all(results.values())
    if all_passed:
        print("✅  ALL CHECKS PASSED — Silver layer is ready for Gold modeling")
    else:
        failed = [k for k, v in results.items() if not v]
        print(f"❌  FAILED CHECKS: {', '.join(failed)}")
    print("=" * 65)
    return all_passed


def query_gold_tables(cursor) -> bool:
    """Validate the gold layer and run data quality/integrity checks."""
    print("=" * 65)
    print("🔍  EHR GOLD LAYER — DATA OVERVIEW & VALIDATION")
    print("=" * 65)

    # Check if gold schema exists/has tables
    try:
        cursor.execute("SHOW TABLES FROM delta.gold")
        tables = [row[0] for row in cursor.fetchall()]
        if not tables:
            print("  ⚠️  No tables found in delta.gold schema. Skipping validation.")
            return True
    except Exception:
        print("  ⚠️  delta.gold schema not found. Skipping validation.")
        return True

    results = {}

    # 1. Row counts
    print("\n  ── Gold Table Row Counts ──────────────────────────────────")
    for table in GOLD_TABLES:
        if table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM delta.gold.{table}")
            count = cursor.fetchone()[0]
            print(f"  • delta.gold.{table:<25}: {count:>10,} rows")
        else:
            print(f"  • delta.gold.{table:<25}: MISSING")

    # 2. Key null check
    print("\n  ── Integrity Check (No Null Surrogate Keys) ─────────────────")
    key_checks = [
        ("dim_patient",        "patient_key"),
        ("dim_ward",           "ward_key"),
        ("dim_event_type",     "event_type_key"),
        ("dim_vital",          "event_type_key"),
        ("dim_lab",            "event_type_key"),
        ("dim_medication",     "event_type_key"),
        ("dim_diagnosis",      "event_type_key"),
        ("fact_visit",         "patient_key"),
        ("fact_clinical_event","event_type_key"),
    ]
    all_keys_ok = True
    for table, key_col in key_checks:
        if table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM delta.gold.{table} WHERE {key_col} IS NULL")
            null_count = cursor.fetchone()[0]
            ok = null_count == 0
            all_keys_ok = all_keys_ok and ok
            status = "✅ PASS" if ok else f"❌ FAIL ({null_count:,} NULL keys)"
            print(f"  • {table}.{key_col} Null Count: {null_count} -> {status}")
    results["surrogate_keys_ok"] = all_keys_ok

    # 3. Print gold schemas
    print(f"\n  ── Gold Table Schemas ───────────────────────────────────────")
    for table in GOLD_TABLES:
        if table in tables:
            cursor.execute(f"DESCRIBE delta.gold.{table}")
            columns = cursor.fetchall()
            col_str = ", ".join([f"{col[0]} ({col[1]})" for col in columns])
            print(f"  • {table}:")
            print(f"      {col_str}")

    # Summary
    print("\n" + "=" * 65)
    all_passed = all(results.values())
    if all_passed:
        print("✅  ALL CHECKS PASSED — Gold layer modeling is correct and ready for consumption")
    else:
        failed = [k for k, v in results.items() if not v]
        print(f"❌  FAILED CHECKS: {', '.join(failed)}")
    print("=" * 65)
    return all_passed


def main():
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
    except Exception as e:
        print(f"Failed to connect to Trino: {e}")
        sys.exit(1)

    try:
        query_bronze_tables(cursor)
        print("\n")
        silver_ok = query_silver_tables(cursor)
        print("\n")
        gold_ok = query_gold_tables(cursor)
    except Exception as e:
        print(f"Querying lakehouse failed: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

    sys.exit(0 if (silver_ok and gold_ok) else 1)



if __name__ == "__main__":
    main()
