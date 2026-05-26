#!/usr/bin/env python3
"""
EHR Bronze Layer Overview
Queries Trino (Delta Lakehouse Bronze Layer) to print record counts
for all tables and verify successful ingestion across both batch and stream pipelines.
"""

import os
import trino
from dotenv import load_dotenv

# Load environment variables
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
load_dotenv(os.path.join(project_root, ".env"))


def check_trino():
    trino_port = int(os.getenv("TRINO_PORT", "8090"))
    trino_host = os.getenv("TRINO_HOST", "localhost")
    trino_user = os.getenv("TRINO_USER", "trino")

    print("\n🔱 Trino (delta.bronze.*) counts:")
    try:
        conn   = trino.dbapi.connect(host=trino_host, port=trino_port, user=trino_user)
        cursor = conn.cursor()

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
            
            # Print sample rows (using ORDER BY rand() to get random items)
            col_names = [col[0] for col in columns]
            cursor.execute(f"SELECT * FROM delta.bronze.{t} ORDER BY rand() LIMIT 3")
            samples = cursor.fetchall()
            if samples:
                print("    Sample Rows:")
                for sample in samples:
                    # Format as dict-like structure for readability
                    row_dict = dict(zip(col_names, sample))
                    # Format datetime/timestamps to string representation
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

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"  ❌ Failed to query Trino: {e}")


def main():
    print("=" * 60)
    print("🔍  EHR BRONZE LAYER — DATA OVERVIEW")
    print("=" * 60)
    check_trino()
    print("=" * 60)


if __name__ == "__main__":
    main()

