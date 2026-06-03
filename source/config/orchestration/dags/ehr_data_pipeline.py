from __future__ import annotations

import os
import signal
import subprocess
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from datahub_airflow_plugin.entities import Dataset as DatahubDataset


load_dotenv(Path(__file__).resolve().parents[3] / ".env")

# ── Configuration ─────────────────────────────────────────────────────────────
DATAHUB_GMS_URL       = os.getenv("DATAHUB_GMS_URL",  "http://127.0.0.1:8088")
FLINK_REST_URL        = os.getenv("FLINK_REST_URL",    "http://127.0.0.1:8087")
SCRIPTS_DIR           = os.getenv("SCRIPTS_DIR",       "/opt/airflow/scripts")
FEAST_REPO_DIR        = os.getenv("FEAST_REPO_DIR",    "/opt/airflow/feature_store")
STREAM_PUSH_TIMEOUT_S = int(os.getenv("STREAM_PUSH_TIMEOUT_S", "60"))
TRINO_HOST            = os.getenv("TRINO_HOST",  "127.0.0.1")
TRINO_PORT            = int(os.getenv("TRINO_PORT", "8090"))

# Postgres used for run metadata (reuses the Airflow DB so no extra service needed)
_PG_USER = os.getenv("AIRFLOW_DB_USER",     "airflow")
_PG_PASS = os.getenv("AIRFLOW_DB_PASSWORD", "airflow")
_PG_DB   = os.getenv("AIRFLOW_DB_NAME",     "airflow")
_PG_PORT = os.getenv("POSTGRES_PORT",       "5432")
DB_METADATA_CONN = f"postgresql+psycopg2://{_PG_USER}:{_PG_PASS}@127.0.0.1:{_PG_PORT}/{_PG_DB}"

# ── Bronze quality-check contract ────────────────────────────────────────────
# Each entry: required columns, NOT-NULL primary key, minimum row count.
BRONZE_QUALITY_CHECKS: dict[str, dict] = {
    "raw_patients": {
        "pk":       "patient_id",
        "required": ["patient_id", "country", "gender", "dob"],
        "min_rows": 500,
    },
    "raw_visits": {
        "pk":       "visit_id",
        "required": ["visit_id", "patient_id", "admission_timestamp", "ward_id"],
        "min_rows": 1_000,
    },
    "raw_events": {
        "pk":       "event_id",
        "required": ["event_id", "visit_id", "event_timestamp", "event_type", "event_type_id"],
        "min_rows": 5_000,
    },
    "raw_wards": {
        "pk":       "ward_id",
        "required": ["ward_id", "ward_name", "department"],
        "min_rows": 1,
    },
    "raw_event_metadata": {
        "pk":       "event_type_id",
        "required": ["event_type_id", "event_name", "event_type"],
        "min_rows": 1,
    },
}

# ── Dataset URN Helpers ───────────────────────────────────────────────────────

def _trino_urn(schema: str, table: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:trino,delta.{schema}.{table},PROD)"


def _kafka_urn(topic: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:kafka,{topic},PROD)"


def _file_urn(name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:file,data/synthetic/{name},PROD)"


# ── Dataset URNs ──────────────────────────────────────────────────────────────
SOURCE_FILE_URNS = [
    _file_urn(f"{t}.parquet")
    for t in ("patients", "wards", "event_metadata", "visits", "events")
]
BRONZE_URNS = [
    _trino_urn("bronze", t)
    for t in ("raw_patients", "raw_wards", "raw_event_metadata", "raw_visits", "raw_events")
]
SILVER_URNS = [
    _trino_urn("silver", t)
    for t in ("stg_patients", "stg_wards", "stg_event_metadata", "stg_visits", "stg_events")
]
GOLD_URNS = [
    _trino_urn("gold", t)
    for t in (
        "dim_patient", "dim_ward", "dim_vital", "dim_lab",
        "dim_medication", "dim_diagnosis",
        "fact_visit", "fact_clinical_event", "obt_clinical_events",
    )
]
FEATURE_URNS = [
    _trino_urn("gold", t)
    for t in (
        "feat_patient_vitals_6m", "feat_patient_labs_6m", "feat_patient_icd_6m",
        "feat_patient_medication_6m", "feat_patient_demographics", "gold_visit_labels",
    )
]
KAFKA_EVENTS_URN   = _kafka_urn("patient-events")
KAFKA_FEATURES_URN = _kafka_urn("patient-features-24h")
ONLINE_STORE_URN   = "urn:li:dataset:(urn:li:dataPlatform:redis,ehr_feature_store.online,PROD)"

# ── DataFlow / DataJob URNs ───────────────────────────────────────────────────
BATCH_FLOW_URN     = "urn:li:dataFlow:(airflow,ehr_data_pipeline,PROD)"
STREAMING_FLOW_URN = "urn:li:dataFlow:(flink,ehr_patient_event_processor,PROD)"


def _batch_job_urn(task_id: str) -> str:
    return f"urn:li:dataJob:({BATCH_FLOW_URN},{task_id})"


def _stream_job_urn(task_id: str) -> str:
    return f"urn:li:dataJob:({STREAMING_FLOW_URN},{task_id})"


# ── Run Metadata ──────────────────────────────────────────────────────────────

def _ensure_run_metadata_table() -> None:
    """Create ehr_pipeline_runs table in the Airflow Postgres DB if absent."""
    from sqlalchemy import create_engine, text
    engine = create_engine(DB_METADATA_CONN)
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ehr_pipeline_runs (
                run_id           TEXT PRIMARY KEY,
                dag_id           TEXT,
                task_id          TEXT,
                airflow_run_id   TEXT,
                start_ts         TIMESTAMPTZ,
                end_ts           TIMESTAMPTZ,
                status           TEXT,
                input_row_count  BIGINT,
                output_row_count BIGINT,
                error_msg        TEXT
            )
        """))
        conn.commit()


def _upsert_run(
    run_id: str,
    dag_id: str,
    task_id: str,
    airflow_run_id: str,
    start_ts: datetime,
    end_ts: datetime | None,
    status: str,
    input_rows: int,
    output_rows: int,
    error_msg: str | None,
) -> None:
    from sqlalchemy import create_engine, text
    engine = create_engine(DB_METADATA_CONN)
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO ehr_pipeline_runs
                (run_id, dag_id, task_id, airflow_run_id, start_ts, end_ts,
                 status, input_row_count, output_row_count, error_msg)
            VALUES
                (:run_id, :dag_id, :task_id, :airflow_run_id, :start_ts, :end_ts,
                 :status, :input_rows, :output_rows, :error_msg)
            ON CONFLICT (run_id) DO UPDATE SET
                end_ts           = EXCLUDED.end_ts,
                status           = EXCLUDED.status,
                input_row_count  = EXCLUDED.input_row_count,
                output_row_count = EXCLUDED.output_row_count,
                error_msg        = EXCLUDED.error_msg
        """), {
            "run_id": run_id, "dag_id": dag_id, "task_id": task_id,
            "airflow_run_id": airflow_run_id,
            "start_ts": start_ts, "end_ts": end_ts,
            "status": status, "input_rows": input_rows,
            "output_rows": output_rows, "error_msg": error_msg,
        })
        conn.commit()


@contextmanager
def _run_tracker(task_id: str, context: dict, input_rows: int = 0):
    """
    Context manager that records a pipeline run row in ehr_pipeline_runs.

    Usage:
        meta = {}
        with _run_tracker("bronze_ingest", context, meta):
            ... do work ...
            meta["output_rows"] = 12345   # optional
    """
    run_id        = str(uuid.uuid4())
    dag_id        = context.get("dag").dag_id if context.get("dag") else "ehr_data_pipeline"
    airflow_run_id = context.get("run_id", "")
    start_ts      = datetime.now(timezone.utc)

    try:
        _ensure_run_metadata_table()
        _upsert_run(run_id, dag_id, task_id, airflow_run_id,
                    start_ts, None, "RUNNING", input_rows, 0, None)

        meta: dict = {"output_rows": 0}
        yield meta

        _upsert_run(run_id, dag_id, task_id, airflow_run_id,
                    start_ts, datetime.now(timezone.utc),
                    "SUCCESS", input_rows, meta.get("output_rows", 0), None)
        print(f"[run-meta] {task_id} → SUCCESS  run_id={run_id}")

    except Exception as exc:
        _upsert_run(run_id, dag_id, task_id, airflow_run_id,
                    start_ts, datetime.now(timezone.utc),
                    "FAILED", input_rows, 0, str(exc)[:1000])
        print(f"[run-meta] {task_id} → FAILED   run_id={run_id}  err={exc}")
        raise


# ── DataHub Emission Helpers ──────────────────────────────────────────────────

def _get_emitter():
    from datahub.emitter.rest_emitter import DataHubRestEmitter
    return DataHubRestEmitter(DATAHUB_GMS_URL)


def _emit_batch_step(task_id: str, input_urns: list[str], output_urns: list[str]) -> None:
    """Emit DataFlow, DataJob with input/output lineage, and dataset upstreams."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        DataFlowInfoClass,
        DataJobInfoClass,
        DataJobInputOutputClass,
        UpstreamClass,
        UpstreamLineageClass,
    )

    emitter = _get_emitter()
    now_ms  = int(datetime.now(timezone.utc).timestamp() * 1000)
    job_urn = _batch_job_urn(task_id)

    try:
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=BATCH_FLOW_URN,
            aspect=DataFlowInfoClass(
                name="ehr_data_pipeline",
                description=(
                    "EHR batch pipeline: parquet → bronze (Delta/MinIO) "
                    "→ silver (clean) → gold (model) "
                    "→ feature tables → Feast online store (Redis)"
                ),
                project="ehr_feature_store",
            ),
        ))
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=task_id, type="COMMAND", flowUrn=BATCH_FLOW_URN,
                description=f"EHR pipeline step: {task_id}",
            ),
        ))
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=input_urns,
                outputDatasets=output_urns,
            ),
        ))
        for out_urn in output_urns:
            emitter.emit(MetadataChangeProposalWrapper(
                entityUrn=out_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=in_urn,
                            auditStamp=AuditStampClass(
                                time=now_ms, actor="urn:li:corpuser:airflow",
                            ),
                        )
                        for in_urn in input_urns
                    ]
                ),
            ))
        print(f"[datahub] Emitted batch step: {task_id}")
    except Exception as exc:
        print(f"[datahub] WARNING — could not emit step {task_id}: {exc}")


def _emit_stream_step(task_id: str, input_urns: list[str], output_urns: list[str]) -> None:
    """Emit Flink DataFlow and DataJob with lineage for the streaming branch."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        DataFlowInfoClass,
        DataJobInfoClass,
        DataJobInputOutputClass,
        UpstreamClass,
        UpstreamLineageClass,
    )

    emitter = _get_emitter()
    now_ms  = int(datetime.now(timezone.utc).timestamp() * 1000)
    job_urn = _stream_job_urn(task_id)

    try:
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=STREAMING_FLOW_URN,
            aspect=DataFlowInfoClass(
                name="ehr_patient_event_processor",
                description=(
                    "Flink streaming pipeline: patient-events Kafka topic "
                    "→ 24h sliding window aggregates → patient-features-24h topic "
                    "→ Feast online push → Redis"
                ),
                project="ehr_feature_store",
            ),
        ))
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=task_id, type="STREAMING", flowUrn=STREAMING_FLOW_URN,
                description=f"Flink streaming step: {task_id}",
            ),
        ))
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=input_urns, outputDatasets=output_urns,
            ),
        ))
        for out_urn in output_urns:
            emitter.emit(MetadataChangeProposalWrapper(
                entityUrn=out_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=in_urn,
                            auditStamp=AuditStampClass(
                                time=now_ms, actor="urn:li:corpuser:flink",
                            ),
                        )
                        for in_urn in input_urns
                    ]
                ),
            ))
        print(f"[datahub] Emitted stream step: {task_id}")
    except Exception as exc:
        print(f"[datahub] WARNING — could not emit step {task_id}: {exc}")


def _emit_assertion(
    emitter,
    assertion_urn: str,
    dataset_urn: str,
    check_name: str,
    passed: bool,
    run_id: str,
    now_ms: int,
) -> None:
    """Emit one AssertionInfo + AssertionRunEvent to DataHub."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        AssertionInfoClass, AssertionTypeClass,
        AssertionResultClass, AssertionResultTypeClass,
        AssertionRunEventClass, AssertionRunStatusClass,
        DatasetAssertionInfoClass, DatasetAssertionScopeClass,
        AssertionStdOperatorClass,
    )

    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=AssertionInfoClass(
            type=AssertionTypeClass.DATASET,
            datasetAssertion=DatasetAssertionInfoClass(
                dataset=dataset_urn,
                scope=DatasetAssertionScopeClass.DATASET_ROWS,
                operator=AssertionStdOperatorClass._NATIVE_,
                nativeType=check_name,
            ),
        ),
    ))
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=AssertionRunEventClass(
            timestampMillis=now_ms,
            asserteeUrn=dataset_urn,
            runId=run_id,
            assertionUrn=assertion_urn,
            status=AssertionRunStatusClass.COMPLETE,
            result=AssertionResultClass(
                type=(
                    AssertionResultTypeClass.SUCCESS
                    if passed
                    else AssertionResultTypeClass.FAILURE
                ),
                nativeResults={"check": check_name, "passed": str(passed)},
            ),
        ),
    ))


# ── Batch Pipeline Tasks ──────────────────────────────────────────────────────

def bronze_ingest(**context) -> None:
    """
    Ingest raw parquet files into MinIO (Delta Lake) and register in Trino
    under the delta.bronze schema.

    Source: data/synthetic/{patients,wards,event_metadata,visits,events}.parquet
    Target: s3://lakehouse/topics/raw_* + Trino delta.bronze
    """
    with _run_tracker("bronze_ingest", context) as meta:
        subprocess.run(
            ["uv", "run", "python", f"{SCRIPTS_DIR}/2a_ingest_to_bronze.py"],
            cwd=SCRIPTS_DIR, check=True,
        )
        meta["output_rows"] = -1  # row count tracked by the script itself
        _emit_batch_step("bronze_ingest", SOURCE_FILE_URNS, BRONZE_URNS)


def validate_bronze(**context) -> None:
    """
    Data quality gate after bronze ingestion.

    For each bronze table runs three checks:
      1. row_count   — table has at least min_rows rows
      2. null_pk     — primary-key column contains zero NULLs
      3. unique_pk   — primary-key column has no duplicate values

    Each check result is emitted to DataHub as an AssertionRunEvent.
    The task fails if any check does not pass.
    """
    import trino
    from datahub.emitter.rest_emitter import DataHubRestEmitter

    emitter  = DataHubRestEmitter(DATAHUB_GMS_URL)
    now_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    run_id   = f"bronze_validation_{now_ms}"
    failures = []

    conn = trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="airflow",
    )
    cursor = conn.cursor()

    with _run_tracker("validate_bronze", context) as meta:
        for table, spec in BRONZE_QUALITY_CHECKS.items():
            dataset_urn = _trino_urn("bronze", table)
            pk          = spec["pk"]
            min_rows    = spec["min_rows"]

            # ── 1. Row count ─────────────────────────────────────────────────
            cursor.execute(f"SELECT COUNT(*) FROM delta.bronze.{table}")
            row_count   = cursor.fetchone()[0]
            rc_passed   = row_count >= min_rows
            rc_urn      = f"urn:li:assertion:ehr_pipeline.bronze.{table}.row_count"
            try:
                _emit_assertion(emitter, rc_urn, dataset_urn,
                                "row_count_check", rc_passed, run_id, now_ms)
            except Exception as e:
                print(f"[datahub] WARNING assertion emit failed: {e}")
            if not rc_passed:
                failures.append(f"{table}: row_count {row_count} < {min_rows}")
            print(f"[validate] {table}.row_count = {row_count:,}  {'✓' if rc_passed else '✗'}")

            # ── 2. NULL primary key ──────────────────────────────────────────
            cursor.execute(
                f"SELECT COUNT(*) FROM delta.bronze.{table} WHERE {pk} IS NULL"
            )
            null_count = cursor.fetchone()[0]
            nk_passed  = null_count == 0
            nk_urn     = f"urn:li:assertion:ehr_pipeline.bronze.{table}.null_pk"
            try:
                _emit_assertion(emitter, nk_urn, dataset_urn,
                                "null_pk_check", nk_passed, run_id, now_ms)
            except Exception as e:
                print(f"[datahub] WARNING assertion emit failed: {e}")
            if not nk_passed:
                failures.append(f"{table}: {null_count} NULL values in {pk}")
            print(f"[validate] {table}.null_{pk} = {null_count}  {'✓' if nk_passed else '✗'}")

            # ── 3. Uniqueness ────────────────────────────────────────────────
            cursor.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT {pk})
                FROM delta.bronze.{table}
            """)
            dup_count = cursor.fetchone()[0]
            uq_passed = dup_count == 0
            uq_urn    = f"urn:li:assertion:ehr_pipeline.bronze.{table}.unique_pk"
            try:
                _emit_assertion(emitter, uq_urn, dataset_urn,
                                "unique_pk_check", uq_passed, run_id, now_ms)
            except Exception as e:
                print(f"[datahub] WARNING assertion emit failed: {e}")
            # raw_events intentionally has duplicates — log as warning not failure
            if not uq_passed and table != "raw_events":
                failures.append(f"{table}: {dup_count} duplicate {pk} values")
            dup_label = "(expected — will dedup in silver)" if table == "raw_events" and not uq_passed else ""
            print(f"[validate] {table}.dup_{pk} = {dup_count}  {'✓' if uq_passed else '✗'} {dup_label}")

        meta["output_rows"] = len(BRONZE_QUALITY_CHECKS) * 3  # checks run

    cursor.close()
    conn.close()

    if failures:
        raise ValueError(f"Bronze quality checks FAILED:\n" + "\n".join(f"  • {f}" for f in failures))

    print(f"[validate] All bronze quality checks passed.")


def silver_transform(**context) -> None:
    """
    Clean, deduplicate, and standardise bronze tables → silver layer (PySpark).

    Source: delta.bronze.raw_*
    Target: s3://lakehouse/topics/stg_* + Trino delta.silver
    """
    with _run_tracker("silver_transform", context) as meta:
        subprocess.run(
            ["uv", "run", "python", f"{SCRIPTS_DIR}/3_transform_to_silver.py"],
            cwd=SCRIPTS_DIR, check=True,
        )
        meta["output_rows"] = -1
        _emit_batch_step("silver_transform", BRONZE_URNS, SILVER_URNS)


def gold_transform(**context) -> None:
    """
    Model conformed dimensions, facts, and the OBT from silver → gold (PySpark).

    Source: delta.silver.stg_*
    Target: s3://lakehouse/topics/{dim_*,fact_*,obt_*} + Trino delta.gold
    """
    with _run_tracker("gold_transform", context) as meta:
        subprocess.run(
            ["uv", "run", "python", f"{SCRIPTS_DIR}/4_transform_to_gold.py"],
            cwd=SCRIPTS_DIR, check=True,
        )
        meta["output_rows"] = -1
        _emit_batch_step("gold_transform", SILVER_URNS, GOLD_URNS)


def compute_features(**context) -> None:
    """
    Compute 6-month rolling patient feature aggregates from the OBT → feat_* tables.

    Source: delta.gold.obt_clinical_events + dim_*/fact_*
    Target: delta.gold.feat_patient_{vitals,labs,icd,medication,demographics}_6m
            + delta.gold.gold_visit_labels
    """
    with _run_tracker("compute_features", context) as meta:
        subprocess.run(
            ["uv", "run", "python", f"{SCRIPTS_DIR}/5_compute_features.py"],
            cwd=SCRIPTS_DIR, check=True,
        )
        meta["output_rows"] = -1
        _emit_batch_step("compute_features", GOLD_URNS, FEATURE_URNS)


def feast_materialize(**context) -> None:
    """
    Apply Feast feature definitions and materialize batch features into the
    online store (Redis) for low-latency serving.

    1. feast apply   — registers/updates feature views from feature_definitions.py
    2. feast materialize — pushes the past 365 days of feat_* data to Redis
    """
    from feast import FeatureStore

    with _run_tracker("feast_materialize", context) as meta:
        subprocess.run(
            ["feast", "--chdir", FEAST_REPO_DIR, "apply"],
            check=True,
        )
        print("[feast] Applied feature definitions.")

        store    = FeatureStore(repo_path=FEAST_REPO_DIR)
        end_dt   = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=365)
        store.materialize(start_date=start_dt, end_date=end_dt)
        print(f"[feast] Materialized {start_dt.date()} → {end_dt.date()}")
        meta["output_rows"] = -1
        _emit_batch_step("feast_materialize", FEATURE_URNS, [ONLINE_STORE_URN])


# ── Streaming Pipeline Tasks ──────────────────────────────────────────────────

def ensure_flink_job(**context) -> None:
    """
    Ensure the PyFlink streaming processor is running via the Flink REST API.

    The job consumes the 'patient-events' Kafka topic, computes 24-hour
    sliding-window aggregates per patient, and writes results to the
    'patient-features-24h' topic.

    If the job is already running it is left untouched. If not, the script
    is submitted as a background process (non-blocking).
    """
    import requests

    flink_job_name = "ehr_patient_event_processor"

    with _run_tracker("flink_stream_processor", context) as meta:
        try:
            resp = requests.get(f"{FLINK_REST_URL}/jobs", timeout=10)
            resp.raise_for_status()
            running_ids = [j["id"] for j in resp.json().get("jobs", []) if j["status"] == "RUNNING"]
            for job_id in running_ids:
                detail = requests.get(f"{FLINK_REST_URL}/jobs/{job_id}", timeout=10).json()
                if flink_job_name in detail.get("name", ""):
                    print(f"[flink] Job '{flink_job_name}' already running (id={job_id}).")
                    _emit_stream_step("flink_stream_processor", [KAFKA_EVENTS_URN], [KAFKA_FEATURES_URN])
                    meta["output_rows"] = 0
                    return
        except Exception as exc:
            print(f"[flink] WARNING — could not query Flink REST API ({FLINK_REST_URL}): {exc}")

        print(f"[flink] Submitting {SCRIPTS_DIR}/6_flink_stream_processor.py as background process ...")
        subprocess.Popen(
            ["uv", "run", "python", f"{SCRIPTS_DIR}/6_flink_stream_processor.py"],
            cwd=SCRIPTS_DIR,
        )
        print("[flink] Job submitted (non-blocking).")
        meta["output_rows"] = 0
        _emit_stream_step("flink_stream_processor", [KAFKA_EVENTS_URN], [KAFKA_FEATURES_URN])


def push_stream_features(**context) -> None:
    """
    Run the Feast streaming feature push pipeline for STREAM_PUSH_TIMEOUT_S
    seconds to drain any backlog from the 'patient-features-24h' Kafka topic
    into the Redis online store.
    """
    timeout_s = STREAM_PUSH_TIMEOUT_S

    with _run_tracker("push_stream_features", context) as meta:
        print(f"[feature-pipeline] Running stream feature push for {timeout_s}s ...")
        proc = subprocess.Popen(
            ["uv", "run", "python", f"{SCRIPTS_DIR}/7_feature_pipeline.py", "--skip-full-materialize"],
            cwd=SCRIPTS_DIR,
        )
        try:
            proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=15)
        print("[feature-pipeline] Stream push complete.")
        meta["output_rows"] = -1
        _emit_stream_step("push_stream_features", [KAFKA_FEATURES_URN], [ONLINE_STORE_URN])


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="ehr_data_pipeline",
    description=(
        "EHR full pipeline — "
        "BATCH: parquet→bronze→validate→silver→gold→features→feast (Redis); "
        "STREAM: Flink (patient-events→patient-features-24h)→feast push"
    ),
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ehr", "datahub", "feast", "flink", "spark", "delta-lake"],
) as dag:

    # ── Batch branch ─────────────────────────────────────────────────────────
    t_bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=bronze_ingest,
        inlets=[
            DatahubDataset("file", f"data/synthetic/{t}.parquet")
            for t in ("patients", "wards", "event_metadata", "visits", "events")
        ],
    )
    t_validate = PythonOperator(
        task_id="validate_bronze",
        python_callable=validate_bronze,
    )
    t_silver = PythonOperator(
        task_id="silver_transform",
        python_callable=silver_transform,
    )
    t_gold = PythonOperator(
        task_id="gold_transform",
        python_callable=gold_transform,
    )
    t_features = PythonOperator(
        task_id="compute_features",
        python_callable=compute_features,
    )
    t_feast = PythonOperator(
        task_id="feast_materialize",
        python_callable=feast_materialize,
        outlets=[DatahubDataset("redis", "ehr_feature_store.online")],
    )

    # ── Streaming branch (independent of batch) ───────────────────────────────
    t_flink = PythonOperator(
        task_id="flink_stream_processor",
        python_callable=ensure_flink_job,
        inlets=[DatahubDataset("kafka", "patient-events")],
    )
    t_stream_push = PythonOperator(
        task_id="push_stream_features",
        python_callable=push_stream_features,
        outlets=[DatahubDataset("redis", "ehr_feature_store.online")],
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    t_bronze >> t_validate >> t_silver >> t_gold >> t_features >> t_feast
    t_flink >> t_stream_push
