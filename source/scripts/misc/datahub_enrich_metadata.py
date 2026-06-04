#!/usr/bin/env python3
"""
datahub_enrich_metadata.py
--------------------------
Pushes rich metadata (documentation, owners, domain, tags, glossary terms)
for all EHR pipeline datasets to DataHub via the REST API.

Run from the source/ directory:
    uv run scripts/misc/datahub_enrich_metadata.py

Or trigger it from inside Airflow as a one-off task.
"""
from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://127.0.0.1:8088")

from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
    GlossaryTermsClass,
    GlossaryTermAssociationClass,
    DomainsClass,
    AuditStampClass,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_ms() -> int:
    from datetime import datetime, timezone
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _audit() -> AuditStampClass:
    return AuditStampClass(time=_now_ms(), actor="urn:li:corpuser:datahub")


def _dataset_urn(platform: str, name: str, env: str = "PROD") -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


def _tag_urn(tag: str) -> str:
    return f"urn:li:tag:{tag}"


def _term_urn(term: str) -> str:
    return f"urn:li:glossaryTerm:{term}"


def _domain_urn(domain: str) -> str:
    return f"urn:li:domain:{domain}"


def _owner_urn(user: str) -> str:
    return f"urn:li:corpuser:{user}"


def enrich(
    emitter: DataHubRestEmitter,
    urn: str,
    description: str,
    owners: list[str] = None,
    tags: list[str] = None,
    terms: list[str] = None,
    domain: str = None,
    custom_props: dict[str, str] = None,
) -> None:
    """Emit all metadata aspects for a single dataset URN."""
    now = _now_ms()

    # ── Documentation / description ───────────────────────────────────────────
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=DatasetPropertiesClass(
            description=description,
            customProperties=custom_props or {},
        ),
    ))

    # ── Ownership ─────────────────────────────────────────────────────────────
    if owners:
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=_owner_urn(o),
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                    for o in owners
                ],
                lastModified=_audit(),
            ),
        ))

    # ── Tags ─────────────────────────────────────────────────────────────────
    if tags:
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=GlobalTagsClass(
                tags=[TagAssociationClass(tag=_tag_urn(t)) for t in tags],
            ),
        ))

    # ── Glossary Terms ────────────────────────────────────────────────────────
    if terms:
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=GlossaryTermsClass(
                terms=[GlossaryTermAssociationClass(urn=_term_urn(t)) for t in terms],
                auditStamp=_audit(),
            ),
        ))

    # ── Domain ────────────────────────────────────────────────────────────────
    if domain:
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DomainsClass(domains=[_domain_urn(domain)]),
        ))

    print(f"  ✓  {urn.split(',')[1]}")


# ── Dataset catalogue ─────────────────────────────────────────────────────────
#
# Add / edit entries here to enrich metadata for any dataset.
# Keys: urn, description, owners, tags, terms, domain, custom_props
#
DATASETS = [
    # ── Bronze layer ──────────────────────────────────────────────────────────
    dict(
        urn=_dataset_urn("trino", "delta.bronze.raw_patients"),
        description="Raw patient demographics ingested from synthetic Parquet files. "
                    "Contains patient_id, country, gender, and date-of-birth. "
                    "No deduplication or type coercion applied at this stage.",
        owners=["kaylode"],
        tags=["bronze", "ehr", "pii"],
        terms=["PatientIdentifier"],
        domain="healthcare",
        custom_props={"layer": "bronze", "source": "parquet"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.bronze.raw_visits"),
        description="Raw hospital visit records. One row per admission/discharge event. "
                    "Linked to raw_patients via patient_id.",
        owners=["kaylode"],
        tags=["bronze", "ehr"],
        terms=["HospitalVisit"],
        domain="healthcare",
        custom_props={"layer": "bronze", "source": "parquet"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.bronze.raw_events"),
        description="Raw clinical events (vitals, labs, medications, diagnoses). "
                    "High-cardinality table; may contain duplicate event_ids resolved in silver.",
        owners=["kaylode"],
        tags=["bronze", "ehr"],
        terms=["ClinicalEvent"],
        domain="healthcare",
        custom_props={"layer": "bronze", "source": "parquet"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.bronze.raw_wards"),
        description="Hospital ward reference data (ward_id, ward_name, department).",
        owners=["kaylode"],
        tags=["bronze", "ehr", "reference"],
        domain="healthcare",
        custom_props={"layer": "bronze", "source": "parquet"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.bronze.raw_event_metadata"),
        description="Reference table mapping event_type_id to human-readable event names and types.",
        owners=["kaylode"],
        tags=["bronze", "ehr", "reference"],
        domain="healthcare",
        custom_props={"layer": "bronze", "source": "parquet"},
    ),
    # ── Silver layer ──────────────────────────────────────────────────────────
    dict(
        urn=_dataset_urn("trino", "delta.silver.stg_patients"),
        description="Cleaned and deduplicated patient records. Standardised gender codes, "
                    "validated DOB, null-filtered. Source: delta.bronze.raw_patients.",
        owners=["kaylode"],
        tags=["silver", "ehr", "pii"],
        terms=["PatientIdentifier"],
        domain="healthcare",
        custom_props={"layer": "silver", "sla": "daily"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.silver.stg_visits"),
        description="Cleaned visit records with consistent timestamp formats and "
                    "null ward_id records dropped. Source: delta.bronze.raw_visits.",
        owners=["kaylode"],
        tags=["silver", "ehr"],
        terms=["HospitalVisit"],
        domain="healthcare",
        custom_props={"layer": "silver", "sla": "daily"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.silver.stg_events"),
        description="Deduplicated and typed clinical events. Duplicate event_ids resolved "
                    "by keeping latest record. Source: delta.bronze.raw_events.",
        owners=["kaylode"],
        tags=["silver", "ehr"],
        terms=["ClinicalEvent"],
        domain="healthcare",
        custom_props={"layer": "silver", "sla": "daily"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.silver.stg_wards"),
        description="Cleaned ward reference data.",
        owners=["kaylode"],
        tags=["silver", "ehr", "reference"],
        domain="healthcare",
        custom_props={"layer": "silver"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.silver.stg_event_metadata"),
        description="Cleaned event type reference data.",
        owners=["kaylode"],
        tags=["silver", "ehr", "reference"],
        domain="healthcare",
        custom_props={"layer": "silver"},
    ),
    # ── Gold layer ────────────────────────────────────────────────────────────
    dict(
        urn=_dataset_urn("trino", "delta.gold.dim_patient"),
        description="Conformed patient dimension. SCD Type 1. "
                    "Used by all fact tables and ML feature views.",
        owners=["kaylode"],
        tags=["gold", "ehr", "dimension", "pii"],
        terms=["PatientIdentifier"],
        domain="healthcare",
        custom_props={"layer": "gold", "sla": "daily"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.gold.dim_ward"),
        description="Ward dimension with department groupings.",
        owners=["kaylode"],
        tags=["gold", "ehr", "dimension", "reference"],
        domain="healthcare",
        custom_props={"layer": "gold"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.gold.fact_visit"),
        description="Grain: one row per hospital visit. Contains admission/discharge timestamps, "
                    "ward_id FK, and derived length-of-stay in hours.",
        owners=["kaylode"],
        tags=["gold", "ehr", "fact"],
        terms=["HospitalVisit"],
        domain="healthcare",
        custom_props={"layer": "gold", "grain": "visit", "sla": "daily"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.gold.fact_clinical_event"),
        description="Grain: one row per clinical event. Foreign keys to dim_patient, dim_ward. "
                    "Linked to event metadata for typed lookups.",
        owners=["kaylode"],
        tags=["gold", "ehr", "fact"],
        terms=["ClinicalEvent"],
        domain="healthcare",
        custom_props={"layer": "gold", "grain": "event", "sla": "daily"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.gold.obt_clinical_events"),
        description="One Big Table (OBT): denormalised join of fact_clinical_event with all "
                    "dimension tables. Used as the source for feature engineering.",
        owners=["kaylode"],
        tags=["gold", "ehr", "obt"],
        domain="healthcare",
        custom_props={"layer": "gold", "type": "obt", "sla": "daily"},
    ),
    # ── Feature layer ─────────────────────────────────────────────────────────
    dict(
        urn=_dataset_urn("trino", "delta.gold.feat_patient_vitals_6m"),
        description="6-month rolling aggregates of patient vital signs "
                    "(heart_rate, systolic_bp, diastolic_bp, temperature). "
                    "Stats: mean, min, max, std per patient. "
                    "Served online via Feast → Redis.",
        owners=["kaylode"],
        tags=["feature", "ehr", "vitals", "ml-ready"],
        terms=["FeatureView"],
        domain="machine-learning",
        custom_props={"layer": "feature", "feast_view": "patient_vitals", "window": "6m"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.gold.feat_patient_labs_6m"),
        description="6-month rolling lab result aggregates "
                    "(glucose, creatinine, wbc, hemoglobin). "
                    "Stats: mean, min, max, std per patient.",
        owners=["kaylode"],
        tags=["feature", "ehr", "labs", "ml-ready"],
        terms=["FeatureView"],
        domain="machine-learning",
        custom_props={"layer": "feature", "feast_view": "patient_labs", "window": "6m"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.gold.feat_patient_icd_6m"),
        description="6-month ICD-10 chapter diagnosis counts per patient (22 chapters).",
        owners=["kaylode"],
        tags=["feature", "ehr", "icd10", "ml-ready"],
        terms=["FeatureView"],
        domain="machine-learning",
        custom_props={"layer": "feature", "feast_view": "patient_icd", "window": "6m"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.gold.feat_patient_medication_6m"),
        description="6-month medication administration counts per patient "
                    "(lisinopril, metformin, amoxicillin, atorvastatin).",
        owners=["kaylode"],
        tags=["feature", "ehr", "medication", "ml-ready"],
        terms=["FeatureView"],
        domain="machine-learning",
        custom_props={"layer": "feature", "feast_view": "patient_medication", "window": "6m"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.gold.feat_patient_demographics"),
        description="Static patient demographic features (gender, ethnicity, country, age, "
                    "is_deceased). TTL: 10 years (effectively permanent).",
        owners=["kaylode"],
        tags=["feature", "ehr", "demographics", "ml-ready", "pii"],
        terms=["FeatureView", "PatientIdentifier"],
        domain="machine-learning",
        custom_props={"layer": "feature", "feast_view": "patient_demographics"},
    ),
    dict(
        urn=_dataset_urn("trino", "delta.gold.gold_visit_labels"),
        description="Training labels per visit: severity_level, is_readmitted_7d, "
                    "has_inpatient_mortality, has_30day_mortality. "
                    "Offline-only (not served in real-time).",
        owners=["kaylode"],
        tags=["feature", "ehr", "labels", "ml-ready"],
        terms=["FeatureView"],
        domain="machine-learning",
        custom_props={"layer": "feature", "feast_view": "patient_labels", "online": "false"},
    ),
    # ── Kafka topics ──────────────────────────────────────────────────────────
    dict(
        urn=_dataset_urn("kafka", "patient-events"),
        description="Real-time clinical event stream. Produced by the EHR event simulator. "
                    "Consumed by the Flink streaming processor to compute 24h rolling features.",
        owners=["kaylode"],
        tags=["streaming", "ehr", "kafka"],
        terms=["ClinicalEvent"],
        domain="healthcare",
        custom_props={"type": "stream", "format": "json"},
    ),
    dict(
        urn=_dataset_urn("kafka", "patient-features-24h"),
        description="24-hour sliding window patient feature aggregates produced by Flink. "
                    "Consumed by the Feast stream push pipeline to update the Redis online store.",
        owners=["kaylode"],
        tags=["streaming", "feature", "kafka", "ml-ready"],
        terms=["FeatureView"],
        domain="machine-learning",
        custom_props={"type": "stream", "format": "json", "window": "24h"},
    ),
]


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    emitter = DataHubRestEmitter(DATAHUB_GMS_URL)

    # Verify connectivity
    try:
        emitter.test_connection()
        print(f"✓ Connected to DataHub GMS at {DATAHUB_GMS_URL}\n")
    except Exception as e:
        print(f"✗ Cannot reach DataHub GMS: {e}")
        raise SystemExit(1)

    print(f"Enriching metadata for {len(DATASETS)} datasets...\n")
    failed = []
    for ds in DATASETS:
        try:
            enrich(emitter, **ds)
        except Exception as e:
            print(f"  ✗  FAILED {ds['urn']}: {e}")
            failed.append(ds["urn"])

    print(f"\n{'─'*50}")
    print(f"  Done: {len(DATASETS) - len(failed)} succeeded, {len(failed)} failed")
    if failed:
        for f in failed:
            print(f"  ✗  {f}")


if __name__ == "__main__":
    main()
