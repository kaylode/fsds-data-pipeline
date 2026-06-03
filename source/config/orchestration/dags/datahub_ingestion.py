from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator

load_dotenv(Path(__file__).resolve().parents[3] / ".env")

_GMS = os.getenv("DATAHUB_GMS_URL", "http://127.0.0.1:8088")
_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")


def _sink() -> dict:
    return {"type": "datahub-rest", "config": {"server": _GMS}}


RECIPES: list[tuple[str, dict]] = [
    (
        "postgres",
        {
            "source": {
                "type": "postgres",
                "config": {
                    "host_port": f"{_HOST}:{os.getenv('POSTGRES_PORT', '5432')}",
                    "database": os.getenv("POSTGRES_DB", "postgres"),
                    "username": os.getenv("POSTGRES_USER", "postgres"),
                    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
                },
            },
            "sink": _sink(),
        },
    ),
    (
        "kafka",
        {
            "source": {
                "type": "kafka",
                "config": {
                    "connection": {
                        "bootstrap": f"{_HOST}:{os.getenv('KAFKA_PORT', '9092')}",
                        "schema_registry_url": f"http://{_HOST}:{os.getenv('SCHEMA_REGISTRY_PORT', '8081')}",
                    }
                },
            },
            "sink": _sink(),
        },
    ),
    (
        "hive_metastore",
        {
            "source": {
                "type": "hive-metastore",
                "config": {
                    "host_port": f"{_HOST}:{os.getenv('HIVE_METASTORE_PORT', '9083')}",
                },
            },
            "sink": _sink(),
        },
    ),
    (
        "trino",
        {
            "source": {
                "type": "trino",
                "config": {
                    "host_port": f"{_HOST}:{os.getenv('TRINO_PORT', '8090')}",
                    "database": "delta",
                    "username": os.getenv("TRINO_USER", "trino"),
                },
            },
            "sink": _sink(),
        },
    ),
    (
        "minio_lakehouse",
        {
            "source": {
                "type": "s3",
                "config": {
                    "aws_config": {
                        "aws_access_key_id": os.getenv("MINIO_ROOT_USER", "minioadmin"),
                        "aws_secret_access_key": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
                        "aws_endpoint_url": f"http://{_HOST}:{os.getenv('MINIO_PORT', '9000')}",
                        "aws_region": "us-east-1",
                    },
                    "path_specs": [{"include": "s3://lakehouse/topics/{table}/*"}],
                },
            },
            "sink": _sink(),
        },
    ),
]


def _run_ingestion(recipe: dict) -> None:
    from datahub.ingestion.run.pipeline import Pipeline

    pipeline = Pipeline.create(recipe)
    pipeline.run()
    pipeline.raise_from_status()


with DAG(
    dag_id="datahub_metadata_ingestion",
    description="Refresh metadata for all sources (postgres, kafka, hive, trino, minio) in DataHub",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    tags=["datahub", "metadata", "ingestion"],
) as dag:
    for source_name, recipe in RECIPES:
        PythonOperator(
            task_id=f"ingest_{source_name}",
            python_callable=_run_ingestion,
            op_kwargs={"recipe": recipe},
        )
