from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datahub_airflow_plugin.entities import Dataset as DatahubDataset

DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
DATAHUB_GMS_URL = "http://172.17.0.1:8080"
VALID_STATUSES = ["pending", "shipped", "delivered", "cancelled"]

# Paths to 2 tables in our Postgres database, which we will also use as dataset URNs in DataHub.
RAW_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,airflow.public.raw_orders,PROD)"
CLEAN_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,airflow.public.clean_orders,PROD)"


# **context means the parameters passed by Airflow when executing the task, which includes things like the task instance (ti) that we can use to push/pull XComs, the DAG object, execution date, etc. 
# We use this context to pass data between tasks and to get information about the DAG run.
def extract(**context) -> None:
    from sqlalchemy import create_engine

    engine = create_engine(DB_CONN)
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM raw_orders", conn)
    print(f"[extract] Loaded {len(df)} rows")
    context["ti"].xcom_push(key="raw_data", value=df.to_json(orient="records"))


def validate_quality(**context) -> None:
    import great_expectations as ge
    from datahub.emitter.rest_emitter import DataHubRestEmitter
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        AssertionInfoClass, AssertionTypeClass,
        AssertionResultClass, AssertionResultTypeClass,
        AssertionRunEventClass, AssertionRunStatusClass,
        DatasetAssertionInfoClass, DatasetAssertionScopeClass,
        AssertionStdOperatorClass,
    )

    df = pd.read_json(context["ti"].xcom_pull(key="raw_data"), orient="records")
    ge_df = ge.from_pandas(df)

    ge_df.expect_column_to_exist("order_id")
    ge_df.expect_column_to_exist("customer_id")
    ge_df.expect_column_to_exist("amount")
    ge_df.expect_column_to_exist("status")
    ge_df.expect_column_values_to_not_be_null("order_id")
    ge_df.expect_column_values_to_not_be_null("amount")
    ge_df.expect_column_values_to_be_between("amount", min_value=0)
    ge_df.expect_column_values_to_be_in_set("status", VALID_STATUSES)
    ge_df.expect_column_values_to_be_unique("order_id")

    results = ge_df.validate()
    passed = sum(1 for r in results["results"] if r["success"])
    print(f"[validate_quality] {passed}/{len(results['results'])} expectations passed")

    emitter = DataHubRestEmitter(DATAHUB_GMS_URL)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    run_id = f"orders_pipeline_{now_ms}"

    # Loop over each expectation result and emit an assertion info and assertion run event to DataHub.
    # This allows us to track the quality of our dataset in DataHub.
    for result in results["results"]:
        # Get the expectation type and column (if applicable) to construct a unique assertion URN.
        exp_type = result["expectation_config"]["expectation_type"]
        col = result["expectation_config"].get("kwargs", {}).get("column", "")
        # Assertion is uniquely identified by the combination of expectation type and column (if applicable)
        assertion_urn = f"urn:li:assertion:orders_pipeline.{exp_type}.{col}"

        try:
            # Now, let's understand some of the fields we're populating in the AssertionInfoClass:
            # - entityUrn: This is the unique identifier for the assertion in DataHub.
            # - aspect: This is where we define the details of the assertion.
            #   - type: This indicates that our assertion is related to a dataset.
            #   - datasetAssertion: This is where we specify the details of the dataset assertion.
            #     - dataset: The URN of the dataset we're asserting on (in this case, the raw_orders dataset).
            #     - scope: This indicates whether the assertion is at the column level or the row level.
            #     - fields: If it's a column-level assertion, we specify the field (column) we're asserting on.
            #     - operator: This indicates that we're using a native expectation type from Great Expectations, not a custom operator.
            #     - nativeType: This is the specific expectation type from Great Expectations (e.g., expect_column_to_exist).
            # - customProperties: This is a flexible field where we can include any additional information we want to associate with the assertion.
            emitter.emit(MetadataChangeProposalWrapper(
                entityUrn=assertion_urn,
                aspect=AssertionInfoClass(
                    type=AssertionTypeClass.DATASET,
                    datasetAssertion=DatasetAssertionInfoClass(
                        dataset=RAW_URN,
                        scope=DatasetAssertionScopeClass.DATASET_COLUMN if col else DatasetAssertionScopeClass.DATASET_ROWS,
                        fields=[f"urn:li:schemaField:({RAW_URN},{col})"] if col else [],
                        operator=AssertionStdOperatorClass._NATIVE_,
                        nativeType=exp_type,
                    ),
                ),
            ))
            # Next, we emit an AssertionRunEvent to record the result of this assertion for this particular run of the pipeline.
            # - timestampMillis: The timestamp of when this assertion run event occurred.
            # - asserteeUrn: The URN of the entity (dataset) that this assertion is about.
            # - runId: A unique identifier for this run of the pipeline, which allows us to track assertion results across different runs.
            # - assertionUrn: The URN of the assertion that this run event is about (should match the entityUrn we used when emitting the AssertionInfo).
            # - status: Whether this assertion run was a success or failure.
            # - result: This is where we can include the details of the assertion result, including the type (success/failure) and any native results we want to include (e.g., the specific expectation type and whether it passed or failed).
            emitter.emit(MetadataChangeProposalWrapper(
                entityUrn=assertion_urn,
                aspect=AssertionRunEventClass(
                    timestampMillis=now_ms,
                    asserteeUrn=RAW_URN,
                    runId=run_id,
                    assertionUrn=assertion_urn,
                    status=AssertionRunStatusClass.COMPLETE,
                    result=AssertionResultClass(
                        type=AssertionResultTypeClass.SUCCESS if result["success"] else AssertionResultTypeClass.FAILURE,
                        nativeResults={"success": str(result["success"]), "expectation_type": exp_type},
                    ),
                ),
            ))
        except Exception as exc:
            print(f"[validate_quality] WARNING — could not emit assertion: {exc}")

    if not results["success"]:
        failed = [r["expectation_config"]["expectation_type"] for r in results["results"] if not r["success"]]
        raise ValueError(f"Data quality FAILED: {failed}")


def transform(**context) -> None:
    df = pd.read_json(context["ti"].xcom_pull(key="raw_data"), orient="records")
    df["status"] = df["status"].str.upper()
    df["loaded_at"] = datetime.now(timezone.utc).isoformat()
    print(f"[transform] Transformed {len(df)} rows")
    context["ti"].xcom_push(key="clean_data", value=df.to_json(orient="records"))


def load(**context) -> None:
    from sqlalchemy import create_engine
    from datahub.emitter.rest_emitter import DataHubRestEmitter
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        UpstreamLineageClass, UpstreamClass, AuditStampClass,
    )

    df = pd.read_json(context["ti"].xcom_pull(key="clean_data"), orient="records")
    engine = create_engine(DB_CONN)
    with engine.connect() as conn:
        df.to_sql("clean_orders", conn, if_exists="replace", index=False)
    print(f"[load] Wrote {len(df)} rows to clean_orders")

    emitter = DataHubRestEmitter(DATAHUB_GMS_URL)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        # Connect the clean_orders dataset to the raw_orders dataset with an upstream lineage relationship in DataHub. 
        # This allows us to visualize the lineage between these datasets in DataHub and understand that clean_orders is derived from raw_orders.
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=CLEAN_URN,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=RAW_URN,
                        auditStamp=AuditStampClass(time=now_ms, actor="urn:li:corpuser:airflow"), # corpuser represent user Airflow
                    )
                ]
            ),
        ))
        print("[load] Emitted dataset lineage: raw_orders → clean_orders")
    except Exception as exc:
        print(f"[load] WARNING — could not emit dataset lineage: {exc}")


with DAG(
    dag_id="orders_pipeline",
    description="Extract (postgres) => validate => transform => load",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False, # No backfill of historical runs when we first deploy the DAG
    tags=["great-expectations", "datahub", "postgres"],
) as dag:

    t_extract  = PythonOperator(
        task_id="extract",
        python_callable=extract,
        inlets=[DatahubDataset("postgres", "airflow.public.raw_orders")],
    )
    t_validate = PythonOperator(task_id="validate_quality", python_callable=validate_quality)
    t_transform = PythonOperator(task_id="transform",       python_callable=transform)
    t_load     = PythonOperator(
        task_id="load",
        python_callable=load,
        outlets=[DatahubDataset("postgres", "airflow.public.clean_orders")],
    )

    t_extract >> t_validate >> t_transform >> t_load
