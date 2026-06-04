import os

from airflow.plugins_manager import AirflowPlugin

_enabled = os.getenv("AIRFLOW__DATAHUB__ENABLED", "false").lower() == "true"

if _enabled:
    try:
        from datahub_airflow_plugin.datahub_listener import DataHubListener
        from datahub_airflow_plugin._config import DatahubLineageConfig

        _config = DatahubLineageConfig.parse_obj({
            "enabled": True,
            "datahub_conn_id": "datahub_rest_default",
            "cluster": "prod",
            "platform_instance": None,
            "capture_ownership_info": True,
            "capture_ownership_as_group": False,
            "capture_tags_info": True,
            "materialize_iolets": True,
            "capture_airflow_assets": True,
            "capture_executions": True,
            "datajob_url_link": "taskinstance",
            "enable_extractors": True,
            "patch_sql_parser": False,
            "patch_snowflake_schema": False,
            "extract_athena_operator": False,
            "extract_bigquery_insert_job_operator": False,
            "extract_teradata_operator": False,
            "render_templates": False,
            "enable_multi_statement_sql_parsing": False,
            "enable_datajob_lineage": True,
            "dag_filter_pattern": {"allow": [".*"], "deny": []},
            "log_level": None,
            "debug_emitter": False,
            "disable_openlineage_plugin": False,
        })

        class DataHubPlugin(AirflowPlugin):
            name = "datahub_plugin"
            listeners = [DataHubListener(_config)]

    except ImportError:
        # acryl-datahub-airflow-plugin not installed — rebuild image to enable:
        #   make build-airflow
        class DataHubPlugin(AirflowPlugin):  # type: ignore[no-redef]
            name = "datahub_plugin"

else:
    class DataHubPlugin(AirflowPlugin):  # type: ignore[no-redef]
        name = "datahub_plugin"
