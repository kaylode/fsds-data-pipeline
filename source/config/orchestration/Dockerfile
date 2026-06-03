# Custom Airflow image with Great Expectations and DataHub SDK installed.
# Build this image once; both airflow services (webserver + scheduler) use it.
FROM apache/airflow:2.10.2

USER root

RUN /usr/local/bin/python -m pip install --no-cache-dir \
    "great_expectations==0.18.19" \
    "acryl-datahub[airflow]" \
    "acryl-datahub-airflow-plugin" \
    "pandas" \
    "sqlglot" \
    "python-dotenv" \
    "feast[trino]"

USER 50000

