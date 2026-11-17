# Reference for the production Airflow image:
# Airflow 2.10.5 + astronomer-cosmos, with dbt in its own venv at /opt/dbt-venv.
# DAG files are NOT baked in: git-sync delivers them (the bundle on `deploy`).
FROM apache/airflow:2.10.5-python3.12

ARG AIRFLOW_CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
# Build from the repo root: docker build -f deploy/airflow/Dockerfile .
COPY requirements/airflow-image.txt requirements/dbt.txt /tmp/requirements/
RUN pip install --no-cache-dir -r /tmp/requirements/airflow-image.txt --constraint "${AIRFLOW_CONSTRAINTS}"

USER root
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements/dbt.txt \
 && chown -R airflow: /opt/dbt-venv
USER airflow
ENV DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt
