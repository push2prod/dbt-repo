# Airflow DAG file: Cosmos DAGs for the "core" dbt project (see dags/config/core.yml).
from dbt_factory import build_dags

build_dags("core", globals())
