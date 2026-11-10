# Airflow DAG file: Cosmos DAGs for the "operations" dbt project (see dags/config/operations.yml).
from dbt_factory import build_dags

build_dags("operations", globals())
