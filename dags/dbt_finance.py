# Airflow DAG file: Cosmos DAGs for the "finance" dbt project (see dags/config/finance.yml).
from dbt_factory import build_dags

build_dags("finance", globals())
