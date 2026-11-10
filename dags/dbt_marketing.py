# Airflow DAG file: Cosmos DAGs for the "marketing" dbt project (see dags/config/marketing.yml).
from dbt_factory import build_dags

build_dags("marketing", globals())
