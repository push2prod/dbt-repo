# Airflow DAG file: Cosmos DAGs for the "product" dbt project (see dags/config/product.yml).
from dbt_factory import build_dags

build_dags("product", globals())
