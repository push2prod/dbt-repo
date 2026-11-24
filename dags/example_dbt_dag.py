# Airflow DAG file: an example Cosmos DAG built with the DAG factory.
#
# Copy this file to start a DAG for your own dbt project. Keep one DAG file per
# dbt project: each file is parsed on its own, so a big project can't push
# another project's file past the parse timeout. Keep the word "Airflow" in the
# file (this comment does it): Airflow's safe mode skips .py files that don't
# mention both "airflow" and "dag".
#
# `utils.dbt_factory` lives at the repo root (utils/dbt_factory). Airflow imports
# it because the git-sync checkout root is on PYTHONPATH (infra/airflow/values.yaml).
from utils.dbt_factory import dbt_dag

# Assign every DAG to a module-level name: Airflow only collects DAGs it finds in
# the module's globals (CI fails if a dbt_dag(...) result is dropped).
example_daily = dbt_dag(
    # The dbt project: its `name:` in dbt_project.yml (projects/example), not the folder name.
    "example",
    dag_id="dbt_example_daily",
    schedule="0 6 * * *",  # any Airflow schedule: cron, preset ("@daily"), timetable, or None
    # Which dbt nodes the DAG runs, in dbt selector syntax. CI checks that Cosmos
    # selects exactly what `dbt ls` does (infra/checks/test_selector_parity.py).
    select=["tag:daily"],
    exclude=["tag:deprecated"],
    # Defaults come from `dags.defaults` in infra/dbt_bundle.yml (start_date,
    # catchup, max_active_runs, tags, retries, retry_delay_seconds). Pass any of
    # them to override it for this DAG only:
    retries=2,  # default: 1
    tags=["dbt", "example"],  # replaces the default tags; "dbt:example" is always added
    # Any other DbtDag / DAG argument is passed through as-is:
    description="Example: the daily models of the sample dbt project",
)
