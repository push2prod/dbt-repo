# Airflow DAG file: an example DAG that runs dbt models as a Cosmos task group.
#
# Copy this file to start a DAG of your own. It is a normal Airflow DAG: you set
# the schedule, start_date and so on here, and add dbt where you need it with
# dbt_task_group(...), between any other tasks. Keep the word "Airflow" in the
# file (this comment does it): Airflow's safe mode skips .py files that don't
# mention both "airflow" and "dag".
#
# `utils.dbt_factory` lives at the repo root (utils/dbt_factory). Airflow imports
# it because the git-sync checkout root is on PYTHONPATH (infra/airflow/values.yaml).
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from utils.dbt_factory import dbt_task_group

# Assign the DAG to a module-level name (here via `with ... as dag`): Airflow only
# collects DAGs it finds in the module's globals. CI fails if a dbt task group ends
# up in a DAG Airflow didn't collect.
with DAG(
    dag_id="example_daily",
    schedule="0 6 * * *",  # any Airflow schedule: cron, preset ("@daily"), timetable, or None
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "example"],
    description="Example: the daily models of the sample dbt project, between two other tasks",
    # Applies to every task in the DAG, the dbt tasks included. All DAG settings
    # live here, in the DAG file (infra/dbt_bundle.yml has none).
    default_args={
        "owner": "data",
        # Recommended for dbt tasks: one retry. With the KubernetesExecutor a task
        # pod that starts right after a deploy clones the new revision; a retry
        # re-parses and runs on it. dbt rebuilds models whole, so retrying is safe.
        "retries": 1,
        "retry_delay": timedelta(seconds=60),
    },
) as dag:
    start = EmptyOperator(task_id="start")

    daily_models = dbt_task_group(
        # The dbt project: its `name:` in dbt_project.yml (projects/example), not the folder name.
        "example",
        # Unique within the DAG. It prefixes the group's tasks: daily_models.example_orders_run, ...
        group_id="daily_models",
        # Which dbt nodes the group runs, in dbt selector syntax. CI checks that Cosmos
        # selects exactly what `dbt ls` does (infra/checks/test_selector_parity.py).
        select=["tag:daily"],
        exclude=["tag:deprecated"],
        # Where this group's models are built. Without these, the catalog and schema
        # come from infra/dbt_bundle.yml (profile.mapping.profile_args). Each group
        # can set its own; other groups in the DAG are unaffected. A model with its
        # own `schema` config lands in "<this schema>_<its schema>" (dbt's default
        # generate_schema_name), so this also moves those.
        # database="lake",
        schema="analytics_daily",
        # Overrides for this group's tasks only; they win over the DAG's default_args:
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
        # Any other DbtTaskGroup / TaskGroup argument is passed through, e.g. extra
        # arguments for every dbt operator in the group:
        operator_args={"full_refresh": False},
    )

    end = EmptyOperator(task_id="end")

    # A DAG can hold several dbt_task_group(...) calls (same or different projects),
    # each with its own group_id, wired like any other task.
    start >> daily_models >> end
