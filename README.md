# dbt-repo

A monorepo with several dbt projects, deployed to Airflow 2.10 (KubernetesExecutor)
through Cosmos in `LoadMode.DBT_MANIFEST`. CI builds manifests and partial-parse
files, and the DAG processor never runs dbt.

* Design, pipeline, merge strategies, strain-test results: [docs/ci-cd.md](docs/ci-cd.md)
* Projects: `projects/{core,finance,marketing,product,operations}`, sharing `packages/shared_macros`
* DAGs: `dags/config/<project>.yml` (20 DAGs)
* Code rules: ruff, `mypy --strict`, shellcheck, functions ≤ 60 lines, files ≤ 300 lines

```bash
python -m venv /opt/dbt-venv && /opt/dbt-venv/bin/pip install -r requirements/dbt.txt
pip install -r requirements/airflow.txt -r requirements/dev.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
pre-commit install
export DBT_BIN=/opt/dbt-venv/bin/dbt DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt
python ci/build.py --out build/deploy
DEPLOY_DIR=build/deploy pytest
```
