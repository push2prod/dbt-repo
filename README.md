# dbt-repo

A monorepo with several dbt projects, deployed to Airflow through Cosmos in
`LoadMode.DBT_MANIFEST`. CI builds manifests and partial-parse files, and the
DAG processor never runs dbt.

* Design, pipeline and the merge-strategy analysis: [docs/ci-cd.md](docs/ci-cd.md)
* Projects: `projects/core`, `projects/finance`, `projects/marketing`, sharing `packages/shared_macros`
* DAGs: `dags/config/<project>.yml`

```bash
pip install -r requirements/dbt.txt -r requirements/airflow.txt
python ci/build.py --out build/deploy
DEPLOY_DIR=build/deploy pytest
```
