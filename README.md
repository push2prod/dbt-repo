# dbt-repo

dbt projects for **Cosmos** on **Airflow 2.10** (KubernetesExecutor, git-sync).
GitLab CI runs `dbt deps` + `dbt parse` for every changed project and ships
`manifest.json` and `partial_parse.msgpack` with the DAGs, so the DAG processor
never runs dbt.

Design and rationale: [docs/ci-cd.md](docs/ci-cd.md).

## Layout

```
projects/<name>/       dbt project: dbt_project.yml, profiles.yml, packages.yml, models/ ...
packages/<pkg>/        local dbt packages (optional), used as `local: ../../packages/<pkg>`
dags/
  dbt_<name>.py        one DAG file per project (see "Add a project")
  config/<name>.yml    that project's DAGs: dag_id, schedule, select, exclude
  dbt_factory/         Cosmos DAG factory (tooling, added to every bundle)
manifest/              generate.py · files.py · dbtvenv.py · parse.env
ci/checks/             pytest checks on a built bundle
deploy/airflow/        production Helm values + reference Dockerfile for the Airflow image
requirements/          dbt.txt (own venv) · airflow-image.txt · airflow.txt
.gitlab-ci.yml         validate + deploy
```

Rules the factory and generator rely on:

* Each project's **profile name equals its folder name**, its target is **`prod`**,
  and `profiles.yml` lives **inside the project** (dbt runs with `--profiles-dir <project>`).
* `manifest/parse.env` holds **production's** non-secret connection values and
  every `env_var()` the projects read. Secrets use the `DBT_ENV_SECRET_` prefix
  and can be dummies. Otherwise dbt discards the shipped partial parse at runtime.
* The dbt venv on the Airflow image is installed from `requirements/dbt.txt`.

## Run locally

Python 3.12. dbt goes in its **own venv**, as on the Airflow image.

```bash
python3.12 -m venv /opt/dbt-venv && /opt/dbt-venv/bin/pip install -r requirements/dbt.txt
pip install -r requirements/airflow.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
export DBT_BIN=/opt/dbt-venv/bin/dbt DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt
export AIRFLOW_HOME=$(mktemp -d) PYTHONDONTWRITEBYTECODE=1
```

**Generate a bundle** (no warehouse needed; `dbt parse` doesn't connect):
```bash
python -m manifest.generate --source . --out build/bundle
python -m manifest.generate --source . --out build/bundle2 --previous build/bundle   # reuses unchanged projects
```
It prints `built` / `reused` per project. `build/bundle.summary.json` holds the same.

**Validate the bundle:**
```bash
BUNDLE_DIR=build/bundle pytest
```

## CI (`.gitlab-ci.yml`)

| job | runs on | does |
|---|---|---|
| `validate` | MRs, default branch, manual | generate vs. the current `deploy` branch, then `pytest` |
| `deploy` | default branch | commit the bundle to the `deploy` branch (git-sync tracks it) |

Project setup:
* CI/CD variable `DEPLOY_PUSH_TOKEN` (masked, protected): project access token with `write_repository`.
* Settings > CI/CD > General pipelines > "Prevent outdated deployment jobs": on.

## Common tasks

* **Add a DAG:** add an entry to `dags/config/<project>.yml`
  (`dag_id`, `schedule`, `select`, `exclude`, optional `retries`).
* **Add a project:** add `projects/<name>/` (`dbt_project.yml`, `profiles.yml`,
  `packages.yml`, `package-lock.yml`), `dags/config/<name>.yml`, and
  `dags/dbt_<name>.py`:
  ```python
  # Airflow DAG file: Cosmos DAGs for the "<name>" dbt project.
  from dbt_factory import build_dags

  build_dags("<name>", globals())
  ```
* **Shared macros** in local packages are called with the package prefix:
  `{{ shared_macros.cents_to_dollars('amount_cents') }}`.
