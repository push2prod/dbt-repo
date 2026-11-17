# dbt-repo

dbt projects for **Cosmos** on **Airflow 2.10** (KubernetesExecutor, git-sync).
GitLab CI runs `dbt deps` + `dbt parse` for every changed project and commits
the result to the `deploy` branch: the default branch's tree plus, next to each
project, `target/manifest.json` and `dbt_packages/`. The DAG processor never runs dbt.

Design and rationale: [docs/ci-cd.md](docs/ci-cd.md).

## Layout

```
dbt_bundle.yml         where the projects are, the shared profile, DAG settings (all optional)
projects/<dir>/        dbt projects (default glob `projects/*`; any layout via projects.include)
packages/<pkg>/        local dbt packages (optional), used as `local: ../../packages/<pkg>`
profiles/profiles.yml  dummy profile for CI parsing only; not deployed
dags/
  dbt_<name>.py        one DAG file per project (see "Add a project")
  config/<name>.yml    that project's DAGs: dag_id, schedule, select, exclude
  dbt_factory/         Cosmos DAG factory; reads projects.json (generated, only on `deploy`)
manifest/              generate.py · config.py · files.py · dbtvenv.py · parse.env
ci/checks/             pytest checks on the assembled deploy tree, and on the generator
deploy/airflow/        production Helm values + reference Dockerfile for the Airflow image
requirements/          dbt.txt (own venv) · airflow-image.txt · airflow.txt
.gitlab-ci.yml         validate + deploy
```

Rules the generator enforces (it fails the pipeline otherwise):

* A project is named by **`name:` in its `dbt_project.yml`**; names are unique.
  DAG files and DAG configs use that name, not the folder name.
* Every project has a DAG config, and every DAG config has a project.
* Projects use the **one profile** in `dbt_bundle.yml` (or set no `profile:`).
  At runtime Cosmos builds it from the Airflow connection in `profile.mapping`.
* Local packages live inside the repo. `target-path` and `packages-install-path`
  stay at their defaults (Cosmos expects `target/`).

`python -m manifest.generate --list` shows what it finds, without running dbt.

## Run locally

Python 3.12. dbt goes in its **own venv**, as on the Airflow image.

```bash
python3.12 -m venv /opt/dbt-venv && /opt/dbt-venv/bin/pip install -r requirements/dbt.txt
pip install -r requirements/airflow.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
export DBT_BIN=/opt/dbt-venv/bin/dbt DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt
export AIRFLOW_HOME=$(mktemp -d) PYTHONDONTWRITEBYTECODE=1
```

**Generate the overlay** (only generated files; no warehouse needed, `dbt parse` doesn't connect):
```bash
python -m manifest.generate --list                                          # projects found; no dbt
python -m manifest.generate --out build/overlay
python -m manifest.generate --out build/overlay2 --previous build/overlay   # reuses unchanged projects
python -m manifest.generate --out build/one --project core                  # one project
```
It prints `built` / `reused` per project. `build/overlay.summary.json` holds the same.

**Validate it** the way CI does, on this commit's tree + the overlay:
```bash
mkdir -p build/deploy-tree && git archive HEAD | tar -x -C build/deploy-tree
cp -a build/overlay/. build/deploy-tree/
BUNDLE_DIR=build/deploy-tree SUMMARY_FILE=build/overlay.summary.json pytest
```

## CI (`.gitlab-ci.yml`)

| job | runs on | does |
|---|---|---|
| `validate` | MRs, default branch, manual | generate the overlay (reusing `deploy`), assemble the deploy tree, `pytest` |
| `deploy` | default branch | commit this commit's tree + the overlay to `deploy` (git-sync tracks it) |

Project setup:
* CI/CD variable `DEPLOY_PUSH_TOKEN` (masked, protected): project access token with `write_repository`.
* Settings > CI/CD > General pipelines > "Prevent outdated deployment jobs": on.

## Common tasks

* **Add a DAG:** add an entry to `dags/config/<project>.yml`
  (`dag_id`, `schedule`, `select`, `exclude`, optional `retries`).
* **Add a project:** add its directory where `projects.include` finds it
  (`dbt_project.yml`, `packages.yml`, `package-lock.yml`; no `profiles.yml`
  needed), `dags/config/<name>.yml`, and `dags/dbt_<name>.py`, where `<name>`
  is the project's dbt `name:`:
  ```python
  # Airflow DAG file: Cosmos DAGs for the "<name>" dbt project.
  from dbt_factory import build_dags

  build_dags("<name>", globals())
  ```
* **Shared macros** in local packages are called with the package prefix:
  `{{ shared_macros.cents_to_dollars('amount_cents') }}`.
