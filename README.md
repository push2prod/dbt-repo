# dbt-repo

dbt projects for **Cosmos** on **Airflow 2.10** (KubernetesExecutor, git-sync).
GitLab CI runs `dbt deps` + `dbt parse` for every changed project and publishes
the result on the `deploy` branch: the default branch's commit plus one commit
adding, next to each project, `target/manifest.json` and `dbt_packages/`. The
DAG processor never runs dbt.

Design and rationale: [infra/docs/ci-cd.md](infra/docs/ci-cd.md).

## Layout

```
projects/<dir>/        dbt projects (default glob `projects/*`; any layout via projects.include)
  example/             sample project for the example DAG; delete both once you add your own
packages/<pkg>/        local dbt packages (optional), used as `local: ../../packages/<pkg>`
dags/                  the Airflow DAG folder (git-sync subPath): hand-written DAG files
                       calling dbt_dag(...) (see "Add a DAG"), nothing else;
                       example_dbt_dag.py is the commented template to copy
utils/dbt_factory/     Cosmos DAG factory (`from utils.dbt_factory import dbt_dag`);
                       reads projects.json (generated, only on `deploy`)
.gitlab-ci.yml         validate + deploy; every step calls infra/
infra/                 all CI/CD tooling and generation inputs:
  dbt_bundle.yml       the bundle config: projects, shared profile, parse env, DAG defaults
  profiles/profiles.yml  dummy profile for CI parsing only; not used by Airflow
  manifest/            generator: generate.py · config.py · files.py · dbtvenv.py · parse.env
  checks/              pytest checks on the assembled deploy tree and the generator (+ pytest.ini)
  scripts/             fetch_previous.sh · assemble_deploy_tree.sh · publish_deploy.sh
  airflow/             production Helm values + reference Dockerfile for the Airflow image
  requirements/        dbt.txt (own venv) · airflow-image.txt · airflow.txt
  docs/ci-cd.md        design and rationale
```

Rules the generator enforces (it fails the pipeline otherwise):

* A project is named by **`name:` in its `dbt_project.yml`**; names are unique.
  DAG files pass that name to `dbt_dag(...)`, not the folder name.
* Projects use the **one profile** in `infra/dbt_bundle.yml` (or set no `profile:`).
  At runtime Cosmos builds it from the Airflow connection in `profile.mapping`.
* Local packages live inside the repo. `target-path` and `packages-install-path`
  stay at their defaults (Cosmos expects `target/`).

`python -m infra.manifest.generate --list` shows what it finds, without running dbt.

## Run locally

Python 3.12. dbt goes in its **own venv**, as on the Airflow image.

```bash
python3.12 -m venv /opt/dbt-venv && /opt/dbt-venv/bin/pip install -r infra/requirements/dbt.txt
pip install -r infra/requirements/airflow.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
export DBT_BIN=/opt/dbt-venv/bin/dbt DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt
export AIRFLOW_HOME=$(mktemp -d) PYTHONDONTWRITEBYTECODE=1
```

**Generate the overlay** (only generated files; no warehouse needed, `dbt parse` doesn't connect):
```bash
python -m infra.manifest.generate --list                                    # projects found; no dbt
python -m infra.manifest.generate --out build/overlay
python -m infra.manifest.generate --out build/overlay2 --previous build/overlay   # reuses unchanged projects
python -m infra.manifest.generate --out build/one --project core            # one project
```
It prints `built` / `reused` per project. `build/overlay.summary.json` holds the same.

**Validate it** the way CI does, on this commit's tree + the overlay:
```bash
bash infra/scripts/assemble_deploy_tree.sh HEAD build/overlay build/deploy-tree
BUNDLE_DIR=build/deploy-tree SUMMARY_FILE=build/overlay.summary.json pytest -c infra/checks/pytest.ini
```

## CI (`.gitlab-ci.yml`)

| job | runs on | does |
|---|---|---|
| `validate` | MRs, default branch, manual | generate the overlay (reusing `deploy`), assemble the deploy tree, `pytest` |
| `deploy` | default branch | `infra/scripts/publish_deploy.sh`: `deploy` = this commit + one commit adding the overlay; force-pushed with a lease; skipped if the default branch moved on or nothing changed |

Project setup:
* CI/CD variable `DEPLOY_PUSH_TOKEN` (masked, protected): project access token with `write_repository`.
* If `deploy` is a protected branch: allow force push for the token's role
  (Settings > Repository > Protected branches).
* Settings > CI/CD > General pipelines > "Prevent outdated deployment jobs": on.

## Common tasks

* **Add a DAG:** copy [`dags/example_dbt_dag.py`](dags/example_dbt_dag.py)
  (one DAG file per project keeps parse times independent), call the factory and
  assign the result to a module-level name. `<name>` is the project's dbt `name:`:
  ```python
  # Airflow DAG file: Cosmos DAGs for the "<name>" dbt project.
  from utils.dbt_factory import dbt_dag

  campaigns = dbt_dag(
      "<name>",
      dag_id="dbt_<name>_campaigns",
      schedule="0 3 * * *",
      select=["tag:campaigns"],
      exclude=["tag:deprecated"],
      retries=2,                      # overrides dags.defaults.retries for this DAG
  )
  ```
  The example DAG runs the sample project `projects/example` (3 models, one
  excluded by tag). It is there so this template's CI passes before you add a
  project (the generator fails when it finds none) and so the checks exercise a
  real DAG; delete both once your own projects and DAGs are in. Defaults (start_date, retries, tags, ...) come from `dags.defaults` in
  `infra/dbt_bundle.yml`; pass any of them, or any other `DbtDag`/`DAG` argument, to
  override. Keep the word "Airflow" in the file (a comment is enough): Airflow's
  safe mode skips `.py` files that don't contain both "airflow" and "dag".
* **Add a project:** add its directory where `projects.include` finds it
  (`dbt_project.yml`, `packages.yml`, `package-lock.yml`; no `profiles.yml`
  needed), then a DAG file for it as above.
* **Shared macros** in local packages are called with the package prefix:
  `{{ shared_macros.cents_to_dollars('amount_cents') }}`.
