# dbt-repo

CI-built dbt manifests for **Cosmos** on **Airflow 2.10** (KubernetesExecutor,
git-sync). The DAG processor never runs dbt, and a stress test proves it on a
fresh cluster.

Design and rationale: [docs/ci-cd.md](docs/ci-cd.md).

## What is in here

| deliverable | path | what it is |
|---|---|---|
| 1. mock dbt projects | `e2e/fixtures/` | 5 projects (core, finance, marketing, product, operations), a shared macro package, 20 DAGs (each domain owned by one DAG, 5–7 models each) |
| 2. manifest generation + CI | `manifest/`, `dags/dbt_factory/`, `ci/checks/`, `.github/workflows/`, `.gitlab-ci.yml` | `python -m manifest.generate` builds the deploy bundle (git-free, only changed projects re-parsed). CI validates it and commits it to the `deploy` branch |
| 3. patches | `e2e/patches/` | `change.patch` + `expect.yaml` each: add / remove a model, exclude, schedule change, new dependency, shared-macro change |
| 4. e2e stress test | `e2e/` | testcontainers K3s + Postgres, official Helm chart, all DAGs running; applies the patches, regenerates, syncs, checks Airflow |

```
manifest/            generate.py · files.py · dbtvenv.py · parse.env
dags/dbt_factory/    Cosmos DAG factory (tooling, added to every bundle)
ci/checks/           pytest checks on a built bundle
ci/check_lengths.py  size limits
e2e/
  fixtures/          mock content: projects/ · packages/ · dags/ (one DAG file per project + config/)
  patches/           NNN-name/{change.patch, expect.yaml}
  harness/           cluster · gitserver · airflow (chart + REST) · load · patches · diag · settings
  test_e2e.py        the stress test
  Dockerfile         Airflow 2.10.5 + cosmos 1.15.1 + dbt venv (reference for the prod image)
  values-e2e.yaml    test overrides on top of deploy/airflow/values.yaml
deploy/airflow/values.yaml   production Helm values (KubernetesExecutor, git-sync on `deploy`)
requirements/        dbt.txt (own venv) · airflow-image.txt · airflow.txt · e2e.txt · dev.txt
```

## Setup

Python 3.12. dbt goes in its **own venv**, as on the Airflow image.

```bash
python3.12 -m venv /opt/dbt-venv && /opt/dbt-venv/bin/pip install -r requirements/dbt.txt
pip install -r requirements/airflow.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
pip install -r requirements/e2e.txt -r requirements/dev.txt   # no constraints: needs newer pyyaml/ruff/mypy
export DBT_BIN=/opt/dbt-venv/bin/dbt DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt
export AIRFLOW_HOME=$(mktemp -d) PYTHONDONTWRITEBYTECODE=1
pre-commit install
```

## Run

**Generate a bundle** (no warehouse needed; `dbt parse` doesn't connect):
```bash
python -m manifest.generate --source e2e/fixtures --out build/bundle
python -m manifest.generate --source e2e/fixtures --out build/bundle2 --previous build/bundle   # reuses unchanged projects
```
It prints `built` / `reused` per project. `build/bundle.summary.json` holds the same.

**Validate the bundle** (~2.5 min):
```bash
BUNDLE_DIR=build/bundle pytest            # ci/checks
BUNDLE_DIR=build/bundle pytest ci/checks/test_patches.py
```

**Lint, types, size limits:**
```bash
pre-commit run --all-files                # ruff, ruff format, mypy --strict, shellcheck, size limits
```

**E2E stress test** (~15–20 min; needs Docker that allows privileged containers):
```bash
pytest e2e -v -s
E2E_WARMUP_SECONDS=60 E2E_HOLD_SECONDS=60 E2E_PARALLELISM=8 pytest e2e -v -s   # shorter, smaller
```
What it does:
1. Builds the Airflow image.
2. Starts Postgres + K3s and installs the Airflow chart.
3. Syncs the baseline bundle, then checks all 20 DAGs and no import errors.
4. Keeps all DAGs running.
5. Applies `e2e/patches/*`, regenerates, syncs.
6. Checks every `expect.yaml`, heartbeats, parse times, import errors and task failures.

**Compare with the old setup** (Cosmos running `dbt ls`, no manifests):
`E2E_LOAD_MODE=dbt_ls` or `dbt_ls_nocache`, or on GitHub run the e2e workflow with
*compare_cosmos_dbt_ls*. That runs all three modes in parallel and puts a table in the run summary
(details in docs/ci-cd.md).

The report goes to `build/e2e-report.json` (`E2E_REPORT` to change), plus
`build/e2e-diag/` with pod logs on failure. Behind a TLS-inspecting proxy or
Docker Hub limits, set `E2E_EXTRA_CA_FILE` / `E2E_REGISTRY_MIRROR`.

## CI

| | GitHub | GitLab |
|---|---|---|
| PRs | `ci.yml`: lint, validate, commit hygiene | `lint`, `validate` |
| push to master | `deploy.yml` → `deploy` branch | `deploy` (resource_group + "Prevent outdated deployment jobs") |
| e2e | `e2e.yml`: manual + push to master | `e2e`: manual + push to default branch (docker:dind, privileged runner) |

GitLab needs a `DEPLOY_PUSH_TOKEN` variable (project access token, `write_repository`).
`DBT_SOURCE` in the pipelines is `e2e/fixtures` here. A real dbt repo uses `.`.

## Common tasks

* **Add a DAG:** add an entry to `e2e/fixtures/dags/config/<project>.yml`
  (`dag_id`, `schedule`, `select`, `exclude`, optional `retries`).
* **Add a project:** add `projects/<name>/` (`dbt_project.yml`, `profiles.yml`,
  `packages.yml`, `package-lock.yml`), `dags/config/<name>.yml`, and
  `dags/dbt_<name>.py`:
  ```python
  # Airflow DAG file: Cosmos DAGs for the "<name>" dbt project.
  from dbt_factory import build_dags

  build_dags("<name>", globals())
  ```
* **Add a patch:** edit a scratch copy of `e2e/fixtures/`, `git diff >
  e2e/patches/NNN-name/change.patch`, and write `expect.yaml` (format in
  `e2e/patches/README.md`). `ci/checks/test_patches.py` verifies it without a cluster.
* **Shared macros** are called with the package prefix:
  `{{ shared_macros.cents_to_dollars('amount_cents') }}`.
* **Parse env:** `manifest/parse.env` must hold production's non-secret values,
  or the shipped partial parse is discarded at runtime.
