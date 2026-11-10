# dbt-repo

A monorepo of dbt projects, run on **Airflow 2.10** (KubernetesExecutor) through
**Cosmos** in `LoadMode.DBT_MANIFEST`.

CI parses every dbt project and ships `manifest.json` plus
`partial_parse.msgpack` with the DAGs. Airflow's DAG processor never runs dbt.
This removes the `dbt ls`-on-every-parse load that made the DAG processor miss
heartbeats.

Background, design decisions and test results: [docs/ci-cd.md](docs/ci-cd.md).

---

## What is implemented

| area | what it does | where |
|---|---|---|
| **dbt projects** | 5 sample projects (core, finance, marketing, product, operations), ~150 models, sharing a local macro package | `projects/`, `packages/shared_macros/` |
| **DAGs** | 20 Cosmos DAGs (5–10 models each), declared in YAML and built by one factory. Explicit `DBT_MANIFEST` (no silent fallback to `dbt ls`), dbt run from its own venv, `retries: 1` | `dags/config/*.yml`, `dags/dbt_factory/` |
| **Build** | Re-parses only projects that changed since what is *deployed* (diff against the deploy's `Source-Commit`, which works with any merge strategy). Unchanged projects are copied byte for byte | `ci/build.py`, `ci/buildlib.py` |
| **Publish** | Commits the build to the `deploy` branch as a linear snapshot history. Publishes only builds of master's tip; the push is compare-and-swap, so stale or racing runs can't overwrite a newer deploy; retries on a lost race | `ci/publish.sh`, `ci/deploy.sh` |
| **Drift check** | Is `deploy` an up-to-date build of master? | `ci/deploy_status.sh` |
| **Tests** (on the built tree) | DAGs import with no subprocess at parse time; Cosmos selection == `dbt ls` for every DAG; shipped partial parse still valid after relocation; every model's SQL compiles | `tests/` |
| **Merge-strategy e2e** | Squash / rebase / merge commit, skipped deploys, stalled and racing runs, add/delete project, force-push, against a local git origin | `ci/e2e_merge_strategies.sh` |
| **Strain test** | Real Airflow 2.10 + Postgres + git-sync stand-in. 10 of 20 DAGs running; a 5-DAG change deployed under load; checks parsing, heartbeats, change visibility, task failures | `strain/` |
| **Code quality** | ruff (lint + format), `mypy --strict`, shellcheck, functions ≤ 60 lines, files ≤ 300 lines | `pyproject.toml`, `ci/check_lengths.py`, `.pre-commit-config.yaml` |
| **CI/CD** | PR checks, deploy on push to master (+ hourly reconcile), weekly strain test | `.github/workflows/` |
| **Airflow config** | Helm values: KubernetesExecutor, git-sync on `deploy`, parsing settings (`min_file_process_interval: 600`, ...) | `deploy/airflow/values.yaml` |

### How a change reaches Airflow

```
PR ──► ci.yml: lint · build · tests ──► merge to master
                                            │
                       deploy.yml: build changed projects → test → publish
                                            │
                           `deploy` branch (snapshot commit, Source-Commit: <master sha>)
                                            │
         git-sync sidecar (scheduler / DAG processor) ── re-parse within seconds (mtime)
         git-sync init container (each task pod)      ── one clone per task, files never change
```

---

## Repository layout

```
projects/<name>/          dbt project: models, seeds, profiles.yml (env vars), packages.yml + package-lock.yml
packages/shared_macros/   local dbt package used by all projects (call as shared_macros.<macro>())
dags/
  dbt_<project>.py        2-line DAG file per project
  config/<project>.yml    the project's DAGs: dag_id, schedule, select, exclude
  dbt_factory/            builds the Cosmos DbtDags
ci/
  build.py, buildlib.py   build the deploy tree
  publish.sh, deploy.sh   publish to `deploy` (deploy.sh = build → test → publish, with retry)
  deploy_status.sh        drift check
  parse.env               env for `dbt parse` (non-secret values must equal prod)
  check_lengths.py        size limits
  e2e_merge_strategies.sh merge-strategy end-to-end check
tests/                    pytest suite, runs against a built deploy tree
strain/                   real-Airflow strain test (run.py + stack/scenario/observe modules)
requirements/             dbt.txt (own venv) · airflow.txt · dev.txt
deploy/airflow/values.yaml
docs/ci-cd.md
```

---

## Setup

Requires Python 3.12 and git. dbt goes in **its own venv**, as on the Airflow image.

```bash
# dbt venv (same pins as CI and the image)
python3.12 -m venv /opt/dbt-venv
/opt/dbt-venv/bin/pip install -r requirements/dbt.txt

# Airflow-side + dev tools (in your active venv)
pip install -r requirements/airflow.txt -r requirements/dev.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt

export DBT_BIN=/opt/dbt-venv/bin/dbt          # used by the build and tests
export DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt   # used by the DAGs (Cosmos)
export AIRFLOW_HOME=$(mktemp -d)              # keeps test runs away from a real Airflow home

pre-commit install                            # lint on every commit
```

Any path works for the dbt venv; just point `DBT_BIN` / `DBT_EXECUTABLE` at it.

---

## Running things

### Build the deploy tree

```bash
python ci/build.py --out build/deploy                          # build every project
python ci/build.py --out build/deploy --previous origin/deploy # only what changed vs. the deploy branch
python ci/build.py --out build/deploy --no-reuse               # force a full rebuild
```

`build/deploy/` is exactly what gets committed to `deploy`. `build/deploy.meta.json`
records the source commit and which projects were built or reused.

No warehouse is needed: `dbt parse` doesn't connect.

### Tests

```bash
DEPLOY_DIR=build/deploy pytest              # ~75s; needs a build first
DEPLOY_DIR=build/deploy pytest tests/test_selector_parity.py -k finance
```

| test | checks |
|---|---|
| `test_dag_integrity.py` | all DAG files import, no import errors, < 5s per file, every DAG has tasks, **no subprocess at parse time** |
| `test_selector_parity.py` | per DAG: Cosmos manifest selection == `dbt ls` (catches selector syntax Cosmos reads differently, e.g. `fqn:` wildcards) |
| `test_partial_parse.py` | shipped `partial_parse.msgpack` is reused after moving the tree and changing the secret |
| `test_compile.py` | every model renders (`dbt compile --no-introspect --no-populate-cache`, no warehouse) |

### Lint, types, size limits

```bash
pre-commit run --all-files        # everything below at once
ruff check . && ruff format --check .
mypy                              # strict; config in pyproject.toml
git ls-files -z '*.sh' strain/k8s_pod/airflow | xargs -0 shellcheck
python ci/check_lengths.py        # functions <= 60 lines, files <= 300
```

### Merge-strategy end-to-end check

```bash
bash ci/e2e_merge_strategies.sh   # ~70s; local git origin, no network, no warehouse
```

It runs 8 scenarios: squash, rebase (shared-package change hidden behind `HEAD~1`),
no-op merge commit, a skipped deploy, a stalled run publishing late, two runs racing,
add/delete project, and force-push. Each prints `ok:` or `FAIL:`.

### Strain test: the full stack on a real Airflow

```bash
DBT_VENV=/opt/dbt-venv python strain/run.py          # ~5 min, KubernetesExecutor model
python strain/run.py --executor-model local          # tasks share the git-sync checkout (Celery/Local)
python strain/run.py --stale-worktree-timeout 0      # git-sync's default; combine with --executor-model local
python strain/run.py --workdir /tmp/strain --warmup 120 --hold 120 --parallelism 16
```

**Needs:** the Postgres server binaries in `/usr/lib/postgresql/<ver>/bin`
(Debian/Ubuntu `postgresql` package; preinstalled on GitHub's ubuntu runners),
`psql`, and a free port 54329. If you run as root, it starts Postgres as the
`postgres` user.

**What it starts, all local:**
- Postgres, as Airflow's metadata DB and the "warehouse".
- A bare git origin with master + deploy.
- `strain/gitsync.sh` following `deploy`.
- Airflow 2.10: scheduler plus standalone DAG processor, with the
  `values.yaml` parsing settings.
- In `k8s` mode, every task runs from its own fresh clone of `deploy`, like a
  pod with a git-sync init container (`strain/k8s_pod/airflow`).

**What it checks:**
1. All 20 DAGs parse.
2. 10 DAGs are kept running (real `dbt run`s).
3. Under load, it changes 5 DAGs: model added, model removed, exclude added,
   schedule changed, model added with a dependency. Then it deploys.
4. Throughout: 20 DAGs, 0 import errors and healthy heartbeats at every sample;
   all 5 changes visible; the other 15 unchanged; every DAG re-parsed despite
   the 600s interval; no failed tasks; parse time per file.

Output ends with `RESULT: PASS` or `FAIL`. `<workdir>/report.json` holds the
numbers, and `<workdir>/logs/` holds the scheduler, DAG processor, git-sync,
deploy and task-pod logs. Typical result on 4 cores: 16 concurrent dbt tasks,
load average ~15, changes visible ~9s after publish, 0 failed tasks.

### Deploy by hand (rarely needed)

```bash
bash ci/deploy_status.sh               # in sync with master, or drift (exit 1)
bash ci/deploy.sh                      # build → test → publish to origin/deploy
SKIP_TESTS=1 bash ci/deploy.sh --no-reuse
```

Normally `.github/workflows/deploy.yml` does this on every push to master, plus
an hourly reconcile and a manual trigger with a `full_rebuild` option.

---

## CI workflows

| workflow | trigger | jobs |
|---|---|---|
| `ci.yml` | pull request, merge queue | `lint`; `build-and-test` (build vs. current deploy, pytest, deploy-diff summary, artifact); `commit-hygiene` (no `[skip ci]` in PR commits, which would skip the deploy after a squash merge) |
| `deploy.yml` | push to master, hourly, manual | drift check → `ci/deploy.sh` → drift check |
| `strain.yml` | weekly, manual | `strain/run.py` on a 4-vCPU runner; report + logs as artifact |

Recommended branch protection: on `master`, require the three `ci` jobs, a merge
queue and squash merges only. On `deploy`, allow pushes only from
`github-actions[bot]`, with no force pushes.

---

## Common tasks

**Add a DAG to an existing project:** add an entry to `dags/config/<project>.yml`:

```yaml
  - dag_id: dbt_finance_tax
    schedule: "0 6 * * *"
    select: ["tag:tax"]
    exclude: ["tag:deprecated"]   # optional
    retries: 2                    # optional, default 1
```

**Add a project:**
1. Create `projects/<name>/` with `dbt_project.yml`, `profiles.yml` (copy one,
   change the profile name), `packages.yml`, and `package-lock.yml` (from
   `dbt deps`).
2. Add `dags/config/<name>.yml`.
3. Add `dags/dbt_<name>.py`:
   ```python
   # Airflow DAG file: Cosmos DAGs for the "<name>" dbt project.
   from dbt_factory import build_dags

   build_dags("<name>", globals())
   ```

The build picks it up automatically.

**Use a shared macro:** call it with the package prefix:
`{{ shared_macros.cents_to_dollars('amount_cents') }}`. Without the prefix,
`dbt parse` still succeeds but every task fails, and `test_compile.py` catches it.

**Change dbt / Cosmos / Airflow versions:**
1. Update `requirements/*.txt` and the image together. `ci/build.py` refuses to
   build if the dbt venv doesn't match `requirements/dbt.txt`.
2. Run the strain workflow.
3. Note: Cosmos ≥ 1.16 needs Airflow ≥ 2.11.

**Environment that `dbt parse` sees:** `ci/parse.env`. Its non-secret values
(host, user, database, ...) must equal production, or the shipped partial
parse is thrown away at runtime. Secrets use the `DBT_ENV_SECRET_` prefix, and
those can be dummies.
