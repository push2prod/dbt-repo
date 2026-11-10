# Design: manifests in CI, Cosmos in Airflow

## Problem

Cosmos without a manifest falls back to `LoadMode.DBT_LS`: every DAG file runs
`dbt ls` on every parse. At ~20 DAGs and a 30s parse interval the DAG processor
runs out of CPU, misses heartbeats, and looks hung to the scheduler.

## Solution

* CI runs `dbt deps` + `dbt parse` per project and ships `target/manifest.json`
  and `target/partial_parse.msgpack` with the DAGs (the *bundle*).
* Cosmos reads the manifest in an explicit `LoadMode.DBT_MANIFEST` (no silent
  fallback to `dbt ls`), so DAG parsing runs no dbt at all.
* The bundle is committed to the `deploy` branch, and git-sync delivers it to Airflow.
* Environment: Airflow 2.10.5, KubernetesExecutor, astronomer-cosmos 1.15.1
  (1.16 needs Airflow ≥ 2.11), dbt 1.11 in its own venv on the image.

## Deliverables

| # | what | where |
|---|---|---|
| 1 | Mock dbt projects + a shared package, used as the baseline | `e2e/fixtures/` |
| 2 | Manifest generation, plus the CI that runs it and deploys | `manifest/`, `dags/dbt_factory/`, `ci/checks/`, `.github/workflows/`, `.gitlab-ci.yml` |
| 3 | Patches that change the manifests | `e2e/patches/` |
| 4 | E2E stress test on a fresh Airflow cluster | `e2e/` |

Tooling (2) is separate from mock content (1, 3). In a real dbt repo,
`manifest/`, `dags/dbt_factory/` and the CI files are reused, and
`--source .` points the generator at the real projects.

## Manifest generation (`manifest/`)

```
python -m manifest.generate --source <dir> --out <bundle> [--previous <old bundle>] [--parse-env <file>]
```

* **No git.** A directory goes in and a directory comes out. `--source` holds `projects/`,
  `packages/` and `dags/`. `--previous` is a plain directory: in CI, a checkout of the `deploy` branch.
* **Only changed projects are re-parsed.** Each project has an *input hash*
  covering its files, the local packages it uses (`packages.yml` `local:`,
  followed transitively), `requirements/dbt.txt`, the parse env, the generator
  itself, and the dbt version. If the previous bundle has the same hash, that
  project is copied **byte for byte**. A fresh `dbt parse` is never
  byte-identical (`generated_at`), so this also keeps the deploy diff to what
  really changed.
* **Bundle layout** mirrors the source, so `local: ../../packages/...` resolves at runtime.
  `dbt deps` symlinks local packages, so the generator dereferences them.

### Partial parse that survives to runtime

dbt silently discards `partial_parse.msgpack`, and re-parses fully in every
task, unless these match between CI and the task pod:

| must match | how |
|---|---|
| dbt-core / adapter versions | `requirements/dbt.txt`. The generator refuses a dbt venv that differs |
| `--profile` / `--target` flags | CI parses with `--profile <project> --target prod`, the flags Cosmos passes |
| non-secret connection fields, env vars read with `env_var()` | `manifest/parse.env` holds the **production** values |

Secrets use the `DBT_ENV_SECRET_` prefix and are excluded from dbt's state hash, so they can be dummies in CI.

## DAGs (`dags/dbt_factory/` + one file per project)

* `dags/dbt_<project>.py` is two lines calling `build_dags("<project>")`.
  **One file per project**, so a big project can't push another project's
  file past the per-file parse timeout.
* `dags/config/<project>.yml` lists that project's DAGs: `dag_id`, `schedule`,
  `select`, `exclude`, optional `retries`.
* The factory sets an explicit `DBT_MANIFEST` load mode, runs dbt from the venv as a
  subprocess (`DBT_EXECUTABLE`), installs no deps at task time, uses partial
  parse, and defaults to `retries: 1`. The retry covers a task pod that starts
  just after a deploy and clones the new revision.

## CI/CD

| stage | GitHub | GitLab | does |
|---|---|---|---|
| lint | `ci.yml: lint` | `lint` | ruff, ruff format, `mypy --strict`, shellcheck, size limits |
| validate | `ci.yml: validate` | `validate` | generate vs. the current `deploy`, then `pytest` (ci/checks) |
| deploy | `deploy.yml` | `deploy` | generate, validate, commit the bundle to `deploy` |
| e2e | `e2e.yml` | `e2e` | the stress test; manual + every push to master |

The deploy push is a few plain lines in the YAML:
1. Check out `deploy` into a worktree.
2. Replace its contents with the bundle and `git add -A -f`.
3. Commit only if something changed (`[skip ci]`, with a `Source-Commit:` trailer).
4. Do a plain push.

Ordering and stale runs are left to the CI system:

* **GitLab:** `resource_group: deploy` runs one deploy at a time, and
  `environment` + **"Prevent outdated deployment jobs"** stops an older pipeline
  from deploying over a newer one. The push uses `DEPLOY_PUSH_TOKEN`, a project
  access token with `write_repository`.
* **GitHub:** `concurrency: deploy` runs one at a time, and the job skips the
  push when master has moved on during the run. The push uses `GITHUB_TOKEN`
  (`contents: write`).

A rejected push (someone moved `deploy` meanwhile) just fails the job. The
next pipeline rebuilds from the new state.

### Checks (`ci/checks/`, run against the built bundle)

| test | guards against |
|---|---|
| `test_dag_integrity` | import errors, slow files, empty DAGs, **any subprocess at parse time** (a `dbt ls` fallback) |
| `test_selector_parity` | Cosmos' Python selector disagreeing with `dbt ls` (e.g. `fqn:` wildcards select nothing in Cosmos) |
| `test_partial_parse` | a shipped msgpack that dbt would throw away at runtime |
| `test_compile` | SQL that parses but can't render, e.g. a package macro without its prefix |
| `test_generate_reuse` | rebuilding the wrong projects (model change → 1 project, DAG config → none, shared package → all) |
| `test_patches` | patches that no longer apply or no longer do what `expect.yaml` says |

## E2E stress test (`e2e/`)

* **Stack, all from testcontainers:**
  * Postgres, as the warehouse plus Airflow's metadata DB.
  * K3s on one Docker network. The test builds the Airflow image (`e2e/Dockerfile`)
    and imports it into k3s.
  * A git server pod (`git daemon`), which git-sync tracks.
  * The official chart 1.16.0, installed by k3s' Helm controller (no helm binary),
    with `deploy/airflow/values.yaml` + `e2e/values-e2e.yaml`. That's the real
    KubernetesExecutor, a git-sync sidecar on the DAG processor and a one-time
    git-sync init container per task pod.
* **Run:**
  1. Generate the baseline bundle with the CI generator and sync it.
  2. Require all 20 DAGs with no import errors.
  3. Keep all 20 DAGs running back to back.
  4. Apply **all** patches at once, regenerate against the baseline, sync.
  5. Hold the load.
* **Pass criteria** (`e2e/harness/settings.py`):
  * every patch expectation visible in Airflow ≤ 60s after the sync;
  * per-file parse time in the DAG processor pod under load ≤ 5s;
  * scheduler and DAG processor heartbeats < 30s at every sample;
  * 20 DAGs and 0 import errors at every sample;
  * all 20 DAGs running at once;
  * every DAG re-parsed after the sync;
  * 0 failed task instances.
* **Output:** `build/e2e/report.json`, plus `build/e2e/e2e-diag/` (pod logs,
  events) on failure.

Why changes show up fast despite `min_file_process_interval: 600`: with
`file_parsing_sort_mode: modified_time` (the default), Airflow 2.10 re-parses
any file whose mtime is newer than its last parse. git-sync writes every
revision fresh.

## Comparison: CI manifests vs. Cosmos running `dbt ls`

`E2E_LOAD_MODE` runs the same stress test against the old setup:

| mode | bundle | Cosmos at parse time |
|---|---|---|
| `manifest` (default) | manifests + partial parse | reads `manifest.json` |
| `dbt_ls` | no `target/` | runs `dbt ls` per DAG, Cosmos' dbt-ls cache **on** (its default) |
| `dbt_ls_nocache` | no `target/` | runs `dbt ls` per DAG on every parse (`[cosmos] enable_cache_dbt_ls = False`) |

The DAG factory switches with `COSMOS_LOAD_MODE=dbt_ls`, which is for this benchmark only.
Everything else is identical: the chart values, parsing settings, load and patches.

The comparison modes are expected to degrade the stack: DAG import timeouts,
missed heartbeats, the DAG processor restarted by its liveness probe, a slow
API. So they are built to keep measuring through that:

* each mode runs on its own runner and cluster (`fail-fast: false`);
* every wait has a deadline, and every API call and in-pod probe has a timeout;
  failures are recorded as data (`api_failures`, `parse_probe_failures`,
  `restarts`);
* checks are recorded but only enforced in `manifest` mode (`E2E_ENFORCE`).

GitHub: run the e2e workflow manually with **compare_cosmos_dbt_ls**. The
`compare` job writes a side-by-side table (`python -m e2e.compare`) to the run
summary. GitLab: the manual `e2e-compare` + `e2e-compare-table` jobs.

Idle, local (no load), one full parse of the 5 DAG files: manifest 1.3s;
`dbt ls` cold 70s (about 14s per file, 4 `dbt ls` runs each); `dbt ls` with a
warm Cosmos cache 0.7s. The cache hides the cost until project files change,
which is every deploy, under load.

## git-sync settings that matter

* `dags.gitSync.ref: deploy`. git-sync v4 (chart ≥ 1.11) reads `ref`, and the
  chart's default is `v2-2-stable`. `branch` alone does nothing.
* KubernetesExecutor task pods clone once (init container, `GITSYNC_ONE_TIME`),
  so a running task's files never change. With Celery or Local executors, set
  `GITSYNC_STALE_WORKTREE_TIMEOUT` above your longest task.
* Every task pod clones `deploy`: keep `depth: 1` and the bundle small.

## Runner requirements and knobs

* Docker that allows privileged containers: GitHub's `ubuntu-latest`, or GitLab
  `docker:dind` on a privileged runner. Size: 4 vCPU / 16 GB at `E2E_PARALLELISM=12`.
* Optional env: `E2E_EXTRA_CA_FILE` (TLS-inspecting proxy), `E2E_REGISTRY_MIRROR`
  (Docker Hub rate limits, e.g. `https://mirror.gcr.io`), `E2E_*_IMAGE`,
  `E2E_WARMUP_SECONDS`, `E2E_HOLD_SECONDS`, `E2E_PARALLELISM`.
