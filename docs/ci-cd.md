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

`.gitlab-ci.yml`:

| job | runs on | does |
|---|---|---|
| `validate` | MRs, default branch, manual | generate vs. the current `deploy`, then `pytest` (ci/checks) |
| `deploy` | default branch | commit the bundle from `validate` to `deploy` |

The deploy push is a few plain lines in the YAML:
1. Check out `deploy` into a worktree.
2. Replace its contents with the bundle and `git add -A -f`.
3. Commit only if something changed (`[skip ci]`, with a `Source-Commit:` trailer).
4. Do a plain push.

Ordering and stale runs are left to GitLab: `resource_group: deploy` runs one
deploy at a time, and `environment` + **"Prevent outdated deployment jobs"**
stops an older pipeline from deploying over a newer one. The push uses
`DEPLOY_PUSH_TOKEN`, a project access token with `write_repository`.

A rejected push (someone moved `deploy` meanwhile) just fails the job. The
next pipeline rebuilds from the new state.

### Checks (`ci/checks/`, run against the built bundle)

| test | guards against |
|---|---|
| `test_dag_integrity` | import errors, slow files, empty DAGs, **any subprocess at parse time** (a `dbt ls` fallback) |
| `test_selector_parity` | Cosmos' Python selector disagreeing with `dbt ls` (e.g. `fqn:` wildcards select nothing in Cosmos) |
| `test_partial_parse` | a shipped msgpack that dbt would throw away at runtime |
| `test_compile` | SQL that parses but can't render, e.g. a package macro without its prefix |

## git-sync settings that matter

* `dags.gitSync.ref: deploy`. git-sync v4 (chart ≥ 1.11) reads `ref`, and the
  chart's default is `v2-2-stable`. `branch` alone does nothing.
* KubernetesExecutor task pods clone once (init container, `GITSYNC_ONE_TIME`),
  so a running task's files never change. With Celery or Local executors, set
  `GITSYNC_STALE_WORKTREE_TIMEOUT` above your longest task.
* Every task pod clones `deploy`: keep `depth: 1` and the bundle small.
