# Design: manifests in CI, Cosmos in Airflow

## Problem

Cosmos without a manifest falls back to `LoadMode.DBT_LS`: every DAG file runs
`dbt ls` on every parse. At ~20 DAGs and a 30s parse interval the DAG processor
runs out of CPU, misses heartbeats, and looks hung to the scheduler.

## Solution

* CI finds the dbt projects (`dbt_bundle.yml`), runs `dbt deps` + `dbt parse`
  per project and ships `target/manifest.json` with the DAGs (the *bundle*).
* Cosmos reads the manifest in an explicit `LoadMode.DBT_MANIFEST` (no silent
  fallback to `dbt ls`), so DAG parsing runs no dbt at all.
* The bundle is committed to the `deploy` branch, and git-sync delivers it to Airflow.
* Environment: Airflow 2.10.5, KubernetesExecutor, astronomer-cosmos 1.10.1,
  dbt 1.11 in its own venv on the image.

## Configuration (`dbt_bundle.yml`)

One file at the repo root describes the layout; every key is optional and
defaults to the values shown in it (`manifest/config.py`):

* `projects.include` / `exclude`: globs for directories holding a `dbt_project.yml`
  (`"**"` allowed). A project is named by `name:` in its `dbt_project.yml`.
* `profile`: the one profile every project uses. `name` and `target` are what
  CI parses with and what Cosmos passes at task time. `ci_profiles_dir` holds a
  dummy `profiles.yml` for CI only. `mapping` is the Cosmos profile mapping
  (class, `conn_id`, `profile_args`) the tasks use.
* `parse_env`: values for `env_var()` calls during `dbt parse` in CI.
* `dags`: the DAG directory, the per-project DAG config path (`{name}`), and DAG defaults.

## Manifest generation (`manifest/`)

```
python -m manifest.generate --out <bundle> [--source .] [--config dbt_bundle.yml] [--previous <old bundle>]
python -m manifest.generate --list        # the projects found and their settings; runs no dbt
python -m manifest.generate --out <bundle> --project <name>   # one project, for local use
```

* **No git.** A directory goes in and a directory comes out. `--previous` is a
  plain directory: in CI, a checkout of the `deploy` branch.
* **Fails loudly on layout problems** instead of shipping a bundle without a project:
  two projects with the same `name:`, a project using another profile, a
  project without a DAG config (or a DAG config without a project), a local
  package outside the repo, a non-default `target-path` / `packages-install-path`.
* **Only changed projects are re-parsed.** Each project has an *input hash*
  covering its files, the local packages it uses (`packages.yml` `local:`,
  followed transitively), `requirements/dbt.txt`, the CI profile, the parse env,
  the generator itself, and the dbt version. If the previous bundle has the
  same hash at the same path, that project is copied **byte for byte**. A fresh
  `dbt parse` is never byte-identical (`generated_at`), so this also keeps the
  deploy diff to what really changed.
* **Bundle layout** mirrors the repo, so `local: ../../packages/...` resolves at
  runtime. Only the local packages the projects use are copied. `dbt deps`
  symlinks local packages, so the generator dereferences them.
* **The registry.** The generator writes `<dags dir>/dbt_factory/projects.json`:
  every project's path and DAG config, the profile name, target and mapping, and
  the DAG defaults. The DAG factory and the CI checks read it instead of
  assuming a layout, so they can't drift from what CI built.
* **No partial parse.** Only `manifest.json` ships. dbt reuses a
  `partial_parse.msgpack` only if the runtime profile hashes the same as the CI
  one (profile name, target, and the non-secret connection fields). CI parses
  with a dummy profile and the tasks use a Cosmos profile mapping, so a shipped
  msgpack would be thrown away; the factory sets `partial_parse=False`.

## DAGs (`dags/dbt_factory/` + one file per project)

* `dags/dbt_<project>.py` is two lines calling `build_dags("<project>")`, with
  the project's dbt `name:`.
  **One file per project**, so a big project can't push another project's
  file past the per-file parse timeout.
* `dags/config/<project>.yml` (`dags.config` in `dbt_bundle.yml`) lists that project's DAGs: `dag_id`, `schedule`,
  `select`, `exclude`, optional `retries`.
* The factory sets an explicit `DBT_MANIFEST` load mode, runs dbt from the venv as a
  subprocess (`DBT_EXECUTABLE`), installs no deps at task time, builds the
  profile from the Airflow connection (`profile.mapping`), and defaults to
  `retries: 1`. The retry covers a task pod that starts
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
| `test_generate` | layout errors not caught; rebuilding the wrong projects (model change → 1 project, DAG config → none, shared package → its users) |
| `test_compile` | SQL that parses but can't render, e.g. a package macro without its prefix |

## git-sync settings that matter

* `dags.gitSync.ref: deploy`. git-sync v4 (chart ≥ 1.11) reads `ref`, and the
  chart's default is `v2-2-stable`. `branch` alone does nothing.
* KubernetesExecutor task pods clone once (init container, `GITSYNC_ONE_TIME`),
  so a running task's files never change. With Celery or Local executors, set
  `GITSYNC_STALE_WORKTREE_TIMEOUT` above your longest task.
* Every task pod clones `deploy`: keep `depth: 1` and the bundle small.
