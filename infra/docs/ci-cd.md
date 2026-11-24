# Design: manifests in CI, Cosmos in Airflow

## Problem

Cosmos without a manifest falls back to `LoadMode.DBT_LS`: every DAG file runs
`dbt ls` on every parse. At ~20 DAGs and a 30s parse interval the DAG processor
runs out of CPU, misses heartbeats, and looks hung to the scheduler.

## Solution

* CI finds the dbt projects (`infra/dbt_bundle.yml`), runs `dbt deps` + `dbt parse`
  per project, and keeps only the generated files (the *overlay*):
  `<project>/target/manifest.json`, `<project>/dbt_packages/`, and the registry
  `utils/dbt_factory/projects.json`.
* Cosmos reads the manifest in an explicit `LoadMode.DBT_MANIFEST` (no silent
  fallback to `dbt ls`), so DAG parsing runs no dbt at all.
* The `deploy` branch is the default branch's commit plus one commit that adds
  the overlay, each file in place next to its project. git-sync delivers it to Airflow.
* Environment: Airflow 2.10.5, KubernetesExecutor, astronomer-cosmos 1.10.1,
  dbt 1.11 in its own venv on the image.

## Repo layout: dbt content at the root, tooling in `infra/`

The repo root is a plain dbt repo: `projects/`, `packages/`, `dags/` (the
Airflow DAG folder, git-sync `subPath: dags`), `utils/` (Python the DAG files
import, e.g. the DAG factory), plus `.gitlab-ci.yml`, which GitLab
only reads from the root. Everything CI/CD needs lives in `infra/`:

| path | what |
|---|---|
| `infra/dbt_bundle.yml` | the bundle config (below) |
| `infra/profiles/profiles.yml` | dummy dbt profile for CI parsing; Airflow doesn't use it |
| `infra/manifest/` | the generator (`python -m infra.manifest.generate`), `parse.env` |
| `infra/checks/` | pytest checks + `pytest.ini` (`pytest -c infra/checks/pytest.ini`) |
| `infra/scripts/` | the CI shell steps: `fetch_previous.sh`, `assemble_deploy_tree.sh`, `publish_deploy.sh` |
| `infra/airflow/` | Helm values (`values.yaml`) and the reference image `Dockerfile` |
| `infra/requirements/` | `dbt.txt` (dbt venv), `airflow-image.txt`, `airflow.txt` |

The DAG factory is `utils/dbt_factory/` (`dags.factory_dir`), outside the DAG
folder, so `dags/` holds only DAG files. DAG files import it as
`from utils.dbt_factory import dbt_dag`. That needs the git-sync checkout root on
`sys.path`, which Airflow doesn't add by itself (it adds only the DAG folder):
`infra/airflow/values.yaml` sets `PYTHONPATH=/opt/airflow/dags/repo`, the
checkout root for the chart's default `dags.mountPath` (`$AIRFLOW_HOME/dags`) and
git-sync's link `repo`. The chart's top-level `env` reaches every Airflow
container, KubernetesExecutor task pods included, so scheduler, DAG processor and
tasks all import the same way. `repo` is the symlink git-sync swaps, exactly like
the DAG folder under it, so DAG files and factory always come from one
revision's checkout. The registry `projects.json` is generated into the factory
package. The CI checks put the deploy tree's root on `sys.path` the same way.

The template ships one sample project, `projects/example`, and the commented DAG
`dags/example_dbt_dag.py` that uses it: the generator fails when it finds no
project, so this keeps the template's own CI meaningful, and the example is the
file to copy for new DAGs. Delete both once real projects are in.

`infra/` is on `deploy` too (it is part of the source commit) but never imported by
Airflow: nothing imports `infra`, and it is outside the DAG folder.

## Configuration (`infra/dbt_bundle.yml`)

One file describes the layout; every key is optional and defaults to the values
shown in it (`infra/manifest/config.py`; a check fails if the two disagree). Paths
in it are relative to the repo root (`--source`), not to the file. `--config`
points the generator at another file. Unknown keys, wrong types and an invalid
`dbt_packages` value fail with an error naming the key:

* `projects.include` / `exclude`: globs for directories holding a `dbt_project.yml`
  (`"**"` allowed). A project is named by `name:` in its `dbt_project.yml`.
* `profile`: the one profile every project uses. `name` and `target` are what
  CI parses with and what Cosmos passes at task time. `ci_profiles_dir` holds a
  dummy `profiles.yml` for CI only. `mapping` is the Cosmos profile mapping
  (class, `conn_id`, `profile_args`) the tasks use.
* `parse_env`: values for `env_var()` calls during `dbt parse` in CI.
* `dbt_packages`: `ship` (default) commits each project's `dbt_packages/`, built
  in CI, so tasks run no `dbt deps`. `install` commits only the manifest and
  Cosmos runs `dbt deps` in every task: that needs network to the package
  sources, and can't work with `local:` packages (Cosmos runs each task in a
  temp dir, where `../../packages/x` doesn't resolve), so the generator refuses it then.
* `dags`: `dir`, the DAG folder (git-sync `subPath`); `factory_dir`, the DAG factory
  package (default `utils/dbt_factory`, imported by that dotted path; the registry is
  written into it); `defaults` for `dbt_dag(...)`. Both paths must stay inside the repo.

## Manifest generation (`infra/manifest/`)

```
python -m infra.manifest.generate --out <overlay> [--source .] [--config infra/dbt_bundle.yml] [--previous <deploy checkout>]
python -m infra.manifest.generate --list        # the projects found and their settings; runs no dbt
python -m infra.manifest.generate --out <overlay> --project <name>   # one project, for local use
```

* **No git.** A directory goes in and a directory comes out. `--previous` is a
  plain directory: in CI, a checkout of the `deploy` branch.
* **dbt never runs in the source tree.** Each project, with the local packages
  it uses at the same relative paths, is copied to a temp dir and parsed there.
  `--out` receives only the generated files.
* **Fails loudly on layout problems** instead of deploying without a project:
  two projects with the same `name:`, a project using another profile, a local
  package outside the repo, a non-default `target-path` / `packages-install-path`.
* **Only changed projects are re-parsed.** Each project has an *input hash*
  covering its files, the local packages it uses (`packages.yml` `local:`,
  followed transitively), `infra/requirements/dbt.txt`, the CI profile, the parse env,
  the `dbt_packages` mode, the generator itself, and the dbt version. The hashes
  are kept in the registry. If the previous deploy has the same hash at the same
  path, that project's generated files are copied **byte for byte**. A fresh
  `dbt parse` is never byte-identical (`generated_at`), so this also keeps the
  deploy diff to what really changed.
* **In place.** Generated files go next to their project, so the deploy commit
  has the same layout as the source and `local: ../../packages/...` resolves at
  runtime. `dbt deps` symlinks local packages; the shipped `dbt_packages/` has
  them dereferenced.
* **The registry.** The generator writes `<dags.factory_dir>/projects.json` (`utils/dbt_factory/projects.json`):
  every project's path and input hash, the profile name, target and mapping,
  and the DAG defaults. The DAG factory and the CI checks read it instead of
  assuming a layout, so they can't drift from what CI built.
* **No partial parse.** Only `manifest.json` ships. dbt reuses a
  `partial_parse.msgpack` only if the runtime profile hashes the same as the CI
  one (profile name, target, and the non-secret connection fields). CI parses
  with a dummy profile and the tasks use a Cosmos profile mapping, so a shipped
  msgpack would be thrown away; the factory sets `partial_parse=False`.

## DAGs (`utils/dbt_factory/` + one file per project in `dags/`)

* DAG files are written by hand. Each DAG is one call,
  `dbt_dag("<project>", dag_id=..., schedule=..., select=[...], exclude=[...])`,
  with the project's dbt `name:`, assigned to a module-level name. **One file
  per project** keeps a big project from pushing another project's file past
  the per-file parse timeout.
* `dbt_dag` fills in what must not vary: an explicit `DBT_MANIFEST` load mode,
  the shipped manifest's path, dbt from the venv as a subprocess
  (`DBT_EXECUTABLE`), `install_dbt_deps` per `dbt_packages`, the profile from
  the Airflow connection (`profile.mapping`). Everything else is a normal
  `DbtDag` argument; defaults come from `dags.defaults`, including
  `retries: 1`. The retry covers a task pod that starts
  just after a deploy and clones the new revision.

## CI/CD

`.gitlab-ci.yml`:

| job | runs on | does |
|---|---|---|
| `validate` | MRs, default branch, manual | generate vs. the current `deploy`, then `pytest` (infra/checks) |
| `deploy` | default branch | `deploy` = this commit + one commit adding the overlay from `validate` |

`validate` extracts the current `deploy` tree (`infra/scripts/fetch_previous.sh`),
builds the overlay against it, assembles the exact deploy tree
(`infra/scripts/assemble_deploy_tree.sh`: `git archive $CI_COMMIT_SHA` + the
overlay) and runs the checks on it. `deploy` runs `infra/scripts/publish_deploy.sh`.
CI calls every script as `bash infra/scripts/<script>.sh`, so a checkout that
drops the exec bit (or a `noexec` build dir) doesn't break the jobs. The deploy script:
1. Skip if the default branch has moved on since this pipeline's commit (a newer
   pipeline deploys; also covered by "Prevent outdated deployment jobs").
2. Note the current `deploy` SHA (`git ls-remote`): the lease.
3. Check out `$CI_COMMIT_SHA` in a temp worktree, copy the overlay in, and add
   exactly the overlay's files (`-f`: `target/` and `dbt_packages/` are gitignored).
4. Commit (`deploy: generated files for <sha> [skip ci]`, `Source-Commit:` trailer).
5. Skip if `deploy` already is that: same parent and same tree (a re-run job).
6. `git push --force-with-lease=refs/heads/deploy:<that SHA>`.

So `git log deploy` is the default branch's history plus one commit, the deploy
commit's parent is the source commit it was built from, and `git show deploy`
(or `git diff master deploy`) is only generated files. Between two deploys the
diff is the source change plus the manifests (and registry hashes) of the
projects it touched.

`deploy` is force-pushed every time. The lease only replaces the deploy commit
the job saw (or, the first time, requires that there is none), so a push that
races another one is rejected instead of overwriting it. Ordering is left to
GitLab: `resource_group: deploy` runs one deploy at a time, and `environment` +
**"Prevent outdated deployment jobs"** stops an older pipeline from deploying
over a newer one. The push uses `DEPLOY_PUSH_TOKEN`, a project access token
with `write_repository`; a protected `deploy` branch must allow force push for
its role.

Old deploy commits don't stay on any branch. Roll back by reverting on the
default branch; the next pipeline deploys that. A rejected push just fails the
job, and the next pipeline rebuilds from the new state.

### Checks (`infra/checks/`, run against the assembled deploy tree)

| test | guards against |
|---|---|
| `test_dag_integrity` | import errors, slow files, empty DAGs, **any subprocess at parse time** (a `dbt ls` fallback), a DAG file whose `dbt_dag(...)` creates no DAG (not assigned to a module-level name, or skipped by safe mode), a Cosmos DAG not in `DBT_MANIFEST` mode or without its manifest |
| `test_selector_parity` | Cosmos' Python selector disagreeing with `dbt ls`, for every Cosmos DAG the DAG files create (e.g. `fqn:` wildcards select nothing in Cosmos) |
| `test_generate` | layout errors not caught; `infra/dbt_bundle.yml` out of sync with the defaults; rebuilding the wrong projects (model change → 1 project, DAG file → none, shared package → its users) |
| `test_compile` | SQL that parses but can't render, e.g. a package macro without its prefix |

## git-sync settings that matter

* `dags.gitSync.ref: deploy`. git-sync v4 (chart ≥ 1.11) reads `ref`, and the
  chart's default is `v2-2-stable`. `branch` alone does nothing.
* `deploy` is force-pushed. git-sync fetches the ref and checks out the commit it
  points to, so it doesn't need fast-forwards; confirm once on your setup.
* KubernetesExecutor task pods clone once (init container, `GITSYNC_ONE_TIME`),
  so a running task's files never change. With Celery or Local executors, set
  `GITSYNC_STALE_WORKTREE_TIMEOUT` above your longest task.
* Every task pod clones `deploy`: keep `depth: 1`.
