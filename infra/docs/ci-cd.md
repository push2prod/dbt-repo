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
  dbt 1.11 (dbt-trino) in its own venv on the image; the warehouse is Trino.

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
`from utils.dbt_factory import dbt_task_group`. That needs the git-sync checkout root on
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
shown in it (`infra/manifest/config.py`). A real repo changes the values (catalog,
schema, conn_id, project globs ...); a check makes sure the file still loads,
spells out every setting and names an importable profile mapping. Paths
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
  written into it). Both paths must stay inside the repo. There are no DAG
  settings in the config: schedule, start_date, catchup, retries, tags ... are
  set in the DAG files, since the factory builds task groups, not DAGs. An old
  config with `dags.defaults` or `dags.task_defaults` fails with an error saying so.

## Warehouse: Trino (and how to switch)

The template targets **Trino**. The warehouse appears in exactly four places;
switching to another one (e.g. back to Postgres, or Snowflake, BigQuery ...) is
the same four changes, nothing else:

| # | what | where | Trino (now) |
|---|---|---|---|
| 1 | dbt adapter, in the dbt venv (CI and the image) | `infra/requirements/dbt.txt` | `dbt-trino==1.11.0` (must fit the `dbt-core` / `dbt-adapters` / `dbt-common` pins) |
| 2 | dummy CI profile (`type` = that adapter; never connects) | `infra/profiles/profiles.yml` | `type: trino`, `method: none`, dummy host/port/user/database/schema |
| 3 | Airflow provider for the connection type | `infra/requirements/airflow-image.txt` | `apache-airflow-providers-trino==6.0.0` (from the Airflow 2.10.5 constraints) |
| 4 | Cosmos profile mapping + Airflow connection | `infra/dbt_bundle.yml` `profile.mapping` | `utils.dbt_factory.profiles.TrinoNoAuthProfileMapping`, `conn_id: trino_default`, `profile_args: {database: <catalog>, schema: <schema>, http_scheme: http}` |

Notes for Trino:

* **No authentication is the default.** Cosmos 1.10 ships only LDAP (password),
  JWT and certificate Trino mappings, and LDAP fails on a connection without a
  password ("cannot find value for secret field password"). So the repo has its
  own `TrinoNoAuthProfileMapping` (`utils/dbt_factory/profiles.py`, a subclass of
  Cosmos' Trino base mapping): dbt-trino `method: none`, no secret. The factory
  imports it by its class path like any Cosmos mapping.
* The Airflow connection (`trino_default`, type `trino`) supplies host, port and
  login (dbt's `user`); `database` (the Trino catalog) and `schema` have no
  connection fields, so they go in `profile_args` (every Trino mapping requires
  them). Replace the `your_catalog` / `your_schema` placeholders. Cosmos maps no
  `http_scheme`: `http` for a plain server, `https` if it has TLS.
* **With authentication**, switch `profile.mapping.class` to a Cosmos mapping,
  same `profile_args`:
  * `cosmos.profiles.TrinoLDAPProfileMapping`: login + password from the
    connection; set `http_scheme: https` (the Trino client refuses passwords over http).
  * `cosmos.profiles.TrinoJWTProfileMapping`: `jwt__token` in the connection extras.
  * `cosmos.profiles.TrinoCertificateProfileMapping`: client cert/key paths in the
    extras `certs__client_cert_path`, `certs__client_key_path`.
* dbt runs in its own venv, so dbt-trino's `trino` client and the provider's
  (Airflow constraints) never have to agree.
* Models must be valid Trino SQL. `test_compile` renders every model's Jinja in
  CI but runs nothing against Trino, so a dialect error (e.g. Postgres-only
  syntax) only shows up when the task runs.

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
  and the dbt task defaults. The task-group factory and the CI checks read it instead of
  assuming a layout, so they can't drift from what CI built.
* **No partial parse.** Only `manifest.json` ships. dbt reuses a
  `partial_parse.msgpack` only if the runtime profile hashes the same as the CI
  one (profile name, target, and the non-secret connection fields). CI parses
  with a dummy profile and the tasks use a Cosmos profile mapping, so a shipped
  msgpack would be thrown away; the factory sets `partial_parse=False`.

## DAGs (`dags/`) and the task-group factory (`utils/dbt_factory/`)

* DAG files are normal, hand-written Airflow DAGs (`with DAG(...)` or `@dag`):
  schedule, start_date, catchup, tags and other tasks are theirs. dbt goes in as
  `dbt_task_group("<project>", group_id=..., select=[...], exclude=[...])`, a
  Cosmos `DbtTaskGroup` wired like any task (`start >> group >> end`). A DAG can
  hold several groups, of one or more projects; `group_id` is unique per DAG and
  prefixes the group's task ids. Keep big projects' DAGs in separate files, so
  one can't push another past the per-file parse timeout.
* `dbt_task_group` fills in what must not vary: an explicit `DBT_MANIFEST` load
  mode, the shipped manifest's path, dbt from the venv as a subprocess
  (`DBT_EXECUTABLE`), `install_dbt_deps` per `dbt_packages`, the profile from the
  Airflow connection (`profile.mapping`). Everything else is a normal
  `DbtTaskGroup` argument, passed as-is. The group's tasks inherit the DAG's
  `default_args`; a group's own `default_args` override them for its tasks.
  Recommended in every DAG with dbt tasks: `default_args={"retries": 1, ...}`.
  With the KubernetesExecutor a task pod that starts just after a deploy clones
  the new revision; the retry re-parses and runs on it, and dbt rebuilds models
  whole, so it is safe. Called outside a DAG it raises.
* **Per-group database / schema.** `dbt_task_group(..., database=..., schema=...)`
  overrides those keys of `profile.mapping.profile_args` for that group only
  (`profile_args={...}` overrides any other profile field; `database`/`schema`
  win over it). Each call builds its own ProfileConfig; the registry is never
  changed, so other groups in the DAG keep the bundle values
  (`infra/checks/test_factory.py`). Why this works with a CI-built manifest
  (checked in Cosmos 1.10.1 + dbt-trino 1.11):
  * At task time Cosmos copies the project to a temp dir, writes `profiles.yml`
    from the group's ProfileConfig and runs dbt with `--no-partial-parse`
    (`partial_parse=False`). dbt re-parses with *that* profile, so relation names
    use the overridden catalog/schema. The shipped `manifest.json` (copied into
    the temp `target/`) holds the CI dummy values (`"ci_dummy"."ci_dummy".<model>`)
    but dbt doesn't plan a run from it; it is overwritten.
  * At DAG parse time Cosmos uses the manifest only for the graph (nodes,
    dependencies, selection), not for relation names. The one place the CI
    values still matter: a selector on them (`config.schema:...`,
    `config.database:...`) matches the manifest's dummy values, not the override.
  * Dataset / lineage URIs: Cosmos builds them after a run from that run's
    artifacts via OpenLineage, so they would carry the runtime catalog/schema;
    the image doesn't install OpenLineage, so none are emitted.
  * dbt's default `generate_schema_name`: a model without a `schema` config goes
    to the (overridden) target schema; one with `schema: x` goes to
    `<target schema>_x`. A custom `generate_schema_name` macro decides otherwise.
    A model's own `database` config wins over the overridden database.

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
| `test_dag_integrity` | import errors, slow files, empty DAGs, **any subprocess at parse time** (a `dbt ls` fallback), a `dbt_task_group(...)` whose DAG Airflow doesn't collect (not assigned to a module-level name, or the file skipped by safe mode), a dbt task group not in `DBT_MANIFEST` mode, without its manifest, or with no tasks |
| `test_selector_parity` | Cosmos' Python selector disagreeing with `dbt ls`, for every dbt task group the DAG files create (e.g. `fqn:` wildcards select nothing in Cosmos) |
| `test_generate` | layout errors not caught; an `infra/dbt_bundle.yml` that doesn't load, leaves out a setting, or names a mapping class that isn't importable; rebuilding the wrong projects (model change → 1 project, DAG file → none, shared package → its users) |
| `test_compile` | SQL that parses but can't render, e.g. a package macro without its prefix |
| `test_factory` | a `dbt_task_group(..., database=, schema=, profile_args=)` override that leaks into other groups or the registry, or doesn't reach the rendered profile |

## git-sync settings that matter

* `dags.gitSync.ref: deploy`. git-sync v4 (chart ≥ 1.11) reads `ref`, and the
  chart's default is `v2-2-stable`. `branch` alone does nothing.
* `deploy` is force-pushed. git-sync fetches the ref and checks out the commit it
  points to, so it doesn't need fast-forwards; confirm once on your setup.
* KubernetesExecutor task pods clone once (init container, `GITSYNC_ONE_TIME`),
  so a running task's files never change. With Celery or Local executors, set
  `GITSYNC_STALE_WORKTREE_TIMEOUT` above your longest task.
* Every task pod clones `deploy`: keep `depth: 1`.
