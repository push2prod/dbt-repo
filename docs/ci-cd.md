# CI/CD for the dbt monorepo → Airflow (Cosmos)

## Why

Cosmos with no manifest falls back to `LoadMode.DBT_LS`: each DAG file runs
`dbt ls` on every parse. With ~20 DAGs at a 30s parse interval the DAG
processor runs out of CPU, misses heartbeats, and the scheduler treats it
as hung.

The fix is to build `manifest.json` (and `partial_parse.msgpack`) **once, in
CI**, ship it with the DAGs, and have Cosmos read it in
`LoadMode.DBT_MANIFEST`. DAG parsing then does no dbt work at all.

## Repo layout

```
projects/<name>/        one dbt project each (core, finance, marketing, product, operations)
  profiles.yml          env-var based, the same file for CI and prod
  package-lock.yml      committed, so `dbt deps` is reproducible
packages/shared_macros  a local dbt package used by every project
dags/
  dbt_<project>.py      2-line DAG files (5 files -> 20 DAGs, 5-10 models each)
  config/<project>.yml  which DAGs exist: schedule + select/exclude
  dbt_factory/          builds Cosmos DbtDags (explicit DBT_MANIFEST)
ci/
  build.py              assembles the deploy tree; re-parses only changed projects
  publish.sh            freshness check + compare-and-swap commit onto `deploy`
  deploy.sh             build → test → publish, retried when publish loses a race
  deploy_status.sh      is deploy an up-to-date build of master's tip?
  parse.env             env for `dbt parse` (non-secret values = prod)
  e2e_merge_strategies.sh  local end-to-end check of all merge strategies
tests/                  run against the built tree
strain/                 real Airflow 2.10 strain test (see below)
requirements/dbt.txt    the single dbt pin; its own venv in CI and on the image
requirements/airflow.txt  Airflow 2.10.5 + Cosmos 1.15.1
deploy/airflow/values.yaml  git-sync + [scheduler] parsing settings for 2.10
```

## Pipeline

```
PR / merge queue  (.github/workflows/ci.yml)
  build.py --previous origin/deploy  → build/deploy
  pytest:
    test_dag_integrity     DagBag import, no import errors, parse budget,
                           and *no subprocess at parse time* (catches any
                           DBT_LS fallback)
    test_selector_parity   for every DAG: Cosmos' manifest selection == `dbt ls`
    test_partial_parse     msgpack still valid after moving the tree + a different secret
    test_compile           every model's SQL renders (dbt compile, no warehouse)
  commit-hygiene           no [skip ci] markers in PR title/commits
  diff summary vs current deploy, artifact upload

push to master  (.github/workflows/deploy.yml)  + hourly reconcile + manual
  deploy_status → deploy.sh [build.py → pytest → publish.sh, retry on 75] → deploy_status
  → `deploy` branch ([skip ci])

Airflow: git-sync tracks `deploy`, subPath dags/
```

The deploy tree mirrors the repo, so relative paths such as
`local: ../../packages/shared_macros` still resolve:

```
.airflowignore                (projects/, packages/)
dags/…
packages/…
projects/<name>/…
  dbt_packages/               symlinks dereferenced (dbt deps symlinks local packages)
  target/manifest.json
  target/partial_parse.msgpack
  target/build-info.json      dbt version, commit the manifest was built at
```

## Design decisions

### 1. Re-parse only changed projects, diffing against what is *deployed*
Every deploy commit carries a `Source-Commit:` trailer: the master commit it
is a build of. `build.py` takes the diff from there:

```
git diff --name-only <Source-Commit of origin/deploy> <master tip>
```

* changed path under `projects/<name>/` → re-run `dbt deps` + `dbt parse` for that project
* changed local package it uses (`packages.yml` → `local:`) → re-parse that project
* changed `requirements/dbt.txt`, `ci/parse.env`, `ci/build.py`,
  or a different dbt version recorded in `build-info.json` → re-parse all
* new project → build it, deleted project → it is simply absent from the new tree
* anything else → copy the project's files from the current deploy commit,
  **byte for byte** (`git archive`)

The diff is taken against the deployed source and never against `HEAD~1`
or `github.event.before`. That is what makes it correct for any merge strategy:

| situation | `HEAD~1` diff | diff vs deployed source |
|---|---|---|
| squash: 1 commit | ok | ok |
| merge commit | ok (first parent) | ok |
| rebase: N commits in one push | **misses N-1 commits** | ok |
| previous deploy failed / skipped | **misses that push** | ok |
| master force-pushed | meaningless | ok (tree-to-tree diff needs no ancestry) |

If the deployed source commit is missing from the clone (shallow checkout,
or history rewritten and garbage-collected), everything is rebuilt. That is
why the workflows use `fetch-depth: 0`.

Re-parsing an unchanged project is avoided on purpose. A manifest is never
byte-identical between runs (`generated_at`, `invocation_id`), so re-parsing
everything would turn every deploy into a full diff for git-sync and Airflow.

### 2. How `deploy` stays consistent with master (publish.sh)

**`deploy` is not "master + a commit".** It is its own linear history of
snapshots. Each commit holds the complete deploy tree, its parent is the
previous deploy commit, and `Source-Commit:` points at master. The commit is
made with `write-tree`/`commit-tree` from the build output, so nothing is
ever *merged* or *rebased* onto anything. There are no content conflicts, and
the merge strategy on master can't produce any.

Two checks, both in `publish.sh`, keep it consistent:

1. **Freshness.** Publish only if the built source is still `origin/master`'s
   tip. Otherwise skip (exit 0): the newer commit has its own run queued, and
   the hourly reconcile is a backstop. A stalled or re-run old job therefore
   can't publish an old state, even if it wakes up long after.
2. **Compare-and-swap.** The new commit's parent is the deploy commit the
   build *read* (`build/deploy.meta.json`). The push is a fast-forward with
   `--force-with-lease=deploy:<that commit>`, so the server rejects it if
   deploy moved in the meantime. A rejected run exits 75, and `ci/deploy.sh`
   rebuilds against the new deploy tip and retries (up to 3 times). It is
   never overwritten and never merged.

Plus: an identical tree means no commit. So `Source-Commit` can lag master by
commits that change nothing deployable (README, tests, CI). `ci/deploy_status.sh`
defines "in sync" exactly: *no deployable path changed between Source-Commit
and master's tip*. The deploy workflow runs it before and after, and fails if
drift remains.

Rollback: `git revert` the deploy commit on `deploy` (it fast-forwards), or
pin git-sync's `rev` to an older deploy SHA. Every deploy commit stays
reachable because the branch is never rewritten.

#### Why not "deploy = master + 1 generated commit"?
That model is what causes the problems you'd expect:

* Each new deploy commit sits on a *new* master commit, not on the previous
  deploy, so pushing it is never a fast-forward. You must force-push every
  time.
* A blind force-push has no compare-and-swap. A stalled run that wakes up
  late overwrites a newer deploy, and deploy is now *behind* master with
  nothing to detect it.
* The old deploy commits become unreachable, so rollback-by-SHA breaks once GitHub GCs them.
* The variant "merge master into deploy, then add a commit" avoids the force,
  but it inherits master's merge-strategy quirks (rebased/force-pushed master
  → duplicated history and conflicts on generated paths).

If you need deploy to literally contain master's files, put them in the
snapshot tree. The model stays the same.

### 3. Partial parse that actually survives to runtime
Shipping `partial_parse.msgpack` only helps if dbt accepts it at task time.
dbt silently discards it, and does a **full parse in every task**, when any
of these differ from parse time (verified with dbt-core 1.11):

| must match | how |
|---|---|
| dbt-core / adapter version | `requirements/dbt.txt`, enforced by `build.py` (`check_pins`) |
| `--profile` / `--target` CLI flags | CI parses with `--profile <name> --target prod`, the same flags Cosmos passes |
| non-secret connection fields (host, user, dbname, schema, …) | `ci/parse.env` holds the **real prod values** |
| env vars read with `env_var()` | same: put prod values in `parse.env` |
| file contents incl. `dbt_packages/` | shipped together in one commit |

Secrets **don't** matter if they use the `DBT_ENV_SECRET_` prefix; a dummy
value in CI is fine. The project's absolute path doesn't matter either (git-sync
and Cosmos' tmp-dir copy both move it). `test_partial_parse.py` checks all of
this in CI.

So "dummy env vars are fine" holds for the manifest, not for the msgpack.
For the msgpack, only secrets may be dummy.

### 4. Explicit `LoadMode.DBT_MANIFEST`
`AUTOMATIC` falls back to `DBT_LS` when the manifest is missing, which brings
back the original problem without any warning. Explicit mode turns a missing
manifest into a DAG import error. `test_dag_integrity` also blocks
`subprocess` during import, so any parse-time dbt call fails CI.

### 5. Selector parity is tested, not assumed
In manifest mode Cosmos implements dbt's selector syntax itself, in Python,
and only part of it. `test_selector_parity` runs every DAG's select/exclude
through both `dbt ls` and Cosmos and requires the same models, seeds and snapshots.

With Cosmos 1.15/1.16 these match dbt: `tag:`, `path:`, bare paths, `+`/`n+`
graph operators, `@`, intersections (`a,b`), `config.materialized:`,
`resource_type:`, `package:`, and exclude with graph operators.
**`fqn:` with wildcards (`fqn:finance.marts.*`) does not.** dbt selects 3
models, Cosmos selects none, and nothing errors. Use `path:` instead. The test
would catch this.

## Merge strategy: what changes and what doesn't

With the two mechanisms above, **correctness doesn't depend on the merge
strategy**. `e2e_merge_strategies.sh` checks every row below against a local
origin.

| | squash | merge commit | rebase |
|---|---|---|---|
| commits landing on master per PR | 1 | merge commit + PR commits | N, new SHAs |
| deploy runs per PR | 1 | 1 | 1 per push (N if pushed one by one; all but the last skip at the freshness check) |
| which projects get re-parsed | diff vs deployed source: the same in all three | ← | ← |
| deploy commit per PR | ≤ 1 | ≤ 1 | ≤ 1 per push (intermediate commits are never deployed) |
| `Source-Commit` ↔ PR | 1:1 | 1:1 | 1:N (points at the last commit) |
| `[skip ci]` risk | **high**: squash message contains every PR commit's message | PR title only | last PR commit |
| revert a PR | 1 commit | `revert -m1` | N commits |

What remains strategy-dependent is traceability and the `[skip ci]` trap:

* **`[skip ci]`**: GitHub skips push workflows when the head commit message
  contains it. Under squash, one WIP commit saying `[skip ci]` would skip
  the deploy. Mitigated by the `commit-hygiene` PR check and the hourly
  reconcile. Because of the diff-vs-deployed rule, the reconcile picks up
  the skipped changes.
* **Rebase**: intermediate commits are never tested or deployed. Use the merge
  queue (`merge_group` in `ci.yml`) or "require branches up to date".

**Recommendation:** squash + merge queue. 1 PR = 1 master commit = ≤ 1 deploy
commit, a one-commit revert, and a 1:1 audit trail.

### Scenarios checked by `ci/e2e_merge_strategies.sh`

| # | scenario | outcome |
|---|---|---|
| 0 | first deploy | all built |
| 1 | squash merge of a finance change | only finance re-parsed; core/marketing bytes unchanged |
| 2 | rebase merge: shared-macro commit, then a marketing commit | `HEAD~1` shows only marketing; all 3 re-parsed |
| 3 | merge commit, README only | no deploy commit; status "in sync" |
| 4 | one push's deploy failed, next push lands | status "drift"; both pushes' projects re-parsed |
| 5 | stalled run built X, Y deployed meanwhile, X publishes late | skipped (freshness) |
| 6 | two runs build the same tip and race | loser rejected (exit 75), retries, no-op |
| 7 | add a project / delete a project | built / removed |
| 8 | master force-pushed | new tip deployed |

## Strain test: real Airflow 2.10 (`strain/run.py`)

The tests above load the DAG files in-process. `strain/run.py` checks the
whole path on a real Airflow instead:

* **Setup:** Postgres (metadata + "warehouse"), Airflow 2.10.5 + Cosmos
  1.15.1, standalone DAG processor, the `[scheduler]` settings from
  `deploy/airflow/values.yaml`, and dbt in its own venv.
* **Deploy path:** `deploy` is published by `ci/deploy.sh` to a local
  origin. `strain/gitsync.sh` follows it for the scheduler and DAG processor,
  with git-sync's atomic symlink-swap layout.
* **Task execution:** `--executor-model k8s` (default) models the
  KubernetesExecutor. Every task runs from its own fresh shallow clone of
  `deploy`, like a pod with the chart's one-time git-sync init container
  (`strain/k8s_pod/airflow`). `--executor-model local` runs tasks from the
  scheduler's git-sync checkout, as Celery/Local executors do.
* **Load:** 10 of the 20 DAGs are kept running back to back. Every model is
  a real `dbt run`, up to 16 at once.
* **The change:** while loaded, 5 DAGs change on master, five different ways:
  a model added, a model deleted, an exclude added in DAG config, a schedule
  changed, and a model added with a new dependency. Then it deploys.

Results on a 4-core machine (~5 minutes per run):

| check | k8s model, git-sync timeout 0 | local model, timeout 600s | local model, timeout 0 |
|---|---|---|---|
| peak strain (running tasks / load average) | 16 / 15 | 16 / 18 | 16 / ~18 |
| 20 DAGs serialized, 0 import errors, every sample | yes | yes | yes |
| max heartbeat age, scheduler / processor (limit 30s) | 5.8s / 4.9s | 6.0s / 4.9s | ok |
| parse time per DAG file under load (probe) | worst 1.3s | worst 2.3s | ok |
| 5 changes visible after publish | **9s** | 10s | 10s |
| other 15 DAGs unchanged | yes | yes | yes |
| all 20 re-parsed after deploy (interval 600s) | yes | yes | yes |
| task failures | **0** (146 ok, 142 pods on 2 revisions) | 0 (234 ok) | **10 failed, 26 skipped** |

### What it found (and what's fixed)
1. **Deploys don't wait for `min_file_process_interval`.** With
   `file_parsing_sort_mode=modified_time` (the default), Airflow 2.10
   re-parses any file whose mtime is newer than its last parse. git-sync
   writes every revision fresh, so a deploy is live within seconds even with
   a 600s interval. A long interval is therefore safe; it only stops pointless
   re-parsing between deploys.
2. **The KubernetesExecutor avoids the files-swapped-under-a-task problem.**
   Cosmos runs dbt from a temp dir of *symlinks* into the checkout a task
   started on. A long-lived git-sync sidecar (Celery/Local) deletes that
   checkout right after the next swap by default, and in-flight tasks fail
   (the last column). KubernetesExecutor pods clone once and never update,
   so this can't happen, even with timeout 0. For Celery/Local you'd need
   `GITSYNC_STALE_WORKTREE_TIMEOUT` above your longest task. git-sync has it
   from v4.0.0; the Airflow chart 1.15 ships v4.1.0, and v3 doesn't have it.
3. **Residual k8s race: parse/run skew.** A pod clones whatever `deploy` is
   when it *starts*, which can be newer than what the scheduler parsed. A task
   queued just before a deploy then runs the new SQL, which is usually what you
   want. If the deploy *removed* that task, the pod won't find it and the task
   fails. Not observed here (Airflow marked the removed tasks `removed` first),
   but the window exists in principle. `retries: 1` covers it.
4. **`dbt parse` doesn't validate SQL.** A package macro called without its
   prefix (`cents_to_dollars()` instead of `shared_macros.cents_to_dollars()`)
   parses fine, gives a valid manifest and valid DAGs, and fails every task.
   Hence `tests/test_compile.py`: `dbt compile --no-introspect
   --no-populate-cache` renders every model without a warehouse.
5. **Cosmos 1.16 requires Airflow ≥ 2.11.** On 2.10 use Cosmos 1.15.x.
   Selector parity is re-checked by CI on whatever version is pinned.
6. **Every task pod clones `deploy`**: 480 KB packed / 6.6 MB checked out here,
   0.2s locally. Keep `depth: 1` and keep the deploy tree to dags/, packages/
   and projects/. Cosmos's partial-parse cache starts empty in each pod, so
   every task uses the shipped, CI-verified msgpack.

## Operational settings (Airflow)
See `deploy/airflow/values.yaml` (Airflow 2.10 keys): `min_file_process_interval: 600`
with `file_parsing_sort_mode: modified_time`, `parsing_processes` ≤ cores − 1,
KubernetesExecutor, standalone DAG processor with a CPU request and no CPU limit,
git-sync on `deploy` with `subPath: dags`, `depth: 1` and a short
`GITSYNC_STALE_WORKTREE_TIMEOUT` for the sidecar,
`DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt`.

## Running locally

```bash
python -m venv /opt/dbt-venv && /opt/dbt-venv/bin/pip install -r requirements/dbt.txt
pip install -r requirements/airflow.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
export DBT_BIN=/opt/dbt-venv/bin/dbt DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt
python ci/build.py --out build/deploy --previous origin/deploy   # --no-reuse to rebuild all
DEPLOY_DIR=build/deploy pytest
bash ci/e2e_merge_strategies.sh                  # ~70s, no network needed
DBT_VENV=/opt/dbt-venv python strain/run.py      # ~5 min, needs Postgres binaries
```

## Branch protection
* `master`: require `ci / build-and-test` and `ci / commit-hygiene`; enable
  the merge queue; pick squash as the only allowed merge method (recommended).
* `deploy`: only `github-actions[bot]` may push; no force pushes; nobody
  commits by hand. It is build output.

## Future options
* **Kubernetes execution mode**: CI also builds a dbt image (projects +
  dbt_packages) tagged with the deploy commit and writes the tag into
  `build-info.json` next to the manifest, so DAGs, manifest and image always
  match. The Airflow image no longer needs dbt.
* **No per-model tasks needed?** Use one pod per selector (`dbt build
  --select tag:x`). Parse cost is close to zero and there is no manifest coupling.
