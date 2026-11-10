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
projects/<name>/        one dbt project each (core, finance, marketing)
  profiles.yml          env-var based, the same file for CI and prod
  package-lock.yml      committed, so `dbt deps` is reproducible
packages/shared_macros  a local dbt package used by every project
dags/
  dbt_<project>.py      2-line DAG files
  config/<project>.yml  which DAGs exist: schedule + select/exclude
  dbt_factory/          builds Cosmos DbtDags (explicit DBT_MANIFEST)
ci/
  build.py              assembles the deploy tree (deps + parse per project)
  slim_manifest.py      optional manifest shrinking (SLIM_MANIFEST=1)
  publish.sh            commits the tree onto the `deploy` branch
  parse.env             env for `dbt parse` (non-secret values = prod)
  e2e_merge_strategies.sh  local end-to-end check of all merge strategies
tests/                  run against the built tree
requirements/dbt.txt    the single dbt pin, shared by CI and runtime
deploy/airflow/values.yaml  git-sync + DAG processor settings
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
  commit-hygiene           no [skip ci] markers in PR title/commits
  diff summary vs current deploy, artifact upload

push to master  (.github/workflows/deploy.yml)  + hourly reconcile + manual
  build.py → pytest → publish.sh → `deploy` branch ([skip ci])

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
  target/build-info.json      input hash, dbt version
```

## Design decisions

### 1. Content-addressed builds, not diff-based ones
`build.py` gives each project an **input hash**: the bytes of its tracked
files, its local packages (followed transitively), `requirements/dbt.txt`,
`ci/parse.env` and the build scripts. If `origin/deploy` already contains a
build with the same hash, that output is reused byte for byte. Otherwise the
project is rebuilt.

That gives three properties:

* **The merge strategy doesn't matter** (see below). Nothing reads
  `HEAD~1`, `github.event.before`, or the commit count of a push.
* **No churn.** `manifest.json` has `generated_at`/`invocation_id`, so a
  rebuild always changes bytes. Reuse keeps unchanged projects identical, and
  a no-op merge (README, CI-only change) publishes nothing.
* **Shared code is handled.** Changing `packages/shared_macros` rebuilds
  every project that depends on it, even though nothing under `projects/x`
  changed. A `paths:` filter or a `git diff --name-only` would miss this.

Bump `BUILD_FORMAT` in `build.py`, or run the deploy workflow with
`full_rebuild`, to force a rebuild of everything.

### 2. Publish with plumbing, linear, no force
`publish.sh` writes the tree into a temporary index (`add -f`, because
projects' `.gitignore` excludes `target/` and `dbt_packages/`), then runs
`commit-tree` on top of the current deploy tip and does a plain push.
Each deploy commit carries `Source-Commit:` and `Source-Tree:` trailers.

* identical tree → no commit
* the current deploy's source already contains ours (an old run re-run) → refuse
* master was force-pushed (old source isn't an ancestor) → deploy the new tip
* someone else pushed `deploy` meanwhile → push fails, and the next run rebuilds on top

Rollback: `git revert` the deploy commit on `deploy`, or pin git-sync's
`rev` to an older deploy SHA.

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

With Cosmos 1.16 these match dbt: `tag:`, `path:`, bare paths, `+`/`n+`
graph operators, `@`, intersections (`a,b`), `config.materialized:`,
`resource_type:`, `package:`, and exclude with graph operators.
**`fqn:` with wildcards (`fqn:finance.marts.*`) does not.** dbt selects 3
models, Cosmos selects none, and nothing errors. Use `path:` instead. The test
would catch this.

## Merge strategy: what changes and what doesn't

| | squash | merge commit | rebase |
|---|---|---|---|
| commits landing on master per PR | 1 | merge commit (+ PR commits as 2nd parent) | N, new SHAs |
| deploy runs per PR | 1 | 1 | 1 (one push), but N if commits are pushed one by one |
| what gets deployed | squash commit | merge commit | tip only. Intermediate commits are never built |
| `git diff HEAD~1` correct? | yes | yes (first parent) | **no**, only the last commit |
| PR-branch SHA == deployed SHA? | no | no | no |
| head-commit message GitHub checks for `[skip ci]` | concatenated PR commit messages (default) | "Merge pull request …" + PR title | the PR's last commit |
| revert a PR | 1 commit | `revert -m1` the merge | N commits |
| `Source-Commit` ↔ PR | 1:1 | 1:1 | 1:N |

What the design does with each row:

* **Change detection**: content hashes, so the `HEAD~1` row doesn't apply.
  `e2e_merge_strategies.sh` scenario 2 reproduces the rebase trap: a
  shared-macro commit followed by a marketing commit. `HEAD~1` shows only
  marketing, yet all three projects are rebuilt.
* **No SHA equals the PR's SHA**, so "build in the PR, promote the same
  artifact by SHA on merge" doesn't work with *any* strategy. Content
  hashing gives the same result in practice: a PR build and the post-merge
  build have identical input hashes when nothing else landed in between.
  `Source-Tree` (the tree hash) is the stable identifier if you need one.
* **`[skip ci]` gotcha**: the most dangerous one under squash. One WIP
  commit saying `[skip ci]` ends up in the squash message and **silently
  skips the deploy**. Defended three ways: the `commit-hygiene` PR check, the
  hourly reconcile run, and `deploy`'s own `[skip ci]` (which only matters for
  the deploy branch, since workflows only listen on master).
* **Untested intermediate states (rebase)**: never deployed, because only
  the tip is built. Still, use a merge queue (`merge_group` is in `ci.yml`)
  or "require branches to be up to date", so the tested tree is the
  merged tree.
* **Bursts of merges**: `concurrency: deploy` with `cancel-in-progress:
  false`. A running publish is never killed, and pending runs collapse to the
  newest.
* **Force-push / history rewrite** (e.g. a rebase-heavy flow that rewrites
  master): `publish.sh` sees that the previous source is no longer an ancestor
  and deploys the new tip (scenario 5).

**Recommendation:** squash merges + merge queue. You get 1 PR = 1 master
commit = 1 deploy commit, a one-commit revert, and the clearest audit trail. The
pipeline stays correct with merge or rebase too. Those mainly cost traceability.

## Operational settings (Airflow)
See `deploy/airflow/values.yaml`: `min_file_process_interval: 600`,
`parsing_processes` ≤ cores − 1, a CPU request and no CPU limit for the DAG
processor, git-sync on `deploy` with `subPath: dags`.

## Running locally

```bash
pip install -r requirements/dbt.txt -r requirements/airflow.txt
python ci/build.py --out build/deploy            # add --slim to try slimming
DEPLOY_DIR=build/deploy pytest
bash ci/e2e_merge_strategies.sh                  # ~45s, no network needed
```

## Branch protection
* `master`: require `ci / build-and-test` and `ci / commit-hygiene`; enable
  the merge queue; pick squash as the only allowed merge method (recommended).
* `deploy`: only `github-actions[bot]` may push; no force pushes; nobody
  commits by hand. It is build output.

## Future options
* **Kubernetes execution mode**: CI also builds a dbt image (projects +
  dbt_packages) tagged with the input hash and writes the tag into
  `build-info.json` next to the manifest, so DAGs, manifest and image always
  match. The Airflow image no longer needs dbt.
* **No per-model tasks needed?** Use one pod per selector (`dbt build
  --select tag:x`). Parse cost is close to zero and there is no manifest coupling.
* **Large projects**: `SLIM_MANIFEST=1` (keep-list matched to Cosmos 1.16,
  verified by the parity test), and `orjson` in the Airflow image.
