#!/usr/bin/env python3
"""Assemble the Airflow deploy tree from a master checkout.

Output layout (mirrors the repo, so relative paths like
``local: ../../packages/shared_macros`` still resolve at runtime):

    <out>/
      .airflowignore
      dags/                            copied from the source commit
      packages/                        local dbt packages, copied
      projects/<name>/                 project sources (tracked files only)
        dbt_packages/                  `dbt deps` output, symlinks dereferenced
        target/manifest.json           read by Cosmos at DAG parse time
        target/partial_parse.msgpack   lets task-level dbt runs skip a full parse
        target/build-info.json         dbt version + commit the manifest was built at
    <out>.meta.json                    base deploy commit + source commit, read by publish.sh

Which projects get a new manifest
---------------------------------
Only *changed* projects are re-parsed. The rest keep the exact bytes from the
current deploy. "Changed" is measured against **what is deployed**, not
against the previous commit:

    git diff --name-only <Source-Commit of origin/deploy> <HEAD>

* Squash, merge commit, rebase, 1 or N commits per push: the diff covers all
  of it. `HEAD~1` / `github.event.before` would miss commits under rebase
  merges and after a skipped or failed deploy.
* A tree-to-tree diff needs no ancestry, so it also works after a force-push
  to master.
* A project is changed if a file under ``projects/<name>/`` changed, or one
  of the local packages it uses (``packages.yml`` ``local:``).
* Everything is rebuilt when there is no previous deploy, its source commit
  is unknown, or a global input changed (dbt pins, parse env, build script),
  or when the dbt version recorded in build-info.json differs.
* Deleted projects disappear from the deploy tree; new ones are built.

Unchanged projects are never re-parsed. A rebuilt manifest is never
byte-identical (``generated_at``, ``invocation_id``), so re-parsing
everything would turn every deploy into a full diff.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import Any

from buildlib import (
    DBT_BIN,
    REPO,
    changed_paths,
    check_pins,
    copy_tracked,
    dbt_version,
    dereference_symlinks,
    git,
    git_out,
    load_env_file,
    local_package_dirs,
    log,
    source_commit_of,
    touches,
)

PROJECTS_DIR = "projects"
TARGET = "prod"  # must equal ProfileConfig.target_name in dags/dbt_factory
# Changes here invalidate every project's build output.
GLOBAL_INPUTS = ["requirements/dbt.txt", "ci/parse.env", "ci/build.py", "ci/buildlib.py"]
KEEP_TARGET_FILES = {"manifest.json", "partial_parse.msgpack"}


def previous_build(base: str | None, project_rel: str) -> tuple[dict[str, Any] | None, bytes | None]:
    """(build-info, tar of the project's tree) from the base deploy commit."""
    if not base:
        return None, None
    info = git("show", f"{base}:{project_rel}/target/build-info.json", check=False)
    tar = git("archive", "--format=tar", base, project_rel, check=False)
    if info.returncode or tar.returncode:
        return None, None
    return json.loads(info.stdout), tar.stdout


def build_project(name: str, out: Path, env: dict[str, str]) -> None:
    proj = out / PROJECTS_DIR / name
    common = ["--project-dir", str(proj), "--profiles-dir", str(proj), "--no-use-colors"]
    run_env = {**os.environ, **env, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    # Same --profile/--target flags Cosmos passes to task-level dbt commands
    # (cosmos/operators/local.py). dbt stores these flags in the partial-parse
    # state, so any difference makes the shipped msgpack useless.
    for cmd in (["deps"], ["parse", "--profile", name, "--target", TARGET]):
        log(f"{name}: dbt {' '.join(cmd)}")
        r = subprocess.run([DBT_BIN, *cmd, *common], env=run_env, capture_output=True, text=True, check=False)
        if r.returncode != 0:
            sys.exit(f"[build] {name}: dbt {cmd[0]} failed\n{r.stdout}\n{r.stderr}")
    dereference_symlinks(proj / "dbt_packages")
    shutil.rmtree(proj / "logs", ignore_errors=True)
    target = proj / "target"
    for p in target.iterdir():
        if p.name not in KEEP_TARGET_FILES:
            shutil.rmtree(p) if p.is_dir() else p.unlink()
    missing = KEEP_TARGET_FILES - {p.name for p in target.iterdir()}
    if missing:
        sys.exit(f"[build] {name}: dbt parse did not produce {sorted(missing)}")


def plan(base: str | None, no_reuse: bool) -> tuple[str | None, list[str] | None]:
    """Return (reason to rebuild everything or None, changed paths since the deployed source)."""
    if no_reuse:
        return "--no-reuse", None
    if not base:
        return "no previous deploy", None
    prev_source = source_commit_of(base)
    if not prev_source:
        return f"{base} has no Source-Commit trailer", None
    if git("cat-file", "-e", f"{prev_source}^{{commit}}", check=False).returncode:
        return f"deployed source {prev_source[:12]} not in this clone (shallow? history rewritten and GC'd?)", None
    paths = changed_paths(prev_source)
    log(f"diff vs deployed source {prev_source[:12]}: {len(paths)} path(s) changed")
    hit = [g for g in GLOBAL_INPUTS if g in paths]
    if hit:
        return f"global input changed: {', '.join(hit)}", paths
    return None, paths


def project_reason(
    rel: str, deps: list[str], prev_info: dict[str, Any] | None, toolchain: dict[str, str], paths: list[str] | None
) -> str | None:
    """Why this project must be re-parsed, or None if its previous build can be reused."""
    if prev_info is None:
        return "not in previous deploy"
    if {k: prev_info.get(k) for k in toolchain} != toolchain:
        return "toolchain changed"
    if paths is not None and touches(paths, rel):
        return "project changed"
    hit = [d for d in deps if paths is not None and touches(paths, d)]
    return f"package changed: {', '.join(hit)}" if hit else None


def build_or_reuse(src: Path, out: Path, ctx: dict[str, Any]) -> str:
    """Re-parse one project into ``out`` or copy its previous build; return a summary line."""
    name, rel = src.name, src.relative_to(REPO).as_posix()
    deps = [d.relative_to(REPO).as_posix() for d in sorted(local_package_dirs(src))]
    rebuild_all = ctx["rebuild_all"]
    prev_info, prev_tar = (None, None) if rebuild_all else previous_build(ctx["base"], rel)
    reason = rebuild_all or project_reason(rel, deps, prev_info, ctx["toolchain"], ctx["paths"])
    if reason is None and prev_info is not None and prev_tar is not None:
        with tarfile.open(fileobj=io.BytesIO(prev_tar)) as tf:
            tf.extractall(out, filter="tar")
        return f"reused (built at {str(prev_info.get('built_at_commit', '?'))[:12]})"
    copy_tracked(src, out / rel)
    build_project(name, out, ctx["env"])
    info = {"project": name, **ctx["toolchain"], "built_at_commit": ctx["source"], "local_packages": deps}
    (out / rel / "target/build-info.json").write_text(json.dumps(info, indent=2, sort_keys=True) + "\n")
    return f"built ({reason})"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--previous", help="git ref of the current deploy (e.g. origin/deploy)")
    ap.add_argument("--no-reuse", action="store_true", help="rebuild every project")
    return ap.parse_args()


def resolve_ref(ref: str | None) -> str | None:
    if not ref:
        return None
    r = git("rev-parse", "-q", "--verify", f"{ref}^{{commit}}", check=False)
    return r.stdout.decode().strip() or None


def write_outputs(out: Path, meta: dict[str, Any], summary: dict[str, str]) -> None:
    out.with_name(out.name + ".meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    if gh := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(gh, "a") as fh:
            fh.write("| project | result |\n|---|---|\n" + "".join(f"| {k} | {v} |\n" for k, v in summary.items()))


def main() -> None:
    args = parse_args()
    check_pins()
    out = args.out.resolve()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    base = resolve_ref(args.previous)
    rebuild_all, paths = plan(base, args.no_reuse)
    if rebuild_all:
        log(f"rebuilding all projects: {rebuild_all}")
    ctx: dict[str, Any] = {
        "base": base,
        "rebuild_all": rebuild_all,
        "paths": paths,
        "toolchain": {"dbt_core": dbt_version()},
        "source": git_out("rev-parse", "HEAD"),
        # PARSE_ENV lets a test environment (strain/) bake its own warehouse
        # address into the parse state. Production always uses ci/parse.env.
        "env": load_env_file(Path(os.environ.get("PARSE_ENV", REPO / "ci/parse.env"))),
    }
    copy_tracked(REPO / "dags", out / "dags")
    copy_tracked(REPO / "packages", out / "packages")
    (out / ".airflowignore").write_text("# only dags/ holds DAG files\nprojects/\npackages/\n")
    summary: dict[str, str] = {}
    for project_yml in sorted((REPO / PROJECTS_DIR).glob("*/dbt_project.yml")):
        summary[project_yml.parent.name] = build_or_reuse(project_yml.parent, out, ctx)
        log(f"{project_yml.parent.name}: {summary[project_yml.parent.name]}")
    write_outputs(out, {"source_commit": ctx["source"], "base_deploy": base, "projects": summary}, summary)


if __name__ == "__main__":
    main()
