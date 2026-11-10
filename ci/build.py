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
  is unknown, or a global input changed (dbt pins, parse env, build scripts),
  or when the toolchain recorded in build-info.json differs.
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

import yaml

REPO = Path(__file__).resolve().parent.parent
PROJECTS_DIR = "projects"
TARGET = "prod"  # must equal ProfileConfig.target_name in dags/dbt_factory
# Changes here invalidate every project's build output.
GLOBAL_INPUTS = ["requirements/dbt.txt", "ci/parse.env", "ci/build.py", "ci/slim_manifest.py"]
KEEP_TARGET_FILES = {"manifest.json", "partial_parse.msgpack"}


def log(msg: str) -> None:
    print(f"[build] {msg}", flush=True)


def git(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=REPO, check=check, capture_output=True)


def git_out(*args: str) -> str:
    return git(*args).stdout.decode().strip()


def tracked_files(path: Path) -> list[Path]:
    """Files git would ship for ``path`` (tracked plus untracked-but-not-ignored)."""
    out = git("ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", str(path.relative_to(REPO)))
    return sorted(f for f in (REPO / p for p in out.stdout.decode().split("\0") if p) if f.is_file())


def local_package_dirs(project_dir: Path, seen: set[Path] | None = None) -> set[Path]:
    """Local packages a project depends on, followed transitively."""
    seen = set() if seen is None else seen
    for fname in ("packages.yml", "dependencies.yml"):
        spec = project_dir / fname
        if spec.exists():
            for pkg in (yaml.safe_load(spec.read_text()) or {}).get("packages", []) or []:
                if "local" in pkg:
                    pkg_dir = (project_dir / pkg["local"]).resolve()
                    if pkg_dir not in seen:
                        seen.add(pkg_dir)
                        local_package_dirs(pkg_dir, seen)
    return seen


def source_commit_of(deploy_ref: str) -> str | None:
    msg = git("log", "-1", "--format=%(trailers:key=Source-Commit,valueonly)", deploy_ref, check=False)
    sha = msg.stdout.decode().strip().splitlines()
    return sha[0].strip() if sha else None


def changed_paths(since: str) -> list[str]:
    """Paths that differ between commit ``since`` and the working tree (= HEAD in CI)."""
    diff = git_out("diff", "--name-only", "--no-renames", since).splitlines()
    untracked = git_out("ls-files", "--others", "--exclude-standard").splitlines()
    return sorted(set(diff) | set(untracked))


def touches(paths: list[str], prefix: str) -> bool:
    prefix = prefix.rstrip("/") + "/"
    return any(p.startswith(prefix) or p == prefix[:-1] for p in paths)


def copy_tracked(src: Path, dst: Path) -> None:
    for f in tracked_files(src):
        target = dst / f.relative_to(src)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, target)


def load_env_file(path: Path) -> dict[str, str]:
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env


def dbt_version() -> str:
    from importlib.metadata import version

    return version("dbt-core")


def check_pins() -> None:
    """Refuse to build with a dbt that differs from requirements/dbt.txt."""
    from importlib.metadata import PackageNotFoundError, version

    bad = []
    for line in (REPO / "requirements/dbt.txt").read_text().splitlines():
        line = line.split("#")[0].strip()
        if "==" in line:
            name, want = (s.strip() for s in line.split("=="))
            try:
                have = version(name)
            except PackageNotFoundError:
                have = "<missing>"
            if have != want:
                bad.append(f"{name}: installed {have}, pinned {want}")
    if bad:
        sys.exit("[build] installed dbt toolchain does not match requirements/dbt.txt:\n  " + "\n  ".join(bad))


def previous_build(base: str | None, project_rel: str) -> tuple[dict | None, bytes | None]:
    """(build-info, tar of the project's tree) from the base deploy commit."""
    if not base:
        return None, None
    info = git("show", f"{base}:{project_rel}/target/build-info.json", check=False)
    tar = git("archive", "--format=tar", base, project_rel, check=False)
    if info.returncode or tar.returncode:
        return None, None
    return json.loads(info.stdout), tar.stdout


def dereference_symlinks(root: Path) -> None:
    """`dbt deps` symlinks local packages; a symlink to a CI path dangles in Airflow."""
    for p in sorted(root.rglob("*"), key=lambda x: len(x.parts)):
        if p.is_symlink():
            real = p.resolve()
            p.unlink()
            shutil.copytree(real, p, symlinks=False) if real.is_dir() else shutil.copy2(real, p)
    for git_dir in root.rglob(".git"):  # git-type packages: never nest a repo
        shutil.rmtree(git_dir) if git_dir.is_dir() else git_dir.unlink()


def build_project(name: str, out: Path, env: dict[str, str], slim: bool) -> None:
    proj = out / PROJECTS_DIR / name
    common = ["--project-dir", str(proj), "--profiles-dir", str(proj), "--no-use-colors"]
    run_env = {**os.environ, **env, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    # Same --profile/--target flags Cosmos passes to task-level dbt commands
    # (cosmos/operators/local.py). dbt stores these flags in the partial-parse
    # state, so any difference makes the shipped msgpack useless.
    for cmd in (["deps"], ["parse", "--profile", name, "--target", TARGET]):
        log(f"{name}: dbt {' '.join(cmd)}")
        r = subprocess.run(["dbt", *cmd, *common], env=run_env, capture_output=True, text=True)
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
    if slim:
        from slim_manifest import slim_file

        before, after = slim_file(target / "manifest.json")
        log(f"{name}: slimmed manifest {before // 1024} KiB -> {after // 1024} KiB")


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


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--previous", help="git ref of the current deploy (e.g. origin/deploy)")
    ap.add_argument("--no-reuse", action="store_true", help="rebuild every project")
    ap.add_argument(
        "--slim",
        action=argparse.BooleanOptionalAction,
        default=os.environ.get("SLIM_MANIFEST", "0") == "1",
        help="strip fields Cosmos does not read from manifest.json (env SLIM_MANIFEST=1)",
    )
    args = ap.parse_args()
    sys.path.insert(0, str(Path(__file__).parent))

    check_pins()
    out = args.out.resolve()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    env = load_env_file(REPO / "ci/parse.env")
    source = git_out("rev-parse", "HEAD")

    base = None
    if args.previous:
        r = git("rev-parse", "-q", "--verify", f"{args.previous}^{{commit}}", check=False)
        base = r.stdout.decode().strip() or None
    rebuild_all, paths = plan(base, args.no_reuse)
    if rebuild_all:
        log(f"rebuilding all projects: {rebuild_all}")

    copy_tracked(REPO / "dags", out / "dags")
    copy_tracked(REPO / "packages", out / "packages")
    (out / ".airflowignore").write_text("# only dags/ holds DAG files\nprojects/\npackages/\n")

    toolchain = {"dbt_core": dbt_version(), "slim_manifest": args.slim}
    summary: dict[str, str] = {}
    for project_yml in sorted((REPO / PROJECTS_DIR).glob("*/dbt_project.yml")):
        src = project_yml.parent
        name, rel = src.name, src.relative_to(REPO).as_posix()
        deps = [d.relative_to(REPO).as_posix() for d in sorted(local_package_dirs(src))]

        reason = rebuild_all
        prev_info, prev_tar = (None, None) if rebuild_all else previous_build(base, rel)
        if not reason and prev_info is None:
            reason = "not in previous deploy"
        elif not reason and {k: prev_info.get(k) for k in toolchain} != toolchain:
            reason = "toolchain changed"
        elif not reason and touches(paths, rel):
            reason = "project changed"
        elif not reason and (hit := [d for d in deps if touches(paths, d)]):
            reason = f"package changed: {', '.join(hit)}"

        if reason is None:
            with tarfile.open(fileobj=io.BytesIO(prev_tar)) as tf:
                tf.extractall(out, filter="tar")
            summary[name] = f"reused (built at {prev_info.get('built_at_commit', '?')[:12]})"
        else:
            copy_tracked(src, out / rel)
            build_project(name, out, env, args.slim)
            info = {"project": name, **toolchain, "built_at_commit": source, "local_packages": deps}
            (out / rel / "target/build-info.json").write_text(json.dumps(info, indent=2, sort_keys=True) + "\n")
            summary[name] = f"built ({reason})"
        log(f"{name}: {summary[name]}")

    meta = {"source_commit": source, "base_deploy": base, "projects": summary}
    out.with_name(out.name + ".meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    if gh := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(gh, "a") as fh:
            fh.write("| project | result |\n|---|---|\n" + "".join(f"| {k} | {v} |\n" for k, v in summary.items()))


if __name__ == "__main__":
    main()
