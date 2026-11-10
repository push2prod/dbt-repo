#!/usr/bin/env python3
"""Assemble the Airflow deploy tree from a master checkout.

Output layout (mirrors the repo, so relative paths like
``local: ../../packages/shared_macros`` still resolve at runtime):

    <out>/
      .airflowignore
      dags/                            copied as-is
      packages/                        local dbt packages, copied as-is
      projects/<name>/                 project sources (tracked files only)
        dbt_packages/                  `dbt deps` output, symlinks dereferenced
        target/manifest.json           read by Cosmos at DAG parse time
        target/partial_parse.msgpack   lets task-level dbt runs skip a full parse
        target/build-info.json         input hash + toolchain, used for reuse

Change detection uses content, not git history. Each project gets an
*input hash* built from the bytes of every file that can affect its
manifest: the project, its local packages (followed transitively), the pinned
dbt requirements, the parse env and this build tooling. If the previous deploy
already has a project built from the same input hash, that output is reused
byte for byte. That means:

* the result does not depend on the merge strategy (squash / merge / rebase),
  how many commits a push contained, or force-pushes;
* unchanged projects produce no diff on the deploy branch, so git-sync and
  Airflow see no churn;
* a change to a shared package rebuilds every project that uses it, even
  though no file under ``projects/<name>`` changed.
"""

from __future__ import annotations

import argparse
import hashlib
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
BUILD_FORMAT = 1  # bump to force a rebuild of every project
# Files outside a project that affect every project's build output.
GLOBAL_INPUTS = ["requirements/dbt.txt", "ci/parse.env", "ci/build.py", "ci/slim_manifest.py"]
TARGET = "prod"  # must equal ProfileConfig.target_name in dags/dbt_factory
KEEP_TARGET_FILES = {"manifest.json", "partial_parse.msgpack"}


def log(msg: str) -> None:
    print(f"[build] {msg}", flush=True)


def git(*args: str, cwd: Path = REPO, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, check=check, capture_output=True)


def tracked_files(path: Path) -> list[Path]:
    """Files git would ship for ``path``: tracked plus untracked-but-not-ignored.

    Including untracked files makes local runs match CI. In CI the checkout is
    clean, so this is exactly the committed tree.
    """
    out = git("ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", str(path.relative_to(REPO)))
    files = [REPO / p for p in out.stdout.decode().split("\0") if p]
    return sorted(f for f in files if f.is_file())  # skip deleted-but-staged


def local_package_dirs(project_dir: Path, seen: set[Path] | None = None) -> set[Path]:
    """Local packages a project depends on, followed transitively."""
    seen = set() if seen is None else seen
    for fname in ("packages.yml", "dependencies.yml"):
        spec = project_dir / fname
        if not spec.exists():
            continue
        for pkg in (yaml.safe_load(spec.read_text()) or {}).get("packages", []) or []:
            if "local" in pkg:
                pkg_dir = (project_dir / pkg["local"]).resolve()
                if pkg_dir not in seen:
                    seen.add(pkg_dir)
                    local_package_dirs(pkg_dir, seen)
    return seen


def input_hash(project_dir: Path) -> tuple[str, list[str]]:
    h = hashlib.sha256(f"build-format:{BUILD_FORMAT}\n".encode())
    roots = [project_dir, *sorted(local_package_dirs(project_dir))]
    files = [f for root in roots for f in tracked_files(root)] + [REPO / g for g in GLOBAL_INPUTS]
    for f in files:
        rel = f.relative_to(REPO).as_posix()
        h.update(rel.encode() + b"\0" + hashlib.sha256(f.read_bytes()).hexdigest().encode() + b"\n")
    return h.hexdigest(), [r.relative_to(REPO).as_posix() for r in roots]


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
        if "==" not in line:
            continue
        name, want = (s.strip() for s in line.split("=="))
        try:
            have = version(name)
        except PackageNotFoundError:
            have = "<missing>"
        if have != want:
            bad.append(f"{name}: installed {have}, pinned {want}")
    if bad:
        sys.exit("[build] installed dbt toolchain does not match requirements/dbt.txt:\n  " + "\n  ".join(bad))


def previous_build(prev_ref: str | None, project_rel: str) -> tuple[dict | None, bytes | None]:
    """Return (build-info, tar of the project's tree) from the previous deploy ref."""
    if not prev_ref:
        return None, None
    info = git("show", f"{prev_ref}:{project_rel}/target/build-info.json", check=False)
    if info.returncode != 0:
        return None, None
    tar = git("archive", "--format=tar", prev_ref, project_rel, check=False)
    if tar.returncode != 0:
        return None, None
    return json.loads(info.stdout), tar.stdout


def dereference_symlinks(root: Path) -> None:
    """Replace symlinks with real copies.

    `dbt deps` symlinks local packages into dbt_packages/. A symlink to a CI
    runner path is dangling in Airflow, so ship real files.
    """
    for p in sorted(root.rglob("*"), key=lambda x: len(x.parts)):
        if p.is_symlink():
            real = p.resolve()
            p.unlink()
            if real.is_dir():
                shutil.copytree(real, p, symlinks=False)
            else:
                shutil.copy2(real, p)
    for git_dir in root.rglob(".git"):  # git-type packages: never nest a repo
        shutil.rmtree(git_dir) if git_dir.is_dir() else git_dir.unlink()


def build_project(name: str, out: Path, env: dict[str, str], slim: bool) -> None:
    proj = out / "projects" / name
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


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--previous", help="git ref of the previous deploy (e.g. origin/deploy) to reuse unchanged builds")
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
    prev_ref = None if args.no_reuse else args.previous
    if prev_ref and git("rev-parse", "-q", "--verify", f"{prev_ref}^{{commit}}", check=False).returncode != 0:
        log(f"previous deploy ref {prev_ref!r} not found; building everything")
        prev_ref = None

    copy_tracked(REPO / "dags", out / "dags")
    copy_tracked(REPO / "packages", out / "packages")
    (out / ".airflowignore").write_text("# only dags/ holds DAG files\nprojects/\npackages/\n")

    summary = {}
    for project_yml in sorted((REPO / "projects").glob("*/dbt_project.yml")):
        src = project_yml.parent
        name, rel = src.name, src.relative_to(REPO).as_posix()
        digest, inputs = input_hash(src)
        info = {
            "project": name,
            "input_hash": digest,
            "inputs": inputs,
            "dbt_core": dbt_version(),
            "slim_manifest": args.slim,
        }
        prev_info, prev_tar = previous_build(prev_ref, rel)
        if prev_info == info:
            with tarfile.open(fileobj=io.BytesIO(prev_tar)) as tf:
                tf.extractall(out, filter="tar")
            log(f"{name}: unchanged ({digest[:12]}), reusing previous build")
            summary[name] = "reused"
            continue
        copy_tracked(src, out / rel)
        build_project(name, out, env, args.slim)
        (out / rel / "target/build-info.json").write_text(json.dumps(info, indent=2, sort_keys=True) + "\n")
        summary[name] = "built"
    log("summary: " + ", ".join(f"{k}={v}" for k, v in summary.items()))
    if gh_out := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(gh_out, "a") as fh:
            fh.write("| project | result |\n|---|---|\n" + "".join(f"| {k} | {v} |\n" for k, v in summary.items()))


if __name__ == "__main__":
    main()
