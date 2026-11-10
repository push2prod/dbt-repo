"""Generic helpers for ci/build.py: git, file copying, env files, the dbt venv.

Changes to this file invalidate every project's build (it is in GLOBAL_INPUTS).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
# dbt lives in its own venv, as on the Airflow image (/opt/dbt-venv). Point
# DBT_BIN at that venv's dbt; its python is used to check the pins.
DBT_BIN = os.environ.get("DBT_BIN") or shutil.which("dbt") or "dbt"


def log(msg: str) -> None:
    print(f"[build] {msg}", flush=True)


def git(*args: str, check: bool = True) -> subprocess.CompletedProcess[bytes]:
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
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env


def dbt_python() -> str:
    return str(Path(DBT_BIN).resolve().parent / "python")


def installed_versions() -> dict[str, str]:
    code = (
        "import json, importlib.metadata as m; "
        "print(json.dumps({d.metadata['Name'].lower(): d.version for d in m.distributions()}))"
    )
    out = subprocess.run([dbt_python(), "-c", code], check=True, capture_output=True, text=True).stdout
    versions: dict[str, str] = json.loads(out)
    return versions


def dbt_version() -> str:
    return installed_versions()["dbt-core"]


def check_pins() -> None:
    """Refuse to build with a dbt (venv) that differs from requirements/dbt.txt."""
    have_all = installed_versions()
    bad = []
    for raw in (REPO / "requirements/dbt.txt").read_text().splitlines():
        line = raw.split("#")[0].strip()
        if "==" in line:
            name, want = (s.strip() for s in line.split("=="))
            have = have_all.get(name.lower(), "<missing>")
            if have != want:
                bad.append(f"{name}: installed {have}, pinned {want}")
    if bad:
        sys.exit("[build] installed dbt toolchain does not match requirements/dbt.txt:\n  " + "\n  ".join(bad))


def dereference_symlinks(root: Path) -> None:
    """`dbt deps` symlinks local packages; a symlink to a CI path dangles in Airflow."""
    for p in sorted(root.rglob("*"), key=lambda x: len(x.parts)):
        if p.is_symlink():
            real = p.resolve()
            p.unlink()
            shutil.copytree(real, p, symlinks=False) if real.is_dir() else shutil.copy2(real, p)
    for git_dir in root.rglob(".git"):  # git-type packages: never nest a repo
        shutil.rmtree(git_dir) if git_dir.is_dir() else git_dir.unlink()
