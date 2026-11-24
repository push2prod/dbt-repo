"""Walking, copying and hashing the inputs of a project. No git involved."""

from __future__ import annotations

import hashlib
import shutil
from collections.abc import Iterable, Iterator
from pathlib import Path

import yaml

# Build output and caches. Never an input, never copied from the source tree.
SKIP_DIRS = {"target", "dbt_packages", "logs", "__pycache__", ".git"}
SKIP_SUFFIXES = {".pyc"}


def source_files(root: Path) -> Iterator[Path]:
    """Every source file under ``root``, sorted, without build output or caches."""
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root)
        if any(part in SKIP_DIRS for part in rel.parts) or path.suffix in SKIP_SUFFIXES:
            continue
        if path.is_file():
            yield path


def copy_sources(src: Path, dst: Path) -> None:
    for f in source_files(src):
        target = dst / f.relative_to(src)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, target)


def local_package_dirs(project_dir: Path, seen: set[Path] | None = None) -> set[Path]:
    """Local dbt packages a project depends on (packages.yml ``local:``), followed transitively."""
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


def input_hash(repo: Path, roots: Iterable[Path], extra: dict[str, str]) -> str:
    """sha256 over the relative path + content of every source file under ``roots``, plus ``extra``."""
    h = hashlib.sha256()
    for key in sorted(extra):
        h.update(f"{key}={extra[key]}\n".encode())
    for root in roots:
        for f in source_files(root):
            rel = f.resolve().relative_to(repo.resolve()).as_posix()
            h.update(rel.encode() + b"\0" + hashlib.sha256(f.read_bytes()).hexdigest().encode() + b"\n")
    return h.hexdigest()


def dereference_symlinks(root: Path) -> None:
    """`dbt deps` symlinks local packages; a link to a CI path would dangle in Airflow."""
    for p in sorted(root.rglob("*"), key=lambda x: len(x.parts)):
        if p.is_symlink():
            real = p.resolve()
            p.unlink()
            if real.is_dir():
                shutil.copytree(real, p, symlinks=False, ignore=shutil.ignore_patterns(*SKIP_DIRS))
            else:
                shutil.copy2(real, p)
    for git_dir in root.rglob(".git"):  # git-type packages: never nest a repo
        if git_dir.is_dir():
            shutil.rmtree(git_dir)
        else:
            git_dir.unlink()


def load_env_file(path: Path) -> dict[str, str]:
    env = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env
