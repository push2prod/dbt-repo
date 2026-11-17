"""Validation checks. They run against the *deploy tree*: the source commit
with the generated overlay on top, the same files CI commits to `deploy`.

    python -m manifest.generate --out build/overlay
    mkdir build/deploy-tree && git archive HEAD | tar -x -C build/deploy-tree
    cp -a build/overlay/. build/deploy-tree/
    BUNDLE_DIR=build/deploy-tree SUMMARY_FILE=build/overlay.summary.json pytest
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from manifest.dbtvenv import DBT_BIN
from manifest.files import load_env_file

REPO = Path(__file__).resolve().parents[2]
# The deploy tree under test. Read at import time too: parametrized tests need
# the project / DAG list during collection, before fixtures run.
BUNDLE = Path(os.environ.get("BUNDLE_DIR", REPO / "build" / "deploy-tree")).resolve()
SUMMARY_FILE = Path(os.environ.get("SUMMARY_FILE", REPO / "build" / "overlay.summary.json")).resolve()


def _load(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text()) if path.is_file() else {}
    return data


# Written by manifest.generate next to the bundle: where the registry is, and the CI profile.
SUMMARY = _load(SUMMARY_FILE)
# projects.json in the deploy tree: project paths, profile, target. Empty when there's none.
REGISTRY = _load(BUNDLE / SUMMARY["registry"]) if "registry" in SUMMARY else {}
PROJECTS = sorted(REGISTRY.get("projects", {}))
DAGS_DIR = BUNDLE / REGISTRY.get("dags_dir", "dags")

DagSpecs = Callable[[str], list[dict[str, Any]]]


@pytest.fixture(scope="session")
def bundle_dir() -> Path:
    if not REGISTRY:
        pytest.exit(f"no deploy tree at {BUNDLE} (or no {SUMMARY_FILE}); see ci/checks/conftest.py", returncode=2)
    return BUNDLE


@pytest.fixture(scope="session")
def parse_env() -> dict[str, str]:
    path = SUMMARY.get("parse_env")
    return load_env_file(Path(path)) if path else {}


@pytest.fixture(scope="session")
def dbt_bin() -> str:
    """dbt from its own venv (DBT_BIN), as on the Airflow image."""
    return DBT_BIN


@pytest.fixture(scope="session")
def dbt_tree(
    bundle_dir: Path, parse_env: dict[str, str], dbt_bin: str, tmp_path_factory: pytest.TempPathFactory
) -> Path:
    """A scratch copy of the deploy tree that dbt can write to, with packages installed.

    With `dbt_packages: install` the deploy tree has no dbt_packages/ (Cosmos runs
    `dbt deps` per task), so run it here once per project, as the tasks will.
    """
    work = tmp_path_factory.mktemp("dbt") / "tree"
    shutil.copytree(bundle_dir, work, symlinks=False, ignore=shutil.ignore_patterns(".git"))
    if REGISTRY.get("dbt_packages") != "ship":
        for project in PROJECTS:
            cmd = [dbt_bin, "deps", "--project-dir", str(project_path(work, project)), *dbt_profile_args()]
            r = subprocess.run(cmd, env={**os.environ, **parse_env}, capture_output=True, text=True, check=False)
            assert r.returncode == 0, f"dbt deps failed for {project}:\n{r.stdout}{r.stderr}"
    return work


def project_path(root: Path, project: str) -> Path:
    """Where ``project`` lives under ``root`` (the bundle, or a copy of it)."""
    return root / REGISTRY["projects"][project]["path"]


def dbt_profile_args() -> list[str]:
    """The profile flags CI parsed with: the dummy CI profile, the runtime profile name and target."""
    profile = REGISTRY["profile"]
    return ["--profiles-dir", SUMMARY["ci_profiles_dir"], "--profile", profile["name"], "--target", profile["target"]]


def load_dag_specs(dags_dir: Path) -> DagSpecs:
    """Import dags/dbt_factory under a private name.

    That way the DagBag test still imports the real `dbt_factory` from the
    bundle, not this cached copy from the repo.
    """
    spec = importlib.util.spec_from_file_location("_dbt_factory_specs", dags_dir / "dbt_factory" / "__init__.py")
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load dbt_factory from {dags_dir}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    specs: DagSpecs = mod.dag_specs
    return specs
