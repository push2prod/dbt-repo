"""Validation checks. They run against the *deploy tree*: the source commit
with the generated overlay on top, the same files CI commits to `deploy`.

    python -m infra.manifest.generate --out build/overlay
    infra/scripts/assemble_deploy_tree.sh HEAD build/overlay build/deploy-tree
    BUNDLE_DIR=build/deploy-tree SUMMARY_FILE=build/overlay.summary.json pytest -c infra/checks/pytest.ini
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

import pytest

from infra.manifest.dbtvenv import DBT_BIN
from infra.manifest.files import load_env_file

if TYPE_CHECKING:
    from airflow.models.dagbag import DagBag

REPO = Path(__file__).resolve().parents[2]
# The deploy tree under test. Read at import time too: parametrized tests need
# the project / DAG list during collection, before fixtures run.
BUNDLE = Path(os.environ.get("BUNDLE_DIR", REPO / "build" / "deploy-tree")).resolve()
SUMMARY_FILE = Path(os.environ.get("SUMMARY_FILE", REPO / "build" / "overlay.summary.json")).resolve()


def _load(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text()) if path.is_file() else {}
    return data


# Written by infra.manifest.generate next to the bundle: where the registry is, and the CI profile.
SUMMARY = _load(SUMMARY_FILE)
# projects.json in the deploy tree: project paths, profile, target. Empty when there's none.
REGISTRY = _load(BUNDLE / SUMMARY["registry"]) if "registry" in SUMMARY else {}
PROJECTS = sorted(REGISTRY.get("projects", {}))
DAGS_DIR = BUNDLE / REGISTRY.get("dags_dir", "dags")


@pytest.fixture(scope="session")
def bundle_dir() -> Path:
    if not REGISTRY:
        pytest.exit(f"no deploy tree at {BUNDLE} (or no {SUMMARY_FILE}); see infra/checks/conftest.py", returncode=2)
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


@pytest.fixture(scope="session")
def dagbag(bundle_dir: Path) -> Iterator[DagBag]:
    """The deploy tree's DAG dir parsed once, as the DAG processor would.

    While the DAG files are parsed, starting any process fails the parse: Cosmos
    in DBT_LS mode (or AUTOMATIC with a missing manifest) runs `dbt ls` there.
    """
    mp = pytest.MonkeyPatch()
    mp.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    mp.setenv("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    # Import Airflow + Cosmos *before* the guard goes up: their own import-time
    # probes (e.g. platform.architecture() shelling out to `file`) are not the
    # DAG files' fault. After this, only the DAG files' own work runs.
    import cosmos  # noqa: F401
    from airflow.models.dagbag import DagBag

    def forbidden(*args: object, **kwargs: object) -> NoReturn:
        raise AssertionError(f"subprocess started at DAG parse time: {args[:1]}")

    with pytest.MonkeyPatch.context() as guard:
        for name in ("Popen", "run", "call", "check_call", "check_output"):
            guard.setattr(subprocess, name, forbidden)
        # As in Airflow: the DAG folder, and the checkout root (PYTHONPATH in
        # infra/airflow/values.yaml) so `utils.dbt_factory` is the deploy tree's copy.
        for name in [m for m in sys.modules if m == "utils" or m.startswith("utils.")]:
            del sys.modules[name]
        sys.path[:0] = [str(DAGS_DIR), str(BUNDLE)]
        bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False, safe_mode=True)
    yield bag
    mp.undo()


def dbt_dags(bag: DagBag) -> dict[str, Any]:
    """{dag_id: DbtDag} for every Cosmos DAG in ``bag``."""
    from cosmos import DbtDag

    return {dag_id: dag for dag_id, dag in bag.dags.items() if isinstance(dag, DbtDag)}
