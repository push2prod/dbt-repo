"""Tests run against a *built* deploy tree (ci/build.py output), the same
bytes that get pushed to the deploy branch.

    python ci/build.py --out build/deploy
    DEPLOY_DIR=build/deploy pytest tests/
"""

from __future__ import annotations

import importlib.util
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from buildlib import DBT_BIN, load_env_file

REPO = Path(__file__).resolve().parent.parent
PROJECTS = sorted(p.parent.name for p in (REPO / "projects").glob("*/dbt_project.yml"))

DagSpecs = Callable[[str], list[dict[str, Any]]]


@pytest.fixture(scope="session")
def deploy_dir() -> Path:
    d = Path(os.environ.get("DEPLOY_DIR", REPO / "build" / "deploy")).resolve()
    if not (d / "dags").is_dir():
        pytest.exit(f"no deploy tree at {d}; run ci/build.py --out {d} first", returncode=2)
    return d


@pytest.fixture(scope="session")
def parse_env() -> dict[str, str]:
    return load_env_file(Path(os.environ.get("PARSE_ENV", REPO / "ci" / "parse.env")))


@pytest.fixture(scope="session")
def dbt_bin() -> str:
    """dbt from its own venv (DBT_BIN), as on the Airflow image."""
    return DBT_BIN


def projects(deploy: Path) -> list[str]:
    return sorted(p.parent.name for p in (deploy / "projects").glob("*/dbt_project.yml"))


def load_dag_specs(dags_dir: Path) -> DagSpecs:
    """Import dags/dbt_factory under a private name.

    That way the DagBag test still imports the real `dbt_factory` from the
    deploy tree, not this cached copy from the repo.
    """
    spec = importlib.util.spec_from_file_location("_dbt_factory_specs", dags_dir / "dbt_factory" / "__init__.py")
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load dbt_factory from {dags_dir}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    specs: DagSpecs = mod.dag_specs
    return specs
