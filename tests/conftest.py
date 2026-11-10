"""Tests run against a *built* deploy tree (ci/build.py output), the same
bytes that get pushed to the deploy branch.

    python ci/build.py --out build/deploy
    DEPLOY_DIR=build/deploy pytest tests/
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def deploy_dir() -> Path:
    d = Path(os.environ.get("DEPLOY_DIR", REPO / "build" / "deploy")).resolve()
    if not (d / "dags").is_dir():
        pytest.exit(f"no deploy tree at {d}; run ci/build.py --out {d} first", returncode=2)
    return d


@pytest.fixture(scope="session")
def parse_env() -> dict[str, str]:
    sys.path.insert(0, str(REPO / "ci"))
    from build import load_env_file

    return load_env_file(REPO / "ci" / "parse.env")


def projects(deploy: Path) -> list[str]:
    return sorted(p.parent.name for p in (deploy / "projects").glob("*/dbt_project.yml"))


def load_dag_specs(dags_dir: Path):
    """Import dags/dbt_factory under a private name.

    That way the DagBag test still imports the real `dbt_factory` from the
    deploy tree, not this cached copy from the repo.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location("_dbt_factory_specs", dags_dir / "dbt_factory" / "__init__.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.dag_specs


PROJECTS = sorted(p.parent.name for p in (REPO / "projects").glob("*/dbt_project.yml"))
