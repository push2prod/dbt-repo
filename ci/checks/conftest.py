"""Validation checks. They run against a *built* bundle, the same bytes CI
pushes to the deploy branch.

    python -m manifest.generate --out build/bundle
    BUNDLE_DIR=build/bundle pytest
"""

from __future__ import annotations

import importlib.util
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from manifest.dbtvenv import DBT_BIN
from manifest.files import load_env_file

REPO = Path(__file__).resolve().parents[2]
# The bundle under test. Read at import time too: parametrized tests need the
# project / DAG list during collection, before fixtures run.
BUNDLE = Path(os.environ.get("BUNDLE_DIR", REPO / "build" / "bundle")).resolve()
PROJECTS = sorted(p.parent.name for p in (BUNDLE / "projects").glob("*/dbt_project.yml"))

DagSpecs = Callable[[str], list[dict[str, Any]]]


@pytest.fixture(scope="session")
def bundle_dir() -> Path:
    if not (BUNDLE / "dags").is_dir():
        pytest.exit(f"no bundle at {BUNDLE}; run python -m manifest.generate --out {BUNDLE} first", returncode=2)
    return BUNDLE


@pytest.fixture(scope="session")
def parse_env() -> dict[str, str]:
    return load_env_file(Path(os.environ.get("PARSE_ENV", REPO / "manifest" / "parse.env")))


@pytest.fixture(scope="session")
def dbt_bin() -> str:
    """dbt from its own venv (DBT_BIN), as on the Airflow image."""
    return DBT_BIN


def projects(deploy: Path) -> list[str]:
    return sorted(p.parent.name for p in (deploy / "projects").glob("*/dbt_project.yml"))


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
