"""Validation checks. They run against a *built* bundle, the same bytes CI
pushes to the deploy branch.

    python -m manifest.generate --source . --out build/bundle
    BUNDLE_DIR=build/bundle pytest
"""

from __future__ import annotations

import importlib.util
import json
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
SUMMARY_FILE = BUNDLE.with_name(BUNDLE.name + ".summary.json")


def _load(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text()) if path.is_file() else {}
    return data


# Written by manifest.generate next to the bundle: where the registry is, and the CI profile.
SUMMARY = _load(SUMMARY_FILE)
# projects.json inside the bundle: project paths, profile, target. Empty when there's no bundle.
REGISTRY = _load(BUNDLE / SUMMARY["registry"]) if "registry" in SUMMARY else {}
PROJECTS = sorted(REGISTRY.get("projects", {}))
DAGS_DIR = BUNDLE / REGISTRY.get("dags_dir", "dags")

DagSpecs = Callable[[str], list[dict[str, Any]]]


@pytest.fixture(scope="session")
def bundle_dir() -> Path:
    if not REGISTRY:
        pytest.exit(f"no bundle at {BUNDLE}; run python -m manifest.generate --out {BUNDLE} first", returncode=2)
    return BUNDLE


@pytest.fixture(scope="session")
def parse_env() -> dict[str, str]:
    path = SUMMARY.get("parse_env")
    return load_env_file(Path(path)) if path else {}


@pytest.fixture(scope="session")
def dbt_bin() -> str:
    """dbt from its own venv (DBT_BIN), as on the Airflow image."""
    return DBT_BIN


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
