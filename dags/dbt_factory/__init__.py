"""Build Cosmos DAGs from the bundle registry (projects.json, written by manifest.generate).

Parse-time cost: one small JSON read, then one YAML read plus one
manifest.json read per project. No dbt subprocess runs at parse time.

* ``LoadMode.DBT_MANIFEST`` is set explicitly. With ``AUTOMATIC``, a
  missing manifest silently falls back to ``DBT_LS``: a `dbt ls` subprocess
  on every parse, which is the problem this repo exists to remove. Explicit
  mode makes a missing manifest show up as a DAG import error.
* Every path comes from projects.json, relative to the bundle root, so the
  factory assumes no repo layout. ``resolve()`` follows git-sync's symlink to
  the concrete revision directory. One parse therefore reads DAG code, project
  and manifest from the same deploy commit, even if git-sync swaps the symlink
  mid-parse.
* The dbt profile comes from an Airflow connection (Cosmos ``profile_mapping``),
  with the profile name and target dbt parsed with in CI.
* dbt runs from its own venv (``DBT_EXECUTABLE``, default /opt/dbt-venv/bin/dbt),
  so it is invoked as a subprocess at *task* time. dbt isn't importable from
  Airflow's environment.
"""

from __future__ import annotations

import functools
import importlib
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from cosmos import DbtDag, ExecutionConfig, LoadMode, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode

FACTORY_DIR = Path(__file__).resolve().parent
DBT_EXECUTABLE = os.environ.get("DBT_EXECUTABLE", "/opt/dbt-venv/bin/dbt")


@functools.cache
def registry() -> dict[str, Any]:
    reg: dict[str, Any] = json.loads((FACTORY_DIR / "projects.json").read_text())
    return reg


def bundle_root() -> Path:
    """The bundle root: dags_dir in the registry is relative to it."""
    return FACTORY_DIR.parents[len(Path(registry()["dags_dir"]).parts)]


def _project(project: str) -> dict[str, Any]:
    projects: dict[str, Any] = registry()["projects"]
    if project not in projects:
        raise KeyError(f"dbt project {project!r} is not in projects.json (known: {sorted(projects)})")
    entry: dict[str, Any] = projects[project]
    return entry


@functools.cache
def load_config(project: str) -> dict[str, Any]:
    rel = _project(project)["dag_config"]
    cfg = yaml.safe_load((bundle_root() / rel).read_text())
    if not isinstance(cfg, dict) or not isinstance(cfg.get("dags"), list):
        raise ValueError(f"{rel} must be a mapping with a `dags` list")
    return cfg


def dag_specs(project: str) -> list[dict[str, Any]]:
    """The DAG specs for a project with defaults applied. Also used by the CI parity test."""
    cfg = load_config(project)
    return [{**registry()["dag_defaults"], **cfg.get("defaults", {}), **spec} for spec in cfg["dags"]]


def profile_config() -> ProfileConfig:
    """The shared profile: Cosmos renders it from the Airflow connection at task time."""
    profile = registry()["profile"]
    mapping = profile["mapping"]
    module, _, cls_name = mapping["class"].rpartition(".")
    cls = getattr(importlib.import_module(module), cls_name)
    return ProfileConfig(
        profile_name=profile["name"],
        target_name=profile["target"],
        profile_mapping=cls(conn_id=mapping["conn_id"], profile_args=dict(mapping.get("profile_args") or {})),
    )


def build_dags(project: str, namespace: dict[str, Any]) -> None:
    project_dir = bundle_root() / _project(project)["path"]
    project_config = ProjectConfig(
        dbt_project_path=project_dir,
        manifest_path=project_dir / "target" / "manifest.json",
        project_name=project,
        # dbt_packages/ ships in the deploy tree, built by CI with the pinned dbt.
        install_dbt_deps=False,
        # CI does not ship partial_parse.msgpack (its profile is a dummy, not the runtime mapping).
        partial_parse=False,
    )
    for spec in dag_specs(project):
        dag_id = spec["dag_id"]
        namespace[dag_id] = DbtDag(
            dag_id=dag_id,
            schedule=spec.get("schedule"),
            start_date=datetime.fromisoformat(str(spec["start_date"])),
            catchup=spec["catchup"],
            max_active_runs=spec["max_active_runs"],
            tags=[*spec["tags"], f"dbt:{project}"],
            default_args={"retries": spec["retries"], "retry_delay": timedelta(seconds=spec["retry_delay_seconds"])},
            project_config=project_config,
            profile_config=profile_config(),
            execution_config=ExecutionConfig(
                dbt_executable_path=DBT_EXECUTABLE,
                invocation_mode=InvocationMode.SUBPROCESS,
            ),
            render_config=RenderConfig(
                load_method=LoadMode.DBT_MANIFEST,
                select=spec.get("select", []),
                exclude=spec.get("exclude", []),
            ),
        )
