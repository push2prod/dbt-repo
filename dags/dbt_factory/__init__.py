"""Build Cosmos DAGs from dags/config/<project>.yml.

Parse-time cost: one small YAML read plus one manifest.json read per project.
No dbt subprocess runs at parse time.

* ``LoadMode.DBT_MANIFEST`` is set explicitly. With ``AUTOMATIC``, a
  missing manifest silently falls back to ``DBT_LS``: a `dbt ls` subprocess
  on every parse, which is the problem this repo exists to remove. Explicit
  mode makes a missing manifest show up as a DAG import error.
* Paths are relative to this file. git-sync exposes the deploy branch
  through an atomically swapped symlink, so DAG code, project and manifest
  always come from the same deploy commit.
"""

from __future__ import annotations

import functools
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from cosmos import DbtDag, ExecutionConfig, LoadMode, ProfileConfig, ProjectConfig, RenderConfig

DAGS_DIR = Path(__file__).resolve().parent.parent
DEPLOY_ROOT = DAGS_DIR.parent
CONFIG_DIR = DAGS_DIR / "config"
PROJECTS_DIR = DEPLOY_ROOT / "projects"

DEFAULTS: dict[str, Any] = {
    "start_date": "2024-01-01",
    "catchup": False,
    "max_active_runs": 1,
    "tags": ["dbt"],
}


@functools.cache
def load_config(project: str) -> dict[str, Any]:
    return yaml.safe_load((CONFIG_DIR / f"{project}.yml").read_text())


def dag_specs(project: str) -> list[dict[str, Any]]:
    """The DAG specs for a project with defaults applied. Also used by the CI parity test."""
    cfg = load_config(project)
    return [{**DEFAULTS, **cfg.get("defaults", {}), **spec} for spec in cfg["dags"]]


def build_dags(project: str, namespace: dict[str, Any]) -> None:
    project_dir = PROJECTS_DIR / project
    project_config = ProjectConfig(
        dbt_project_path=project_dir,
        manifest_path=project_dir / "target" / "manifest.json",
        project_name=project,
        # dbt_packages/ ships in the deploy tree, built by CI with the pinned dbt.
        install_dbt_deps=False,
        partial_parse=True,
    )
    profile_config = ProfileConfig(
        profile_name=project,
        target_name="prod",
        profiles_yml_filepath=project_dir / "profiles.yml",
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
            project_config=project_config,
            profile_config=profile_config,
            execution_config=ExecutionConfig(),
            render_config=RenderConfig(
                load_method=LoadMode.DBT_MANIFEST,
                select=spec.get("select", []),
                exclude=spec.get("exclude", []),
            ),
        )
