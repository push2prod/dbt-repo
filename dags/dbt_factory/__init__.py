"""Build Cosmos DAGs for the dbt projects in the registry (projects.json, written by manifest.generate).

DAG files are written by hand and call ``dbt_dag`` once per DAG:

    # Airflow DAG file: Cosmos DAGs for the "marketing" dbt project.
    from dbt_factory import dbt_dag

    marketing_campaigns = dbt_dag(
        "marketing",                       # the project's dbt `name:`
        dag_id="dbt_marketing_campaigns",
        schedule="0 3 * * *",
        select=["tag:campaigns"],
    )

Assign the result to a module-level name: Airflow only picks up DAGs it finds
in the file's globals. Keep "airflow" somewhere in the file (the comment above
does it): Airflow's safe mode skips files without both "airflow" and "dag". CI
fails on either mistake (ci/checks/test_dag_integrity.py). Any other ``DbtDag``/``DAG`` argument (description,
callbacks, ``operator_args``, ``default_args`` ...) is passed through.

The deploy branch is the source tree plus the generated files, each in place:
<project>/target/manifest.json and this directory's projects.json.

Parse-time cost: one small JSON read, then one manifest.json read per DAG.
No dbt subprocess runs at parse time.

* ``LoadMode.DBT_MANIFEST`` is set explicitly. With ``AUTOMATIC``, a
  missing manifest silently falls back to ``DBT_LS``: a `dbt ls` subprocess
  on every parse, which is the problem this repo exists to remove. Explicit
  mode makes a missing manifest show up as a DAG import error.
* Every path comes from projects.json, relative to the repo root, so the
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
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from cosmos import DbtDag, ExecutionConfig, LoadMode, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode

FACTORY_DIR = Path(__file__).resolve().parent
DBT_EXECUTABLE = os.environ.get("DBT_EXECUTABLE", "/opt/dbt-venv/bin/dbt")
# Every dag_id dbt_dag() created in this process. CI compares it with what Airflow
# collected: a DAG not assigned to a module-level name is silently dropped.
CREATED: list[str] = []


@functools.cache
def registry() -> dict[str, Any]:
    reg: dict[str, Any] = json.loads((FACTORY_DIR / "projects.json").read_text())
    return reg


def bundle_root() -> Path:
    """The repo root of the deploy checkout: dags_dir in the registry is relative to it."""
    return FACTORY_DIR.parents[len(Path(registry()["dags_dir"]).parts)]


def project_dir(project: str) -> Path:
    projects: dict[str, Any] = registry()["projects"]
    if project not in projects:
        raise KeyError(f"dbt project {project!r} is not in projects.json (known: {sorted(projects)})")
    return bundle_root() / projects[project]["path"]


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


def project_config(project: str) -> ProjectConfig:
    path = project_dir(project)
    return ProjectConfig(
        dbt_project_path=path,
        manifest_path=path / "target" / "manifest.json",
        project_name=project,
        # `dbt_packages: ship`: dbt_packages/ is in the deploy commit, built by CI.
        # `install`: Cosmos runs `dbt deps` in each task's temp dir.
        install_dbt_deps=registry()["dbt_packages"] != "ship",
        # CI does not ship partial_parse.msgpack (its profile is a dummy, not the runtime mapping).
        partial_parse=False,
    )


def dbt_dag(
    project: str,
    dag_id: str,
    *,
    select: Sequence[str] = (),
    exclude: Sequence[str] = (),
    schedule: Any = None,
    **dag_kwargs: Any,
) -> DbtDag:
    """One Cosmos DAG for ``project`` (its dbt `name:`), running the nodes ``select``/``exclude`` pick.

    Defaults (dbt_bundle.yml `dags.defaults`): start_date, catchup,
    max_active_runs, tags, retries, retry_delay_seconds. Pass any of them, or any
    other DbtDag/DAG argument, to override. ``default_args`` is merged with the
    retry defaults; ``tags`` gets ``dbt:<project>`` appended.
    """
    defaults = dict(registry()["dag_defaults"])
    start_date = dag_kwargs.pop("start_date", defaults.pop("start_date"))
    if not isinstance(start_date, datetime):
        start_date = datetime.fromisoformat(str(start_date))
    retries = dag_kwargs.pop("retries", defaults.pop("retries"))
    retry_delay = timedelta(seconds=dag_kwargs.pop("retry_delay_seconds", defaults.pop("retry_delay_seconds")))
    tags = [*dag_kwargs.pop("tags", defaults.pop("tags")), f"dbt:{project}"]
    default_args = {"retries": retries, "retry_delay": retry_delay, **dag_kwargs.pop("default_args", {})}
    for key, value in defaults.items():  # catchup, max_active_runs
        dag_kwargs.setdefault(key, value)

    CREATED.append(dag_id)
    return DbtDag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        tags=tags,
        default_args=default_args,
        project_config=project_config(project),
        profile_config=profile_config(),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=list(select),
            exclude=list(exclude),
        ),
        **dag_kwargs,
    )
