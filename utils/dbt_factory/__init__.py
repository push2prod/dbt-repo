"""Cosmos task groups for the dbt projects in the registry (projects.json, written by infra.manifest.generate).

DAG files are written by hand, as normal Airflow DAGs, and add dbt where they
need it with ``dbt_task_group`` (a Cosmos ``DbtTaskGroup``), between any other tasks:

    # Airflow DAG file: the marketing dbt models, between two other tasks.
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from utils.dbt_factory import dbt_task_group

    with DAG("marketing_daily", schedule="0 3 * * *", start_date=datetime(2024, 1, 1), catchup=False):
        start = EmptyOperator(task_id="start")
        campaigns = dbt_task_group(
            "marketing",                       # the project's dbt `name:`
            group_id="campaigns",             # unique within the DAG; prefixes its task ids
            select=["tag:campaigns"],
        )
        start >> campaigns >> EmptyOperator(task_id="end")

A DAG can hold several groups, from the same or different projects. The DAG
and its settings (schedule, start_date, catchup, tags, ``default_args`` such as
retries) are the DAG file's business: the group's tasks inherit the DAG's
``default_args`` like any other task. Keep "airflow"
somewhere in the file (the comment above does it): Airflow's safe mode skips
files without both "airflow" and "dag". CI fails on a group that doesn't end up
in a collected DAG (infra/checks/test_dag_integrity.py). Any other
``DbtTaskGroup``/``TaskGroup`` argument (``operator_args``, ``default_args``,
``tooltip`` ...) is passed through.

This package lives outside the DAG folder (utils/dbt_factory, dbt_bundle
`dags.factory_dir`), so DAG files import it as ``utils.dbt_factory``: Airflow has
the git-sync checkout root on PYTHONPATH (infra/airflow/values.yaml), next to the
DAG folder Airflow adds itself.

The deploy branch is the source tree plus the generated files, each in place:
<project>/target/manifest.json and this directory's projects.json.

Parse-time cost: one small JSON read, then one manifest.json read per task group.
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
from pathlib import Path
from typing import Any

from airflow.models.dag import DagContext
from cosmos import DbtTaskGroup, ExecutionConfig, LoadMode, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode

FACTORY_DIR = Path(__file__).resolve().parent
DBT_EXECUTABLE = os.environ.get("DBT_EXECUTABLE", "/opt/dbt-venv/bin/dbt")
# (dag_id, group_id) of every group dbt_task_group() created in this process. CI
# checks each one is in a DAG Airflow collected: a DAG not assigned to a
# module-level name (or not created by `with DAG(...)` / a called @dag) is dropped.
CREATED: list[tuple[str, str]] = []


@functools.cache
def registry() -> dict[str, Any]:
    reg: dict[str, Any] = json.loads((FACTORY_DIR / "projects.json").read_text())
    return reg


def bundle_root() -> Path:
    """The repo root of the deploy checkout: every path in the registry is relative to it."""
    return FACTORY_DIR.parents[len(Path(registry()["factory_dir"]).parts) - 1]


def project_dir(project: str) -> Path:
    projects: dict[str, Any] = registry()["projects"]
    if project not in projects:
        raise KeyError(f"dbt project {project!r} is not in projects.json (known: {sorted(projects)})")
    return bundle_root() / projects[project]["path"]


def profile_config(overrides: dict[str, Any] | None = None) -> ProfileConfig:
    """The shared profile: Cosmos renders it from the Airflow connection at task time.

    ``overrides`` replace keys of `profile.mapping.profile_args` (e.g. database,
    schema) for this ProfileConfig only; each call builds its own mapping.
    """
    profile = registry()["profile"]
    mapping = profile["mapping"]
    module, _, cls_name = mapping["class"].rpartition(".")
    cls = getattr(importlib.import_module(module), cls_name)
    profile_args = {**(mapping.get("profile_args") or {}), **(overrides or {})}  # copies; registry untouched
    return ProfileConfig(
        profile_name=profile["name"],
        target_name=profile["target"],
        profile_mapping=cls(conn_id=mapping["conn_id"], profile_args=profile_args),
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


def dbt_task_group(
    project: str,
    group_id: str,
    *,
    select: Sequence[str] = (),
    exclude: Sequence[str] = (),
    database: str | None = None,
    schema: str | None = None,
    profile_args: dict[str, Any] | None = None,
    **group_kwargs: Any,
) -> DbtTaskGroup:
    """A Cosmos task group running the nodes of ``project`` (its dbt `name:`) that ``select``/``exclude`` pick.

    Call it inside a DAG (``with DAG(...)``, an ``@dag`` function, or pass
    ``dag=``). ``group_id`` must be unique within the DAG and prefixes the
    group's task ids (``<group_id>.<model>_run``).

    ``database`` (the Trino catalog) and ``schema`` override the profile's
    values from infra/dbt_bundle.yml `profile.mapping.profile_args` for this
    group only; ``profile_args`` overrides any other profile field the same way
    (``database`` / ``schema`` win over it). Tasks re-parse the project at run
    time with that profile, so models land in the overridden catalog/schema.

    The group's tasks inherit the DAG's ``default_args`` (retries, owner ...).
    Every other argument (``default_args`` for this group only, ``operator_args``,
    ``tooltip``, ``prefix_group_id`` ...) goes to ``DbtTaskGroup`` as-is.
    """
    dag = group_kwargs.get("dag") or DagContext.get_current_dag()
    if dag is None:
        raise RuntimeError(
            f"dbt_task_group({project!r}, group_id={group_id!r}) must be called inside a DAG "
            "(`with DAG(...):`, an @dag function) or be given dag="
        )
    group = DbtTaskGroup(
        group_id=group_id,
        project_config=project_config(project),
        profile_config=profile_config(
            {
                **(profile_args or {}),
                **({"database": database} if database is not None else {}),
                **({"schema": schema} if schema is not None else {}),
            }
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=list(select),
            exclude=list(exclude),
        ),
        **group_kwargs,
    )
    CREATED.append((dag.dag_id, group.group_id))
    return group
