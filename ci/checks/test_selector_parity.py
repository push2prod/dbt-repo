"""Cosmos' Python selector (manifest mode) must pick the same nodes as dbt.

In DBT_LS mode dbt does the selection itself. In DBT_MANIFEST mode Cosmos
re-implements dbt's selector syntax in Python, and only part of it. This test
runs every select/exclude from dags/config through both and compares them, so
a selector Cosmos reads differently (or ignores) fails CI instead of quietly
changing which models a DAG runs.

`dbt ls` needs no warehouse connection; it runs once per DAG, in CI only.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from _pytest.mark.structures import ParameterSet
from conftest import DAGS_DIR, PROJECTS, dbt_profile_args, load_dag_specs, project_path

# Resource types that become Airflow tasks. Tests are attached by Cosmos'
# test_behavior and compared separately by dbt's indirect selection rules, so
# they are left out here.
TASK_TYPES = {"model", "seed", "snapshot"}


@dataclass(frozen=True)
class Selection:
    project: str
    select: list[str]
    exclude: list[str]


def _specs() -> Iterator[ParameterSet]:
    dag_specs = load_dag_specs(DAGS_DIR)
    for project in PROJECTS:
        for spec in dag_specs(project):
            yield pytest.param(project, spec, id=spec["dag_id"])


def dbt_ls(dbt_bin: str, project_dir: Path, sel: Selection, env: dict[str, str]) -> set[str]:
    cmd = [dbt_bin, "ls", "--project-dir", str(project_dir), *dbt_profile_args(), "--quiet", "--no-use-colors"]
    cmd += ["--output", "json", "--output-keys", "unique_id", "resource_type"]
    if sel.select:
        cmd += ["--select", *sel.select]
    if sel.exclude:
        cmd += ["--exclude", *sel.exclude]
    r = subprocess.run(cmd, env={**os.environ, **env}, capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stdout + r.stderr
    rows = [json.loads(line) for line in r.stdout.splitlines() if line.startswith("{")]
    return {str(row["unique_id"]) for row in rows if row["resource_type"] in TASK_TYPES}


def cosmos_select(project_dir: Path, sel: Selection) -> set[str]:
    from cosmos.config import ExecutionConfig, ProjectConfig, RenderConfig
    from cosmos.constants import LoadMode
    from cosmos.dbt.graph import DbtGraph

    graph = DbtGraph(
        project=ProjectConfig(dbt_project_path=project_dir, manifest_path=project_dir / "target/manifest.json"),
        render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST, select=sel.select, exclude=sel.exclude),
        execution_config=ExecutionConfig(dbt_project_path=project_dir),
    )
    graph.load_from_dbt_manifest()
    return {str(uid) for uid, n in graph.filtered_nodes.items() if n.resource_type.value in TASK_TYPES}


@pytest.mark.parametrize(("project", "spec"), list(_specs()))
def test_manifest_selection_matches_dbt_ls(
    bundle_dir: Path, parse_env: dict[str, str], dbt_bin: str, tmp_path: Path, project: str, spec: dict[str, Any]
) -> None:
    # Run dbt ls on a copy so it cannot rewrite the shipped target/ files.
    work = tmp_path / "deploy"
    shutil.copytree(bundle_dir, work, symlinks=False)
    sel = Selection(project, list(spec.get("select", [])), list(spec.get("exclude", [])))

    expected = dbt_ls(dbt_bin, project_path(work, project), sel, parse_env)
    actual = cosmos_select(project_path(bundle_dir, project), sel)

    assert expected, f"{spec['dag_id']}: selector matches nothing in dbt"
    assert actual == expected, (
        f"{spec['dag_id']}: Cosmos manifest selection differs from dbt ls\n"
        f"  only in dbt ls: {sorted(expected - actual)}\n  only in Cosmos: {sorted(actual - expected)}"
    )
