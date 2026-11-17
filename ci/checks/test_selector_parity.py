"""Cosmos' Python selector (manifest mode) must pick the same nodes as dbt.

In DBT_LS mode dbt does the selection itself. In DBT_MANIFEST mode Cosmos
re-implements dbt's selector syntax in Python, and only part of it. This test
takes every Cosmos DAG the DAG files create, and compares the nodes it rendered
with what `dbt ls` selects for the same select/exclude, so a selector Cosmos
reads differently (or ignores) fails CI instead of quietly changing which
models a DAG runs.

`dbt ls` needs no warehouse connection; it runs once per DAG, in CI only.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from conftest import PROJECTS, dbt_dags, dbt_profile_args, project_path

if TYPE_CHECKING:
    from airflow.models.dagbag import DagBag

# Resource types that become Airflow tasks. Tests are attached by Cosmos'
# test_behavior and compared separately by dbt's indirect selection rules, so
# they are left out here.
TASK_TYPES = {"model", "seed", "snapshot"}


def dbt_ls(dbt_bin: str, project_dir: Path, select: list[str], exclude: list[str], env: dict[str, str]) -> set[str]:
    cmd = [dbt_bin, "ls", "--project-dir", str(project_dir), *dbt_profile_args(), "--quiet", "--no-use-colors"]
    cmd += ["--output", "json", "--output-keys", "unique_id", "resource_type"]
    if select:
        cmd += ["--select", *select]
    if exclude:
        cmd += ["--exclude", *exclude]
    r = subprocess.run(cmd, env={**os.environ, **env}, capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stdout + r.stderr
    rows = [json.loads(line) for line in r.stdout.splitlines() if line.startswith("{")]
    return {str(row["unique_id"]) for row in rows if row["resource_type"] in TASK_TYPES}


@pytest.mark.parametrize("project", PROJECTS)
def test_manifest_selection_matches_dbt_ls(
    dagbag: DagBag, dbt_tree: Path, parse_env: dict[str, str], dbt_bin: str, project: str
) -> None:
    dags = {d: dag for d, dag in dbt_dags(dagbag).items() if dag.dbt_graph.project.project_name == project}
    if not dags:
        pytest.skip(f"no DAG uses project {project!r}")
    errors = []
    for dag_id, dag in sorted(dags.items()):
        rc = dag.dbt_graph.render_config
        select, exclude = list(rc.select or []), list(rc.exclude or [])
        # dbt ls runs in the scratch copy so it cannot rewrite the deploy tree's target/ files.
        expected = dbt_ls(dbt_bin, project_path(dbt_tree, project), select, exclude, parse_env)
        actual = {str(u) for u, n in dag.dbt_graph.filtered_nodes.items() if n.resource_type.value in TASK_TYPES}
        if not expected:
            errors.append(f"{dag_id}: selector matches nothing in dbt (select={select}, exclude={exclude})")
        elif actual != expected:
            errors.append(
                f"{dag_id}: Cosmos manifest selection differs from dbt ls (select={select}, exclude={exclude})\n"
                f"  only in dbt ls: {sorted(expected - actual)}\n  only in Cosmos: {sorted(actual - expected)}"
            )
    assert not errors, "\n".join(errors)
