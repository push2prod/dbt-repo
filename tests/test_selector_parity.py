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
import subprocess

import pytest

from conftest import PROJECTS, REPO, load_dag_specs

# Resource types that become Airflow tasks. Tests are attached by Cosmos'
# test_behavior and compared separately by dbt's indirect selection rules, so
# they are left out here.
TASK_TYPES = {"model", "seed", "snapshot"}


def _specs():
    dag_specs = load_dag_specs(REPO / "dags")
    for project in PROJECTS:
        for spec in dag_specs(project):
            yield pytest.param(project, spec, id=spec["dag_id"])


def dbt_ls(dbt_bin, project_dir, project, select, exclude, env) -> set[str]:
    cmd = [dbt_bin, "ls", "--project-dir", str(project_dir), "--profiles-dir", str(project_dir),
           "--profile", project, "--target", "prod", "--output", "json", "--output-keys", "unique_id", "resource_type",
           "--quiet", "--no-use-colors"]
    if select:
        cmd += ["--select", *select]
    if exclude:
        cmd += ["--exclude", *exclude]
    r = subprocess.run(cmd, env={**os.environ, **env}, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
    rows = [json.loads(line) for line in r.stdout.splitlines() if line.startswith("{")]
    return {row["unique_id"] for row in rows if row["resource_type"] in TASK_TYPES}


def cosmos_select(project_dir, select, exclude) -> set[str]:
    from cosmos.config import ExecutionConfig, ProjectConfig, RenderConfig
    from cosmos.constants import LoadMode
    from cosmos.dbt.graph import DbtGraph

    graph = DbtGraph(
        project=ProjectConfig(dbt_project_path=project_dir, manifest_path=project_dir / "target/manifest.json"),
        render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST, select=select, exclude=exclude),
        execution_config=ExecutionConfig(dbt_project_path=project_dir),
    )
    graph.load_from_dbt_manifest()
    return {uid for uid, n in graph.filtered_nodes.items() if n.resource_type.value in TASK_TYPES}


@pytest.mark.parametrize("project,spec", list(_specs()))
def test_manifest_selection_matches_dbt_ls(deploy_dir, parse_env, dbt_bin, tmp_path, project, spec):
    # Run dbt ls on a copy so it cannot rewrite the shipped target/ files.
    import shutil

    work = tmp_path / "deploy"
    shutil.copytree(deploy_dir, work, symlinks=False)
    project_dir = work / "projects" / project
    select, exclude = spec.get("select", []), spec.get("exclude", [])

    expected = dbt_ls(dbt_bin, project_dir, project, select, exclude, parse_env)
    actual = cosmos_select(deploy_dir / "projects" / project, select, exclude)

    assert expected, f"{spec['dag_id']}: selector matches nothing in dbt"
    assert actual == expected, (
        f"{spec['dag_id']}: Cosmos manifest selection differs from dbt ls\n"
        f"  only in dbt ls: {sorted(expected - actual)}\n  only in Cosmos: {sorted(actual - expected)}"
    )
