"""Every model's SQL must render: `dbt compile` without a warehouse.

`dbt parse` (what produces the manifest) is lenient. It accepts SQL that
fails as soon as a task renders it, e.g. a package macro called without its
package prefix (`cents_to_dollars(...)` instead of
`shared_macros.cents_to_dollars(...)`). The strain test found exactly that,
with every DAG parsing fine and every task failing.

`--no-introspect --no-populate-cache` keeps dbt off the warehouse, so this
runs in CI with the same dummy CI profile. Models that need
introspective queries (run_query, adapter.get_columns_in_relation, ...) can't
be checked this way.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
from conftest import PROJECTS, dbt_profile_args, project_path


@pytest.mark.parametrize("project", PROJECTS)
def test_all_models_compile(dbt_tree: Path, parse_env: dict[str, str], dbt_bin: str, project: str) -> None:
    project_dir = project_path(dbt_tree, project)
    r = subprocess.run(
        [
            dbt_bin,
            "compile",
            "--no-introspect",
            "--no-populate-cache",
            "--project-dir",
            str(project_dir),
            *dbt_profile_args(),
            "--no-use-colors",
        ],
        env={**os.environ, **parse_env},
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0, f"dbt compile failed for {project}:\n{r.stdout[-3000:]}{r.stderr[-1000:]}"
