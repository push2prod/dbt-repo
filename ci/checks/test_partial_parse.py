"""The shipped partial_parse.msgpack must still be valid at runtime.

dbt throws the msgpack away when its parse state differs, for example a
different dbt version, a different --profile/--target, different non-secret
connection fields, or different env vars. It does so silently and then runs a
full parse in *every task*. This test copies the bundle to a new
location (as git-sync and Cosmos' tmp dir do) and uses a different *secret*
(as prod does), then checks that dbt reuses the msgpack.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
from conftest import PROJECTS


@pytest.mark.parametrize("project", PROJECTS)
def test_partial_parse_is_reused_after_relocation(
    bundle_dir: Path, parse_env: dict[str, str], dbt_bin: str, tmp_path: Path, project: str
) -> None:
    work = tmp_path / "elsewhere"
    shutil.copytree(bundle_dir, work, symlinks=False)
    project_dir = work / "projects" / project
    env = {**os.environ, **parse_env, "DBT_ENV_SECRET_PG_PASSWORD": "the-real-one-differs"}
    r = subprocess.run(
        [
            dbt_bin,
            "--log-format",
            "json",
            "--log-level",
            "debug",
            "ls",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(project_dir),
            "--profile",
            project,
            "--target",
            "prod",
            "--resource-type",
            "model",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0, r.stdout + r.stderr
    out = r.stdout
    assert "Unable to do partial parsing" not in out, _reason(out)
    assert "Partial parsing enabled" in out, "dbt did not use partial_parse.msgpack"


def _reason(out: str) -> str:
    return "\n".join(line for line in out.splitlines() if "partial pars" in line.lower())[:2000]
