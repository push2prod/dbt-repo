"""Only changed projects are re-parsed; everything else is reused byte for byte.

Each case copies the repo, changes something, and runs the generator with
the already-built bundle as ``--previous``, as CI does with the deploy branch.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from conftest import PROJECTS, REPO

from manifest.files import SKIP_DIRS

SOURCE = "e2e/fixtures"  # the mock dbt content, relative to the repo


def _copy_repo(dst: Path) -> Path:
    """Tooling + mock content, so the copy can be modified and regenerated."""
    ignore = shutil.ignore_patterns(*SKIP_DIRS, "build", ".venv", "*.pyc")
    for name in ("dags", "manifest", "requirements", SOURCE):
        shutil.copytree(REPO / name, dst / name, ignore=ignore)
    return dst


def _generate(repo: Path, previous: Path, out: Path) -> dict[str, str]:
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    cmd = ["python", "-m", "manifest.generate", "--source", SOURCE, "--out", str(out), "--previous", str(previous)]
    r = subprocess.run(cmd, cwd=repo, env=env, capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stdout + r.stderr
    summary = json.loads(out.with_name(out.name + ".summary.json").read_text())
    projects: dict[str, str] = summary["projects"]
    return projects


def _built(result: dict[str, str]) -> set[str]:
    return {p for p, state in result.items() if state == "built"}


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ("nothing", set()),
        ("model", {"finance"}),
        ("dag_config", set()),
        ("shared_package", set(PROJECTS)),
    ],
)
def test_only_changed_projects_rebuilt(bundle_dir: Path, tmp_path: Path, change: str, expected: set[str]) -> None:
    repo = _copy_repo(tmp_path / "repo")
    src = repo / SOURCE
    if change == "model":
        (src / "projects/finance/models/billing/billing_extra.sql").write_text("select 1 as x\n")
    elif change == "dag_config":
        cfg = src / "dags/config/core.yml"
        cfg.write_text(cfg.read_text().replace('"0 2 * * *"', '"5 2 * * *"'))
    elif change == "shared_package":
        macro = src / "packages/shared_macros/macros/cents_to_dollars.sql"
        macro.write_text(macro.read_text().replace("precision=2", "precision=3"))
    out = tmp_path / "bundle"
    result = _generate(repo, bundle_dir, out)
    assert _built(result) == expected, result
    for project in set(PROJECTS) - expected:  # reused = identical bytes
        reused = out / "projects" / project / "target" / "manifest.json"
        assert reused.read_bytes() == (bundle_dir / "projects" / project / "target" / "manifest.json").read_bytes()
