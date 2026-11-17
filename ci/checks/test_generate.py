"""manifest.generate on small repos it builds in a temp dir.

* Discovery and layout validation (no dbt): the errors a broken layout must
  raise instead of silently shipping a bundle without some project.
* Reuse (runs dbt): only the projects whose inputs changed are re-parsed.

Independent of the bundle under test (BUNDLE_DIR).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from manifest.config import ConfigError, discover, load_config

REPO = Path(__file__).resolve().parents[2]

PROFILES = """\
dbt:
  target: prod
  outputs:
    prod:
      type: postgres
      host: dummy
      port: 5432
      user: dummy
      password: dummy
      dbname: dummy
      schema: dummy
      threads: 1
"""


def write(root: Path, files: dict[str, str]) -> None:
    for rel, content in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(content))


def project(name: str, packages: list[str] | None = None, profile: str = "dbt", extra: str = "") -> dict[str, str]:
    files = {
        "dbt_project.yml": f"name: {name}\nversion: '1.0'\nprofile: {profile}\n{extra}",
        "models/a.sql": "select 1 as id\n",
        "models/b.sql": "select * from {{ ref('a') }}\n",
    }
    if packages:
        files["packages.yml"] = "packages:\n" + "".join(f"  - local: {p}\n" for p in packages)
    return files


def dag_files(name: str) -> dict[str, str]:
    return {
        f"dags/dbt_{name}.py": f'from dbt_factory import build_dags\n\nbuild_dags("{name}", globals())\n',
        f"dags/config/{name}.yml": f"dags:\n  - dag_id: dbt_{name}\n    select: ['path:models']\n",
    }


def repo(tmp_path: Path, projects: dict[str, dict[str, str]], extra: dict[str, str] | None = None) -> Path:
    """A source dir: ``projects`` maps a project path to its files."""
    src = tmp_path / "src"
    files = {"profiles/profiles.yml": PROFILES, **(extra or {})}
    for path, pfiles in projects.items():
        files.update({f"{path}/{k}": v for k, v in pfiles.items()})
        name = next(line.split(":", 1)[1].strip() for line in pfiles["dbt_project.yml"].splitlines())
        files.update(dag_files(name))
    write(src, files)
    return src


# --- discovery (no dbt) ---------------------------------------------------


def test_defaults_find_projects_one_level_deep(tmp_path: Path) -> None:
    src = repo(tmp_path, {"projects/a": project("a"), "projects/b": project("b")})
    found = discover(load_config(src))
    assert [(p.name, p.path.as_posix()) for p in found] == [("a", "projects/a"), ("b", "projects/b")]


def test_nested_layout_named_by_dbt_project(tmp_path: Path) -> None:
    src = repo(
        tmp_path,
        {"teams/x/core_dir": project("x_core", ["../../../shared/macros"]), "teams/y/sales": project("y_sales")},
        {
            "shared/macros/dbt_project.yml": "name: macros\nversion: '1.0'\nprofile: other\n",
            "dbt_bundle.yml": "projects:\n  include: ['**']\n",
        },
    )
    found = discover(load_config(src))
    # The local package matches "**" too, but is not a project (and its profile isn't checked).
    assert [(p.name, p.path.as_posix()) for p in found] == [("x_core", "teams/x/core_dir"), ("y_sales", "teams/y/sales")]
    assert [p.as_posix() for p in found[0].packages] == ["shared/macros"]


def test_build_output_and_installed_packages_are_skipped(tmp_path: Path) -> None:
    src = repo(
        tmp_path,
        {"projects/a": project("a")},
        {
            "dbt_bundle.yml": "projects:\n  include: ['**']\n",
            "projects/a/dbt_packages/dep/dbt_project.yml": "name: dep\n",
            "build/bundle/projects/a/dbt_project.yml": "name: a\n",
        },
    )
    assert [p.name for p in discover(load_config(src), skip=[src / "build"])] == ["a"]


@pytest.mark.parametrize(
    ("projects", "extra", "error"),
    [
        ({"projects/a": project("a"), "other/a": project("a")}, {"dbt_bundle.yml": "projects: {include: ['*/*']}"}, "is used by"),
        ({"projects/a": project("a", profile="a")}, {}, "uses profile 'a'"),
        ({"projects/a": project("a", extra="target-path: out\n")}, {}, "target-path"),
        ({"projects/a": project("a", ["../../../elsewhere"])}, {}, "outside the source dir"),
        ({"projects/a": project("a")}, {"dags/config/gone.yml": "dags: []\n"}, "no project has that name"),
    ],
    ids=["duplicate-name", "other-profile", "target-path", "package-outside", "orphan-dag-config"],
)
def test_layout_errors(tmp_path: Path, projects: dict[str, dict[str, str]], extra: dict[str, str], error: str) -> None:
    src = repo(tmp_path, projects, extra)
    with pytest.raises(ConfigError, match=error):
        discover(load_config(src))


def test_missing_dag_config(tmp_path: Path) -> None:
    src = repo(tmp_path, {"projects/a": project("a")})
    (src / "dags/config/a.yml").unlink()
    with pytest.raises(ConfigError, match="has no DAG config"):
        discover(load_config(src))


def test_unknown_config_key(tmp_path: Path) -> None:
    src = repo(tmp_path, {"projects/a": project("a")}, {"dbt_bundle.yml": "profile: {targte: prod}\n"})
    with pytest.raises(ConfigError, match="unknown key profile.'targte'"):
        load_config(src)


# --- generate + reuse (runs dbt) ------------------------------------------


def generate(src: Path, out: Path, previous: Path | None = None) -> dict[str, str]:
    cmd = [sys.executable, "-m", "manifest.generate", "--source", str(src), "--out", str(out)]
    if previous:
        cmd += ["--previous", str(previous)]
    env = {**os.environ, "PYTHONPATH": str(REPO), "PYTHONDONTWRITEBYTECODE": "1"}
    r = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stdout + r.stderr
    result: dict[str, str] = json.loads(out.with_name(out.name + ".summary.json").read_text())["projects"]
    return result


def test_only_changed_projects_are_reparsed(tmp_path: Path) -> None:
    src = repo(
        tmp_path,
        {"projects/a": project("a", ["../../packages/shared"]), "projects/b": project("b")},
        {"packages/shared/dbt_project.yml": "name: shared\nversion: '1.0'\n", "packages/shared/macros/m.sql": "{% macro m() %}1{% endmacro %}\n"},
    )
    v1 = tmp_path / "v1"
    assert generate(src, v1) == {"a": "built", "b": "built"}

    # The bundle mirrors the source and carries the registry.
    reg = json.loads((v1 / "dags/dbt_factory/projects.json").read_text())
    assert reg["projects"] == {
        "a": {"path": "projects/a", "dag_config": "dags/config/a.yml"},
        "b": {"path": "projects/b", "dag_config": "dags/config/b.yml"},
    }
    assert (v1 / "packages/shared/macros/m.sql").is_file()
    assert sorted(p.name for p in (v1 / "projects/a/target").iterdir()) == ["build-info.json", "manifest.json"]

    cases = [
        ("nothing", lambda: None, {"a": "reused", "b": "reused"}),
        ("model in b", lambda: (src / "projects/b/models/c.sql").write_text("select 2 as id\n"), {"a": "reused", "b": "built"}),
        ("DAG config", lambda: (src / "dags/config/a.yml").write_text("dags: []\n"), {"a": "reused", "b": "reused"}),
        ("shared package", lambda: (src / "packages/shared/macros/n.sql").write_text("-- n\n"), {"a": "built", "b": "reused"}),
    ]
    previous = v1
    for i, (what, change, expected) in enumerate(cases):
        change()
        out = tmp_path / f"v{i + 2}"
        assert generate(src, out, previous) == expected, what
        previous = out
    shutil.rmtree(tmp_path / "v1")
