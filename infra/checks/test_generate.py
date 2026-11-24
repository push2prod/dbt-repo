"""infra.manifest.generate on small repos it builds in a temp dir.

* Discovery and layout validation (no dbt): the errors a broken layout must
  raise instead of silently shipping a bundle without some project.
* The overlay (runs dbt): only generated files, each in place; only the
  projects whose inputs changed are re-parsed.

Independent of the deploy tree under test (BUNDLE_DIR).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml

from infra.manifest.config import CONFIG_FILE, DEFAULTS, ConfigError, discover, load_config

REPO = Path(__file__).resolve().parents[2]

PROFILES = """\
dbt:
  target: prod
  outputs:
    prod:
      type: trino
      method: none
      host: dummy
      port: 8080
      user: dummy
      database: dummy
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
        f"dags/dbt_{name}.py": (
            f"# Airflow DAG file for the {name} dbt project.\n"
            "from datetime import datetime\n\n"
            "from airflow import DAG\n\n"
            "from utils.dbt_factory import dbt_task_group\n\n"
            f'with DAG("dbt_{name}", schedule=None, start_date=datetime(2024, 1, 1)) as dag:\n'
            f'    dbt_task_group("{name}", group_id="dbt", select=["path:models"])\n'
        ),
    }


def repo(tmp_path: Path, projects: dict[str, dict[str, str]], extra: dict[str, str] | None = None) -> Path:
    """A source dir: ``projects`` maps a project path to its files."""
    src = tmp_path / "src"
    factory = (REPO / "utils/dbt_factory/__init__.py").read_text()
    files = {
        "infra/profiles/profiles.yml": PROFILES,
        "utils/__init__.py": "",
        "utils/dbt_factory/__init__.py": factory,
        **(extra or {}),
    }
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
            CONFIG_FILE: "projects:\n  include: ['**']\ndbt_packages: ship\n",
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
            CONFIG_FILE: "projects:\n  include: ['**']\n",
            "projects/a/dbt_packages/dep/dbt_project.yml": "name: dep\n",
            "build/bundle/projects/a/dbt_project.yml": "name: a\n",
        },
    )
    assert [p.name for p in discover(load_config(src), skip=[src / "build"])] == ["a"]


@pytest.mark.parametrize(
    ("projects", "extra", "error"),
    [
        ({"projects/a": project("a"), "other/a": project("a")}, {CONFIG_FILE: "projects: {include: ['*/*']}"}, "is used by"),
        ({"projects/a": project("a", profile="a")}, {}, "uses profile 'a'"),
        ({"projects/a": project("a", extra="target-path: out\n")}, {}, "target-path"),
        ({"projects/a": project("a", ["../../../elsewhere"])}, {}, "outside the source dir"),
        (
            {"projects/a": project("a", ["../../packages/p"])},
            {"packages/p/dbt_project.yml": "name: p\n", CONFIG_FILE: "dbt_packages: install\n"},
            "set `dbt_packages: ship`",
        ),
    ],
    ids=["duplicate-name", "other-profile", "target-path", "package-outside", "local-pkg-install"],
)
def test_layout_errors(tmp_path: Path, projects: dict[str, dict[str, str]], extra: dict[str, str], error: str) -> None:
    src = repo(tmp_path, projects, extra)
    with pytest.raises(ConfigError, match=error):
        discover(load_config(src))


def test_unknown_config_key(tmp_path: Path) -> None:
    src = repo(tmp_path, {"projects/a": project("a")}, {CONFIG_FILE: "profile: {targte: prod}\n"})
    with pytest.raises(ConfigError, match="unknown key profile.'targte'"):
        load_config(src)


@pytest.mark.parametrize("value", ["../outside", "/abs/dbt_factory", ""])
def test_factory_dir_must_stay_in_repo(tmp_path: Path, value: str) -> None:
    src = repo(tmp_path, {"projects/a": project("a")}, {CONFIG_FILE: f"dags: {{factory_dir: '{value}'}}\n"})
    with pytest.raises(ConfigError, match="dags.factory_dir must be a path inside the repo"):
        load_config(src)


@pytest.mark.parametrize("key", ["defaults", "task_defaults"])
def test_removed_dag_defaults_keys_fail_clearly(tmp_path: Path, key: str) -> None:
    """DAG settings moved to the DAG files; an old config must say so, not be ignored."""
    src = repo(tmp_path, {"projects/a": project("a")}, {CONFIG_FILE: f"dags: {{{key}: {{retries: 1}}}}\n"})
    with pytest.raises(ConfigError, match=f"dags.{key} was removed.*belong in the DAG file"):
        load_config(src)


def test_repo_config_is_valid_and_matches_defaults() -> None:
    """infra/dbt_bundle.yml documents the defaults: it must load, and equal config.DEFAULTS."""
    raw = yaml.safe_load((REPO / CONFIG_FILE).read_text())
    assert load_config(REPO).raw == DEFAULTS
    assert raw == DEFAULTS, "infra/dbt_bundle.yml and infra/manifest/config.py DEFAULTS disagree"


# --- generate + reuse (runs dbt) ------------------------------------------


def generate(src: Path, out: Path, previous: Path | None = None) -> dict[str, str]:
    cmd = [sys.executable, "-m", "infra.manifest.generate", "--source", str(src), "--out", str(out)]
    if previous:
        cmd += ["--previous", str(previous)]
    env = {**os.environ, "PYTHONPATH": str(REPO), "PYTHONDONTWRITEBYTECODE": "1"}
    r = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stdout + r.stderr
    result: dict[str, str] = json.loads(out.with_name(out.name + ".summary.json").read_text())["projects"]
    return result


def files_in(root: Path) -> list[str]:
    return sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file())


def test_install_mode_overlay_is_only_manifests(tmp_path: Path) -> None:
    src = repo(tmp_path, {"projects/a": project("a"), "teams/t/b": project("b")})
    (src / CONFIG_FILE).write_text("projects:\n  include: ['projects/*', 'teams/*/*']\ndbt_packages: install\n")
    out = tmp_path / "overlay"
    assert generate(src, out) == {"a": "built", "b": "built"}
    assert files_in(out) == [
        "projects/a/target/manifest.json",
        "teams/t/b/target/manifest.json",
        "utils/dbt_factory/projects.json",
    ]
    reg = json.loads((out / "utils/dbt_factory/projects.json").read_text())
    assert reg["dbt_packages"] == "install"
    assert {n: e["path"] for n, e in reg["projects"].items()} == {"a": "projects/a", "b": "teams/t/b"}
    assert not (src / "projects/a/target").exists(), "dbt must not run in the source tree"


def test_only_changed_projects_are_reparsed(tmp_path: Path) -> None:
    src = repo(
        tmp_path,
        {"projects/a": project("a", ["../../packages/shared"]), "projects/b": project("b")},
        {
            "packages/shared/dbt_project.yml": "name: shared\nversion: '1.0'\n",
            "packages/shared/macros/m.sql": "{% macro m() %}1{% endmacro %}\n",
        },
    )
    v1 = tmp_path / "v1"
    assert generate(src, v1) == {"a": "built", "b": "built"}

    # ship mode (the default): dbt_packages/ (dereferenced) next to target/, nothing else from the source.
    reg = json.loads((v1 / "utils/dbt_factory/projects.json").read_text())
    assert {n: e["path"] for n, e in reg["projects"].items()} == {"a": "projects/a", "b": "projects/b"}
    assert "projects/a/dbt_packages/shared/macros/m.sql" in files_in(v1)
    assert not (v1 / "projects/a/dbt_packages/shared").is_symlink()
    assert {f for f in files_in(v1) if "dbt_packages" not in f} == {
        "utils/dbt_factory/projects.json",
        "projects/a/target/manifest.json",
        "projects/b/target/manifest.json",
    }

    cases = [
        ("nothing", lambda: None, {"a": "reused", "b": "reused"}),
        ("model in b", lambda: (src / "projects/b/models/c.sql").write_text("select 2 as id\n"), {"a": "reused", "b": "built"}),
        ("DAG file", lambda: (src / "dags/dbt_a.py").write_text("# Airflow DAG file, changed\n"), {"a": "reused", "b": "reused"}),
        ("shared package", lambda: (src / "packages/shared/macros/n.sql").write_text("-- n\n"), {"a": "built", "b": "reused"}),
    ]
    previous = v1
    for i, (what, change, expected) in enumerate(cases):
        change()
        out = tmp_path / f"v{i + 2}"
        assert generate(src, out, previous) == expected, what
        if what == "nothing":
            # Reused files are byte-identical, so they stay out of the deploy diff.
            assert (out / "projects/a/target/manifest.json").read_bytes() == (v1 / "projects/a/target/manifest.json").read_bytes()
            assert "projects/a/dbt_packages/shared/macros/m.sql" in files_in(out)
        previous = out
