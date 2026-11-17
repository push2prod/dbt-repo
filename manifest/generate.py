"""Build the Airflow deploy bundle from a checkout. No git: a directory in, a directory out.

    python -m manifest.generate --out bundle/ [--source .] [--config dbt_bundle.yml]
                                [--previous previous-bundle/] [--project NAME ...] [--no-reuse]
    python -m manifest.generate --list [--source .] [--config dbt_bundle.yml]

The layout comes from ``dbt_bundle.yml`` in ``--source`` (see manifest/config.py
for every key and its default). The bundle mirrors the source's paths, so
``local: ../../packages/shared`` still resolves at runtime:

    <out>/
      <dags.dir>/                      DAG files + DAG configs, copied as-is
        dbt_factory/                   Cosmos DAG factory (tooling, added to every bundle)
          projects.json                the registry: every project's path, profile, target, DAG config
      <project path>/                  project sources, for every project found
        dbt_packages/                  `dbt deps` output, symlinks dereferenced
        target/manifest.json           read by Cosmos at DAG parse time
        target/build-info.json         input hash + dbt version
      <package path>/                  local dbt packages the projects use
    <out>.summary.json                 built / reused per project, + what the CI checks need (outside the bundle)

The factory and the CI checks read projects.json instead of assuming a layout.

Only changed projects are re-parsed. Each project's *input hash* covers its
files, the local packages it uses, the dbt pins, the CI profile, the parse env
and this generator. If ``--previous`` holds a build of the project at the same
path with the same hash, it is copied byte for byte. A rebuilt manifest is never
byte-identical (``generated_at``), so this also keeps unchanged projects out of
the deploy diff.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

from manifest.config import Config, ConfigError, Project, discover, load_config
from manifest.dbtvenv import check_pins, deps_and_parse
from manifest.files import copy_sources, dereference_symlinks, input_hash, load_env_file

REPO = Path(__file__).resolve().parent.parent
FACTORY = REPO / "dags" / "dbt_factory"
REGISTRY = "projects.json"


def log(msg: str) -> None:
    print(f"[manifest] {msg}", flush=True)


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else "-"


def global_inputs(config: Config, dbt_core: str) -> dict[str, str]:
    """Inputs that affect every project. A change here re-parses everything."""
    generator = "".join(_digest(p) for p in sorted((REPO / "manifest").glob("*.py")))
    profile = config.profile
    return {
        "dbt_core": dbt_core,
        "requirements": _digest(REPO / "requirements" / "dbt.txt"),
        "parse_env": _digest(config.parse_env) if config.parse_env else "-",
        "ci_profiles": _digest(config.ci_profiles_dir / "profiles.yml"),
        "profile": f"{profile['name']}/{profile['target']}",
        "generator": hashlib.sha256(generator.encode()).hexdigest(),
    }


def previous_hash(previous: Path | None, project: Project) -> str | None:
    info = previous / project.path / "target" / "build-info.json" if previous else None
    if info is None or not info.is_file():
        return None
    data = json.loads(info.read_text())
    if data.get("project") not in (None, project.name):
        return None
    value = data.get("input_hash")
    return str(value) if value else None


def registry(config: Config, projects: list[Project]) -> dict[str, Any]:
    profile = config.profile
    return {
        "version": 1,
        "dags_dir": config.dags_dir.as_posix(),
        "profile": {"name": profile["name"], "target": profile["target"], "mapping": profile["mapping"]},
        "dag_defaults": config.raw["dags"]["defaults"],
        "projects": {
            p.name: {"path": p.path.as_posix(), "dag_config": config.dag_config(p.name).as_posix()} for p in projects
        },
    }


def generate(
    config: Config,
    out: Path,
    previous: Path | None,
    reuse: bool,
    only: set[str] | None = None,
) -> dict[str, str]:
    """Write the bundle to ``out``; return {project: "built" | "reused"}."""
    source = config.source
    projects = discover(config, skip=[out, *([previous] if previous else [])])
    if only:
        unknown = only - {p.name for p in projects}
        if unknown:
            raise ConfigError(f"--project: no such project {sorted(unknown)}")
        projects = [p for p in projects if p.name in only]
    if not projects:
        raise ConfigError(f"no dbt projects found for include {config.raw['projects']['include']} in {source}")
    profiles_yml = config.ci_profiles_dir / "profiles.yml"
    if not profiles_yml.is_file():
        raise ConfigError(f"CI profile not found: {profiles_yml} (profile.ci_profiles_dir)")

    dbt_core = check_pins(REPO / "requirements" / "dbt.txt")
    env = load_env_file(config.parse_env) if config.parse_env and config.parse_env.is_file() else {}
    extra = global_inputs(config, dbt_core)
    profile = config.profile

    if out.exists():
        shutil.rmtree(out)
    dags = out / config.dags_dir
    copy_sources(source / config.dags_dir, dags)
    copy_sources(FACTORY, dags / "dbt_factory")
    for pkg in sorted({pkg for p in projects for pkg in p.packages}):
        copy_sources(source / pkg, out / pkg)
    (dags / "dbt_factory" / REGISTRY).write_text(json.dumps(registry(config, projects), indent=2) + "\n")

    # Only dags_dir holds DAG files, in case Airflow's dags_folder is the bundle root.
    top = config.dags_dir.parts[0]
    ignore = sorted({q.parts[0] for p in projects for q in (p.path, *p.packages)} - {top})
    (out / ".airflowignore").write_text("# only the DAG dir holds DAG files\n" + "".join(f"{d}/\n" for d in ignore))

    result: dict[str, str] = {}
    for project in projects:
        digest = input_hash(source, [source / project.path, *(source / p for p in project.packages)], extra)
        dst = out / project.path
        if reuse and previous is not None and previous_hash(previous, project) == digest:
            shutil.copytree(previous / project.path, dst)
            result[project.name] = "reused"
        else:
            copy_sources(source / project.path, dst)
            deps_and_parse(dst, env, config.ci_profiles_dir, profile["name"], profile["target"])
            dereference_symlinks(dst / "dbt_packages")
            info = {
                "project": project.name,
                "input_hash": digest,
                "dbt_core": dbt_core,
                "local_packages": [p.as_posix() for p in project.packages],
            }
            (dst / "target" / "build-info.json").write_text(json.dumps(info, indent=2, sort_keys=True) + "\n")
            result[project.name] = "built"
        log(f"{project.name} ({project.path.as_posix()}): {result[project.name]}")
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", type=Path, default=Path("."), help="repo root holding dbt_bundle.yml (default: .)")
    ap.add_argument("--config", type=Path, help="config file (default: <source>/dbt_bundle.yml; missing = defaults)")
    ap.add_argument("--out", type=Path, help="bundle directory to write (replaced)")
    ap.add_argument("--previous", type=Path, help="previous bundle, e.g. a checkout of the deploy branch")
    ap.add_argument("--project", action="append", help="build only this project (repeatable; for local use)")
    ap.add_argument("--no-reuse", action="store_true", help="re-parse every project")
    ap.add_argument("--list", action="store_true", help="print the projects found and exit; runs no dbt")
    args = ap.parse_args()
    try:
        config = load_config(args.source.resolve(), args.config.resolve() if args.config else None)
        if args.list:
            projects = discover(config, skip=[args.out] if args.out else [])
            print(json.dumps(registry(config, projects), indent=2))
            return
        if args.out is None:
            ap.error("--out is required (or use --list)")
        out = args.out.resolve()
        previous = args.previous.resolve() if args.previous and args.previous.is_dir() else None
        only = set(args.project) if args.project else None
        result = generate(config, out, previous, reuse=not args.no_reuse, only=only)
    except ConfigError as e:
        sys.exit(f"[manifest] {e}")
    summary = {
        "projects": result,
        "registry": (config.dags_dir / "dbt_factory" / REGISTRY).as_posix(),
        "ci_profiles_dir": str(config.ci_profiles_dir),
        "parse_env": str(config.parse_env) if config.parse_env and config.parse_env.is_file() else None,
    }
    out.with_name(out.name + ".summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
