"""Build the deploy overlay: the generated files the deploy commit adds to a source commit.

    python -m manifest.generate --out overlay/ [--source .] [--config dbt_bundle.yml]
                                [--previous deploy-checkout/] [--project NAME ...] [--no-reuse]
    python -m manifest.generate --list [--source .] [--config dbt_bundle.yml]

The `deploy` branch is the source commit as-is (``git archive``), with this
overlay copied on top. Every generated file sits in place, next to what it
belongs to:

    <out>/
      <project path>/target/manifest.json     read by Cosmos at DAG parse time
      <project path>/dbt_packages/            only with `dbt_packages: ship`
      <dags.dir>/dbt_factory/projects.json    the registry: each project's path and input
                                              hash, the profile, DAG defaults
    <out>.summary.json                        built / reused per project, + what the CI checks
                                              need (outside the overlay, never deployed)

The layout comes from ``dbt_bundle.yml`` in ``--source`` (manifest/config.py has
every key and its default). The DAG factory and the CI checks read projects.json
instead of assuming a layout.

dbt never runs in the source tree: each project (with the local packages it
uses) is copied to a temp dir, parsed there, and only the results are kept.

Only changed projects are re-parsed. Each project's *input hash* covers its
files, the local packages it uses, the dbt pins, the CI profile, the parse env,
the dbt_packages mode and this generator. If ``--previous`` (a checkout of the
deploy branch, or an earlier overlay) has the project at the same path with the
same hash, its files are copied byte for byte. A rebuilt manifest is never
byte-identical (``generated_at``), so this also keeps unchanged projects out of
the deploy diff.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

from manifest.config import Config, ConfigError, Project, discover, load_config
from manifest.dbtvenv import check_pins, deps_and_parse
from manifest.files import copy_sources, dereference_symlinks, input_hash, load_env_file

REPO = Path(__file__).resolve().parent.parent
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
        "dbt_packages": str(config.raw["dbt_packages"]),
        "generator": hashlib.sha256(generator.encode()).hexdigest(),
    }


def _registry_path(config: Config) -> Path:
    return config.dags_dir / "dbt_factory" / REGISTRY


def previous_hashes(config: Config, previous: Path | None) -> dict[str, tuple[str, str]]:
    """{project: (path, input_hash)} from the previous deploy's registry."""
    reg = previous / _registry_path(config) if previous else None
    if reg is None or not reg.is_file():
        return {}
    projects = json.loads(reg.read_text()).get("projects", {})
    return {n: (str(e.get("path")), str(e.get("input_hash"))) for n, e in projects.items() if e.get("input_hash")}


def registry(config: Config, projects: list[Project], hashes: dict[str, str] | None = None) -> dict[str, Any]:
    profile = config.profile
    return {
        "version": 1,
        "dags_dir": config.dags_dir.as_posix(),
        "dbt_packages": config.raw["dbt_packages"],
        "profile": {"name": profile["name"], "target": profile["target"], "mapping": profile["mapping"]},
        "dag_defaults": config.raw["dags"]["defaults"],
        "projects": {
            p.name: {"path": p.path.as_posix(), **({"input_hash": hashes[p.name]} if hashes else {})}
            for p in projects
        },
    }


def _outputs(config: Config, project: Project) -> list[Path]:
    """The files/dirs a project contributes to the overlay, relative to the source."""
    out = [project.path / "target" / "manifest.json"]
    if config.ship_dbt_packages:
        out.append(project.path / "dbt_packages")
    return out


def _place(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        shutil.copytree(src, dst, symlinks=False)
    else:
        shutil.copy2(src, dst)


def build(config: Config, project: Project, env: dict[str, str], out: Path) -> None:
    """Parse ``project`` in a temp copy and put its outputs into ``out``."""
    source = config.source
    profile = config.profile
    with tempfile.TemporaryDirectory(prefix="manifest-") as tmp:
        work = Path(tmp)
        # Same relative layout as the source, so `local: ../../packages/x` resolves.
        for pkg in project.packages:
            copy_sources(source / pkg, work / pkg)
        dst = work / project.path
        copy_sources(source / project.path, dst)
        deps_and_parse(dst, env, config.ci_profiles_dir, profile["name"], profile["target"])
        if config.ship_dbt_packages and (dst / "dbt_packages").is_dir():
            dereference_symlinks(dst / "dbt_packages")
        for rel in _outputs(config, project):
            if (work / rel).exists():
                _place(work / rel, out / rel)


def generate(
    config: Config,
    out: Path,
    previous: Path | None,
    reuse: bool,
    only: set[str] | None = None,
) -> dict[str, str]:
    """Write the overlay to ``out``; return {project: "built" | "reused"}."""
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
    before = previous_hashes(config, previous) if reuse else {}

    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    result: dict[str, str] = {}
    hashes: dict[str, str] = {}
    for project in projects:
        digest = input_hash(source, [source / project.path, *(source / p for p in project.packages)], extra)
        hashes[project.name] = digest
        same = before.get(project.name) == (project.path.as_posix(), digest)
        if previous is not None and same and (previous / project.path / "target" / "manifest.json").is_file():
            for rel in _outputs(config, project):
                if (previous / rel).exists():
                    _place(previous / rel, out / rel)
            result[project.name] = "reused"
        else:
            build(config, project, env, out)
            result[project.name] = "built"
        log(f"{project.name} ({project.path.as_posix()}): {result[project.name]}")

    reg = registry(config, projects, hashes)
    reg["dbt_core"] = dbt_core
    (out / _registry_path(config)).parent.mkdir(parents=True, exist_ok=True)
    (out / _registry_path(config)).write_text(json.dumps(reg, indent=2) + "\n")
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", type=Path, default=Path("."), help="repo root holding dbt_bundle.yml (default: .)")
    ap.add_argument("--config", type=Path, help="config file (default: <source>/dbt_bundle.yml; missing = defaults)")
    ap.add_argument("--out", type=Path, help="overlay directory to write (replaced)")
    ap.add_argument("--previous", type=Path, help="a checkout of the deploy branch (or an earlier overlay)")
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
        "registry": _registry_path(config).as_posix(),
        "ci_profiles_dir": str(config.ci_profiles_dir),
        "parse_env": str(config.parse_env) if config.parse_env and config.parse_env.is_file() else None,
    }
    out.with_name(out.name + ".summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
