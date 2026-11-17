"""The bundle config (``dbt_bundle.yml``) and project discovery.

Every path in the config is relative to ``--source`` (the repo root in CI).
A missing config file means all defaults.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from manifest.files import SKIP_DIRS, local_package_dirs

CONFIG_FILE = "dbt_bundle.yml"

DEFAULTS: dict[str, Any] = {
    "projects": {
        # Globs, relative to the source, matching directories that hold a dbt_project.yml.
        "include": ["projects/*"],
        "exclude": [],
    },
    "profile": {
        # One profile for every project. CI parses with a dummy profiles.yml from
        # `ci_profiles_dir`; at runtime Cosmos builds the profile from `mapping`.
        "name": "dbt",
        "target": "prod",
        "ci_profiles_dir": "profiles",
        "mapping": {
            "class": "cosmos.profiles.PostgresUserPasswordProfileMapping",
            "conn_id": "dbt_warehouse",
            "profile_args": {},
        },
    },
    # Env for `dbt parse` in CI: values for env_var() calls in the projects. Optional.
    "parse_env": "manifest/parse.env",
    # What the deploy commit adds next to each project's target/manifest.json:
    #   ship:    the project's dbt_packages/, built in CI; no `dbt deps` at runtime.
    #   install: nothing; Cosmos runs `dbt deps` in every task (needs network to the
    #            package sources; no `local:` packages, see discover()).
    "dbt_packages": "ship",
    "dags": {
        "dir": "dags",
        # Defaults for dbt_dag(...) in the factory; a DAG file overrides them per DAG.
        "defaults": {
            "start_date": "2024-01-01",
            "catchup": False,
            "max_active_runs": 1,
            "tags": ["dbt"],
            # One retry covers the KubernetesExecutor parse/run skew: a pod that starts
            # just after a deploy clones the new revision. dbt materializations are
            # rebuilt whole, so a retry is safe.
            "retries": 1,
            "retry_delay_seconds": 60,
        },
    },
}


class ConfigError(Exception):
    """The config, or the repo layout it describes, is invalid."""


@dataclass(frozen=True)
class Project:
    name: str  # `name:` from dbt_project.yml
    path: Path  # relative to the source
    packages: tuple[Path, ...]  # local packages it uses (transitively), relative to the source


@dataclass(frozen=True)
class Config:
    source: Path
    raw: dict[str, Any]

    @property
    def profile(self) -> dict[str, Any]:
        return dict(self.raw["profile"])

    @property
    def ci_profiles_dir(self) -> Path:
        return self.source / self.raw["profile"]["ci_profiles_dir"]

    @property
    def parse_env(self) -> Path | None:
        value = self.raw["parse_env"]
        return self.source / value if value else None

    @property
    def ship_dbt_packages(self) -> bool:
        return bool(self.raw["dbt_packages"] == "ship")

    @property
    def dags_dir(self) -> Path:
        """Relative to the source (and to the bundle root)."""
        return Path(self.raw["dags"]["dir"])


def _merge(base: dict[str, Any], override: dict[str, Any], where: str = "") -> dict[str, Any]:
    out = copy.deepcopy(base)
    for key, value in override.items():
        if key not in base:
            raise ConfigError(f"{CONFIG_FILE}: unknown key {where}{key!r}")
        # Free-form mappings are replaced whole, not merged key by key.
        if isinstance(base[key], dict) and key not in ("profile_args", "defaults"):
            if not isinstance(value, dict):
                raise ConfigError(f"{CONFIG_FILE}: {where}{key} must be a mapping")
            out[key] = _merge(base[key], value, f"{where}{key}.")
        else:
            out[key] = value
    return out


def load_config(source: Path, path: Path | None = None) -> Config:
    path = path or source / CONFIG_FILE
    raw: dict[str, Any] = {}
    if path.is_file():
        raw = yaml.safe_load(path.read_text()) or {}
        if not isinstance(raw, dict):
            raise ConfigError(f"{path}: must be a mapping")
    elif path != source / CONFIG_FILE:
        raise ConfigError(f"config file not found: {path}")
    merged = _merge(DEFAULTS, raw)
    if merged["dbt_packages"] not in ("install", "ship"):
        raise ConfigError(f"{CONFIG_FILE}: dbt_packages must be 'install' or 'ship', not {merged['dbt_packages']!r}")
    return Config(source=source.resolve(), raw=merged)


def _matches(source: Path, patterns: list[str]) -> set[Path]:
    """Directories matched by ``patterns``. A pattern may also end in dbt_project.yml."""
    found: set[Path] = set()
    for pattern in patterns:
        for p in source.glob(pattern):
            d = p.parent if p.name == "dbt_project.yml" else p
            if d.is_dir():
                found.add(d.resolve())
    return found


def _read_project_yml(project_dir: Path) -> dict[str, Any]:
    data = yaml.safe_load((project_dir / "dbt_project.yml").read_text()) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{project_dir}/dbt_project.yml: must be a mapping")
    return data


def discover(config: Config, skip: list[Path] | None = None) -> list[Project]:
    """All projects the config selects, sorted by name. Raises ConfigError on any layout problem."""
    source = config.source
    skip_roots = [s.resolve() for s in skip or []]

    def skipped(d: Path) -> bool:
        rel = d.relative_to(source)
        if any(part in SKIP_DIRS or part.startswith(".") for part in rel.parts):
            return True  # dbt_packages/, target/, .venv/ ...
        return any(d == s or s in d.parents for s in skip_roots)

    dirs = _matches(source, config.raw["projects"]["include"]) - _matches(source, config.raw["projects"]["exclude"])
    dirs = {d for d in dirs if (d / "dbt_project.yml").is_file() and source in d.parents and not skipped(d)}

    errors: list[str] = []
    packages: dict[Path, tuple[Path, ...]] = {}
    for d in sorted(dirs):
        pkgs = []
        for pkg in sorted(local_package_dirs(d)):
            if source not in pkg.parents:
                errors.append(f"{d.relative_to(source)}: local package {pkg} is outside the source dir {source}")
            else:
                pkgs.append(pkg.relative_to(source))
        packages[d] = tuple(pkgs)
    # A local package is not a project, even if a glob matches it.
    used_as_package = {source / p for ps in packages.values() for p in ps}

    profile = config.raw["profile"]["name"]
    by_name: dict[str, list[Path]] = {}
    for d in sorted(dirs - used_as_package):
        rel = d.relative_to(source)
        data = _read_project_yml(d)
        name = data.get("name")
        if not isinstance(name, str) or not name:
            errors.append(f"{rel}: dbt_project.yml has no `name:`")
            continue
        by_name.setdefault(name, []).append(rel)
        if data.get("profile") not in (None, profile):
            errors.append(f"{rel}: dbt_project.yml uses profile {data['profile']!r}; every project must use {profile!r}")
        for key, default in (("target-path", "target"), ("packages-install-path", "dbt_packages")):
            if data.get(key, default) != default:
                errors.append(f"{rel}: `{key}: {data[key]}` is not supported (Cosmos expects {default!r})")
    for name, paths in by_name.items():
        if len(paths) > 1:
            errors.append(f"project name {name!r} is used by {', '.join(map(str, paths))}")
    projects = [Project(name=n, path=ps[0], packages=packages[source / ps[0]]) for n, ps in by_name.items()]

    if not config.ship_dbt_packages:
        for p in projects:
            if p.packages:
                errors.append(
                    f"{p.path}: uses local package(s) {', '.join(x.as_posix() for x in p.packages)}. With "
                    "`dbt_packages: install`, Cosmos runs `dbt deps` in a temp dir where relative `local:` "
                    "paths don't resolve; set `dbt_packages: ship`"
                )
    factory = source / config.dags_dir / "dbt_factory" / "__init__.py"
    if not factory.is_file():
        errors.append(f"{factory.relative_to(source)}: missing; the DAG factory must be in the DAG dir")


    if errors:
        raise ConfigError("invalid dbt layout:\n  " + "\n  ".join(errors))
    return sorted(projects, key=lambda p: p.name)
