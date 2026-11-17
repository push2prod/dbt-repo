"""Build the Airflow deploy bundle from a checkout. No git: a directory in, a directory out.

    python -m manifest.generate --source <dir> --out bundle/ [--previous previous-bundle/] [--parse-env file]

``--source`` holds the dbt content: projects/, packages/ (local dbt packages) and
dags/ (one DAG file per project + dags/config/). In CI it is the repo root
(``DBT_SOURCE: "."``). The DAG
factory (dags/dbt_factory at the repo root) is tooling and is added to every bundle.

Bundle layout (mirrors the repo, so ``local: ../../packages/shared_macros`` still resolves):

    <out>/
      .airflowignore                   only dags/ holds DAG files
      dags/                            DAG files, one per project, + config/ + dbt_factory/
      packages/                        local dbt packages
      projects/<name>/                 project sources
        dbt_packages/                  `dbt deps` output, symlinks dereferenced
        target/manifest.json           read by Cosmos at DAG parse time
        target/partial_parse.msgpack   lets task-level dbt runs skip a full parse
        target/build-info.json         input hash + dbt version
    <out>.summary.json                 built / reused per project (outside the bundle)

Only changed projects are re-parsed. Each project's *input hash* covers its
files, the local packages it uses, the dbt pins, the parse env and this
generator. If ``--previous`` holds a build of the project with the same hash,
it is copied byte for byte. A rebuilt manifest is never byte-identical
(``generated_at``), so this also keeps unchanged projects out of the deploy diff.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

from manifest.dbtvenv import check_pins, deps_and_parse
from manifest.files import copy_sources, dereference_symlinks, input_hash, load_env_file, local_package_dirs

REPO = Path(__file__).resolve().parent.parent
DEFAULT_PARSE_ENV = REPO / "manifest" / "parse.env"
FACTORY = REPO / "dags" / "dbt_factory"


def log(msg: str) -> None:
    print(f"[manifest] {msg}", flush=True)


def global_inputs(parse_env: Path, dbt_core: str) -> dict[str, str]:
    """Inputs that affect every project. A change here re-parses everything."""

    def digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    generator = "".join(digest(p) for p in sorted((REPO / "manifest").glob("*.py")))
    return {
        "dbt_core": dbt_core,
        "requirements": digest(REPO / "requirements" / "dbt.txt"),
        "parse_env": digest(parse_env),
        "generator": hashlib.sha256(generator.encode()).hexdigest(),
    }


def previous_hash(previous: Path | None, name: str) -> str | None:
    info = previous / "projects" / name / "target" / "build-info.json" if previous else None
    if info is None or not info.is_file():
        return None
    value = json.loads(info.read_text()).get("input_hash")
    return str(value) if value else None


def build_project(src: Path, out: Path, env: dict[str, str], info: dict[str, object]) -> None:
    dst = out / "projects" / src.name
    copy_sources(src, dst)
    deps_and_parse(dst, env)
    dereference_symlinks(dst / "dbt_packages")
    (dst / "target" / "build-info.json").write_text(json.dumps(info, indent=2, sort_keys=True) + "\n")


def generate(source: Path, out: Path, previous: Path | None, parse_env: Path, reuse: bool) -> dict[str, str]:
    """Write the bundle for ``source`` to ``out``; return {project: "built" | "reused"}."""
    dbt_core = check_pins(REPO / "requirements" / "dbt.txt")
    env = load_env_file(parse_env)
    extra = global_inputs(parse_env, dbt_core)
    if out.exists():
        shutil.rmtree(out)
    copy_sources(source / "dags", out / "dags")
    copy_sources(FACTORY, out / "dags" / "dbt_factory")
    if (source / "packages").is_dir():
        copy_sources(source / "packages", out / "packages")
    (out / ".airflowignore").write_text("# only dags/ holds DAG files\nprojects/\npackages/\n")
    result: dict[str, str] = {}
    for project_yml in sorted((source / "projects").glob("*/dbt_project.yml")):
        src = project_yml.parent
        packages = sorted(local_package_dirs(src))
        digest = input_hash(source, [src, *packages], extra)
        if reuse and previous is not None and previous_hash(previous, src.name) == digest:
            shutil.copytree(previous / "projects" / src.name, out / "projects" / src.name)
            result[src.name] = "reused"
        else:
            rel_packages = [p.relative_to(source.resolve()).as_posix() for p in packages]
            info: dict[str, object] = {"input_hash": digest, "dbt_core": dbt_core, "local_packages": rel_packages}
            build_project(src, out, env, info)
            result[src.name] = "built"
        log(f"{src.name}: {result[src.name]}")
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", type=Path, default=REPO, help="dir with projects/, packages/, dags/ (default: repo)")
    ap.add_argument("--out", type=Path, required=True, help="bundle directory to write (replaced)")
    ap.add_argument("--previous", type=Path, help="previous bundle, e.g. a checkout of the deploy branch")
    ap.add_argument("--parse-env", type=Path, default=DEFAULT_PARSE_ENV, help="env for `dbt parse`")
    ap.add_argument("--no-reuse", action="store_true", help="re-parse every project")
    args = ap.parse_args()
    out = args.out.resolve()
    previous = args.previous.resolve() if args.previous and args.previous.is_dir() else None
    result = generate(args.source.resolve(), out, previous, args.parse_env.resolve(), reuse=not args.no_reuse)
    summary = out.with_name(out.name + ".summary.json")
    summary.write_text(json.dumps({"projects": result}, indent=2) + "\n")


if __name__ == "__main__":
    main()
