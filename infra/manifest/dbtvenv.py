"""The dbt toolchain: its own venv, pinned by infra/requirements/dbt.txt, as on the Airflow image."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

# Point DBT_BIN at the dbt venv's dbt (CI and the image use /opt/dbt-venv).
DBT_BIN = os.environ.get("DBT_BIN") or shutil.which("dbt") or "dbt"
# Only the manifest ships. partial_parse.msgpack is dropped: it is reused at runtime
# only if the runtime profile (Cosmos profile_mapping) hashes the same as the CI one.
KEEP_TARGET_FILES = {"manifest.json"}


def installed_versions() -> dict[str, str]:
    """name -> version of every distribution in the dbt venv."""
    python = str(Path(DBT_BIN).resolve().parent / "python")
    code = (
        "import json, importlib.metadata as m; "
        "print(json.dumps({d.metadata['Name'].lower(): d.version for d in m.distributions()}))"
    )
    out = subprocess.run([python, "-c", code], check=True, capture_output=True, text=True).stdout
    versions: dict[str, str] = json.loads(out)
    return versions


def check_pins(requirements: Path) -> str:
    """Exit unless the dbt venv matches ``requirements`` exactly. Returns the dbt-core version."""
    have = installed_versions()
    bad = []
    for raw in requirements.read_text().splitlines():
        line = raw.split("#")[0].strip()
        if "==" in line:
            name, want = (s.strip() for s in line.split("=="))
            if have.get(name.lower()) != want:
                bad.append(f"{name}: installed {have.get(name.lower(), '<missing>')}, pinned {want}")
    if bad:
        sys.exit("[manifest] dbt venv does not match infra/requirements/dbt.txt:\n  " + "\n  ".join(bad))
    return have["dbt-core"]


def deps_and_parse(project: Path, env: dict[str, str], profiles_dir: Path, profile: str, target: str) -> None:
    """`dbt deps` + `dbt parse` in ``project``, leaving only the artifacts Airflow needs in target/.

    Parses with the same --profile/--target that Cosmos passes at task time.
    """
    common = ["--project-dir", str(project), "--profiles-dir", str(profiles_dir), "--no-use-colors"]
    run_env = {**os.environ, **env, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    for cmd in (["deps"], ["parse", "--profile", profile, "--target", target]):
        r = subprocess.run([DBT_BIN, *cmd, *common], env=run_env, capture_output=True, text=True, check=False)
        if r.returncode:
            sys.exit(f"[manifest] {project.name}: dbt {cmd[0]} failed\n{r.stdout}\n{r.stderr}")
    shutil.rmtree(project / "logs", ignore_errors=True)
    target_dir = project / "target"
    for p in target_dir.iterdir():
        if p.name not in KEEP_TARGET_FILES:
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()
    missing = KEEP_TARGET_FILES - {p.name for p in target_dir.iterdir()}
    if missing:
        sys.exit(f"[manifest] {project.name}: dbt parse did not produce {sorted(missing)}")
