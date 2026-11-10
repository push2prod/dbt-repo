"""Apply e2e/patches/ to a copy of the mock content, and check their expect.yaml against observed DAGs.

Used by the e2e test (DAGs read from Airflow's API) and by ci/checks (DAGs
read from a local DagBag), so both judge a patch the same way.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypedDict

import yaml

REPO = Path(__file__).resolve().parents[2]
PATCHES_DIR = REPO / "e2e" / "patches"
FIXTURES = REPO / "e2e" / "fixtures"  # the mock dbt content the patches apply to


class DagShape(TypedDict):
    """What the checks compare: task ids, upstream task ids per task, schedule."""

    tasks: list[str]
    upstream: dict[str, list[str]]
    schedule: str


DagShapes = dict[str, DagShape]


@dataclass
class Expectations:
    """All patches' expect.yaml files, merged."""

    rebuilt_projects: set[str] = field(default_factory=set)
    dags: dict[str, dict[str, Any]] = field(default_factory=dict)


def patch_dirs() -> list[Path]:
    return sorted(p for p in PATCHES_DIR.iterdir() if (p / "change.patch").is_file())


def apply_all(source_copy: Path) -> list[str]:
    """`git apply` every patch, in order, inside a copy of e2e/fixtures (needn't be a git repo)."""
    applied = []
    for d in patch_dirs():
        r = subprocess.run(
            ["git", "apply", str(d / "change.patch")], cwd=source_copy, capture_output=True, text=True, check=False
        )
        if r.returncode:
            raise RuntimeError(f"patch {d.name} does not apply:\n{r.stderr}")
        applied.append(d.name)
    return applied


def load_expectations() -> Expectations:
    exp = Expectations()
    for d in patch_dirs():
        spec = yaml.safe_load((d / "expect.yaml").read_text()) or {}
        exp.rebuilt_projects |= set(spec.get("rebuilt_projects", []))
        for dag_id, dag_spec in (spec.get("dags") or {}).items():
            exp.dags.setdefault(dag_id, {}).update(dag_spec)
    return exp


def model_of(task_id: str) -> str:
    """Cosmos task id -> model name: `<model>.run` (task group) or `<model>_run` (single task)."""
    if "." in task_id:
        return task_id.split(".", 1)[0]
    for suffix in ("_run", "_test", "_seed", "_snapshot"):
        if task_id.endswith(suffix):
            return task_id[: -len(suffix)]
    return task_id


def _run_task(shape: DagShape, model: str) -> str | None:
    return next((t for t in shape["tasks"] if model_of(t) == model and not t.endswith("test")), None)


def _check_dag(shape: DagShape, spec: dict[str, Any]) -> list[str]:
    problems = []
    models = {model_of(t) for t in shape["tasks"]}
    problems += [f"missing task for {m}" for m in spec.get("tasks_added", []) if m not in models]
    problems += [f"task for {m} still present" for m in spec.get("tasks_removed", []) if m in models]
    if "schedule" in spec and shape["schedule"] != spec["schedule"]:
        problems.append(f"schedule is {shape['schedule']!r}, want {spec['schedule']!r}")
    for model, ups in (spec.get("upstream") or {}).items():
        task = _run_task(shape, model)
        have = {model_of(u) for u in shape["upstream"].get(task, [])} if task else set()
        problems += [f"{model} does not depend on {u}" for u in ups if u not in have]
    return problems


def unmet(exp: Expectations, shapes: DagShapes) -> dict[str, list[str]]:
    """{dag_id: [problem, ...]} for every expectation not (yet) met. Empty = all met."""
    out = {}
    for dag_id, spec in exp.dags.items():
        problems = ["DAG missing"] if dag_id not in shapes else _check_dag(shapes[dag_id], spec)
        if problems:
            out[dag_id] = problems
    return out
