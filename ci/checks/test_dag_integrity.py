"""Every DAG file imports cleanly, quickly, and without starting a process.

The last point is the main one: Cosmos in DBT_LS mode (or AUTOMATIC with a
missing manifest) runs `dbt ls` at parse time. Starting a process during
import fails the test here instead of starving the DAG processor in prod.
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn

import pytest
from conftest import DAGS_DIR, PROJECTS, load_dag_specs

if TYPE_CHECKING:
    from airflow.models.dagbag import DagBag

PARSE_BUDGET_SECONDS = 5.0  # per file, generous for cold CI runners


@pytest.fixture(scope="module")
def monkeypatch_module() -> Iterator[pytest.MonkeyPatch]:
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture(scope="module")
def dagbag(bundle_dir: Path, monkeypatch_module: pytest.MonkeyPatch) -> DagBag:
    monkeypatch_module.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    monkeypatch_module.setenv("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    # Import Airflow + Cosmos *before* the guard goes up: their own import-time
    # probes (e.g. platform.architecture() shelling out to `file`) are not the
    # DAG files' fault. After this, only the DAG files' own work runs.
    import cosmos  # noqa: F401
    from airflow.models.dagbag import DagBag

    def forbidden(*args: object, **kwargs: object) -> NoReturn:
        raise AssertionError(f"subprocess started at DAG parse time: {args[:1]}")

    for name in ("Popen", "run", "call", "check_call", "check_output"):
        monkeypatch_module.setattr(subprocess, name, forbidden)

    sys.path.insert(0, str(DAGS_DIR))
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False, safe_mode=True)


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, "\n\n".join(f"{k}:\n{v}" for k, v in dagbag.import_errors.items())


def test_expected_dags_present(dagbag: DagBag) -> None:
    # A project whose DAG file is missing (no build_dags("<name>") call) fails here.
    dag_specs = load_dag_specs(DAGS_DIR)
    expected = {s["dag_id"] for p in PROJECTS for s in dag_specs(p)}
    missing = expected - set(dagbag.dag_ids)
    assert not missing, f"DAGs in the DAG configs but not created by any DAG file: {sorted(missing)}"


def test_parse_time_budget(dagbag: DagBag) -> None:
    slow = {s.file: s.duration for s in dagbag.dagbag_stats if _secs(s.duration) > PARSE_BUDGET_SECONDS}
    assert not slow, f"DAG files over {PARSE_BUDGET_SECONDS}s parse budget: {slow}"


def test_every_dag_has_tasks(dagbag: DagBag) -> None:
    empty = [d for d, dag in dagbag.dags.items() if not dag.tasks]
    assert not empty, f"DAGs rendered with no tasks (selector matches nothing?): {empty}"


def _secs(d: timedelta | float) -> float:
    return d.total_seconds() if isinstance(d, timedelta) else float(d)
