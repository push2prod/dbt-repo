"""Every DAG file imports cleanly, quickly, and without starting a process.

The last point is the main one: Cosmos in DBT_LS mode (or AUTOMATIC with a
missing manifest) runs `dbt ls` at parse time. Starting a process during
import fails the test here instead of starving the DAG processor in prod.
"""

from __future__ import annotations

import subprocess
import sys
import time

import pytest

PARSE_BUDGET_SECONDS = 5.0  # per file, generous for cold CI runners


@pytest.fixture(scope="module")
def dagbag(deploy_dir, monkeypatch_module):
    monkeypatch_module.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    monkeypatch_module.setenv("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    # Import Airflow + Cosmos *before* the guard goes up: their own import-time
    # probes (e.g. platform.architecture() shelling out to `file`) are not the
    # DAG files' fault. After this, only the DAG files' own work runs.
    import cosmos  # noqa: F401
    from airflow.models.dagbag import DagBag

    def forbidden(*args, **kwargs):
        raise AssertionError(f"subprocess started at DAG parse time: {args[:1]}")

    for name in ("Popen", "run", "call", "check_call", "check_output"):
        monkeypatch_module.setattr(subprocess, name, forbidden)

    sys.path.insert(0, str(deploy_dir / "dags"))
    start = time.monotonic()
    bag = DagBag(dag_folder=str(deploy_dir / "dags"), include_examples=False, safe_mode=True)
    bag.total_seconds = time.monotonic() - start
    return bag


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, "\n\n".join(f"{k}:\n{v}" for k, v in dagbag.import_errors.items())


def test_expected_dags_present(dagbag, deploy_dir):
    from conftest import load_dag_specs, projects

    dag_specs = load_dag_specs(deploy_dir / "dags")
    expected = {s["dag_id"] for p in projects(deploy_dir) for s in dag_specs(p)}
    assert expected <= set(dagbag.dag_ids)


def test_parse_time_budget(dagbag):
    slow = {s.file: s.duration for s in dagbag.dagbag_stats if _secs(s.duration) > PARSE_BUDGET_SECONDS}
    assert not slow, f"DAG files over {PARSE_BUDGET_SECONDS}s parse budget: {slow}"


def test_every_dag_has_tasks(dagbag):
    empty = [d for d, dag in dagbag.dags.items() if not dag.tasks]
    assert not empty, f"DAGs rendered with no tasks (selector matches nothing?): {empty}"


def _secs(d) -> float:
    return d.total_seconds() if hasattr(d, "total_seconds") else float(d)
