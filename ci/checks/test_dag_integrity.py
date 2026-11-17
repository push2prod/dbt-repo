"""Every DAG file imports cleanly, quickly, without starting a process, and creates its DAGs.

The subprocess guard is the main point: Cosmos in DBT_LS mode (or AUTOMATIC
with a missing manifest) runs `dbt ls` at parse time. Starting a process
during import fails the parse here instead of starving the DAG processor in
prod (see the `dagbag` fixture in conftest.py).
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from conftest import DAGS_DIR, PROJECTS, dbt_dags

if TYPE_CHECKING:
    from airflow.models.dagbag import DagBag

PARSE_BUDGET_SECONDS = 5.0  # per file, generous for cold CI runners


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, "\n\n".join(f"{k}:\n{v}" for k, v in dagbag.import_errors.items())


def test_every_dbt_dag_reaches_airflow(dagbag: DagBag) -> None:
    """dbt_dag(...) called, but Airflow didn't collect the DAG: its result isn't
    assigned to a module-level name, or safe mode skipped the whole file (it must
    contain both "airflow" and "dag")."""
    import dbt_factory  # the bundle's copy: the dagbag fixture put DAGS_DIR first on sys.path

    dropped = sorted(set(dbt_factory.CREATED) - set(dagbag.dag_ids))
    assert not dropped, f"dbt_dag(...) created these DAGs, but Airflow didn't collect them: {dropped}"

    files = sorted(p for p in DAGS_DIR.rglob("*.py") if "dbt_factory" not in p.parts and "dbt_dag(" in p.read_text())
    with_dags = {Path(dag.fileloc).resolve() for dag in dagbag.dags.values()}
    skipped = [p.relative_to(DAGS_DIR).as_posix() for p in files if p.resolve() not in with_dags]
    assert not skipped, f"DAG files that call dbt_dag(...) but create no DAG (no 'airflow' in the file?): {skipped}"


def test_dbt_dags_use_the_shipped_manifest(dagbag: DagBag) -> None:
    from cosmos.constants import LoadMode

    bad = []
    for dag_id, dag in dbt_dags(dagbag).items():
        graph = dag.dbt_graph
        manifest = graph.project.manifest_path
        if graph.render_config.load_method != LoadMode.DBT_MANIFEST:
            bad.append(f"{dag_id}: load_method {graph.render_config.load_method} (must be DBT_MANIFEST)")
        elif manifest is None or not Path(manifest).is_file():
            bad.append(f"{dag_id}: no manifest at {manifest}")
        if graph.project.project_name not in PROJECTS:
            bad.append(f"{dag_id}: project {graph.project.project_name!r} is not in projects.json")
    assert not bad, "\n".join(bad)


def test_parse_time_budget(dagbag: DagBag) -> None:
    slow = {s.file: s.duration for s in dagbag.dagbag_stats if _secs(s.duration) > PARSE_BUDGET_SECONDS}
    assert not slow, f"DAG files over {PARSE_BUDGET_SECONDS}s parse budget: {slow}"


def test_every_dag_has_tasks(dagbag: DagBag) -> None:
    empty = [d for d, dag in dagbag.dags.items() if not dag.tasks]
    assert not empty, f"DAGs rendered with no tasks (selector matches nothing?): {empty}"


def _secs(d: timedelta | float) -> float:
    return d.total_seconds() if isinstance(d, timedelta) else float(d)
