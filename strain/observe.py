"""Read Airflow's state from its metadata DB, and keep the load DAGs busy.

Airflow is imported inside the functions: the module must be importable before
the strain stack has set AIRFLOW_HOME and the DB connection.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from common import LOAD_DAGS, DagShapes, Sample, sh

FAILURE_PATTERNS = {
    "old revision deleted under a running task": "No such file or directory",
    "task missing from the pod's revision (parse/run skew)": "TaskNotFound",
    "macro undefined": "is undefined",
    "relation missing": "does not exist",
    "database error": "Database Error",
    "compilation error": "Compilation Error",
}


def _rows(sql: str) -> list[Any]:
    from airflow import settings
    from sqlalchemy import text

    with settings.Session() as s:
        return list(s.execute(text(sql)).fetchall())


def serialized_dags() -> DagShapes:
    from airflow import settings
    from airflow.models.serialized_dag import SerializedDagModel

    out: DagShapes = {}
    with settings.Session() as s:
        for (dag_id,) in _rows("select dag_id from serialized_dag"):
            dag = SerializedDagModel.get_dag(dag_id, session=s)
            if dag is None:
                continue
            out[dag_id] = {
                "tasks": sorted(dag.task_ids),
                "schedule": str(dag.schedule_interval),
                "upstream": {t.task_id: sorted(t.upstream_task_ids) for t in dag.tasks},
            }
    return out


def sample() -> Sample:
    heartbeat_sql = (
        "select job_type, extract(epoch from now() - max(latest_heartbeat)) from job "
        "where state = 'running' group by job_type"
    )
    hb = {str(k): float(v) for k, v in _rows(heartbeat_sql)}
    ti = {str(k): int(v) for k, v in _rows("select state, count(*) from task_instance group by state")}
    parsed_sql = "select dag_id, last_parsed_time from dag where last_parsed_time is not null"
    return {
        "t": time.time(),
        "load1": os.getloadavg()[0],
        "serialized": int(_rows("select count(*) from serialized_dag")[0][0]),
        "import_errors": int(_rows("select count(*) from import_error")[0][0]),
        "scheduler_hb_age": hb.get("SchedulerJob", 1e9),
        "processor_hb_age": hb.get("DagProcessorJob", 1e9),
        "ti": ti,
        "last_parsed": {str(d): ts.timestamp() for d, ts in _rows(parsed_sql)},
    }


def import_errors() -> list[list[str]]:
    return [[str(f), str(st)] for f, st in _rows("select filename, stacktrace from import_error")]


def keep_loaded(started: dict[str, int]) -> int:
    """Unpause the load DAGs and trigger a new run for each that has none active."""
    from airflow import settings
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.models import DagModel

    active = {str(d) for (d,) in _rows("select distinct dag_id from dag_run where state in ('queued', 'running')")}
    triggered = 0
    with settings.Session() as s:
        for dag_id in LOAD_DAGS:
            dm = s.get(DagModel, dag_id)
            if dag_id in active or dm is None:
                continue
            if dm.is_paused:
                dm.is_paused = False
                s.commit()
            n = started.get(dag_id, 0) + 1
            trigger_dag(dag_id, run_id=f"strain_{n}_{int(time.time())}")
            started[dag_id] = n
            triggered += 1
    return triggered


def parse_probe(dags_folder: str) -> dict[str, float]:
    """Time a full DagBag fill in a separate process, like a DAG processor does."""
    code = (
        "import json,logging,sys; logging.disable(logging.CRITICAL)\n"
        "from airflow.models.dagbag import DagBag\n"
        "b=DagBag(sys.argv[1], include_examples=False)\n"
        "print(json.dumps({s.file: s.duration.total_seconds() for s in b.dagbag_stats}"
        " | {'_errors': len(b.import_errors)}))"
    )
    r = sh([sys.executable, "-c", code, dags_folder], check=False)
    try:
        return {str(k): float(v) for k, v in json.loads(r.stdout.strip().splitlines()[-1]).items()}
    except (ValueError, IndexError):
        return {"_failed": 1.0}


def failure_reasons(log_root: Path) -> dict[str, int]:
    """Group failed task attempts by the first recognisable cause in their log."""
    out: dict[str, int] = {}
    for f in log_root.rglob("attempt=*.log"):
        text = f.read_text(errors="replace")
        if "Task failed with exception" not in text and "Marking task as FAILED" not in text:
            continue
        reason = next((k for k, pat in FAILURE_PATTERNS.items() if pat in text), "other")
        out[reason] = out.get(reason, 0) + 1
    return out


def pod_stats(pod_log: Path) -> dict[str, Any] | None:
    """Summarise strain/k8s_pod/airflow's log: pods per revision and non-zero exits."""
    if not pod_log.exists():
        return None
    pods = [dict(kv.split("=", 1) for kv in line.split()[1:]) for line in pod_log.read_text().splitlines()]
    by_rev: dict[str, int] = {}
    for pod in pods:
        by_rev[pod["rev"]] = by_rev.get(pod["rev"], 0) + 1
    return {"total": len(pods), "by_revision": by_rev, "nonzero_exit": sum(pod["rc"] != "0" for pod in pods)}
