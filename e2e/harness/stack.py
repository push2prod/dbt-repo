"""The running test stack (cluster + Airflow + sync + load) and the check bookkeeping."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

from e2e.harness import settings as s
from e2e.harness.airflow import AirflowAPI, create_namespace, iso_ts
from e2e.harness.cluster import Cluster
from e2e.harness.gitserver import Syncer, deploy_git_server
from e2e.harness.load import Load

DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt-venv/bin/dbt")


class Checks:
    """Named pass/fail results, kept in the report. Enforced only when settings.ENFORCE."""

    def __init__(self, report: dict[str, Any]) -> None:
        self.results: dict[str, dict[str, Any]] = report.setdefault("checks", {})

    def __call__(self, name: str, ok: bool, detail: object = "") -> None:
        self.results[name] = {"ok": bool(ok), "detail": str(detail)}
        print(f"[e2e] {'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)

    def enforce(self) -> None:
        failed = {k: v["detail"] for k, v in self.results.items() if not v["ok"]}
        if s.ENFORCE:
            assert not failed, f"failed checks: {failed}"


@dataclass
class Stack:
    workdir: Path
    cluster: Cluster
    airflow: AirflowAPI
    syncer: Syncer
    load: Load
    report: dict[str, Any] = field(default_factory=lambda: {"load_mode": s.LOAD_MODE})

    @property
    def checks(self) -> Checks:
        return Checks(self.report)

    def generate(self, source: Path, out: Path, previous: Path | None = None) -> dict[str, str]:
        """The CI step (python -m manifest.generate) with a parse env matching this cluster.

        In the dbt_ls comparison modes the manifests are then removed again: the
        bundle looks like the old setup, where Cosmos had to run `dbt ls` itself.
        """
        parse_env = self.workdir / "parse.env"
        parse_env.write_text(
            f"DBT_PG_HOST={self.cluster.pg_ip()}\nDBT_PG_PORT=5432\nDBT_PG_USER={s.PG_USER}\n"
            "DBT_PG_DBNAME=analytics\nDBT_ENV_SECRET_PG_PASSWORD=dummy-at-parse-time\n"
        )
        cmd = [sys.executable, "-m", "manifest.generate", "--source", str(source), "--out", str(out)]
        cmd += ["--parse-env", str(parse_env), *(["--previous", str(previous)] if previous else [])]
        env = {**os.environ, "DBT_BIN": DBT_BIN, "PYTHONDONTWRITEBYTECODE": "1"}
        subprocess.run(cmd, cwd=s.REPO, env=env, check=True)
        result: dict[str, str] = json.loads(out.with_name(out.name + ".summary.json").read_text())["projects"]
        if s.LOAD_MODE != "manifest":
            for target in out.glob("projects/*/target"):
                shutil.rmtree(target)
        return result


def bring_up(workdir: Path, cluster: Cluster) -> Stack:
    cluster.build_airflow_image()
    cluster.load_image(s.AIRFLOW_IMAGE)
    assert cluster.api is not None
    create_namespace(cluster.api)
    deploy_git_server(cluster.api)
    syncer = Syncer(workdir, cluster.host, cluster.host_port(s.GIT_NODEPORT))
    airflow = AirflowAPI(cluster.host, cluster.host_port(s.WEBSERVER_NODEPORT))
    return Stack(workdir, cluster, airflow, syncer, Load(airflow, cluster.api))


def wait_for(predicate: Callable[[], bool], timeout: float, tick: Callable[[], object] | None = None) -> float | None:
    """Poll until ``predicate()`` holds; seconds waited, or None on timeout. API errors count as 'not yet'."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        if tick:
            tick()
        try:
            if predicate():
                return time.time() - t0
        except (requests.RequestException, KeyError, ValueError):
            pass
        time.sleep(5)
    return None


def parse_latency(airflow: AirflowAPI, dag_ids: set[str], t_sync: float) -> float | None:
    """Sync -> Airflow serialized the changed DAGs, from Airflow's own last_parsed_time.

    Independent of how often the test polls: the DAG processor stamps each DAG when
    it parses it, and with min_file_process_interval=600 nothing re-parses them
    again within the test, so the stamp marks the parse that picked up the change.
    """
    try:
        dags = airflow.dags()
    except requests.RequestException:
        return None
    stamps = [iso_ts(d.get("last_parsed_time")) for d in dags if d["dag_id"] in dag_ids]
    if len(stamps) != len(dag_ids) or any(ts is None for ts in stamps):
        return None
    return max(ts for ts in stamps if ts is not None) - t_sync
