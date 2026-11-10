"""E2E stress test: a fresh Airflow 2.10 on K3s (testcontainers), Cosmos DAGs from manifests.

    DBT_BIN=/opt/dbt-venv/bin/dbt pytest e2e        # ~15 min, needs Docker (privileged containers)

1. Start Postgres + K3s, build and load the Airflow image, run git-sync against
   an in-cluster git server, install the official Helm chart (KubernetesExecutor).
2. Baseline: generate the bundle for e2e/fixtures with the same generator CI uses,
   sync it, and require all DAGs to appear with no import errors.
3. Stress: keep every DAG running. Apply all e2e/patches, regenerate (only
   changed projects re-parsed), sync, and require every expect.yaml to show up
   in Airflow within the time limit, with healthy heartbeats, fast parsing,
   no import errors and no failed tasks throughout.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pytest

from e2e.harness import settings as s
from e2e.harness.airflow import AirflowAPI, create_namespace, install_chart
from e2e.harness.cluster import Cluster
from e2e.harness.diag import dump
from e2e.harness.gitserver import Syncer, deploy_git_server
from e2e.harness.load import Load
from e2e.harness.patches import FIXTURES, apply_all, load_expectations, unmet

DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt-venv/bin/dbt")


@dataclass
class Stack:
    workdir: Path
    cluster: Cluster
    airflow: AirflowAPI
    syncer: Syncer
    load: Load
    report: dict[str, Any] = field(default_factory=dict)

    def generate(self, source: Path, out: Path, previous: Path | None = None) -> dict[str, str]:
        """The CI step: python -m manifest.generate, with a parse env matching this cluster."""
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
        return result


def _bring_up(workdir: Path, cluster: Cluster) -> Stack:
    cluster.build_airflow_image()
    cluster.load_image(s.AIRFLOW_IMAGE)
    assert cluster.api is not None
    create_namespace(cluster.api)
    deploy_git_server(cluster.api)
    syncer = Syncer(workdir, cluster.host, cluster.host_port(s.GIT_NODEPORT))
    airflow = AirflowAPI(cluster.host, cluster.host_port(s.WEBSERVER_NODEPORT))
    return Stack(workdir, cluster, airflow, syncer, Load(airflow, cluster.api))


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory, request: pytest.FixtureRequest) -> Iterator[Stack]:
    workdir = tmp_path_factory.mktemp("e2e")
    with Cluster(workdir) as cluster:
        st = _bring_up(workdir, cluster)
        try:
            yield st
        finally:
            if request.session.testsfailed and cluster.api is not None:
                dump(cluster.api, s.REPORT_PATH.parent / "e2e-diag")
            st.report["load"] = st.load.summary()
            st.report["samples"] = [asdict(x) for x in st.load.samples]
            s.REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
            s.REPORT_PATH.write_text(json.dumps(st.report, indent=2, default=str))
            print(f"[e2e] report: {s.REPORT_PATH}")


def _wait(predicate: Callable[[], bool], timeout: float, what: str, tick: Callable[[], object] | None = None) -> float:
    """Poll until ``predicate()`` is truthy; return seconds waited."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        if tick:
            tick()
        if predicate():
            return time.time() - t0
        time.sleep(5)
    raise AssertionError(f"timed out after {timeout:.0f}s waiting for {what}")


def test_baseline_dags_parse(stack: Stack) -> None:
    bundle = stack.workdir / "bundle-v1"
    stack.generate(FIXTURES, bundle)
    stack.syncer.sync(bundle)
    assert stack.cluster.api is not None
    install_chart(stack.cluster.api, stack.cluster.pg_ip())
    stack.airflow.wait_until_up()
    waited = _wait(lambda: len(stack.airflow.dags()) >= s.EXPECTED_DAGS, 600, f"{s.EXPECTED_DAGS} DAGs")
    stack.report["baseline_parse_seconds"] = round(waited, 1)
    assert len(stack.airflow.dags()) == s.EXPECTED_DAGS
    assert stack.airflow.import_errors() == []


def test_patches_under_load(stack: Stack) -> None:
    stack.cluster.seed_warehouse(stack.workdir / "bundle-v1" / "projects", DBT_BIN)
    stack.load.start()
    stack.load.run_for(s.WARMUP_SECONDS)

    source = stack.workdir / "fixtures-patched"
    shutil.copytree(FIXTURES, source)
    stack.report["patches"] = apply_all(source)
    exp = load_expectations()
    built = stack.generate(source, stack.workdir / "bundle-v2", previous=stack.workdir / "bundle-v1")
    stack.report["rebuilt_projects"] = sorted(p for p, v in built.items() if v == "built")
    assert set(stack.report["rebuilt_projects"]) == exp.rebuilt_projects

    t_sync = time.time()
    stack.syncer.sync(stack.workdir / "bundle-v2")
    visible = _wait(lambda: not unmet(exp, stack.airflow.shapes()), 300, "patch expectations", stack.load.tick)
    stack.report["patches_visible_seconds"] = round(visible, 1)
    stack.load.run_for(s.HOLD_SECONDS)
    _assert_healthy(stack, t_sync, visible)


def _assert_healthy(stack: Stack, t_sync: float, visible: float) -> None:
    summary = stack.load.summary()
    last = stack.load.samples[-1]
    reparsed = [d for d, ts in last.last_parsed.items() if ts > t_sync]
    failed = stack.airflow.task_count(["failed", "upstream_failed"])
    stack.report.update(reparsed_after_sync=len(reparsed), failed_tasks=failed)
    assert visible <= s.VISIBLE_WITHIN_SECONDS, f"patches took {visible:.0f}s to show up"
    assert summary["min_dags"] == s.EXPECTED_DAGS, "a DAG disappeared during the run"
    assert summary["max_import_errors"] == 0
    assert summary["max_scheduler_heartbeat_age"] < s.HEARTBEAT_MAX_AGE_SECONDS
    assert summary["max_dag_processor_heartbeat_age"] < s.HEARTBEAT_MAX_AGE_SECONDS
    assert summary["worst_parse_seconds"] is not None
    assert summary["worst_parse_seconds"] <= s.PARSE_FILE_MAX_SECONDS
    assert summary["peak_active_dag_runs"] == s.EXPECTED_DAGS, "not every DAG was running at once"
    assert len(reparsed) == s.EXPECTED_DAGS, f"only {len(reparsed)} DAGs re-parsed after the sync"
    assert failed == 0, f"{failed} failed task instances"
