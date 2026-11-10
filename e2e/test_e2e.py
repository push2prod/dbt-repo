"""E2E stress test: a fresh Airflow 2.10 on K3s (testcontainers), Cosmos DAGs from manifests.

    DBT_BIN=/opt/dbt-venv/bin/dbt pytest e2e                         # ~15 min, needs Docker (privileged)
    E2E_LOAD_MODE=dbt_ls DBT_BIN=... pytest e2e                       # comparison: Cosmos runs `dbt ls`

1. Start Postgres + K3s, build and load the Airflow image, run git-sync against
   an in-cluster git server, install the official Helm chart (KubernetesExecutor).
2. Baseline: generate the bundle for e2e/fixtures with the same generator CI uses,
   sync it; all DAGs must appear with no import errors.
3. Stress: keep every DAG running. Apply all e2e/patches, regenerate (only
   changed projects re-parsed), sync; every expect.yaml must show up in Airflow
   in time, with healthy heartbeats, fast parsing, no import errors, no failed tasks.

Every check is recorded in the report. In ``manifest`` mode failed checks fail
the test; in the ``dbt_ls*`` comparison modes they are only recorded (the stack
is expected to struggle, and the point is to measure how much).
"""

from __future__ import annotations

import json
import shutil
import time
from collections.abc import Iterator
from dataclasses import asdict

import pytest

from e2e.harness import settings as s
from e2e.harness.airflow import install_chart
from e2e.harness.cluster import Cluster
from e2e.harness.diag import dump
from e2e.harness.patches import FIXTURES, Expectations, apply_all, load_expectations, unmet
from e2e.harness.stack import DBT_BIN, Stack, bring_up, parse_latency, wait_for

BASELINE_TIMEOUT = 900  # the dbt_ls modes can take many minutes to parse everything
PATCH_TIMEOUT = 600


def _write_report(st: Stack) -> None:
    st.report["load"] = st.load.summary()
    st.report["samples"] = [asdict(x) for x in st.load.samples]
    s.REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    s.REPORT_PATH.write_text(json.dumps(st.report, indent=2, default=str))
    brief = {k: v for k, v in st.report.items() if k != "samples"}
    print(f"[e2e] report ({s.REPORT_PATH}):\n{json.dumps(brief, indent=2, default=str)}", flush=True)


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory, request: pytest.FixtureRequest) -> Iterator[Stack]:
    workdir = tmp_path_factory.mktemp("e2e")
    with Cluster(workdir) as cluster:
        try:
            st = bring_up(workdir, cluster)
        except Exception:
            if cluster.api is not None:
                dump(cluster.api, s.REPORT_PATH.parent / "e2e-diag")
            raise
        try:
            yield st
        finally:
            failed = any(not c["ok"] for c in st.report.get("checks", {}).values())
            if (request.session.testsfailed or failed) and cluster.api is not None:
                dump(cluster.api, s.REPORT_PATH.parent / "e2e-diag")
            _write_report(st)


def test_baseline_dags_parse(stack: Stack) -> None:
    bundle = stack.workdir / "bundle-v1"
    stack.generate(FIXTURES, bundle)
    stack.syncer.sync(bundle)
    assert stack.cluster.api is not None
    install_chart(stack.cluster.api, stack.cluster.pg_ip())
    stack.airflow.wait_until_up()
    waited = wait_for(lambda: len(stack.airflow.dags()) >= s.EXPECTED_DAGS, BASELINE_TIMEOUT)
    stack.report["baseline_parse_seconds"] = round(waited, 1) if waited is not None else None
    check = stack.checks
    check(f"baseline: {s.EXPECTED_DAGS} DAGs parsed", waited is not None, f"{len(stack.airflow.dags())} DAGs")
    errors = stack.airflow.import_errors()
    check("baseline: no import errors", not errors, [e.get("filename") for e in errors])
    check.enforce()


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
    if s.LOAD_MODE == "manifest":  # without manifests in the previous bundle everything is rebuilt
        stack.checks(
            "only changed projects re-parsed",
            set(stack.report["rebuilt_projects"]) == exp.rebuilt_projects,
            stack.report["rebuilt_projects"],
        )

    t_sync = time.time()
    stack.syncer.sync(stack.workdir / "bundle-v2")
    polled = wait_for(lambda: not unmet(exp, stack.airflow.shapes()), PATCH_TIMEOUT, stack.load.tick)
    visible = parse_latency(stack.airflow, set(exp.dags), t_sync) if polled is not None else None
    stack.report["patches_visible_seconds"] = round(visible, 1) if visible is not None else None
    stack.report["patches_seen_by_poll_seconds"] = round(polled, 1) if polled is not None else None
    stack.load.run_for(s.HOLD_SECONDS)
    _check_health(stack, exp, t_sync, visible)
    stack.checks.enforce()


def _check_health(stack: Stack, exp: Expectations, t_sync: float, visible: float | None) -> None:
    check, summary = stack.checks, stack.load.summary()
    ok = [x for x in stack.load.samples if x.api_ok]
    reparsed = [d for d, ts in (ok[-1].last_parsed if ok else {}).items() if ts > t_sync]
    try:
        failed = stack.airflow.task_count(["failed", "upstream_failed"])
        missing = unmet(exp, stack.airflow.shapes())
    except Exception as e:  # report whatever the API state is, don't crash the measurement
        failed, missing = -1, {"api": [repr(e)]}
    stack.report.update(reparsed_after_sync=len(reparsed), failed_tasks=failed)
    limit = s.VISIBLE_WITHIN_SECONDS
    check(f"patches visible within {limit}s", visible is not None and visible <= limit, visible)
    check("all patch expectations met", not missing, missing)
    check("Airflow API answered every sample", summary["api_failures"] == 0, summary["api_failures"])
    check(f"{s.EXPECTED_DAGS} DAGs at every sample", summary["min_dags"] == s.EXPECTED_DAGS, summary["min_dags"])
    check("no import errors at any sample", summary["max_import_errors"] == 0, summary["max_import_errors"])
    for key in ("max_scheduler_heartbeat_age", "max_dag_processor_heartbeat_age"):
        age = summary[key]
        check(f"{key} < {s.HEARTBEAT_MAX_AGE_SECONDS}s", age is not None and age < s.HEARTBEAT_MAX_AGE_SECONDS, age)
    check("no control-plane restarts", not any(summary["restarts"].values()), summary["restarts"])
    worst = summary["worst_parse_seconds"]
    check(
        f"parse <= {s.PARSE_FILE_MAX_SECONDS}s per file",
        worst is not None and worst <= s.PARSE_FILE_MAX_SECONDS,
        f"worst {worst}, probe failures {summary['parse_probe_failures']}",
    )
    peak = summary["peak_active_dag_runs"]
    check("all DAGs running at once", peak == s.EXPECTED_DAGS, peak)
    check("every DAG re-parsed after the sync", len(reparsed) == s.EXPECTED_DAGS, len(reparsed))
    check("no failed tasks", failed == 0, failed)
