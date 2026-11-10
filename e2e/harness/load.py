"""Stress driver and observations: keep every DAG running, sample Airflow, probe parse times.

Built to keep observing a stack that is struggling: every call has a timeout,
and a failed call is recorded in the sample (``api_ok=False``) instead of
ending the run, so the report shows what went wrong.
"""

from __future__ import annotations

import contextlib
import json
import time
from dataclasses import dataclass, field
from typing import Any

import requests
from kubernetes import client
from kubernetes.stream import stream

from e2e.harness import settings as s
from e2e.harness.airflow import AirflowAPI, iso_ts
from e2e.harness.log import log

_PROBE_TIMEOUT = 180  # seconds for one `airflow dags report` in the DAG processor pod
COMPONENTS = ("scheduler", "dag-processor", "webserver")


def _pod_name(core: client.CoreV1Api, component: str) -> str | None:
    pods = core.list_namespaced_pod(s.NAMESPACE, label_selector=f"component={component}").items
    return pods[0].metadata.name if pods and pods[0].metadata and pods[0].metadata.name else None


# Runs inside the DAG processor pod and parses the way Airflow 2.10's processor
# does: Airflow is already imported (the manager pre-imports `airflow.*` before
# forking), then a fresh forked child per DAG file. Each child imports the DAG
# file and with it dbt_factory + Cosmos, so the numbers include that per-parse
# import cost, as in production.
_PROBE = """
import json, logging, os, sys, time
from pathlib import Path
logging.disable(logging.CRITICAL)
from airflow.configuration import conf
from airflow.models.dagbag import DagBag
folder = conf.get("core", "dags_folder")
sys.path.insert(0, folder)
out = {}
for f in sorted(Path(folder).glob("*.py")):
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(r)
        t = time.monotonic()
        DagBag(str(f), include_examples=False)
        os.write(w, str(time.monotonic() - t).encode())
        os._exit(0)
    os.close(w)
    secs = os.read(r, 64)
    os.waitpid(pid, 0)
    out[f.name] = float(secs or "inf")
print(json.dumps(out))
"""


def parse_probe(api: client.ApiClient) -> dict[str, float]:
    """Parse every DAG file in the DAG processor pod, one forked child per file: {file: seconds}.

    {"_failed": 1} if the probe times out or errors (itself a symptom worth recording).
    """
    core = client.CoreV1Api(api)
    name = _pod_name(core, "dag-processor")
    if name is None:
        return {"_failed": 1.0}
    cmd = ["timeout", str(_PROBE_TIMEOUT), "python", "-c", _PROBE]
    try:
        out = stream(
            core.connect_get_namespaced_pod_exec,
            name,
            s.NAMESPACE,
            container="dag-processor",
            command=cmd,
            stderr=False,
            stdin=False,
            stdout=True,
            tty=False,
            _request_timeout=_PROBE_TIMEOUT + 30,
        )
        line = next((x for x in reversed(out.splitlines()) if x.startswith("{")), "")
        durations: dict[str, float] = json.loads(line) if line else {}
    except (client.ApiException, ValueError, OSError):
        return {"_failed": 1.0}
    if not durations:
        return {"_failed": 1.0}
    return {str(f).rsplit("/", 1)[-1]: float(d) for f, d in durations.items()}


def status_line(smp: Sample, triggered: int) -> str:
    if not smp.api_ok:
        return "Airflow API did not answer"
    hb = smp.heartbeat
    return (
        f"{smp.dags} DAGs, {smp.active_runs} active runs, {smp.running_tasks} running tasks, "
        f"heartbeat scheduler {hb.get('scheduler', float('inf')):.0f}s / processor "
        f"{hb.get('dag_processor', float('inf')):.0f}s, import errors {smp.import_errors}, "
        f"restarts {sum(smp.restarts.values())}, runs triggered {triggered}"
    )


def restarts(api: client.ApiClient) -> dict[str, int]:
    """Container restart count per control-plane component (liveness-probe kills, crashes)."""
    core = client.CoreV1Api(api)
    out = {}
    for comp in COMPONENTS:
        pods = core.list_namespaced_pod(s.NAMESPACE, label_selector=f"component={comp}").items
        out[comp] = sum(cs.restart_count for p in pods for cs in ((p.status and p.status.container_statuses) or []))
    return out


@dataclass
class Sample:
    t: float
    api_ok: bool
    dags: int = 0
    import_errors: int = 0
    heartbeat: dict[str, float] = field(default_factory=dict)
    active_runs: int = 0
    running_tasks: int = 0
    last_parsed: dict[str, float] = field(default_factory=dict)
    restarts: dict[str, int] = field(default_factory=dict)


@dataclass
class Load:
    """Keeps all DAGs busy and records what Airflow looked like while it did."""

    airflow: AirflowAPI
    api: client.ApiClient
    samples: list[Sample] = field(default_factory=list)
    probes: list[dict[str, float]] = field(default_factory=list)
    triggered: int = 0
    _next_probe: float = 0.0
    _next_status: float = 0.0

    def start(self) -> None:
        for d in self.airflow.dags():
            self.airflow.unpause(d["dag_id"])

    def keep_running(self) -> None:
        active = self.airflow.active_run_dags()
        for d in self.airflow.dags():
            if d["dag_id"] not in active:
                self.airflow.trigger(d["dag_id"])
                self.triggered += 1

    def sample(self) -> Sample:
        smp = Sample(t=time.time(), api_ok=True)
        try:
            smp.restarts = restarts(self.api)
        except client.ApiException:
            smp.restarts = {}
        try:
            dags = self.airflow.dags()
            last = {d["dag_id"]: iso_ts(d.get("last_parsed_time")) for d in dags}
            smp.dags = len(dags)
            smp.last_parsed = {k: v for k, v in last.items() if v is not None}
            smp.import_errors = len(self.airflow.import_errors())
            smp.heartbeat = self.airflow.heartbeat_ages()
            smp.active_runs = len(self.airflow.active_run_dags())
            smp.running_tasks = self.airflow.task_count(["running"])
        except (requests.RequestException, KeyError, ValueError):
            smp.api_ok = False
        self.samples.append(smp)
        return smp

    def tick(self) -> Sample:
        # On API errors skip the trigger round; the sample below records the API as unhealthy.
        with contextlib.suppress(requests.RequestException, KeyError, ValueError):
            self.keep_running()
        smp = self.sample()
        if time.time() >= self._next_probe:
            probe = parse_probe(self.api)
            self.probes.append(probe)
            self._next_probe = time.time() + 30
            files = {k: v for k, v in probe.items() if k.endswith(".py")}
            worst = max(files, key=lambda k: files[k]) if files else None
            log(
                f"  parse probe: worst {worst} {files[worst]:.2f}s, all files {sum(files.values()):.2f}s"
                if worst
                else "  parse probe: FAILED / timed out"
            )
        if time.time() >= self._next_status:
            log("  load: " + status_line(smp, self.triggered))
            self._next_status = time.time() + 30
        return smp

    def run_for(self, seconds: float, why: str) -> None:
        log(f"{why}: keeping all DAGs running for {seconds:.0f}s")
        end = time.time() + seconds
        while time.time() < end:
            self.tick()
            time.sleep(5)

    def summary(self) -> dict[str, Any]:
        ok = [x for x in self.samples if x.api_ok]
        files = [{k: v for k, v in p.items() if k.endswith(".py")} for p in self.probes]
        good = [f for f in files if f]

        def peak(key: str) -> float | None:
            vals = [x.heartbeat.get(key, float("inf")) for x in ok]
            return max(vals) if vals else None

        return {
            "samples": len(self.samples),
            "api_failures": len(self.samples) - len(ok),
            "peak_active_dag_runs": max((x.active_runs for x in ok), default=0),
            "peak_running_tasks": max((x.running_tasks for x in ok), default=0),
            "max_scheduler_heartbeat_age": peak("scheduler"),
            "max_dag_processor_heartbeat_age": peak("dag_processor"),
            "max_import_errors": max((x.import_errors for x in ok), default=None),
            "min_dags": min((x.dags for x in ok), default=0),
            "restarts": self.samples[-1].restarts if self.samples else {},
            "parse_probes": len(self.probes),
            "parse_probe_failures": len(self.probes) - len(good),
            "worst_parse_seconds": max((max(f.values()) for f in good), default=None),
            "worst_total_parse_seconds": max((sum(f.values()) for f in good), default=None),
            "dag_runs_triggered": self.triggered,
        }
