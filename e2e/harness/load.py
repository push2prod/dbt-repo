"""Stress driver and observations: keep every DAG running, sample Airflow, probe parse times.

Built to keep observing a stack that is falling over (the dbt_ls comparison
modes): every call has a timeout, and a failed call is recorded in the sample
(``api_ok=False``) instead of ending the run.
"""

from __future__ import annotations

import contextlib
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any

import requests
from kubernetes import client
from kubernetes.stream import stream

from e2e.harness import settings as s
from e2e.harness.airflow import AirflowAPI, iso_ts

_DURATION = re.compile(r"(?:(\d+):)?(\d+):(\d+(?:\.\d+)?)")
_PROBE_TIMEOUT = 180  # seconds for one `airflow dags report` in the DAG processor pod
COMPONENTS = ("scheduler", "dag-processor", "webserver")


def _seconds(value: str) -> float:
    """'0:00:01.234567' (timedelta str) -> 1.234567."""
    m = _DURATION.fullmatch(value.strip())
    if not m:
        return float(value)
    h, mi, sec = m.groups()
    return int(h or 0) * 3600 + int(mi) * 60 + float(sec)


def _pod_name(core: client.CoreV1Api, component: str) -> str | None:
    pods = core.list_namespaced_pod(s.NAMESPACE, label_selector=f"component={component}").items
    return pods[0].metadata.name if pods and pods[0].metadata and pods[0].metadata.name else None


def parse_probe(api: client.ApiClient) -> dict[str, float]:
    """Parse every DAG file in the DAG processor pod (`airflow dags report`): {file: seconds}.

    {"_failed": 1} if the probe times out or errors (itself a symptom worth recording).
    """
    core = client.CoreV1Api(api)
    name = _pod_name(core, "dag-processor")
    if name is None:
        return {"_failed": 1.0}
    cmd = ["timeout", str(_PROBE_TIMEOUT), "airflow", "dags", "report", "-o", "json"]
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
        start = out.find("[")
        rows: list[dict[str, Any]] = json.loads(out[start:]) if start >= 0 else []
    except (client.ApiException, ValueError, OSError):
        return {"_failed": 1.0}
    if not rows:
        return {"_failed": 1.0}
    return {str(r["file"]).rsplit("/", 1)[-1]: _seconds(str(r["duration"])) for r in rows}


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
            self.probes.append(parse_probe(self.api))
            self._next_probe = time.time() + 30
        return smp

    def run_for(self, seconds: float) -> None:
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
