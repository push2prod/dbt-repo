"""Stress driver and observations: keep every DAG running, sample Airflow, probe parse times."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any

from kubernetes import client
from kubernetes.stream import stream

from e2e.harness import settings as s
from e2e.harness.airflow import AirflowAPI, iso_ts

_DURATION = re.compile(r"(?:(\d+):)?(\d+):(\d+(?:\.\d+)?)")


def _seconds(value: str) -> float:
    """'0:00:01.234567' (timedelta str) -> 1.234567."""
    m = _DURATION.fullmatch(value.strip())
    if not m:
        return float(value)
    h, mi, sec = m.groups()
    return int(h or 0) * 3600 + int(mi) * 60 + float(sec)


def parse_probe(api: client.ApiClient) -> dict[str, float]:
    """Parse every DAG file in the DAG processor pod (`airflow dags report`): {file: seconds}."""
    core = client.CoreV1Api(api)
    pods = core.list_namespaced_pod(s.NAMESPACE, label_selector="component=dag-processor").items
    if not pods or pods[0].metadata is None or not pods[0].metadata.name:
        return {}
    name = pods[0].metadata.name
    cmd = ["airflow", "dags", "report", "-o", "json"]
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
    )
    start = out.find("[")
    rows: list[dict[str, Any]] = json.loads(out[start:]) if start >= 0 else []
    return {str(r["file"]).rsplit("/", 1)[-1]: _seconds(str(r["duration"])) for r in rows}


@dataclass
class Sample:
    t: float
    dags: int
    import_errors: int
    heartbeat: dict[str, float]
    active_runs: int
    running_tasks: int
    last_parsed: dict[str, float]


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
        dags = self.airflow.dags()
        last = {d["dag_id"]: iso_ts(d.get("last_parsed_time")) for d in dags}
        smp = Sample(
            t=time.time(),
            dags=len(dags),
            import_errors=len(self.airflow.import_errors()),
            heartbeat=self.airflow.heartbeat_ages(),
            active_runs=len(self.airflow.active_run_dags()),
            running_tasks=self.airflow.task_count(["running"]),
            last_parsed={k: v for k, v in last.items() if v is not None},
        )
        self.samples.append(smp)
        return smp

    def tick(self) -> Sample:
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
        worst = max((max(p.values()) for p in self.probes if p), default=None)
        return {
            "samples": len(self.samples),
            "peak_active_dag_runs": max((x.active_runs for x in self.samples), default=0),
            "peak_running_tasks": max((x.running_tasks for x in self.samples), default=0),
            "max_scheduler_heartbeat_age": max((x.heartbeat["scheduler"] for x in self.samples), default=None),
            "max_dag_processor_heartbeat_age": max((x.heartbeat["dag_processor"] for x in self.samples), default=None),
            "max_import_errors": max((x.import_errors for x in self.samples), default=None),
            "min_dags": min((x.dags for x in self.samples), default=0),
            "parse_probes": len(self.probes),
            "worst_parse_seconds": worst,
            "dag_runs_triggered": self.triggered,
        }
