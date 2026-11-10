"""Install Airflow (official Helm chart) into k3s, and talk to it through its REST API.

The chart is installed by k3s' built-in Helm controller (a ``HelmChart``
resource), so no helm binary is needed. Values are the production
deploy/airflow/values.yaml with e2e/values-e2e.yaml and a few runtime values
(addresses, the test image) layered on top.
"""

from __future__ import annotations

import base64
import copy
import time
from datetime import datetime
from typing import Any

import requests
import yaml
from kubernetes import client

from e2e.harness import settings as s
from e2e.harness.gitserver import IN_CLUSTER_URL
from e2e.harness.patches import DagShape, DagShapes


def deep_merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    """Recursive dict merge; lists and scalars in ``over`` replace those in ``base``."""
    out = copy.deepcopy(base)
    for k, v in over.items():
        out[k] = deep_merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def chart_values(pg_ip: str) -> dict[str, Any]:
    prod = yaml.safe_load((s.REPO / "deploy/airflow/values.yaml").read_text())
    e2e = yaml.safe_load((s.REPO / "e2e/values-e2e.yaml").read_text())
    repo, tag = s.AIRFLOW_IMAGE.split(":")
    runtime: dict[str, Any] = {
        "images": {"airflow": {"repository": repo, "tag": tag, "pullPolicy": "IfNotPresent"}},
        "data": {
            "metadataConnection": {
                "user": s.PG_USER,
                "pass": s.PG_PASSWORD,
                "protocol": "postgresql",
                "host": pg_ip,
                "port": 5432,
                "db": "airflow",
                "sslmode": "disable",
            }
        },
        "dags": {"gitSync": {"repo": IN_CLUSTER_URL}},
        "env": [
            {"name": "DBT_EXECUTABLE", "value": "/opt/dbt-venv/bin/dbt"},
            {"name": "DBT_PG_HOST", "value": pg_ip},
            {"name": "DBT_PG_PORT", "value": "5432"},
            {"name": "DBT_PG_USER", "value": s.PG_USER},
            {"name": "DBT_PG_DBNAME", "value": "analytics"},
            {"name": "DBT_ENV_SECRET_PG_PASSWORD", "value": s.PG_PASSWORD},
        ],
        "config": {"core": {"parallelism": str(s.PARALLELISM)}},
    }
    if s.LOAD_MODE != "manifest":  # comparison: Cosmos runs `dbt ls` while parsing
        runtime["env"].append({"name": "COSMOS_LOAD_MODE", "value": "dbt_ls"})
        cache = "False" if s.LOAD_MODE == "dbt_ls_nocache" else "True"
        runtime["config"]["cosmos"] = {"enable_cache_dbt_ls": cache}
    return deep_merge(deep_merge(prod, e2e), runtime)


def install_chart(api: client.ApiClient, pg_ip: str) -> None:
    chart = requests.get(s.CHART_URL, timeout=120)
    chart.raise_for_status()
    body = {
        "apiVersion": "helm.cattle.io/v1",
        "kind": "HelmChart",
        "metadata": {"name": "airflow", "namespace": "kube-system"},
        "spec": {
            "chartContent": base64.b64encode(chart.content).decode(),
            "targetNamespace": s.NAMESPACE,
            "valuesContent": yaml.safe_dump(chart_values(pg_ip)),
            "timeout": "20m",
        },
    }
    client.CustomObjectsApi(api).create_namespaced_custom_object(
        "helm.cattle.io", "v1", "kube-system", "helmcharts", body
    )


def create_namespace(api: client.ApiClient) -> None:
    client.CoreV1Api(api).create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=s.NAMESPACE)))


def iso_ts(value: str | None) -> float | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() if value else None


class AirflowAPI:
    """The few Airflow 2.10 REST API calls the test needs (basic auth, chart's default admin user)."""

    def __init__(self, host: str, port: int) -> None:
        self.base = f"http://{host}:{port}"
        self.http = requests.Session()
        self.http.auth = ("admin", "admin")

    def _get(self, path: str, **params: str | int | list[str]) -> dict[str, Any]:
        r = self.http.get(f"{self.base}/api/v1{path}", params=params, timeout=30)
        r.raise_for_status()
        body: dict[str, Any] = r.json()
        return body

    def wait_until_up(self, timeout: int = 900) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if self.http.get(f"{self.base}/health", timeout=5).ok and self._get("/dags", limit=1) is not None:
                    return
            except requests.RequestException:
                pass
            time.sleep(5)
        raise TimeoutError(f"Airflow API not up after {timeout}s")

    def dags(self) -> list[dict[str, Any]]:
        dags: list[dict[str, Any]] = self._get("/dags", limit=100, only_active="true")["dags"]
        return dags

    def import_errors(self) -> list[dict[str, Any]]:
        errors: list[dict[str, Any]] = self._get("/importErrors", limit=100)["import_errors"]
        return errors

    def heartbeat_ages(self) -> dict[str, float]:
        """Seconds since the scheduler's / DAG processor's last heartbeat (inf if unknown)."""
        health = self.http.get(f"{self.base}/health", timeout=10).json()
        now = time.time()
        sched = iso_ts(health.get("scheduler", {}).get("latest_scheduler_heartbeat"))
        proc = iso_ts((health.get("dag_processor") or {}).get("latest_dag_processor_heartbeat"))
        return {
            "scheduler": now - sched if sched else float("inf"),
            "dag_processor": now - proc if proc else float("inf"),
        }

    def shape(self, dag_id: str) -> DagShape:
        tasks = self._get(f"/dags/{dag_id}/tasks")["tasks"]
        upstream: dict[str, list[str]] = {t["task_id"]: [] for t in tasks}
        for t in tasks:
            for down in t["downstream_task_ids"]:
                upstream.setdefault(down, []).append(t["task_id"])
        sched = self._get(f"/dags/{dag_id}/details").get("schedule_interval") or {}
        return {
            "tasks": sorted(upstream),
            "upstream": {k: sorted(v) for k, v in upstream.items()},
            "schedule": str(sched.get("value", "")),
        }

    def shapes(self) -> DagShapes:
        return {d["dag_id"]: self.shape(d["dag_id"]) for d in self.dags()}

    def unpause(self, dag_id: str) -> None:
        r = self.http.patch(
            f"{self.base}/api/v1/dags/{dag_id}",
            params={"update_mask": "is_paused"},
            json={"is_paused": False},
            timeout=30,
        )
        r.raise_for_status()

    def trigger(self, dag_id: str) -> None:
        self.http.post(f"{self.base}/api/v1/dags/{dag_id}/dagRuns", json={}, timeout=30).raise_for_status()

    def active_run_dags(self) -> set[str]:
        runs = self._get("/dags/~/dagRuns", state=["queued", "running"], limit=100)["dag_runs"]
        return {r["dag_id"] for r in runs}

    def task_count(self, states: list[str]) -> int:
        r = self.http.post(
            f"{self.base}/api/v1/dags/~/dagRuns/~/taskInstances/list",
            json={"state": states, "page_limit": 1},
            timeout=30,
        )
        r.raise_for_status()
        return int(r.json()["total_entries"])
