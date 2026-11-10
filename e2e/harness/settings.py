"""Images, ports, thresholds and environment knobs for the e2e test."""

from __future__ import annotations

import os
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# Images. Override with E2E_* env vars (e.g. to point at an internal registry).
K3S_IMAGE = os.environ.get("E2E_K3S_IMAGE", "rancher/k3s:v1.31.4-k3s1")
POSTGRES_IMAGE = os.environ.get("E2E_POSTGRES_IMAGE", "postgres:16-alpine")
AIRFLOW_IMAGE = "dbt-repo-airflow:e2e"  # built by the test from e2e/Dockerfile
# The git server runs on the same image (it has git + `git daemon`): already in k3s, no pull.
GIT_SERVER_IMAGE = os.environ.get("E2E_GIT_SERVER_IMAGE", AIRFLOW_IMAGE)
CHART_URL = os.environ.get(
    "E2E_CHART_URL", "https://archive.apache.org/dist/airflow/helm-chart/1.16.0/airflow-1.16.0.tgz"
)

# Optional, for runners behind a TLS-inspecting proxy or Docker Hub rate limits:
#   E2E_EXTRA_CA_FILE    PEM bundle k3s' containerd must trust when pulling images
#   E2E_REGISTRY_MIRROR  e.g. https://mirror.gcr.io, used by k3s for docker.io
EXTRA_CA_FILE = os.environ.get("E2E_EXTRA_CA_FILE")
REGISTRY_MIRROR = os.environ.get("E2E_REGISTRY_MIRROR")

# NodePorts published by the k3s container.
WEBSERVER_NODEPORT = 30080
GIT_NODEPORT = 30418
NAMESPACE = "airflow"

# Warehouse / metadata DB (one Postgres testcontainer, two databases).
PG_USER = "airflow_dbt"
PG_PASSWORD = "e2e-secret"  # runtime secret; the parse env uses a dummy on purpose
PG_ALIAS = "warehouse"

# Stress-test shape and pass criteria.
PARALLELISM = int(os.environ.get("E2E_PARALLELISM", "12"))  # concurrent task pods
WARMUP_SECONDS = int(os.environ.get("E2E_WARMUP_SECONDS", "120"))
HOLD_SECONDS = int(os.environ.get("E2E_HOLD_SECONDS", "120"))
VISIBLE_WITHIN_SECONDS = 60  # every patch visible in Airflow after the sync
PARSE_FILE_MAX_SECONDS = 5.0  # per DAG file, measured in the DAG processor pod under load
HEARTBEAT_MAX_AGE_SECONDS = 30  # scheduler + DAG processor
EXPECTED_DAGS = 20

REPORT_PATH = Path(os.environ.get("E2E_REPORT", REPO / "build" / "e2e-report.json"))
