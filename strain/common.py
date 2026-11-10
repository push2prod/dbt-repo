"""Shared constants, types and helpers for the strain test."""

from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, TypedDict

REPO = Path(__file__).resolve().parent.parent
PG_PORT = 54329
PG_PASSWORD = "strain"  # runtime secret; differs from the parse-time dummy on purpose
HEALTH_THRESHOLD = 30  # [scheduler] scheduler_health_check_threshold default
TOTAL_DAGS = 20

# 10 DAGs whose model selections don't overlap. Two DAGs rebuilding the
# same table at the same moment is a real data-layer race, but not what this
# test is about. 4 of the 5 changed DAGs are among them, so the change lands
# on DAGs with runs in flight.
LOAD_DAGS = [
    "dbt_core_accounts",
    "dbt_core_inventory",
    "dbt_finance_billing",
    "dbt_finance_treasury",
    "dbt_marketing_content",
    "dbt_marketing_audiences",
    "dbt_product_search",
    "dbt_product_catalog",
    "dbt_operations_shipping",
    "dbt_operations_support",
]
CHANGED_DAGS = [
    "dbt_core_accounts",
    "dbt_finance_billing",
    "dbt_marketing_content",
    "dbt_product_search",
    "dbt_operations_warehouse",
]


class DagShape(TypedDict):
    """The parts of a serialized DAG the test compares."""

    tasks: list[str]
    schedule: str
    upstream: dict[str, list[str]]


class Sample(TypedDict):
    """One observation of Airflow's state."""

    t: float
    load1: float
    serialized: int
    import_errors: int
    scheduler_hb_age: float
    processor_hb_age: float
    ti: dict[str, int]
    last_parsed: dict[str, float]


DagShapes = dict[str, DagShape]
Report = dict[str, Any]


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def sh(
    cmd: list[str | Path], check: bool = True, cwd: Path | None = None, stdin: str | None = None
) -> subprocess.CompletedProcess[str]:
    """Run a command, capturing text output. ``stdin`` is fed to the process."""
    return subprocess.run([str(c) for c in cmd], check=check, text=True, capture_output=True, cwd=cwd, input=stdin)
