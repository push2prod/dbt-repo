"""Side-by-side table of e2e reports, one column per load mode (Markdown, for the CI summary).

python -m e2e.compare build/e2e-*/report.json >> "$GITHUB_STEP_SUMMARY"
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

Row = tuple[str, Callable[[dict[str, Any]], object]]


def _load(r: dict[str, Any], key: str) -> object:
    return (r.get("load") or {}).get(key)


def _restarts(r: dict[str, Any]) -> object:
    restarts = _load(r, "restarts")
    if not isinstance(restarts, dict):
        return restarts
    return ", ".join(f"{k} {v}" for k, v in restarts.items() if v) or "0"


def _checks(r: dict[str, Any]) -> str:
    checks = r.get("checks") or {}
    return f"{sum(c['ok'] for c in checks.values())}/{len(checks)}"


ROWS: list[Row] = [
    ("time until all DAGs parsed (s)", lambda r: r.get("baseline_parse_seconds")),
    ("patches visible after sync (s)", lambda r: r.get("patches_visible_seconds")),
    ("worst parse time per DAG file (s)", lambda r: _load(r, "worst_parse_seconds")),
    ("worst full DAG-folder parse (s)", lambda r: _load(r, "worst_total_parse_seconds")),
    ("parse probes that timed out / failed", lambda r: _load(r, "parse_probe_failures")),
    ("max DAG processor heartbeat age (s)", lambda r: _load(r, "max_dag_processor_heartbeat_age")),
    ("max scheduler heartbeat age (s)", lambda r: _load(r, "max_scheduler_heartbeat_age")),
    ("control-plane restarts", _restarts),
    ("max import errors at once", lambda r: _load(r, "max_import_errors")),
    ("min DAGs visible at once", lambda r: _load(r, "min_dags")),
    ("Airflow API failures (samples)", lambda r: _load(r, "api_failures")),
    ("peak running tasks", lambda r: _load(r, "peak_running_tasks")),
    ("failed task instances", lambda r: r.get("failed_tasks")),
    ("checks passed", _checks),
]


def _fmt(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return "∞" if value == float("inf") else f"{value:.1f}"
    return str(value)


def table(reports: list[dict[str, Any]]) -> str:
    modes = [str(r.get("load_mode", "?")) for r in reports]
    lines = ["| metric | " + " | ".join(modes) + " |", "|---|" + "---|" * len(modes)]
    lines += [f"| {name} | " + " | ".join(_fmt(get(r)) for r in reports) + " |" for name, get in ROWS]
    return "\n".join(lines)


def main(paths: list[str]) -> None:
    order = {"manifest": 0, "dbt_ls": 1, "dbt_ls_nocache": 2}
    reports = sorted((json.loads(Path(p).read_text()) for p in paths), key=lambda r: order.get(r.get("load_mode"), 9))
    print("## Cosmos load modes under the e2e stress test\n")
    print("`manifest` = CI-built manifests (this repo). `dbt_ls` = no manifests, Cosmos runs `dbt ls` while")
    print("parsing (its dbt-ls cache on, the default). `dbt_ls_nocache` = same with the cache off.\n")
    print(table(reports))


if __name__ == "__main__":
    main(sys.argv[1:])
