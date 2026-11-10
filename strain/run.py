#!/usr/bin/env python3
"""Strain test: 20 Cosmos DAGs on a real Airflow 2.10, 10 of them running.

Run with the *Airflow* venv's python. dbt comes from its own venv, as on the image:

    DBT_VENV=/opt/dbt-venv python strain/run.py [--workdir /tmp/dbt-strain]

What it builds (all local, nothing external), see stack.py / scenario.py:
  Postgres         metadata DB + the "warehouse" dbt writes to
  origin.git       stand-in for GitHub: master + deploy
  git-sync         strain/gitsync.sh, tracking `deploy` (atomic symlink swap)
  Airflow 2.10     scheduler + standalone DAG processor, with the [scheduler]
                   settings from deploy/airflow/values.yaml
  task execution   --executor-model k8s (default): each task in its own fresh
                   clone of deploy, like a KubernetesExecutor pod with a one-time
                   git-sync init container. local: tasks share the git-sync checkout.

Phases:
  1. deploy v1 via ci/deploy.sh -> all 20 DAGs parsed, 0 import errors
  2. load: 10 DAGs kept running back to back (every model a real `dbt run`)
  3. while loaded: change 5 DAGs on master, run ci/deploy.sh, git-sync picks it up
  4. assert over the whole run: 20 DAGs and 0 import errors at every sample,
     healthy heartbeats, all 5 changes visible while the other 15 stay the
     same, every DAG re-parsed despite the 600s interval, no task failures,
     parse time per file under load.

Exit 0 = all assertions passed. A report goes to <workdir>/report.json.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path

from common import CHANGED_DAGS, HEALTH_THRESHOLD, LOAD_DAGS, TOTAL_DAGS, DagShapes, Report, Sample, log
from observe import failure_reasons, import_errors, keep_loaded, parse_probe, pod_stats, sample, serialized_dags
from scenario import apply_changes, changes_visible, deploy, seed_warehouse, setup_repo
from stack import Stack


class Run:
    """State of one strain-test run: samples, probes, checks and the report."""

    def __init__(self, args: argparse.Namespace, stack: Stack) -> None:
        self.args = args
        self.stack = stack
        self.samples: list[Sample] = []
        self.probes: list[dict[str, float]] = []
        self.started: dict[str, int] = {}
        self.failures: list[str] = []
        self.report: Report = {"config": {k: str(v) for k, v in vars(args).items()}, "checks": {}}
        self.next_probe = 0.0

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        self.report["checks"][name] = {"ok": ok, "detail": detail}
        log(f"{'PASS' if ok else 'FAIL'}  {name}{'  -- ' + detail if detail else ''}")
        if not ok:
            self.failures.append(name)

    def tick(self) -> None:
        keep_loaded(self.started)
        self.samples.append(sample())
        if time.time() >= self.next_probe:
            probe = parse_probe(self.stack.dags_folder)
            self.probes.append({"t": time.time(), "load1": os.getloadavg()[0], **probe})
            self.next_probe = time.time() + 30

    def load_for(self, seconds: int) -> None:
        end = time.time() + seconds
        while time.time() < end:
            self.tick()
            time.sleep(3)

    # ------------------------------------------------------------------ phases
    def phase_initial(self) -> DagShapes:
        t0 = time.time()
        while time.time() - t0 < 300 and sample()["serialized"] < TOTAL_DAGS:
            time.sleep(3)
        initial = serialized_dags()
        self.check(
            "initial: 20 DAGs serialized", len(initial) == TOTAL_DAGS, f"{len(initial)} after {time.time() - t0:.0f}s"
        )
        errors = import_errors()
        self.check("initial: 0 import errors", not errors, json.dumps(errors)[:500])
        if len(initial) != TOTAL_DAGS:
            raise SystemExit(1)
        return initial

    def phase_change(self, dev: Path, initial: DagShapes) -> float:
        """Deploy the 5-DAG change under load; return the publish time."""
        log("phase 3: change 5 DAGs on master and deploy (while loaded)")
        apply_changes(dev)
        t_merge = time.time()
        build_secs = deploy(self.stack, dev)
        t_published = time.time()
        log(f"published after {build_secs:.0f}s; waiting for git-sync + DAG processor")
        visible_at = None
        while time.time() - t_published < self.args.change_timeout:
            self.tick()
            if all(changes_visible(serialized_dags()).values()):
                visible_at = time.time()
                break
            time.sleep(3)
        after = serialized_dags()
        for name, ok in changes_visible(after).items():
            self.check(f"change visible: {name}", ok)
        if visible_at:
            latency = {
                "build_and_publish": round(build_secs, 1),
                "publish_to_visible": round(visible_at - t_published, 1),
            }
            self.report["change_latency_seconds"] = latency
            log(
                f"all 5 changes visible {visible_at - t_published:.0f}s after publish "
                f"({visible_at - t_merge:.0f}s after merge, incl. build)"
            )
        self.check_unchanged(initial, after)
        return t_published

    # ------------------------------------------------------------------ checks
    def check_unchanged(self, initial: DagShapes, after: DagShapes) -> None:
        unchanged = [d for d in initial if d not in CHANGED_DAGS]
        same = [
            d
            for d in unchanged
            if d in after
            and after[d]["tasks"] == initial[d]["tasks"]
            and after[d]["schedule"] == initial[d]["schedule"]
        ]
        differ = sorted(set(unchanged) - set(same))
        self.check(
            "other 15 DAGs unchanged", len(same) == len(unchanged), f"{len(same)}/{len(unchanged)}; differ: {differ}"
        )

    def check_parsing(self, t_published: float) -> None:
        reparsed = [d for d, ts in self.samples[-1]["last_parsed"].items() if ts > t_published]
        self.check(
            "every DAG re-parsed after deploy despite min_file_process_interval=600",
            len(reparsed) == TOTAL_DAGS,
            f"{len(reparsed)}/{TOTAL_DAGS}",
        )
        bad_count = [s for s in self.samples if s["serialized"] != TOTAL_DAGS]
        bad_err = [s for s in self.samples if s["import_errors"]]
        max_sched = max(s["scheduler_hb_age"] for s in self.samples)
        max_proc = max(s["processor_hb_age"] for s in self.samples)
        self.check(
            "all samples: 20 DAGs serialized", not bad_count, f"{len(self.samples)} samples, {len(bad_count)} bad"
        )
        self.check("all samples: 0 import errors", not bad_err, json.dumps(import_errors())[:800] if bad_err else "")
        limit = HEALTH_THRESHOLD
        self.check(f"scheduler heartbeat always < {limit}s", max_sched < limit, f"max age {max_sched:.1f}s")
        self.check(f"DAG processor heartbeat always < {limit}s", max_proc < limit, f"max age {max_proc:.1f}s")

    def check_tasks(self) -> None:
        final_ti = sample()["ti"]
        peak_running = max(s["ti"].get("running", 0) for s in self.samples)
        peak_load = max(s["load1"] for s in self.samples)
        cores = os.cpu_count()
        self.check(
            "strain was real: >= 8 tasks running concurrently at peak",
            peak_running >= 8,
            f"peak running={peak_running}, peak load1={peak_load:.1f} on {cores} cores",
        )
        succeeded = final_ti.get("success", 0)
        failed = final_ti.get("failed", 0) + final_ti.get("upstream_failed", 0)
        self.check("tasks ran", succeeded > 50, f"success={succeeded}")
        reasons = failure_reasons(self.stack.work / "airflow_home" / "logs")
        self.check(
            "no failed tasks (incl. tasks in flight during the swap)",
            failed == 0,
            f"ti states={final_ti}; reasons={reasons}",
        )
        self.report.update(
            failure_reasons=reasons,
            task_instances=final_ti,
            dag_runs_triggered=self.started,
            peak={"running_tasks": peak_running, "load1": peak_load, "cores": cores},
        )
        pods = pod_stats(self.stack.work / "logs" / "pods.log")
        if pods:
            self.report["pods"] = pods
            revs, bad = pods["by_revision"], pods["nonzero_exit"]
            log(f"task pods: {pods['total']} across revisions {revs}, non-zero exits: {bad}")

    def check_probes(self) -> None:
        per_file = [{k: v for k, v in p.items() if k.endswith(".py")} for p in self.probes if "_failed" not in p]
        per_file = [p for p in per_file if p]
        worst = max((max(p.values()) for p in per_file), default=None)
        total = max((sum(p.values()) for p in per_file), default=None)
        self.report["parse_probe"] = {
            "probes": len(self.probes),
            "worst_file_seconds": worst,
            "max_total_seconds": total,
            "load1_during_probes": [round(p["load1"], 1) for p in self.probes],
        }
        detail = (
            f"worst file {worst:.2f}s, whole folder max {total:.2f}s" if worst is not None and total else "no probe"
        )
        self.check("parse probe: every DAG file < 5s under load", worst is not None and worst < 5, detail)

    def execute(self) -> None:
        log("starting postgres")
        self.stack.start_postgres()
        log("creating origin + first deploy (ci/deploy.sh)")
        dev = setup_repo(self.stack.work)
        deploy(self.stack, dev)
        self.stack.start_gitsync(self.args.stale_worktree_timeout)
        log("seeding the warehouse (stand-in for existing source tables)")
        seed_warehouse(self.stack)
        log("starting airflow")
        self.stack.start_airflow(self.args.executor_model)
        initial = self.phase_initial()
        log(f"phase 2: load on {len(LOAD_DAGS)} DAGs, warmup {self.args.warmup}s")
        self.load_for(self.args.warmup)
        last = self.samples[-1]
        log(f"under load: load1={last['load1']:.1f} running tasks={last['ti'].get('running', 0)} ti={last['ti']}")
        t_published = self.phase_change(dev, initial)
        log(f"holding load for {self.args.hold}s after the change")
        self.load_for(self.args.hold)
        self.check_parsing(t_published)
        self.check_tasks()
        self.check_probes()

    def save(self) -> None:
        work = self.stack.work
        (work / "samples.json").write_text(json.dumps(self.samples))
        self.report["failures"] = self.failures
        (work / "report.json").write_text(json.dumps(self.report, indent=2, default=str))
        log(f"report: {work / 'report.json'}   logs: {work / 'logs'}")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workdir", type=Path, default=Path("/tmp/dbt-strain"))
    ap.add_argument("--dbt-venv", type=Path, default=Path(os.environ.get("DBT_VENV", "/opt/dbt-venv")))
    ap.add_argument("--parallelism", type=int, default=16)
    ap.add_argument("--warmup", type=int, default=90, help="seconds of load before the change")
    ap.add_argument("--hold", type=int, default=90, help="seconds of load after the change is visible")
    ap.add_argument("--change-timeout", type=int, default=300)
    ap.add_argument(
        "--executor-model",
        choices=["k8s", "local"],
        default="k8s",
        help="k8s: every task runs from its own fresh clone of deploy, like a KubernetesExecutor pod with a "
        "one-time git-sync init container (strain/k8s_pod/airflow). local: tasks run from the scheduler's "
        "git-sync checkout (Celery/Local executor)",
    )
    ap.add_argument(
        "--stale-worktree-timeout",
        type=int,
        default=600,
        help="seconds git-sync keeps the previous checkout. git-sync's default is 0, which fails tasks still "
        "running from the old revision with --executor-model local",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    work = args.workdir.resolve()
    shutil.rmtree(work, ignore_errors=True)
    (work / "logs").mkdir(parents=True)
    stack = Stack(work, args.dbt_venv.resolve(), args.parallelism)
    run = Run(args, stack)
    try:
        run.execute()
    finally:
        run.save()
        stack.stop()
    log("RESULT: " + ("PASS" if not run.failures else f"FAIL ({len(run.failures)}): {run.failures}"))
    return 0 if not run.failures else 1


if __name__ == "__main__":
    sys.exit(main())
