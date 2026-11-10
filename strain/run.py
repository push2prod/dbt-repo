#!/usr/bin/env python3
"""Strain test: 20 Cosmos DAGs on a real Airflow 2.10, 10 of them running.

Run with the *Airflow* venv's python. dbt comes from its own venv, as on the image:

    DBT_VENV=/opt/dbt-venv python strain/run.py [--workdir /tmp/dbt-strain]

What it builds (all local, nothing external):
  Postgres         metadata DB + the "warehouse" dbt writes to
  origin.git       stand-in for GitHub: master + deploy
  git-sync         strain/gitsync.sh, tracking `deploy` (atomic symlink swap)
  Airflow 2.10     LocalExecutor scheduler + standalone DAG processor, with the
                   [scheduler] settings from deploy/airflow/values.yaml
                   (min_file_process_interval=600, parsing_processes=3)

Phases:
  1. deploy v1 via ci/deploy.sh -> all 20 DAGs parsed, 0 import errors
  2. load: 10 DAGs kept running back to back (every model is a real
     `dbt run` subprocess against Postgres, up to `parallelism` at once)
  3. while loaded: change 5 DAGs on master, run ci/deploy.sh, git-sync picks it up
  4. assert, sampling every few seconds for the whole run:
       - all 20 DAGs serialized, 0 import errors, at every sample
       - scheduler and DAG processor heartbeats never older than the
         health-check threshold (30s)
       - all 5 changes visible in the serialized DagBag, while the other
         15 DAGs keep the same structure
       - every DAG re-parsed after the deploy despite the 600s interval
         (mtime-triggered), and the time it took
       - no task failures, including tasks in flight during the swap
       - parse time per DAG file under load (independent probe)

Exit 0 = all assertions passed. A report goes to <workdir>/report.json.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PG_PORT = 54329
PG_PASSWORD = "strain"  # runtime secret; differs from the parse-time dummy on purpose
HEALTH_THRESHOLD = 30   # [scheduler] scheduler_health_check_threshold default

# 10 DAGs whose model selections don't overlap. Two DAGs rebuilding the
# same table at the same moment is a real data-layer race, but not what this
# test is about. 4 of the 5 changed DAGs are among them, so the change lands
# on DAGs with runs in flight.
LOAD_DAGS = [
    "dbt_core_accounts", "dbt_core_inventory", "dbt_finance_billing", "dbt_finance_treasury",
    "dbt_marketing_content", "dbt_marketing_audiences", "dbt_product_search", "dbt_product_catalog",
    "dbt_operations_shipping", "dbt_operations_support",
]
CHANGED_DAGS = ["dbt_core_accounts", "dbt_finance_billing", "dbt_marketing_content",
                "dbt_product_search", "dbt_operations_warehouse"]


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def sh(cmd, check=True, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=True, **kw)


# --------------------------------------------------------------------------- setup
class Env:
    def __init__(self, work: Path, dbt_venv: Path, parallelism: int, stale_timeout: int):
        self.work = work
        self.dbt_bin = dbt_venv / "bin" / "dbt"
        self.procs: list[subprocess.Popen] = []
        self.pg_data = Path(f"/tmp/dbt-strain-pg-{os.getpid()}")
        self.stale_timeout = stale_timeout
        home = work / "airflow_home"
        self.vars = {
            "AIRFLOW_HOME": str(home),
            "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"postgresql+psycopg2://airflow_dbt:{PG_PASSWORD}@127.0.0.1:{PG_PORT}/airflow",
            "AIRFLOW__CORE__DAGS_FOLDER": str(work / "git" / "repo" / "dags"),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "True",
            "AIRFLOW__CORE__PARALLELISM": str(parallelism),
            "AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG": "16",
            "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT": "60",
            # deploy/airflow/values.yaml
            "AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL": "600",
            "AIRFLOW__SCHEDULER__FILE_PARSING_SORT_MODE": "modified_time",
            "AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL": "120",
            "AIRFLOW__SCHEDULER__PARSING_PROCESSES": "3",
            "AIRFLOW__SCHEDULER__DAG_FILE_PROCESSOR_TIMEOUT": "120",
            "AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR": "True",
            "AIRFLOW__WEBSERVER__SECRET_KEY": "strain",
            # Cosmos keeps a per-DAG partial-parse cache on local disk; isolate it per run.
            "AIRFLOW__COSMOS__CACHE_DIR": str(work / "cosmos-cache"),
            # task runtime: dbt venv + real (non-dummy) warehouse secret
            "DBT_EXECUTABLE": str(self.dbt_bin),
            "DBT_PG_HOST": "127.0.0.1", "DBT_PG_PORT": str(PG_PORT), "DBT_PG_USER": "airflow_dbt",
            "DBT_PG_DBNAME": "analytics", "DBT_ENV_SECRET_PG_PASSWORD": PG_PASSWORD,
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        }
        os.environ.update(self.vars)
        os.environ["PATH"] = f"{Path(sys.executable).parent}:{os.environ['PATH']}"

    def spawn(self, name: str, cmd: list[str], cwd: Path | None = None) -> subprocess.Popen:
        out = open(self.work / "logs" / f"{name}.log", "w")
        p = subprocess.Popen(cmd, cwd=cwd, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
        self.procs.append(p)
        return p

    def start_postgres(self) -> None:
        pg_bin = Path(sh(["bash", "-c", "ls -d /usr/lib/postgresql/*/bin | sort -V | tail -1"]).stdout.strip())
        as_pg = ["runuser", "-u", "postgres", "--"] if os.geteuid() == 0 else []
        self.pg_data.mkdir(mode=0o700)
        if as_pg:
            shutil.chown(self.pg_data, "postgres", "postgres")
        sh([*as_pg, pg_bin / "initdb", "-D", self.pg_data, "-U", "postgres", "--auth=trust", "-E", "UTF8"])
        sh([*as_pg, pg_bin / "pg_ctl", "-D", self.pg_data, "-l", f"{self.pg_data}/pg.log", "-w", "start",
            "-o", f"-p {PG_PORT} -c listen_addresses=127.0.0.1 -c max_connections=300 -k /tmp"])
        self.pg_stop = [*as_pg, pg_bin / "pg_ctl", "-D", self.pg_data, "-m", "fast", "stop"]
        psql = ["psql", "-h", "127.0.0.1", "-p", str(PG_PORT), "-U", "postgres", "-q", "-c"]
        sh([*psql, f"create role airflow_dbt login password '{PG_PASSWORD}'"])
        sh([*psql, "create database airflow owner airflow_dbt"])
        sh([*psql, "create database analytics owner airflow_dbt"])

    def stop(self) -> None:
        for p in self.procs:
            try:
                os.killpg(p.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        time.sleep(3)
        for p in self.procs:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if hasattr(self, "pg_stop"):
            sh(self.pg_stop, check=False)
        shutil.rmtree(self.pg_data, ignore_errors=True)


def setup_repo(env: Env) -> Path:
    """origin.git (bare) + dev clone holding the current repo as master."""
    git_env = {"GIT_AUTHOR_NAME": "strain", "GIT_AUTHOR_EMAIL": "strain@example.invalid",
               "GIT_COMMITTER_NAME": "strain", "GIT_COMMITTER_EMAIL": "strain@example.invalid"}
    os.environ.update(git_env)
    origin, dev = env.work / "origin.git", env.work / "dev"
    sh(["git", "init", "-q", "--bare", "-b", "master", origin])
    sh(["git", "clone", "-q", origin, dev])
    files = sh(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=REPO).stdout
    sh(["bash", "-c", f"cd '{REPO}' && xargs -0 tar cf - | tar xf - -C '{dev}'"], input=files)
    sh(["git", "add", "-A"], cwd=dev)
    sh(["git", "commit", "-qm", "initial"], cwd=dev)
    sh(["git", "push", "-q", "origin", "master"], cwd=dev)
    return dev


def seed_warehouse(env: Env) -> None:
    """Load every project's seeds once: they stand in for the source tables
    that already exist in a real warehouse. Not part of the DAGs."""
    for proj in sorted((env.work / "git/repo/projects").glob("*/dbt_project.yml")):
        d = proj.parent
        r = subprocess.run([str(env.dbt_bin), "seed", "--project-dir", str(d), "--profiles-dir", str(d),
                            "--profile", d.name, "--target", "prod", "--target-path", str(env.work / "seed-target" / d.name),
                            "--log-path", str(env.work / "seed-target" / d.name)],
                           text=True, capture_output=True)
        if r.returncode:
            raise SystemExit(f"dbt seed failed for {d.name}:\n{r.stdout[-2000:]}")


def deploy(env: Env, dev: Path) -> float:
    t = time.time()
    r = subprocess.run(["bash", "ci/deploy.sh"], cwd=dev, text=True, capture_output=True,
                       env={**os.environ, "SKIP_TESTS": "1", "DBT_BIN": str(env.dbt_bin),
                            "PARSE_ENV": str(dev / "strain/parse.env"), "OUT": str(env.work / "build")})
    (env.work / "logs" / "deploy.log").open("a").write(r.stdout + r.stderr)
    if r.returncode:
        raise SystemExit(f"ci/deploy.sh failed:\n{r.stdout}\n{r.stderr}")
    for line in r.stdout.splitlines():
        if line.startswith(("[build] ", "published", "deploy tree")) and ("built" in line or "reused" in line or "published" in line):
            log("  " + line)
    return time.time() - t


# --------------------------------------------------------------------------- the 5 changes
def apply_changes(dev: Path) -> None:
    """Change 5 DAGs, five different ways, across all five projects."""
    p = dev / "projects"
    # 1. dbt_core_accounts: new model -> new task
    (p / "core/models/accounts/accounts_churn.sql").write_text(
        "{{ config(tags=['accounts']) }}\nselect entity_id, events < 2 as churn_risk from {{ ref('dim_accounts_entity') }}\n")
    # 2. dbt_finance_billing: model deleted -> task disappears
    (p / "finance/models/billing/billing_summary.sql").unlink()
    # 3. dbt_marketing_content: selection changed in DAG config only
    cfg = dev / "dags/config/marketing.yml"
    cfg.write_text(cfg.read_text().replace('select: ["tag:content"]', 'select: ["tag:content"]\n    exclude: ["content_summary"]'))
    # 4. dbt_product_search: schedule changed
    cfg = dev / "dags/config/product.yml"
    cfg.write_text(cfg.read_text().replace('"30 7 * * *"', '"15 8 * * *"'))
    # 5. dbt_operations_warehouse (not running): new model with a dependency
    (p / "operations/models/warehouse/warehouse_capacity.sql").write_text(
        "{{ config(tags=['warehouse']) }}\nselect day, events * 1.2 as capacity from {{ ref('fct_warehouse_daily') }}\n")
    sh(["git", "add", "-A"], cwd=dev)
    sh(["git", "commit", "-qm", "change 5 DAGs"], cwd=dev)
    sh(["git", "push", "-q", "origin", "master"], cwd=dev)


def changes_visible(dags: dict) -> dict[str, bool]:
    def has(dag_id, frag):
        return any(frag in t for t in dags.get(dag_id, {}).get("tasks", []))

    wh = dags.get("dbt_operations_warehouse", {})
    cap = next((t for t in wh.get("tasks", []) if "warehouse_capacity" in t and t.endswith("run")), None)
    return {
        "dbt_core_accounts: +accounts_churn task": has("dbt_core_accounts", "accounts_churn"),
        "dbt_finance_billing: -billing_summary task": bool(dags.get("dbt_finance_billing")) and not has("dbt_finance_billing", "billing_summary"),
        "dbt_marketing_content: -content_summary (exclude)": bool(dags.get("dbt_marketing_content")) and not has("dbt_marketing_content", "content_summary"),
        "dbt_product_search: schedule 15 8 * * *": dags.get("dbt_product_search", {}).get("schedule") == "15 8 * * *",
        "dbt_operations_warehouse: +warehouse_capacity downstream of fct": bool(cap) and any(
            "fct_warehouse_daily" in u for u in wh.get("upstream", {}).get(cap, [])),
    }


# --------------------------------------------------------------------------- Airflow-side observation
def airflow_api():
    from airflow import settings
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.models import DagModel
    from airflow.models.serialized_dag import SerializedDagModel
    from sqlalchemy import text

    return settings, trigger_dag, DagModel, SerializedDagModel, text


def serialized_dags() -> dict:
    settings, _, _, SerializedDagModel, text = airflow_api()
    out = {}
    with settings.Session() as s:
        for (dag_id,) in s.execute(text("select dag_id from serialized_dag")).fetchall():
            dag = SerializedDagModel.get_dag(dag_id, session=s)
            if dag is None:
                continue
            out[dag_id] = {
                "tasks": sorted(dag.task_ids),
                "schedule": str(dag.schedule_interval),
                "upstream": {t.task_id: sorted(t.upstream_task_ids) for t in dag.tasks},
            }
    return out


def sample() -> dict:
    settings, _, _, _, text = airflow_api()
    with settings.Session() as s:
        hb = dict(s.execute(text(
            "select job_type, extract(epoch from now() - max(latest_heartbeat)) from job "
            "where state = 'running' group by job_type")).fetchall())
        ti = dict(s.execute(text("select state, count(*) from task_instance group by state")).fetchall())
        return {
            "t": time.time(),
            "load1": os.getloadavg()[0],
            "serialized": s.execute(text("select count(*) from serialized_dag")).scalar(),
            "import_errors": s.execute(text("select count(*) from import_error")).scalar(),
            "scheduler_hb_age": float(hb.get("SchedulerJob", 1e9)),
            "processor_hb_age": float(hb.get("DagProcessorJob", 1e9)),
            "ti": {str(k): v for k, v in ti.items()},
            "last_parsed": {d: ts.timestamp() for d, ts in s.execute(text(
                "select dag_id, last_parsed_time from dag where last_parsed_time is not null")).fetchall()},
        }


def import_errors() -> list:
    settings, _, _, _, text = airflow_api()
    with settings.Session() as s:
        return [list(r) for r in s.execute(text("select filename, stacktrace from import_error")).fetchall()]


def keep_loaded(started: dict) -> int:
    """Unpause the load DAGs and trigger a new run whenever one has none active."""
    settings, trigger_dag, DagModel, _, text = airflow_api()
    triggered = 0
    with settings.Session() as s:
        active = {d for (d,) in s.execute(text(
            "select distinct dag_id from dag_run where state in ('queued', 'running')")).fetchall()}
        for dag_id in LOAD_DAGS:
            if dag_id in active:
                continue
            dm = s.get(DagModel, dag_id)
            if dm is None:
                continue
            if dm.is_paused:
                dm.is_paused = False
                s.commit()
            n = started.get(dag_id, 0) + 1
            trigger_dag(dag_id, run_id=f"strain_{n}_{int(time.time())}")
            started[dag_id] = n
            triggered += 1
    return triggered


def parse_probe(dags_folder: str) -> dict[str, float]:
    """Time a full DagBag fill in a separate process, like a processor does."""
    code = (
        "import json,logging,sys; logging.disable(logging.CRITICAL)\n"
        "from airflow.models.dagbag import DagBag\n"
        "b=DagBag(sys.argv[1], include_examples=False)\n"
        "print(json.dumps({s.file: s.duration.total_seconds() for s in b.dagbag_stats} | {'_errors': len(b.import_errors)}))"
    )
    r = sh([sys.executable, "-c", code, dags_folder], check=False)
    try:
        return json.loads(r.stdout.strip().splitlines()[-1])
    except Exception:
        return {"_failed": 1.0}


def failure_reasons(log_root: Path) -> dict[str, int]:
    """Group failed task attempts by the first recognisable cause in their log."""
    patterns = {
        "old revision deleted under a running task": "No such file or directory",
        "macro undefined": "is undefined",
        "relation missing": "does not exist",
        "database error": "Database Error",
        "compilation error": "Compilation Error",
    }
    out: dict[str, int] = {}
    for f in log_root.rglob("attempt=*.log"):
        text = f.read_text(errors="replace")
        if "Task failed with exception" not in text and "Marking task as FAILED" not in text:
            continue
        reason = next((k for k, pat in patterns.items() if pat in text), "other")
        out[reason] = out.get(reason, 0) + 1
    return out


# --------------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workdir", type=Path, default=Path("/tmp/dbt-strain"))
    ap.add_argument("--dbt-venv", type=Path, default=Path(os.environ.get("DBT_VENV", "/opt/dbt-venv")))
    ap.add_argument("--parallelism", type=int, default=16)
    ap.add_argument("--warmup", type=int, default=90, help="seconds of load before the change")
    ap.add_argument("--hold", type=int, default=90, help="seconds of load after the change is visible")
    ap.add_argument("--change-timeout", type=int, default=300)
    ap.add_argument("--stale-worktree-timeout", type=int, default=600,
                    help="seconds git-sync keeps the previous checkout. git-sync's default is 0, "
                         "which fails tasks still running from the old revision (try --stale-worktree-timeout 0)")
    args = ap.parse_args()

    work = args.workdir.resolve()
    shutil.rmtree(work, ignore_errors=True)
    (work / "logs").mkdir(parents=True)
    env = Env(work, args.dbt_venv.resolve(), args.parallelism, args.stale_worktree_timeout)
    samples: list[dict] = []
    probes: list[dict] = []
    report: dict = {"config": {k: v for k, v in vars(args).items()}, "checks": {}}
    report["config"] = {k: str(v) for k, v in report["config"].items()}
    failures: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        report["checks"][name] = {"ok": bool(ok), "detail": detail}
        log(f"{'PASS' if ok else 'FAIL'}  {name}{'  -- ' + detail if detail else ''}")
        if not ok:
            failures.append(name)

    try:
        log("starting postgres")
        env.start_postgres()
        log("creating origin + first deploy (ci/deploy.sh)")
        dev = setup_repo(env)
        deploy(env, dev)
        os.environ["STALE_WORKTREE_TIMEOUT"] = str(args.stale_worktree_timeout)
        env.spawn("gitsync", ["bash", str(REPO / "strain/gitsync.sh"), str(work / "origin.git"), "deploy",
                              str(work / "git"), "5"])
        for _ in range(60):
            if (work / "git/repo/dags").exists():
                break
            time.sleep(1)
        log("seeding the warehouse (stand-in for existing source tables)")
        seed_warehouse(env)
        log("airflow db migrate")
        sh(["airflow", "db", "migrate"])
        env.spawn("scheduler", ["airflow", "scheduler"])
        env.spawn("dag-processor", ["airflow", "dag-processor"])

        t0 = time.time()
        while time.time() - t0 < 300:
            s = sample()
            if s["serialized"] >= 20:
                break
            time.sleep(3)
        initial = serialized_dags()
        check("initial: 20 DAGs serialized", len(initial) == 20, f"{len(initial)} after {time.time() - t0:.0f}s")
        check("initial: 0 import errors", not import_errors(), json.dumps(import_errors())[:500])
        if len(initial) != 20:
            raise SystemExit(1)

        log(f"phase 2: load on {len(LOAD_DAGS)} DAGs, warmup {args.warmup}s")
        started: dict[str, int] = {}
        next_probe = 0.0

        def tick() -> None:
            nonlocal next_probe
            keep_loaded(started)
            s = sample()
            samples.append(s)
            if time.time() >= next_probe:
                probes.append({"t": time.time(), "load1": os.getloadavg()[0], **parse_probe(env.vars["AIRFLOW__CORE__DAGS_FOLDER"])})
                next_probe = time.time() + 30

        end = time.time() + args.warmup
        while time.time() < end:
            tick()
            time.sleep(3)
        running = samples[-1]["ti"].get("running", 0)
        log(f"under load: load1={samples[-1]['load1']:.1f} running tasks={running} ti={samples[-1]['ti']}")

        log("phase 3: change 5 DAGs on master and deploy (while loaded)")
        apply_changes(dev)
        t_build = time.time()
        build_secs = deploy(env, dev)
        t_published = time.time()
        log(f"published after {build_secs:.0f}s; waiting for git-sync + DAG processor")
        visible_at = None
        while time.time() - t_published < args.change_timeout:
            tick()
            if all(changes_visible(serialized_dags()).values()):
                visible_at = time.time()
                break
            time.sleep(3)
        after = serialized_dags()
        vis = changes_visible(after)
        for name, ok in vis.items():
            check(f"change visible: {name}", ok)
        if visible_at:
            report["change_latency_seconds"] = {"build_and_publish": round(build_secs, 1),
                                                "publish_to_visible": round(visible_at - t_published, 1)}
            log(f"all 5 changes visible {visible_at - t_published:.0f}s after publish "
                f"({visible_at - t_build:.0f}s after merge, incl. build)")

        unchanged = [d for d in initial if d not in CHANGED_DAGS]
        same = [d for d in unchanged if after.get(d, {}).get("tasks") == initial[d]["tasks"]
                and after.get(d, {}).get("schedule") == initial[d]["schedule"]]
        check("other 15 DAGs unchanged", len(same) == 15, f"{len(same)}/15; differ: {sorted(set(unchanged) - set(same))}")

        log(f"holding load for {args.hold}s after the change")
        end = time.time() + args.hold
        while time.time() < end:
            tick()
            time.sleep(3)

        last = samples[-1]["last_parsed"]
        reparsed = [d for d, ts in last.items() if ts > t_published]
        check("every DAG re-parsed after deploy despite min_file_process_interval=600",
              len(reparsed) == 20, f"{len(reparsed)}/20")

        # whole-run invariants
        bad_count = [s for s in samples if s["serialized"] != 20]
        bad_err = [s for s in samples if s["import_errors"]]
        max_sched = max(s["scheduler_hb_age"] for s in samples)
        max_proc = max(s["processor_hb_age"] for s in samples)
        check("all samples: 20 DAGs serialized", not bad_count, f"{len(samples)} samples, {len(bad_count)} bad")
        check("all samples: 0 import errors", not bad_err, json.dumps(import_errors())[:800] if bad_err else "")
        check(f"scheduler heartbeat always < {HEALTH_THRESHOLD}s", max_sched < HEALTH_THRESHOLD, f"max age {max_sched:.1f}s")
        check(f"DAG processor heartbeat always < {HEALTH_THRESHOLD}s", max_proc < HEALTH_THRESHOLD, f"max age {max_proc:.1f}s")

        final_ti = sample()["ti"]
        peak_running = max(s["ti"].get("running", 0) for s in samples)
        peak_load = max(s["load1"] for s in samples)
        check("strain was real: >= 8 tasks running concurrently at peak", peak_running >= 8,
              f"peak running={peak_running}, peak load1={peak_load:.1f} on {os.cpu_count()} cores")
        succeeded = final_ti.get("success", 0)
        failed = final_ti.get("failed", 0) + final_ti.get("upstream_failed", 0)
        check("tasks ran", succeeded > 50, f"success={succeeded}")
        reasons = failure_reasons(work / "airflow_home" / "logs")
        report["failure_reasons"] = reasons
        check("no failed tasks (incl. tasks in flight during the swap)", failed == 0,
              f"ti states={final_ti}; reasons={reasons}")

        per_file = [{k: v for k, v in p.items() if k.endswith(".py")} for p in probes if "_failed" not in p]
        worst = max((max(p.values()) for p in per_file if p), default=None)
        total = [sum(p.values()) for p in per_file if p]
        report["parse_probe"] = {"probes": len(probes), "worst_file_seconds": worst,
                                 "max_total_seconds": max(total, default=None),
                                 "load1_during_probes": [round(p["load1"], 1) for p in probes]}
        check("parse probe: every DAG file < 5s under load", worst is not None and worst < 5,
              f"worst file {worst:.2f}s, whole folder max {max(total):.2f}s" if worst else "no probe")
        report["peak"] = {"running_tasks": peak_running, "load1": peak_load, "cores": os.cpu_count()}
        report["task_instances"] = final_ti
        report["dag_runs_triggered"] = started
    finally:
        (work / "samples.json").write_text(json.dumps(samples))
        report["failures"] = failures
        (work / "report.json").write_text(json.dumps(report, indent=2, default=str))
        log(f"report: {work / 'report.json'}   logs: {work / 'logs'}")
        env.stop()
    log("RESULT: " + ("PASS" if not failures else f"FAIL ({len(failures)}): {failures}"))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
