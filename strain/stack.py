"""The processes under test: Postgres, git-sync stand-in, Airflow scheduler + DAG processor."""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

from common import PG_PASSWORD, PG_PORT, REPO, sh


def airflow_env(work: Path, dbt_bin: Path, parallelism: int) -> dict[str, str]:
    """Airflow + dbt runtime environment, with the [scheduler] values from deploy/airflow/values.yaml."""
    conn = f"postgresql+psycopg2://airflow_dbt:{PG_PASSWORD}@127.0.0.1:{PG_PORT}/airflow"
    return {
        "AIRFLOW_HOME": str(work / "airflow_home"),
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": conn,
        "AIRFLOW__CORE__DAGS_FOLDER": str(work / "git" / "repo" / "dags"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "True",
        "AIRFLOW__CORE__PARALLELISM": str(parallelism),
        "AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG": "16",
        "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT": "60",
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
        "DBT_EXECUTABLE": str(dbt_bin),
        "DBT_PG_HOST": "127.0.0.1",
        "DBT_PG_PORT": str(PG_PORT),
        "DBT_PG_USER": "airflow_dbt",
        "DBT_PG_DBNAME": "analytics",
        "DBT_ENV_SECRET_PG_PASSWORD": PG_PASSWORD,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }


def k8s_pod_env(work: Path) -> dict[str, str]:
    """Scheduler env so every task runs via strain/k8s_pod/airflow (fresh clone per task, like a pod)."""
    real_airflow = shutil.which("airflow")
    if real_airflow is None:
        raise SystemExit("airflow not on PATH")
    return {
        "AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER": "True",
        "PATH": f"{REPO / 'strain/k8s_pod'}:{os.environ['PATH']}",
        "REAL_AIRFLOW": real_airflow,
        "POD_ORIGIN": f"file://{work / 'origin.git'}",
        "POD_ROOT": str(work / "pods"),
        "POD_LOG": str(work / "logs" / "pods.log"),
    }


class Stack:
    """Owns every process the test starts, and stops them all."""

    def __init__(self, work: Path, dbt_venv: Path, parallelism: int) -> None:
        self.work = work
        self.dbt_bin = dbt_venv / "bin" / "dbt"
        self.procs: list[subprocess.Popen[bytes]] = []
        self.pg_data = Path(f"/tmp/dbt-strain-pg-{os.getpid()}")
        self.pg_stop: list[str | Path] = []
        self.vars = airflow_env(work, self.dbt_bin, parallelism)
        os.environ.update(self.vars)
        os.environ["PATH"] = f"{Path(sys.executable).parent}:{os.environ['PATH']}"

    @property
    def dags_folder(self) -> str:
        return self.vars["AIRFLOW__CORE__DAGS_FOLDER"]

    def spawn(self, name: str, cmd: list[str], extra_env: dict[str, str] | None = None) -> None:
        out = (self.work / "logs" / f"{name}.log").open("w")
        env = {**os.environ, **(extra_env or {})}
        self.procs.append(subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, start_new_session=True, env=env))

    def start_postgres(self) -> None:
        pg_bin = Path(sh(["bash", "-c", "ls -d /usr/lib/postgresql/*/bin | sort -V | tail -1"]).stdout.strip())
        as_pg: list[str | Path] = ["runuser", "-u", "postgres", "--"] if os.geteuid() == 0 else []
        self.pg_data.mkdir(mode=0o700)
        if as_pg:
            shutil.chown(self.pg_data, "postgres", "postgres")
        sh([*as_pg, pg_bin / "initdb", "-D", self.pg_data, "-U", "postgres", "--auth=trust", "-E", "UTF8"])
        opts = f"-p {PG_PORT} -c listen_addresses=127.0.0.1 -c max_connections=300 -k /tmp"
        sh([*as_pg, pg_bin / "pg_ctl", "-D", self.pg_data, "-l", f"{self.pg_data}/pg.log", "-w", "start", "-o", opts])
        self.pg_stop = [*as_pg, pg_bin / "pg_ctl", "-D", self.pg_data, "-m", "fast", "stop"]
        psql: list[str | Path] = ["psql", "-h", "127.0.0.1", "-p", str(PG_PORT), "-U", "postgres", "-q", "-c"]
        sh([*psql, f"create role airflow_dbt login password '{PG_PASSWORD}'"])
        sh([*psql, "create database airflow owner airflow_dbt"])
        sh([*psql, "create database analytics owner airflow_dbt"])

    def start_gitsync(self, stale_timeout: int) -> None:
        os.environ["STALE_WORKTREE_TIMEOUT"] = str(stale_timeout)
        origin, root = str(self.work / "origin.git"), str(self.work / "git")
        self.spawn("gitsync", ["bash", str(REPO / "strain/gitsync.sh"), origin, "deploy", root, "5"])
        for _ in range(60):
            if (self.work / "git/repo/dags").exists():
                return
            time.sleep(1)
        raise SystemExit("git-sync stand-in did not produce a checkout within 60s")

    def start_airflow(self, executor_model: str) -> None:
        sh(["airflow", "db", "migrate"])
        sched_env: dict[str, str] = {}
        if executor_model == "k8s":
            (self.work / "pods").mkdir()
            sched_env = k8s_pod_env(self.work)
        self.spawn("scheduler", ["airflow", "scheduler"], extra_env=sched_env)
        self.spawn("dag-processor", ["airflow", "dag-processor"])

    def _signal_all(self, sig: signal.Signals) -> None:
        for p in self.procs:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(p.pid, sig)

    def stop(self) -> None:
        self._signal_all(signal.SIGTERM)
        time.sleep(3)
        self._signal_all(signal.SIGKILL)
        if self.pg_stop:
            sh(self.pg_stop, check=False)
        shutil.rmtree(self.pg_data, ignore_errors=True)
