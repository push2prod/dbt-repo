"""The repo side of the scenario: origin, deploys, seeds, and the 5 DAG changes."""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

from common import REPO, DagShapes, log, sh
from stack import Stack


def setup_repo(work: Path) -> Path:
    """origin.git (bare) + dev clone holding the current repo as master."""
    for key in ("GIT_AUTHOR", "GIT_COMMITTER"):
        os.environ[f"{key}_NAME"] = "strain"
        os.environ[f"{key}_EMAIL"] = "strain@example.invalid"
    origin, dev = work / "origin.git", work / "dev"
    sh(["git", "init", "-q", "--bare", "-b", "master", origin])
    sh(["git", "clone", "-q", origin, dev])
    files = sh(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=REPO).stdout
    sh(["bash", "-c", f"cd '{REPO}' && xargs -0 tar cf - | tar xf - -C '{dev}'"], stdin=files)
    sh(["git", "add", "-A"], cwd=dev)
    sh(["git", "commit", "-qm", "initial"], cwd=dev)
    sh(["git", "push", "-q", "origin", "master"], cwd=dev)
    return dev


def seed_warehouse(stack: Stack) -> None:
    """Load every project's seeds once. They stand in for source tables a real warehouse already has."""
    for proj in sorted((stack.work / "git/repo/projects").glob("*/dbt_project.yml")):
        d = proj.parent
        scratch = str(stack.work / "seed-target" / d.name)
        cmd = [str(stack.dbt_bin), "seed", "--project-dir", str(d), "--profiles-dir", str(d), "--profile", d.name]
        cmd += ["--target", "prod", "--target-path", scratch, "--log-path", scratch]
        r = subprocess.run(cmd, text=True, capture_output=True, check=False)
        if r.returncode:
            raise SystemExit(f"dbt seed failed for {d.name}:\n{r.stdout[-2000:]}")


def deploy(stack: Stack, dev: Path) -> float:
    """Run ci/deploy.sh in the dev clone (build + publish to origin's deploy). Returns seconds taken."""
    t = time.time()
    env = {
        **os.environ,
        "SKIP_TESTS": "1",
        "DBT_BIN": str(stack.dbt_bin),
        "PARSE_ENV": str(dev / "strain/parse.env"),
        "OUT": str(stack.work / "build"),
    }
    r = subprocess.run(["bash", "ci/deploy.sh"], cwd=dev, text=True, capture_output=True, env=env, check=False)
    with (stack.work / "logs" / "deploy.log").open("a") as fh:
        fh.write(r.stdout + r.stderr)
    if r.returncode:
        raise SystemExit(f"ci/deploy.sh failed:\n{r.stdout}\n{r.stderr}")
    for line in r.stdout.splitlines():
        if line.startswith(("[build] ", "published")) and any(w in line for w in ("built", "reused", "published")):
            log("  " + line)
    return time.time() - t


def _edit(path: Path, old: str, new: str) -> None:
    text = path.read_text()
    if old not in text:
        raise SystemExit(f"{path}: expected text not found: {old!r}")
    path.write_text(text.replace(old, new))


def apply_changes(dev: Path) -> None:
    """Change 5 DAGs, five different ways, across all five projects, and push to master."""
    p = dev / "projects"
    # 1. dbt_core_accounts: new model -> new task
    (p / "core/models/accounts/accounts_churn.sql").write_text(
        "{{ config(tags=['accounts']) }}\n"
        "select entity_id, events < 2 as churn_risk from {{ ref('dim_accounts_entity') }}\n"
    )
    # 2. dbt_finance_billing: model deleted -> task disappears
    (p / "finance/models/billing/billing_summary.sql").unlink()
    # 3. dbt_marketing_content: selection changed in DAG config only
    _edit(
        dev / "dags/config/marketing.yml",
        'select: ["tag:content"]',
        'select: ["tag:content"]\n    exclude: ["content_summary"]',
    )
    # 4. dbt_product_search: schedule changed
    _edit(dev / "dags/config/product.yml", '"30 7 * * *"', '"15 8 * * *"')
    # 5. dbt_operations_warehouse (not running): new model with a dependency
    (p / "operations/models/warehouse/warehouse_capacity.sql").write_text(
        "{{ config(tags=['warehouse']) }}\nselect day, events * 1.2 as capacity from {{ ref('fct_warehouse_daily') }}\n"
    )
    sh(["git", "add", "-A"], cwd=dev)
    sh(["git", "commit", "-qm", "change 5 DAGs"], cwd=dev)
    sh(["git", "push", "-q", "origin", "master"], cwd=dev)


def changes_visible(dags: DagShapes) -> dict[str, bool]:
    """For each of the 5 changes: is it reflected in the serialized DAGs?"""

    def has(dag_id: str, fragment: str) -> bool:
        shape = dags.get(dag_id)
        return shape is not None and any(fragment in t for t in shape["tasks"])

    def present_without(dag_id: str, fragment: str) -> bool:
        return dag_id in dags and not has(dag_id, fragment)

    wh = dags.get("dbt_operations_warehouse")
    cap = next((t for t in wh["tasks"] if "warehouse_capacity" in t and t.endswith("run")), None) if wh else None
    cap_wired = bool(wh and cap and any("fct_warehouse_daily" in u for u in wh["upstream"].get(cap, [])))
    search = dags.get("dbt_product_search")
    return {
        "dbt_core_accounts: +accounts_churn task": has("dbt_core_accounts", "accounts_churn"),
        "dbt_finance_billing: -billing_summary task": present_without("dbt_finance_billing", "billing_summary"),
        "dbt_marketing_content: -content_summary (exclude)": present_without(
            "dbt_marketing_content", "content_summary"
        ),
        "dbt_product_search: schedule 15 8 * * *": bool(search and search["schedule"] == "15 8 * * *"),
        "dbt_operations_warehouse: +warehouse_capacity downstream of fct": cap_wired,
    }
