"""patches/ still apply and do what their expect.yaml says, without a cluster.

Applies all patches to a repo copy, regenerates against the built bundle (as
the e2e does), checks which projects were re-parsed, then loads the patched
bundle's DAGs in a fresh process and evaluates every expectation with the same
code the e2e uses. A patch that drifted from the projects fails here in
seconds instead of in the 15-minute e2e.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from test_generate_reuse import SOURCE, _built, _copy_repo, _generate

from e2e.harness.patches import DagShapes, apply_all, load_expectations, unmet

# Runs in a subprocess so `dbt_factory` is imported from the patched bundle.
SHAPES_SCRIPT = """
import json, logging, sys
logging.disable(logging.CRITICAL)
sys.path.insert(0, sys.argv[1])
from airflow.models.dagbag import DagBag
bag = DagBag(sys.argv[1], include_examples=False)
assert not bag.import_errors, bag.import_errors
print(json.dumps({d: {"tasks": sorted(dag.task_ids), "schedule": str(dag.schedule_interval),
                      "upstream": {t.task_id: sorted(t.upstream_task_ids) for t in dag.tasks}}
                  for d, dag in bag.dags.items()}))
"""


def _dag_shapes(dags_dir: Path) -> DagShapes:
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    r = subprocess.run(
        [sys.executable, "-c", SHAPES_SCRIPT, str(dags_dir)], env=env, capture_output=True, text=True, check=False
    )
    assert r.returncode == 0, r.stderr[-3000:]
    shapes: DagShapes = json.loads(r.stdout.strip().splitlines()[-1])
    return shapes


def test_patches_apply_and_meet_expectations(bundle_dir: Path, tmp_path: Path) -> None:
    repo = _copy_repo(tmp_path / "repo")
    applied = apply_all(repo / SOURCE)
    assert applied, "no patches found"
    exp = load_expectations()

    out = tmp_path / "bundle"
    result = _generate(repo, bundle_dir, out)
    assert _built(result) == exp.rebuilt_projects, result

    before = _dag_shapes(bundle_dir / "dags")
    assert unmet(exp, before), "expectations already met before patching: they test nothing"
    after = _dag_shapes(out / "dags")
    assert unmet(exp, after) == {}
    assert sorted(after) == sorted(before), "patches must not add or remove DAGs"
