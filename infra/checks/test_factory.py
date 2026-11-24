"""dbt_task_group(...) per-group profile overrides (database / schema / profile_args).

Builds a throwaway DAG with the deploy tree's factory (the `dagbag` fixture
puts its root first on sys.path) and inspects each group's ProfileConfig:
what Cosmos renders into profiles.yml at task time.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytest
from conftest import PROJECTS

if TYPE_CHECKING:
    from airflow.models.dagbag import DagBag


def _profile_args(group: Any) -> dict[str, Any]:
    """The profile_args of the mapping the group's dbt tasks render their profile from."""
    configs = {id(t.profile_config): t.profile_config for t in group.children.values()}
    assert len(configs) == 1, "all tasks of a group share one ProfileConfig"
    return dict(next(iter(configs.values())).profile_mapping.profile_args)


@pytest.mark.skipif(not PROJECTS, reason="no dbt project in the deploy tree")
def test_groups_get_their_own_database_and_schema(dagbag: DagBag, monkeypatch: pytest.MonkeyPatch) -> None:
    from airflow import DAG

    from utils import dbt_factory

    # This throwaway DAG isn't in the DagBag: keep its groups out of CREATED, which
    # test_every_dbt_task_group_reaches_airflow checks against the collected DAGs.
    monkeypatch.setattr(dbt_factory, "CREATED", [])
    bundle_args = dict(dbt_factory.registry()["profile"]["mapping"]["profile_args"])
    project = PROJECTS[0]
    with DAG("ci_profile_overrides", schedule=None, start_date=datetime(2024, 1, 1)):
        default = dbt_factory.dbt_task_group(project, group_id="default")
        daily = dbt_factory.dbt_task_group(project, group_id="daily", database="lake", schema="analytics_daily")
        other = dbt_factory.dbt_task_group(project, group_id="other", schema="analytics_other")
        extra = dbt_factory.dbt_task_group(project, group_id="extra", profile_args={"threads": 8}, schema="s")

    assert _profile_args(default) == bundle_args
    assert _profile_args(daily) == {**bundle_args, "database": "lake", "schema": "analytics_daily"}
    assert _profile_args(other) == {**bundle_args, "schema": "analytics_other"}
    assert _profile_args(extra) == {**bundle_args, "threads": 8, "schema": "s"}
    # Overrides never leak into the shared registry (or into the next group).
    assert dbt_factory.registry()["profile"]["mapping"]["profile_args"] == bundle_args

    # What Cosmos renders at task time, from a fake connection (if this mapping can use it).
    conn_id = dbt_factory.registry()["profile"]["mapping"]["conn_id"]
    monkeypatch.setenv(f"AIRFLOW_CONN_{conn_id.upper()}", "trino://ci:ci@ci-dummy:8080/")
    mapping = next(iter(daily.children.values())).profile_config.profile_mapping
    if mapping.can_claim_connection():
        assert (mapping.profile["database"], mapping.profile["schema"]) == ("lake", "analytics_daily")
