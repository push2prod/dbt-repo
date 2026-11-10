#!/usr/bin/env python3
"""Shrink manifest.json to the fields Cosmos reads in LoadMode.DBT_MANIFEST.

Optional: only worth it for large projects, where parse time is mostly spent
in json.loads. This uses a *keep*-list, so new or unexpected fields are
dropped rather than kept. The list matches astronomer-cosmos 1.16
(cosmos/dbt/graph.py `_build_dbt_node_from_manifest_resource` and the
selector). Check it again when upgrading Cosmos: tests/test_selector_parity.py
runs against the slim manifest in CI, so a missing field shows up as a parity
failure.

Only manifest.json is changed. partial_parse.msgpack is left alone, so
task-level dbt runs are unaffected.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

TOP_LEVEL_KEEP = {"metadata", "nodes", "sources", "exposures", "selectors", "parent_map", "child_map"}
NODE_KEEP = {
    "unique_id",
    "name",
    "resource_type",
    "package_name",
    "original_file_path",
    "path",
    "fqn",
    "tags",
    "config",
    "depends_on",
    "freshness",  # sources: Cosmos decides whether to render freshness tasks
    "test_metadata",
    "attached_node",
}
CONFIG_DROP = {"docs", "contract", "grants", "persist_docs", "column_types", "unique_key_docs"}
METADATA_KEEP = {"dbt_schema_version", "dbt_version", "project_name", "project_id", "adapter_type"}


def slim(manifest: dict) -> dict:
    out = {k: v for k, v in manifest.items() if k in TOP_LEVEL_KEEP}
    out["metadata"] = {k: v for k, v in manifest.get("metadata", {}).items() if k in METADATA_KEEP}
    for section in ("nodes", "sources", "exposures"):
        slimmed = {}
        for uid, node in manifest.get(section, {}).items():
            n = {k: v for k, v in node.items() if k in NODE_KEEP}
            if isinstance(n.get("config"), dict):
                n["config"] = {k: v for k, v in n["config"].items() if k not in CONFIG_DROP}
            if isinstance(n.get("depends_on"), dict):
                n["depends_on"] = {"nodes": n["depends_on"].get("nodes", [])}
            slimmed[uid] = n
        out[section] = slimmed
    return out


def slim_file(path: Path) -> tuple[int, int]:
    before = path.stat().st_size
    data = slim(json.loads(path.read_text()))
    path.write_text(json.dumps(data, separators=(",", ":"), sort_keys=True))
    return before, path.stat().st_size


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        b, a = slim_file(Path(arg))
        print(f"{arg}: {b} -> {a} bytes")
