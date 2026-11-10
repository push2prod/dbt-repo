#!/usr/bin/env python3
"""Enforce size limits: functions <= 60 lines, files <= 300 lines.

Limits come from ``[tool.repo-limits]`` in pyproject.toml.
* Files: every tracked Python file and shell script (by extension or shebang).
* Functions: Python functions and methods, including nested ones, measured from
  the ``def`` line (or its first decorator) to the last line of the body.

Exit 1 and list every violation if any limit is exceeded.

    python ci/check_lengths.py            # tracked files
    python ci/check_lengths.py a.py b.sh  # just these (pre-commit passes them)
"""

from __future__ import annotations

import ast
import subprocess
import sys
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def limits() -> tuple[int, int]:
    cfg = tomllib.loads((REPO / "pyproject.toml").read_text())["tool"]["repo-limits"]
    return int(cfg["max-function-lines"]), int(cfg["max-file-lines"])


def kind(path: Path) -> str | None:
    """'py', 'sh' or None (not checked)."""
    if path.suffix == ".py":
        return "py"
    if path.suffix == ".sh":
        return "sh"
    try:
        first = path.open("rb").readline()
    except OSError:
        return None
    if first.startswith(b"#!") and (b"bash" in first or b"/sh" in first):
        return "sh"
    if first.startswith(b"#!") and b"python" in first:
        return "py"
    return None


def tracked_files() -> list[Path]:
    out = subprocess.run(["git", "ls-files", "-z"], cwd=REPO, check=True, capture_output=True).stdout
    return [REPO / p for p in out.decode().split("\0") if p]


def function_violations(path: Path, max_lines: int) -> list[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            start = min([node.lineno, *(d.lineno for d in node.decorator_list)])
            length = (node.end_lineno or node.lineno) - start + 1
            if length > max_lines:
                found.append(f"{path}:{start}: function {node.name}() is {length} lines (max {max_lines})")
    return found


def violations(paths: list[Path]) -> list[str]:
    max_fn, max_file = limits()
    found = []
    for path in paths:
        k = kind(path) if path.is_file() else None
        if k is None:
            continue
        rel = path.relative_to(REPO) if path.is_relative_to(REPO) else path
        n = len(path.read_text(errors="replace").splitlines())
        if n > max_file:
            found.append(f"{rel}: file is {n} lines (max {max_file})")
        if k == "py":
            found += [v.replace(str(path), str(rel)) for v in function_violations(path, max_fn)]
    return found


def main() -> int:
    paths = [Path(a).resolve() for a in sys.argv[1:]] or tracked_files()
    found = violations(paths)
    for v in found:
        print(v)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
