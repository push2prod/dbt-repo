"""One log format for the e2e run: elapsed time since start, so gaps in the CI log are visible."""

from __future__ import annotations

import time

_T0 = time.monotonic()


def log(msg: str) -> None:
    elapsed = int(time.monotonic() - _T0)
    print(f"[e2e +{elapsed // 60:02d}:{elapsed % 60:02d}] {msg}", flush=True)
