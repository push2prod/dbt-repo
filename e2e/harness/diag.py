"""On failure: dump pods, events and component logs from the cluster for debugging."""

from __future__ import annotations

from pathlib import Path

from kubernetes import client

from e2e.harness import settings as s


def _pod_logs(core: client.CoreV1Api, pod: client.V1Pod, ns: str, dest: Path) -> str:
    """Write every container's log (init containers included); return a one-line summary."""
    name = pod.metadata.name if pod.metadata and pod.metadata.name else "?"
    spec = pod.spec
    containers = [c.name for c in (spec.init_containers or [])] + [c.name for c in spec.containers] if spec else []
    for c in containers:
        try:
            text = core.read_namespaced_pod_log(name, ns, container=c, tail_lines=400)
        except client.ApiException as e:
            text = f"<no log: {e.reason}>"
        (dest / f"{ns}__{name}__{c}.log").write_text(str(text))
    return f"{ns}/{name} {pod.status.phase if pod.status else '?'}"


def dump(api: client.ApiClient, dest: Path) -> None:
    """Write pod list, events and logs (incl. failed task pods, which are kept) under ``dest``."""
    core = client.CoreV1Api(api)
    dest.mkdir(parents=True, exist_ok=True)
    lines = []
    for ns in (s.NAMESPACE, "kube-system"):
        lines += [_pod_logs(core, pod, ns, dest) for pod in core.list_namespaced_pod(ns).items]
        for e in core.list_namespaced_event(ns).items:
            obj = e.involved_object
            lines.append(f"EVENT {e.type} {obj.kind}/{obj.name}: {e.reason} {e.message}")
    (dest / "pods-and-events.txt").write_text("\n".join(lines) + "\n")
