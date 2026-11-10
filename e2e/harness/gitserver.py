"""A throwaway git server inside k3s that git-sync tracks, and the sync step that pushes to it.

In production CI pushes the bundle to the `deploy` branch of the real repo; here
the test does the same against `git-server` (git daemon, receive-pack enabled),
so the cluster never needs credentials or outside git access.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

from kubernetes import client

from e2e.harness import settings as s

REPO_NAME = "airflow.git"
IN_CLUSTER_URL = f"git://git-server.{s.NAMESPACE}.svc.cluster.local:9418/{REPO_NAME}"

_SERVE = (
    f"git init --bare /srv/{REPO_NAME} && git -C /srv/{REPO_NAME} symbolic-ref HEAD refs/heads/deploy && "
    "exec git daemon --reuseaddr --export-all --enable=receive-pack --base-path=/srv /srv"
)


def deploy_git_server(api: client.ApiClient) -> None:
    core, apps = client.CoreV1Api(api), client.AppsV1Api(api)
    labels = {"app": "git-server"}
    container = client.V1Container(
        name="git",
        image=s.GIT_SERVER_IMAGE,
        command=["sh", "-c", _SERVE],
        ports=[client.V1ContainerPort(container_port=9418)],
        readiness_probe=client.V1Probe(tcp_socket=client.V1TCPSocketAction(port=9418), period_seconds=2),
    )
    pod = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels=labels), spec=client.V1PodSpec(containers=[container])
    )
    spec = client.V1DeploymentSpec(replicas=1, selector=client.V1LabelSelector(match_labels=labels), template=pod)
    apps.create_namespaced_deployment(
        s.NAMESPACE, client.V1Deployment(metadata=client.V1ObjectMeta(name="git-server"), spec=spec)
    )
    port = client.V1ServicePort(port=9418, target_port=9418, node_port=s.GIT_NODEPORT, name="git")
    svc = client.V1ServiceSpec(type="NodePort", selector=labels, ports=[port])
    core.create_namespaced_service(
        s.NAMESPACE, client.V1Service(metadata=client.V1ObjectMeta(name="git-server"), spec=svc)
    )
    for _ in range(120):
        dep = apps.read_namespaced_deployment("git-server", s.NAMESPACE)
        if dep.status is not None and (dep.status.ready_replicas or 0) >= 1:
            return
        time.sleep(2)
    raise TimeoutError("git-server did not become ready")


class Syncer:
    """Pushes bundles to the in-cluster repo's `deploy` branch, one commit per sync."""

    def __init__(self, workdir: Path, host: str, port: int) -> None:
        self.clone = workdir / "sync-clone"
        self.url = f"git://{host}:{port}/{REPO_NAME}"
        self.count = 0

    def _git(self, *args: str) -> None:
        cmd = ["git", "-c", "user.name=e2e", "-c", "user.email=e2e@example.invalid", *args]
        subprocess.run(cmd, cwd=self.clone, check=True, capture_output=True)

    def sync(self, bundle: Path) -> None:
        if not self.clone.exists():
            self.clone.mkdir(parents=True)
            self._git("init", "-q", "-b", "deploy")
            self._git("remote", "add", "origin", self.url)
        for child in self.clone.iterdir():
            if child.name != ".git":
                shutil.rmtree(child) if child.is_dir() else child.unlink()
        shutil.copytree(bundle, self.clone, dirs_exist_ok=True)
        self.count += 1
        self._git("add", "-A", "-f")
        self._git("commit", "-q", "--allow-empty", "-m", f"sync {self.count}")
        for attempt in range(10):  # the NodePort can take a moment right after the pod is ready
            try:
                self._git("push", "-q", "origin", "deploy")
                return
            except subprocess.CalledProcessError:
                if attempt == 9:
                    raise
                time.sleep(3)
