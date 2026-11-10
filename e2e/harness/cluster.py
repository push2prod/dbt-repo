"""The containers: Postgres (warehouse + Airflow metadata) and K3s, on one Docker network.

Everything goes through the Docker API (image builds, archive uploads), no
host bind mounts, so it also works against a remote daemon such as GitLab's
docker:dind service.
"""

from __future__ import annotations

import io
import os
import subprocess
import tarfile
import time
from pathlib import Path
from typing import Any

import docker
import yaml
from kubernetes import client, config
from testcontainers.community.k3s import K3SContainer
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.network import Network

from e2e.harness import settings as s


def _log(msg: str) -> None:
    print(f"[e2e] {msg}", flush=True)


def _tar_bytes(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def registries_yaml(mirror: str | None, ca_path: str | None) -> str:
    """k3s containerd registry config: optional docker.io mirror and extra CA."""
    cfg: dict[str, Any] = {}
    if mirror:
        cfg["mirrors"] = {"docker.io": {"endpoint": [mirror]}}
    if ca_path:
        hosts = ["registry-1.docker.io", "registry.k8s.io", *([mirror.split("://")[-1]] if mirror else [])]
        cfg["configs"] = {h: {"tls": {"ca_file": ca_path}} for h in hosts}
    return yaml.safe_dump(cfg)


class Cluster:
    """Postgres + K3s. Use as a context manager; everything is removed on exit."""

    def __init__(self, workdir: Path) -> None:
        self.workdir = workdir
        self.docker = docker.from_env()
        self.network = Network()
        self.pg = PostgresContainer(s.POSTGRES_IMAGE, username=s.PG_USER, password=s.PG_PASSWORD, dbname="airflow")
        self.k3s: K3SContainer | None = None
        self.api: client.ApiClient | None = None

    def __enter__(self) -> Cluster:
        self.network.create()
        self.pg.with_network(self.network).with_network_aliases(s.PG_ALIAS).start()
        self.pg.exec(["psql", "-U", s.PG_USER, "-d", "airflow", "-c", "create database analytics"])
        self.k3s = self._start_k3s()
        return self

    def __exit__(self, *exc: object) -> None:
        for c in (self.k3s, self.pg):
            if c is not None:
                c.stop()
        self.network.remove()

    # ------------------------------------------------------------------ k3s
    def _k3s_image(self) -> str:
        """K3S_IMAGE, or a derived image with the registry config baked in (no bind mounts)."""
        if not (s.EXTRA_CA_FILE or s.REGISTRY_MIRROR):
            return s.K3S_IMAGE
        ca_target = "/etc/rancher/k3s/extra-ca.crt" if s.EXTRA_CA_FILE else None
        files = {"registries.yaml": registries_yaml(s.REGISTRY_MIRROR, ca_target).encode()}
        lines = [f"FROM {s.K3S_IMAGE}", "COPY registries.yaml /etc/rancher/k3s/registries.yaml"]
        if s.EXTRA_CA_FILE:
            files["extra-ca.crt"] = Path(s.EXTRA_CA_FILE).read_bytes()
            lines.append(f"COPY extra-ca.crt {ca_target}")
        files["Dockerfile"] = ("\n".join(lines) + "\n").encode()
        tag = "dbt-repo-k3s:e2e"
        self.docker.images.build(fileobj=io.BytesIO(_tar_bytes(files)), custom_context=True, tag=tag, rm=True)
        return tag

    def _start_k3s(self) -> K3SContainer:
        k3s = K3SContainer(self._k3s_image(), enable_cgroup_mount=s.K3S_CGROUP_MOUNT)
        k3s.with_command(f"server --disable traefik --disable metrics-server --tls-san={k3s.get_container_host_ip()}")
        k3s.with_network(self.network).with_exposed_ports(s.WEBSERVER_NODEPORT, s.GIT_NODEPORT)
        t = time.time()
        k3s.start()
        config.load_kube_config_from_dict(yaml.safe_load(k3s.config_yaml()))  # type: ignore[no-untyped-call]
        self.api = client.ApiClient()
        _log(f"k3s up in {time.time() - t:.0f}s")
        return k3s

    def host_port(self, node_port: int) -> int:
        assert self.k3s is not None
        return int(self.k3s.get_exposed_port(node_port))

    @property
    def host(self) -> str:
        assert self.k3s is not None
        return str(self.k3s.get_container_host_ip())

    # ------------------------------------------------------------------ images
    def build_airflow_image(self) -> None:
        t = time.time()
        self.docker.images.build(path=str(s.REPO), dockerfile="e2e/Dockerfile", tag=s.AIRFLOW_IMAGE, rm=True)
        _log(f"built {s.AIRFLOW_IMAGE} in {time.time() - t:.0f}s")

    def load_image(self, tag: str) -> None:
        """docker save -> upload into the k3s container -> ctr import (so pods never pull it)."""
        assert self.k3s is not None
        t = time.time()
        image_tar = self.workdir / "image.tar"
        with image_tar.open("wb") as fh:
            for chunk in self.docker.images.get(tag).save(named=True):
                fh.write(chunk)
        outer = self.workdir / "image-upload.tar"
        with tarfile.open(outer, "w") as tf:
            tf.add(image_tar, arcname="image.tar")
        with outer.open("rb") as fh:
            self.k3s.get_wrapped_container().put_archive("/tmp", fh)
        code, out = self.k3s.exec(["ctr", "-n", "k8s.io", "images", "import", "/tmp/image.tar"])
        if code:
            raise RuntimeError(f"ctr import failed: {out.decode()[-2000:]}")
        image_tar.unlink()
        outer.unlink()
        _log(f"loaded {tag} into k3s in {time.time() - t:.0f}s")

    # ------------------------------------------------------------------ warehouse
    def pg_ip(self) -> str:
        """The Postgres container's address on the shared network (reachable from pods)."""
        container = self.pg.get_wrapped_container()
        container.reload()
        return str(container.attrs["NetworkSettings"]["Networks"][self.network.name]["IPAddress"])

    def seed_warehouse(self, projects_dir: Path, dbt_bin: str) -> None:
        """`dbt seed` every project from the host: the seeds stand in for existing source tables."""
        env = {
            **os.environ,
            "DBT_PG_HOST": self.pg.get_container_host_ip(),
            "DBT_PG_PORT": str(self.pg.get_exposed_port(5432)),
            "DBT_PG_USER": s.PG_USER,
            "DBT_PG_DBNAME": "analytics",
            "DBT_ENV_SECRET_PG_PASSWORD": s.PG_PASSWORD,
        }
        for proj in sorted(projects_dir.glob("*/dbt_project.yml")):
            d, scratch = proj.parent, str(self.workdir / "seed" / proj.parent.name)
            cmd = [dbt_bin, "seed", "--project-dir", str(d), "--profiles-dir", str(d), "--target-path", scratch]
            r = subprocess.run([*cmd, "--log-path", scratch], env=env, capture_output=True, text=True, check=False)
            if r.returncode:
                raise RuntimeError(f"dbt seed failed for {d.name}:\n{r.stdout[-2000:]}")
        _log("warehouse seeded")
