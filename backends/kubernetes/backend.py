from __future__ import annotations

import time
from typing import Any

from backends.base import RuntimeBackend
from backends.kubernetes.options import SparkApplicationOption
from backends.kubernetes.utils import (
    DEFAULT_IMAGE,
    DEFAULT_NAMESPACE,
    DEFAULT_SPARK_VERSION,
    build_spark_application_cr,
)


class KubernetesBackend(RuntimeBackend):
    """PoC backend boundary backed by Kubernetes CustomObjectsApi."""

    group = "sparkoperator.k8s.io"
    version = "v1beta2"
    plural = "sparkapplications"

    def __init__(
        self,
        namespace: str = DEFAULT_NAMESPACE,
        custom_api: Any | None = None,
    ) -> None:
        self.namespace = namespace

        if custom_api is not None:
            self.custom_api = custom_api
            return

        try:
            from kubernetes import client, config
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "kubernetes package is required for real Kubernetes backend usage"
            ) from exc

        config.load_kube_config()
        self.custom_api = client.CustomObjectsApi()

    def submit_job(
        self,
        *,
        name: str,
        main_file: str,
        num_executors: int = 1,
        image: str = DEFAULT_IMAGE,
        namespace: str = DEFAULT_NAMESPACE,
        spark_version: str = DEFAULT_SPARK_VERSION,
        options: list[SparkApplicationOption] | None = None,
    ) -> str:
        app = build_spark_application_cr(
            name=name,
            main_file=main_file,
            num_executors=num_executors,
            namespace=namespace,
            image=image,
            spark_version=spark_version,
        )

        if options:
            for option in options:
                option(app, self)

        payload = app.model_dump(mode="json", by_alias=True, exclude_none=True)

        self.custom_api.create_namespaced_custom_object(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=self.plural,
            body=payload,
        )
        return name

    def get_job(self, name: str) -> dict[str, Any]:
        return self.custom_api.get_namespaced_custom_object(
            group=self.group,
            version=self.version,
            namespace=self.namespace,
            plural=self.plural,
            name=name,
        )

    def list_jobs(self) -> list[dict[str, Any]]:
        response = self.custom_api.list_namespaced_custom_object(
            group=self.group,
            version=self.version,
            namespace=self.namespace,
            plural=self.plural,
        )
        return response.get("items", [])

    def delete_job(self, name: str) -> None:
        self.custom_api.delete_namespaced_custom_object(
            group=self.group,
            version=self.version,
            namespace=self.namespace,
            plural=self.plural,
            name=name,
        )

    def wait_for_job_completion(self, name: str, timeout: int = 30) -> dict[str, Any]:
        if timeout <= 0:
            raise ValueError("timeout must be positive")

        start = time.time()
        while time.time() - start < timeout:
            job = self.get_job(name)
            state = (
                job.get("status", {})
                .get("applicationState", {})
                .get("state", "")
                .upper()
            )
            if state == "COMPLETED":
                return job
            if state == "FAILED":
                raise RuntimeError(f"Job failed: {name}")
            time.sleep(0.5)

        raise TimeoutError(f"Timeout waiting for job completion: {name}")
