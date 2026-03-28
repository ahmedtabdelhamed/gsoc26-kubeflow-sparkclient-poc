from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from api.spark_client import SparkClient
from backends.kubernetes.backend import KubernetesBackend
from backends.kubernetes.options import Labels
from backends.kubernetes.utils import build_spark_application_cr
from kubeflow_spark_api import models


class FakeCustomObjectsApi:
    """In-memory stand-in for Kubernetes CustomObjectsApi used in demo/tests."""

    def __init__(self) -> None:
        self._store: dict[str, dict[str, Any]] = {}
        self._read_count: dict[str, int] = {}

    def create_namespaced_custom_object(
        self,
        *,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        del group, version, namespace, plural
        name = body["metadata"]["name"]
        self._store[name] = {
            **body,
            "status": {"applicationState": {"state": "SUBMITTED"}},
        }
        self._read_count[name] = 0
        return body

    def get_namespaced_custom_object(
        self,
        *,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
    ) -> dict[str, Any]:
        del group, version, namespace, plural
        if name not in self._store:
            raise KeyError(f"Job not found: {name}")

        self._read_count[name] += 1
        if self._read_count[name] >= 2:
            self._store[name]["status"]["applicationState"]["state"] = "COMPLETED"
        return self._store[name]

    def list_namespaced_custom_object(
        self,
        *,
        group: str,
        version: str,
        namespace: str,
        plural: str,
    ) -> dict[str, Any]:
        del group, version, namespace, plural
        return {"items": list(self._store.values())}

    def delete_namespaced_custom_object(
        self,
        *,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
    ) -> None:
        del group, version, namespace, plural
        self._store.pop(name, None)


def build_valid_app() -> models.SparkoperatorV1beta2SparkApplication:
    app = build_spark_application_cr(
        name="pi-job",
        main_file="local:///opt/spark/examples/src/main/python/pi.py",
        num_executors=2,
        labels={"team": "ml"},
    )
    return app


def serialization_demo(client: SparkClient) -> None:
    print("=== Serialization Demo ===")
    app = build_valid_app()
    payload = app.model_dump(mode="json", by_alias=True, exclude_none=True)
    print(json.dumps(payload, indent=2))
    print("Alias check -> mainApplicationFile:", payload["spec"]["mainApplicationFile"])
    job_name = client.submit_job(
        name="pi-job",
        main_file="local:///opt/spark/examples/src/main/python/pi.py",
        num_executors=2,
        options=[Labels({"team": "ml"})],
    )
    print("Submitted to CustomObjectsApi as:", job_name)
    print(
        "Job status after wait:",
        client.wait_for_job_completion(job_name)["status"]["applicationState"]["state"],
    )


def fail_fast_demo() -> None:
    print("\n=== Fail-fast Validation Demo ===")
    try:
        _ = models.SparkoperatorV1beta2SparkApplication(
            apiVersion="sparkoperator.k8s.io/v1beta2",
            kind="SparkApplication",
            metadata={"name": "invalid-name"},
            spec=models.SparkoperatorV1beta2SparkApplicationSpec(
                type="Python",
                mode="cluster",
                image="gcr.io/spark-operator/spark-py:v3.5.0",
                mainApplicationFile="local:///opt/spark/examples/src/main/python/pi.py",
                sparkVersion="3.5.0",
                restartPolicy={"type": "Never"},
                driver=models.SparkoperatorV1beta2DriverSpec(cores=0, memory="512x"),
                executor=models.SparkoperatorV1beta2ExecutorSpec(
                    instances=-1,
                    cores=1,
                    memory="512m",
                ),
            ),
        )
    except ValidationError as exc:
        print("Validation failed before Kubernetes call:")
        print(exc)


if __name__ == "__main__":
    backend = KubernetesBackend(custom_api=FakeCustomObjectsApi())
    client = SparkClient(backend)
    serialization_demo(client)
    fail_fast_demo()
