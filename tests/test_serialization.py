from __future__ import annotations

import json
from pathlib import Path
import unittest
from typing import Any

from pydantic import ValidationError

from backends.kubernetes.backend import KubernetesBackend
from backends.kubernetes.options import DriverNodeSelector, Labels
from backends.kubernetes.utils import build_spark_application_cr
from kubeflow_spark_api import models


class FakeCustomObjectsApi:
    def __init__(self) -> None:
        self.store: dict[str, dict[str, Any]] = {}

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
        self.store[name] = {
            **body,
            "status": {"applicationState": {"state": "COMPLETED"}},
        }
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
        return self.store[name]

    def list_namespaced_custom_object(
        self,
        *,
        group: str,
        version: str,
        namespace: str,
        plural: str,
    ) -> dict[str, Any]:
        del group, version, namespace, plural
        return {"items": list(self.store.values())}

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
        self.store.pop(name, None)


class TestSerialization(unittest.TestCase):
    def test_golden_payload_contract(self) -> None:
        app = build_spark_application_cr(
            name="pi-job",
            main_file="local:///opt/spark/examples/src/main/python/pi.py",
            num_executors=2,
            labels={"team": "ml"},
        )
        payload = app.model_dump(mode="json", by_alias=True, exclude_none=True)
        expected = json.loads(
            Path("tests/expected_payload.json").read_text(encoding="utf-8")
        )
        self.assertEqual(payload, expected)

    def test_options_mutate_model_before_submission(self) -> None:
        backend = KubernetesBackend(custom_api=FakeCustomObjectsApi())
        job_name = backend.submit_job(
            name="pi-job-options",
            main_file="local:///opt/spark/examples/src/main/python/pi.py",
            options=[
                Labels({"environment": "test"}),
                DriverNodeSelector({"nodepool": "cpu"}),
            ],
        )

        job = backend.get_job(job_name)
        self.assertEqual(job["metadata"]["labels"]["environment"], "test")
        self.assertEqual(
            job["spec"]["driver"]["nodeSelector"]["nodepool"],
            "cpu",
        )

    def test_backend_lifecycle_calls_custom_objects_api(self) -> None:
        backend = KubernetesBackend(custom_api=FakeCustomObjectsApi())
        name = backend.submit_job(
            name="pi-job-lifecycle",
            main_file="local:///opt/spark/examples/src/main/python/pi.py",
        )

        listed = backend.list_jobs()
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0]["metadata"]["name"], name)

        completed = backend.wait_for_job_completion(name, timeout=1)
        self.assertEqual(
            completed["status"]["applicationState"]["state"],
            "COMPLETED",
        )

        backend.delete_job(name)
        self.assertEqual(backend.list_jobs(), [])

    def test_fail_fast_validation(self) -> None:
        with self.assertRaises(ValidationError):
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


if __name__ == "__main__":
    unittest.main()
