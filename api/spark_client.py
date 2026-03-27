from __future__ import annotations

from typing import Any

from backends.base import RuntimeBackend


class SparkClient:
    """Thin API layer that delegates execution to the backend."""

    def __init__(self, backend: RuntimeBackend) -> None:
        self.backend = backend

    def submit_job(
        self,
        *,
        name: str,
        main_file: str,
        num_executors: int = 1,
        image: str = "gcr.io/spark-operator/spark-py:v3.5.0",
        namespace: str = "kubeflow-user",
        spark_version: str = "3.5.0",
        options: list[Any] | None = None,
    ) -> str:
        return self.backend.submit_job(
            name=name,
            main_file=main_file,
            num_executors=num_executors,
            image=image,
            namespace=namespace,
            spark_version=spark_version,
            options=options,
        )

    def get_job(self, name: str):
        return self.backend.get_job(name)

    def list_jobs(self):
        return self.backend.list_jobs()

    def delete_job(self, name: str) -> None:
        self.backend.delete_job(name)

    def wait_for_job_completion(self, name: str, timeout: int = 30):
        return self.backend.wait_for_job_completion(name, timeout=timeout)
