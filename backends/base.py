from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class RuntimeBackend(ABC):
    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def get_job(self, name: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def list_jobs(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def delete_job(self, name: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def wait_for_job_completion(self, name: str, timeout: int = 30) -> dict[str, Any]:
        raise NotImplementedError
