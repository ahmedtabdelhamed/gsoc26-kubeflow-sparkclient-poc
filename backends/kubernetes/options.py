from __future__ import annotations

from typing import Protocol

from kubeflow_spark_api import models


class SparkApplicationOption(Protocol):
    def __call__(
        self,
        app: models.SparkoperatorV1beta2SparkApplication,
        backend: object,
    ) -> None:
        ...


class Labels:
    """Attach top-level metadata labels to a SparkApplication."""

    def __init__(self, labels: dict[str, str]) -> None:
        self._labels = labels

    def __call__(
        self,
        app: models.SparkoperatorV1beta2SparkApplication,
        backend: object,
    ) -> None:
        labels = app.metadata.setdefault("labels", {})
        if not isinstance(labels, dict):
            raise TypeError("metadata.labels must be a mapping")
        labels.update(self._labels)


class DriverNodeSelector:
    """Set node selector on the driver pod spec."""

    def __init__(self, selector: dict[str, str]) -> None:
        self._selector = selector

    def __call__(
        self,
        app: models.SparkoperatorV1beta2SparkApplication,
        backend: object,
    ) -> None:
        app.spec.driver.node_selector = self._selector


class ExecutorNodeSelector:
    """Set node selector on the executor pod spec."""

    def __init__(self, selector: dict[str, str]) -> None:
        self._selector = selector

    def __call__(
        self,
        app: models.SparkoperatorV1beta2SparkApplication,
        backend: object,
    ) -> None:
        app.spec.executor.node_selector = self._selector
