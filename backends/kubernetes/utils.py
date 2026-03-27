from __future__ import annotations

from kubeflow_spark_api import models

DEFAULT_IMAGE = "gcr.io/spark-operator/spark-py:v3.5.0"
DEFAULT_NAMESPACE = "kubeflow-user"
DEFAULT_SPARK_VERSION = "3.5.0"


def build_spark_application_cr(
    *,
    name: str,
    main_file: str,
    num_executors: int = 1,
    namespace: str = DEFAULT_NAMESPACE,
    image: str = DEFAULT_IMAGE,
    spark_version: str = DEFAULT_SPARK_VERSION,
    driver_cores: int = 1,
    driver_memory: str = "512m",
    executor_cores: int = 1,
    executor_memory: str = "512m",
    service_account: str | None = "spark-sa",
    labels: dict[str, str] | None = None,
) -> models.SparkoperatorV1beta2SparkApplication:
    metadata: dict[str, object] = {"name": name, "namespace": namespace}
    if labels:
        metadata["labels"] = labels

    driver = models.SparkoperatorV1beta2DriverSpec(
        cores=driver_cores,
        memory=driver_memory,
        serviceAccount=service_account,
    )
    executor = models.SparkoperatorV1beta2ExecutorSpec(
        instances=num_executors,
        cores=executor_cores,
        memory=executor_memory,
    )

    spec = models.SparkoperatorV1beta2SparkApplicationSpec(
        type="Python",
        mode="cluster",
        image=image,
        mainApplicationFile=main_file,
        sparkVersion=spark_version,
        restartPolicy={"type": "Never"},
        driver=driver,
        executor=executor,
    )

    return models.SparkoperatorV1beta2SparkApplication(
        apiVersion="sparkoperator.k8s.io/v1beta2",
        kind="SparkApplication",
        metadata=metadata,
        spec=spec,
    )
