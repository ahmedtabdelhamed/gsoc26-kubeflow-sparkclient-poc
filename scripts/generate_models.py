from __future__ import annotations

import subprocess
import sys


def run(command: list[str]) -> None:
    result = subprocess.run(command, check=False)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def main() -> None:
    run([sys.executable, "scripts/fetch_crd_schema.py"])

    run(
        [
            sys.executable,
            "-m",
            "datamodel_code_generator",
            "--input",
            "./schemas/spark_application_schema.json",
            "--input-file-type",
            "jsonschema",
            "--output",
            "./kubeflow_spark_api/models/generated_models.py",
            "--output-model-type",
            "pydantic_v2.BaseModel",
            "--use-annotated",
            "--strict-nullable",
            "--snake-case-field",
            "--allow-population-by-field-name",
            "--target-python-version",
            "3.10",
        ]
    )

    print("Model regeneration completed at ./kubeflow_spark_api/models/generated_models.py")


if __name__ == "__main__":
    main()
