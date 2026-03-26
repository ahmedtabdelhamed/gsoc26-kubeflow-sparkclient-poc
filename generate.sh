#!/usr/bin/env bash
set -euo pipefail

# Issue #2818 automation: regenerate Pydantic v2 models from upstream Spark Operator CRD.
# This script is the maintainable regeneration contract for future Spark Operator updates.

PYTHON_CMD=()
if command -v python >/dev/null 2>&1; then
  PYTHON_CMD=(python)
elif command -v py >/dev/null 2>&1; then
  PYTHON_CMD=(py -3)
elif command -v py.exe >/dev/null 2>&1; then
  PYTHON_CMD=(py.exe -3)
elif [ -x /c/Windows/py.exe ]; then
  PYTHON_CMD=(/c/Windows/py.exe -3)
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_CMD=(python3)
else
  echo "No Python interpreter found in PATH." >&2
  exit 1
fi

"${PYTHON_CMD[@]}" ./scripts/fetch_crd_schema.py

"${PYTHON_CMD[@]}" -m datamodel_code_generator \
  --input ./schemas/spark_application_schema.json \
  --input-file-type jsonschema \
  --output ./kubeflow_spark_api/models/generated_models.py \
  --output-model-type pydantic_v2.BaseModel \
  --use-annotated \
  --strict-nullable \
  --snake-case-field \
  --allow-population-by-field-name \
  --target-python-version 3.10

echo "Model regeneration completed at ./kubeflow_spark_api/models/generated_models.py"
