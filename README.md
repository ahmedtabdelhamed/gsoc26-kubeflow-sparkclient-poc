# gsoc26-kubeflow-sparkclient-poc

This PoC explicitly addresses **Issue #2818** by replacing legacy dictionary CRD construction with **auto-generated Pydantic v2 models** and a typed SparkApplication submission path.

## Why this PoC matters

- Faster local feedback with strict validation (fail fast).
- Maintainable regeneration pipeline when Spark Operator schema changes.
- Clean Kubernetes payload output via alias-aware serialization.
- Concrete bridge from PoC code to Kubeflow SDK patterns: builder, options, and backend delegation.

## Architecture

1. Client Layer (thin API)
- `api/spark_client.py` exposes flat `submit_job(...)` arguments and delegates.

2. Backend Layer (execution boundary)
- `backends/kubernetes/backend.py` uses Kubernetes `CustomObjectsApi` for create/get/list/delete/wait.
- Demo/tests inject a fake `CustomObjectsApi` implementation for deterministic local runs.

3. Builder and Options Layer (extensibility boundary)
- `backends/kubernetes/utils.py` provides `build_spark_application_cr(...)`.
- `backends/kubernetes/options.py` provides callable options (for example labels and node selectors).

4. Model Layer (generated SDK core)
- `kubeflow_spark_api/models/` contains typed components.

Flow: user input -> builder -> options mutation -> typed model -> `model_dump(mode="json", by_alias=True, exclude_none=True)` -> Kubernetes API boundary.

## Design patterns shown

- Backend Delegation Pattern
- Builder Pattern for SparkApplication CRD
- Options Pattern for advanced customization
- Fail-Fast Validation Pattern
- Boundary Serialization Pattern
- Progressive Complexity Pattern
- Contract Testing Pattern

## PoC scope vs production scope

Current PoC scope:
- Batch job CR creation and submission path with typed SparkApplication models.
- Core lifecycle APIs: submit, get, list, wait, delete.
- Minimal observability baseline via SparkApplication status polling.

Production scope (future work):
- Driver/executor pod log streaming via Kubernetes CoreV1 APIs.
- Structured event streaming and richer status surfaces.
- Metrics export integration (for example Prometheus) and dashboards.

## Quickstart (under 60 seconds)

1. Install dependencies
```bash
pip install -r requirements.txt
```

2. Run demo
```bash
python demo.py
```

3. Run automated proof
```bash
python -m unittest tests/test_serialization.py -v
```

Real cluster mode note:
- The demo and tests use an injected fake Kubernetes API client by design.
- For real cluster execution, instantiate `KubernetesBackend()` without injection and ensure kubeconfig is available.

## Regeneration automation

Use the maintainable generation command in `generate.sh`:

```bash
bash generate.sh
```

Cross-platform alternative (recommended on Windows):

```bash
python scripts/generate_models.py
```

Required flags included:
- `--output-model-type pydantic_v2.BaseModel`
- `--use-annotated`
- `--strict-nullable`

Generated artifact:
- `kubeflow_spark_api/models/generated_models.py`

Upstream refresh:
- Run `REFRESH_CRD=1 bash generate.sh` to refresh `schemas/sparkoperator_crd.yaml` from upstream before regeneration.
- Default `bash generate.sh` is deterministic and regenerates from the checked-in schema snapshot.

CI verification:
- CI runs `python scripts/generate_models.py` and fails if `git diff --exit-code` detects drift in generated files.

## Repository layout

- `schemas/sparkoperator_crd.yaml` -> source schema
- `generate.sh` -> regeneration entrypoint
- `scripts/fetch_crd_schema.py` -> schema extraction and optional upstream refresh
- `api/spark_client.py` -> thin client delegation layer
- `backends/base.py` -> backend interface contract
- `backends/kubernetes/backend.py` -> Kubernetes CustomObjectsApi execution boundary
- `backends/kubernetes/utils.py` -> SparkApplication builder
- `backends/kubernetes/options.py` -> extensibility options
- `kubeflow_spark_api/models/` -> typed model components
- `demo.py` -> DX demo (serialization + fail-fast)
- `tests/expected_payload.json` -> golden payload
- `tests/test_serialization.py` -> contract tests

## Project 12 milestone alignment

1. Batch Job Submission (core feature)
- Implemented submit/get/list/wait/delete flow using SparkApplication CRD and typed builder.

2. Observability and Monitoring
- Baseline implemented via status polling and terminal state handling.
- Advanced metrics/events/exporters are intentionally left for next iterations.

3. Data Transfer and Lakehouse use cases
- This PoC establishes the backend and CR submission foundation required to run ETL-style Spark jobs.

4. Documentation and Examples
- Includes executable demo, golden payload contract tests, regeneration pipeline, and CI drift checks.
