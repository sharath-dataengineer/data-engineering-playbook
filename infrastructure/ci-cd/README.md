# CI/CD for Data Pipelines

> Chapter from the Data Engineering Playbook — infrastructure

## About This Chapter

- **What this is.** A practical guide to building Continuous Integration and Continuous Deployment (CI/CD) systems specifically for data pipelines — covering testing strategies, safe deployment patterns, rollback approaches, and environment management.
- **Who it's for.** Mid-level to senior data engineers who have written pipelines that work in development but struggle to deploy them safely, catch breaking changes early, or recover quickly when something goes wrong in production.
- **What you'll take away.**
  - A testing pyramid designed for data pipelines — from fast unit tests to full end-to-end validation
  - Concrete GitHub Actions workflows and deployment patterns for PySpark, dbt, and Airflow
  - Rollback strategies that actually work when data has already been written

---

Imagine your team merges a dbt model change on a Friday afternoon. The pipeline runs, loads data into production tables, and downstream dashboards start showing incorrect numbers. You roll back the code — but the data is already there. Reverting the deploy did nothing, because CI/CD for data isn't just about code. It's about code, schema, data quality, and state all changing at the same time. This chapter is about building a system that catches those problems before they reach production and gives you real options for recovery when they do.

---

## TL;DR

- CI/CD for data is different from app CI/CD because rolling back a deploy does not undo data that has already been written to tables.
- Build a testing pyramid: fast unit tests, integration tests against real infrastructure, contract tests for data quality, and end-to-end tests for the full pipeline path.
- Add schema compatibility checks to CI so a breaking change (dropped column, changed type) fails the pull request before it reaches consumers.
- Use branch-based promotion — feature branch to dev, dev to staging, staging to prod — with automated tests at each stage and a manual approval gate before production.
- Deploy Airflow DAGs safely using git-sync or image-based baking; never edit DAG files in place on a running scheduler.
- Make rollback a data operation, not just a code operation: use idempotent partition rewrites, Iceberg or Delta time travel, and feature flags to cut traffic back to the old pipeline.
- Keep test environments structurally identical to production — same table formats, same IAM roles (AWS permissions), same file formats. Otherwise your integration tests are testing a different system.
- Artifact management (versioned Docker images, Python packages, dbt compiled manifests) lets you deploy exactly what was tested, not a best-effort re-assembly.

---

## Why This Matters in Production

A mid-size fintech runs hourly Spark jobs that compute customer spending summaries. Their deployment process is: merge to main, SSH into the cluster, copy the new `.py` file, restart the job. It works until the day a junior engineer accidentally drops the `account_type` column from the output schema. The downstream ML feature pipeline reads that column. It fails silently (wrong defaults fill in). The model degrades. Nobody notices for three days.

A CI/CD system for data would have caught this at several checkpoints:
1. A schema compatibility check in CI would have failed the pull request the moment it detected a column removal.
2. A contract test would have flagged that the output DataFrame no longer matches the registered schema.
3. A staging deployment would have let them validate the change against real infrastructure before touching production data.

The cost of the incident was not the code change — it was the missing safety net.

---

## How It Works

### CI/CD for Data vs. App CI/CD

In application development, CI (Continuous Integration) means: automatically run tests when code changes, catch bugs before merge. CD (Continuous Deployment or Delivery) means: automatically deploy validated code to environments. Rollback is straightforward — redeploy the previous version and you're back to the old state.

For data pipelines, the same definitions apply, but with a critical difference: **the pipeline's output is data that persists independently of the code**. If your pipeline runs and writes 50 million rows to a production table with a wrong transformation, reverting the code does not remove those rows. You need additional strategies — idempotent writes, time travel, feature flags — to actually recover. Every CI/CD decision for data pipelines has to account for this.

---

### The Testing Pyramid for Data Pipelines

Think of tests as a pyramid: the base is many fast, cheap tests; the top is fewer slow, expensive tests. For data pipelines, the four layers are:

#### Layer 1 — Unit Tests

Test one transformation function with a tiny in-memory DataFrame. No cluster needed. Runs in seconds.

```python
# test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.transformations import compute_spending_summary

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()

def test_compute_spending_summary_excludes_refunds(spark):
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("transaction_type", StringType()),
    ])
    data = [
        ("c1", 100.0, "purchase"),
        ("c1", -20.0, "refund"),   # should be excluded
        ("c2", 50.0, "purchase"),
    ]
    df = spark.createDataFrame(data, schema)
    result = compute_spending_summary(df)

    c1_total = result.filter("customer_id = 'c1'").collect()[0]["total_spend"]
    assert c1_total == 100.0, "Refunds should not reduce total_spend"
```

Unit tests validate logic in isolation. They are the cheapest tests to run and should be the majority of your test suite.

#### Layer 2 — Integration Tests

Run the job against real infrastructure — a real S3 bucket, a real Redshift cluster, a real Kafka topic — with controlled test data. Integration tests catch the things unit tests cannot: IAM permission errors, network connectivity issues between services, wrong config values for test vs. production.

```python
# test_integration.py
# Runs against a dedicated test S3 bucket and Glue catalog
def test_pipeline_writes_to_s3(spark, test_s3_bucket, test_glue_catalog):
    run_pipeline(
        input_path=f"s3://{test_s3_bucket}/input/sample/",
        output_path=f"s3://{test_s3_bucket}/output/spending_summary/",
        catalog=test_glue_catalog,
    )
    result = spark.read.parquet(f"s3://{test_s3_bucket}/output/spending_summary/")
    assert result.count() > 0
    assert "customer_id" in result.columns
    assert "total_spend" in result.columns
```

Integration tests are slower (minutes, not seconds) and require real cloud resources. Run them on pull requests to main or staging branches, not on every commit.

#### Layer 3 — Contract Tests

Validate that the pipeline's output matches the data contract — the agreed schema, acceptable null rates, value ranges, and row count expectations. A data contract (the formal agreement between a producer and consumer about what data will be produced and what it will look like) is only useful if you verify it automatically.

```python
# test_contract.py
def test_spending_summary_contract(spark, test_output_path):
    df = spark.read.parquet(test_output_path)
    total_rows = df.count()

    # Schema check
    assert "customer_id" in df.columns
    assert "total_spend" in df.columns
    assert "as_of_date" in df.columns

    # Null rate check — customer_id must never be null
    null_customer_ids = df.filter("customer_id IS NULL").count()
    assert null_customer_ids == 0, "customer_id must not be null"

    # Value range check — total_spend should be non-negative
    negative_spend = df.filter("total_spend < 0").count()
    assert negative_spend == 0, "total_spend must be non-negative"

    # Row count in expected range
    assert 1_000 <= total_rows <= 10_000_000, f"Unexpected row count: {total_rows}"
```

Contract tests act as a firewall between the pipeline and its consumers. If the output drifts from what consumers expect, the contract test fails in CI before the change ever ships.

#### Layer 4 — End-to-End Tests

Run the full pipeline — source extraction, transformation, load, and serving — on a realistic sample dataset. These are the most expensive tests. Run them on merges to staging or before promoting to production. End-to-end tests catch orchestration bugs, dependency ordering issues, and problems that only appear when all the pieces run together.

---

### GitHub Actions Workflow Examples

A single GitHub Actions workflow file that runs all test layers on a pull request:

```yaml
# .github/workflows/pipeline-ci.yml
name: Pipeline CI

on:
  pull_request:
    branches: [main, staging]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.10"
      - name: Install dependencies
        run: pip install -r requirements-dev.txt
      - name: Run unit tests
        run: pytest tests/unit/ -v --tb=short

  dbt-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install dbt
        run: pip install dbt-spark
      - name: Run dbt compile (catches syntax errors)
        run: dbt compile --profiles-dir .dbt --target ci
      - name: Run dbt tests
        run: dbt test --profiles-dir .dbt --target ci

  airflow-dag-validation:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install Airflow
        run: pip install apache-airflow==2.8.0
      - name: Validate all DAG files parse without errors
        run: |
          for dag_file in dags/*.py; do
            echo "Validating $dag_file"
            python -c "import importlib.util; spec = importlib.util.spec_from_file_location('dag', '$dag_file'); mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)"
          done

  schema-compatibility:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
        with:
          fetch-depth: 0
      - name: Check for breaking schema changes
        run: python scripts/check_schema_compatibility.py --base-branch origin/main
```

---

### Schema Compatibility Checks in CI

A breaking schema change is one where a downstream consumer will fail after the change ships: removing a column, renaming a column, narrowing a type (e.g., `BIGINT` to `INT`), or making a nullable column non-nullable. Detect these automatically in CI.

```python
# scripts/check_schema_compatibility.py
import subprocess, json, sys

BREAKING_CHANGES = {"column_removed", "type_narrowed", "nullable_to_required"}

def get_schema(ref, schema_path):
    result = subprocess.run(
        ["git", "show", f"{ref}:{schema_path}"],
        capture_output=True, text=True
    )
    return json.loads(result.stdout)

def detect_breaking_changes(old_schema, new_schema):
    old_cols = {c["name"]: c for c in old_schema["columns"]}
    new_cols = {c["name"]: c for c in new_schema["columns"]}
    issues = []

    for col_name, old_col in old_cols.items():
        if col_name not in new_cols:
            issues.append({"type": "column_removed", "column": col_name})
        else:
            new_col = new_cols[col_name]
            if old_col["type"] != new_col["type"]:
                issues.append({
                    "type": "type_changed",
                    "column": col_name,
                    "from": old_col["type"],
                    "to": new_col["type"],
                })
    return issues

if __name__ == "__main__":
    issues = detect_breaking_changes(
        get_schema("origin/main", "schemas/spending_summary.json"),
        get_schema("HEAD", "schemas/spending_summary.json"),
    )
    if issues:
        print("Breaking schema changes detected:")
        for issue in issues:
            print(f"  {issue}")
        sys.exit(1)
    print("Schema compatibility check passed.")
```

For teams using a schema registry (a central store that tracks schema versions and enforces compatibility rules — common with Confluent Schema Registry for Kafka or AWS Glue Schema Registry for S3), the CI check calls the registry's compatibility API instead of doing the diff manually.

---

### Artifact Management

An artifact (a versioned, immutable build output) ensures that what you tested is exactly what you deploy. Three types matter most for data pipelines:

**Versioned Docker images for Spark jobs.** Build the image in CI, tag it with the Git SHA (the unique commit identifier), push to a container registry.

```yaml
# In GitHub Actions
- name: Build and push Spark job image
  run: |
    IMAGE_TAG="${{ github.sha }}"
    docker build -t myregistry/spending-pipeline:${IMAGE_TAG} .
    docker push myregistry/spending-pipeline:${IMAGE_TAG}
    # Also tag as latest for the current environment
    docker tag myregistry/spending-pipeline:${IMAGE_TAG} myregistry/spending-pipeline:staging
    docker push myregistry/spending-pipeline:staging
```

**Python packages for shared libraries.** If your transformation logic is shared across pipelines, package it and publish it to a private PyPI registry. Each pipeline pins to a specific version.

**dbt compiled artifacts.** The `dbt compile` command produces a `manifest.json` that records every model, test, and dependency. Store it per-deployment so you can trace exactly which SQL ran in production on any given day.

---

### Branch-Based Promotion

Each environment gets its own branch. Code flows in one direction: feature branch → dev → staging → prod.

```
feature/fix-null-handling
        |
        | PR + unit tests + schema check
        v
      dev  ← merged here, integration tests run automatically
        |
        | PR + integration + contract tests
        v
    staging ← manual review, end-to-end tests run
        |
        | Manual approval gate (team lead or on-call approves)
        v
      prod
```

The manual approval gate before production is deliberate. Automated tests catch most problems, but a human reviewing the staging run results before promoting to production is cheap insurance. In GitHub Actions, use `environment: production` with a required reviewer to enforce this.

```yaml
deploy-prod:
  needs: [deploy-staging, e2e-tests]
  environment:
    name: production
    url: https://your-monitoring-dashboard
  # GitHub will pause here until a listed reviewer approves
  steps:
    - name: Deploy to production
      run: ./scripts/deploy.sh --env prod --image-tag ${{ github.sha }}
```

---

### Rollback Strategies for Data

This is where data CI/CD diverges most sharply from application CI/CD. If your Spark job wrote bad data to a production partition, redeploying the old Docker image does nothing for the rows already written.

**Strategy 1 — Idempotent partition rewrites.** Design pipelines to fully overwrite the partition they are responsible for on every run. If the run was wrong, re-run with the corrected code and the partition is cleanly replaced.

```python
# Spark — always overwrite the target partition, never append
df.write \
  .mode("overwrite") \
  .partitionBy("event_date") \
  .parquet("s3://prod-bucket/spending_summary/")
```

This only works if your pipeline logic is idempotent (running it twice on the same input produces the same output with no side effects). Pipelines that append to a table without deduplication logic cannot safely rerun.

**Strategy 2 — Iceberg or Delta time travel.** Apache Iceberg and Delta Lake both maintain a transaction log of every write. You can restore a table to a prior snapshot without touching the current data files.

```sql
-- Iceberg: restore the table to its state before the bad run
CALL catalog.system.rollback_to_snapshot(
  'prod.spending_summary',
  snapshot_id => 4567890123456789012  -- the snapshot ID before the bad write
);

-- Delta Lake equivalent
RESTORE TABLE prod.spending_summary TO VERSION AS OF 42;
```

Know your snapshot retention policy. If the bad write happened 8 days ago and your retention is 7 days, the snapshot is gone.

**Strategy 3 — Feature flags to cut traffic.** A feature flag (a runtime configuration switch that controls which code path executes) lets you redirect consumers to the old pipeline output without redeploying anything.

```python
# In the downstream consumer
if feature_flags.is_enabled("use_new_spending_summary_v2"):
    df = spark.read.table("prod.spending_summary_v2")
else:
    df = spark.read.table("prod.spending_summary_v1")
```

Run old and new pipelines in parallel during rollout. If the new pipeline has a problem, flip the flag. No redeployment, no data recovery needed.

---

### Deploying Airflow DAGs Safely

Airflow DAGs (Directed Acyclic Graphs — the Python files that define your workflow's tasks and dependencies) have a specific deployment challenge: the Airflow scheduler parses DAG files continuously. If you update a file mid-run, you can corrupt the run state.

**Option 1 — git-sync.** A sidecar container (a helper container that runs alongside the main Airflow container) continuously pulls the latest DAGs from a Git repo. The Airflow scheduler reads from this synced directory. Merging to the `airflow-dags` branch is the deployment action.

```yaml
# In your Airflow Helm values (Kubernetes deployment config)
gitSync:
  enabled: true
  repo: "https://github.com/yourorg/airflow-dags"
  branch: "main"
  subPath: "dags/"
  period: 60  # sync every 60 seconds
```

**Option 2 — Image-based DAG deployment.** Bake the DAG files into the Airflow Docker image. No git-sync sidecar needed. Deploying new DAGs means deploying a new image — slower, but gives you a complete artifact that includes both the DAGs and their dependencies.

**DAG versioning to avoid mid-run problems.** For long-running DAGs (runs that span multiple hours), do not modify the DAG file in place while a run is active. Use versioned DAG IDs (`spending_pipeline_v2`) and route new runs to the new version while letting old runs finish on the old one.

---

### Environment Parity

Test environments that don't mirror production structure give you false confidence. Specific rules:

| What to mirror | Why it matters |
|---|---|
| Table format (Iceberg vs. Parquet vs. Delta) | A job that works on Parquet may fail on Iceberg due to schema evolution behavior |
| IAM roles and permission boundaries | The most common integration test failure is a permission that exists in prod but not in test |
| Data volume (at least 1% of prod) | Schema errors hide at scale; a 1,000-row test set won't catch partition skew or type overflow |
| Partitioning scheme | A pipeline that writes to `dt=2024-01-01` partitions may behave differently if test doesn't partition |
| Secrets management approach | Use the same secrets manager (e.g., AWS Secrets Manager), just different secret paths |

The test environment does not need to be the same size as production. It needs to be the same shape.

---

## Decision Guide

| Situation | Recommended approach |
|---|---|
| Need fast feedback on every commit | Unit tests only — run in under 2 minutes |
| Validating a schema change | Schema compatibility check in CI + contract tests against staging |
| New pipeline reaching staging | Integration tests against real infrastructure with test data |
| Promoting to production | Manual approval gate after end-to-end tests pass in staging |
| Bad data written to a partition | Idempotent rewrite with corrected code |
| Bad data in an Iceberg/Delta table | Time travel rollback to pre-write snapshot |
| Need to cut traffic from a bad new pipeline | Feature flag pointing consumers back to old pipeline |
| Deploying DAGs to a busy Airflow cluster | git-sync for frequent small changes; image-based for dependency changes |
| Shared transformation library used by many jobs | Publish versioned Python package, pin versions per pipeline |
| Schema registry available (Kafka/Glue) | Use registry API for compatibility check in CI instead of custom script |

---

## Anti-Patterns

| Anti-pattern | What you observe | Fix |
|---|---|---|
| No tests in CI, only manual QA before deploy | Bad data reaches production regularly; postmortems happen on a monthly cycle | Add unit and contract tests to CI; fail the PR if tests don't pass |
| Rolling back code and calling it done | Data in production still contains the bad values; downstream consumers report wrong numbers | Treat rollback as a two-step operation: revert code AND restore data via time travel or idempotent rewrite |
| Test environment with different table format than prod | Integration tests pass, pipeline fails in production due to schema evolution behavior | Mirror production's table format exactly in all test environments |
| Editing DAG files directly on the Airflow server | Mid-run failures, corrupted task states, scheduler errors | Use git-sync or image-based deployment; never edit DAG files in place |
| One shared environment for dev and staging | Dev changes break staging; no stable environment for pre-prod validation | Dedicate separate environments with separate IAM roles and separate storage buckets |
| Running full end-to-end tests on every commit | CI takes 45 minutes; engineers stop waiting and merge anyway | Layer your tests: unit on every commit, integration on PR to main, end-to-end on merge to staging |
| No schema compatibility check | A downstream team's pipeline silently breaks because a column was renamed | Add automated schema diff to CI; breaking changes fail the PR before merge |
| Appending to tables without deduplication | Reruns double-count data; rollback requires manual row deletion | Design pipelines to overwrite partitions; add deduplication logic if append is required |

---

## Interview Talking Points

**Q: How is CI/CD for data pipelines different from CI/CD for applications?**

The core difference is state. In application deployments, rolling back means redeploying the previous version — the system returns to its prior state. In data pipelines, the pipeline's output is data that persists independently of the code. If the pipeline ran and wrote bad data, reverting the deploy does nothing for those rows. Data CI/CD requires additional layers: idempotent pipeline design, time-travel-capable storage formats, and in some cases feature flags to redirect consumers — so rollback is a data operation, not just a code operation.

**Q: Walk me through your testing strategy for a PySpark pipeline.**

I use a four-layer pyramid. Unit tests validate individual transformation functions using tiny in-memory DataFrames — no cluster, runs in seconds. Integration tests run the job against real infrastructure (real S3, real catalog) with controlled test data, catching IAM and config issues. Contract tests verify the output schema, null rates, and value ranges match the data contract. End-to-end tests run the full pipeline on a realistic sample before promoting to staging. The key is running the right tests at the right stage: unit tests on every commit, integration tests on PRs to main, end-to-end tests before staging promotion.

**Q: How do you handle schema changes safely in a CI/CD pipeline?**

Every PR that touches a schema definition runs an automated compatibility check. The check diffs the schema in the PR branch against the schema in the base branch and fails if it detects breaking changes — removed columns, narrowed types, nullable-to-required changes. For teams using a schema registry, the check calls the registry's compatibility API. This catches the breaking change at the PR stage, before it reaches the consumers who depend on that schema.

**Q: How do you roll back a data pipeline that has already written bad data?**

Three options depending on the architecture. First, if the pipeline is idempotent and uses partition overwrite, fix the code and rerun — the partition is cleanly replaced. Second, if the table is Iceberg or Delta, use time travel to restore the table to the snapshot that existed before the bad write. Third, use a feature flag to cut consumer traffic back to the old pipeline output while you fix the new one — this works when you're running old and new pipelines in parallel during a rollout. In practice, I combine all three: idempotent pipelines as the baseline, time travel as the safety net, feature flags for zero-downtime cutover.

**Q: What does environment parity mean and why does it matter?**

Environment parity means test environments mirror production structure: same table formats, same IAM roles, same partitioning scheme, same secrets management approach. It matters because integration tests against a non-parity environment are testing a different system. The classic failure mode is a pipeline that passes all tests in a flat-Parquet test environment, then fails in production because the production tables are Iceberg with schema evolution enabled and the behavior differs. If your test environment is the wrong shape, a passing CI is false confidence.
