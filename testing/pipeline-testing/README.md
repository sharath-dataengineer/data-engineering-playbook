# Testing Data Pipelines

> Chapter from the Data Engineering Playbook — testing

## About This Chapter

- **What this is.** A practical guide to testing data pipelines at every level — from isolated unit tests on a single transformation function to end-to-end validation of a full pipeline stack — plus patterns for catching the failures that unit tests will never find.
- **Who it's for.** Mid-level and senior data engineers who have written pipeline tests before but find that their tests keep missing real production failures, or who are starting to build out a testing strategy from scratch.
- **What you'll take away.**
  - A clear mental model of the four levels of pipeline testing and when each one earns its keep
  - Concrete patterns for testing PySpark jobs, dbt models, Kafka consumers, and incremental loads
  - A CI/CD integration strategy that runs the right tests at the right time without slowing every PR to a crawl

---

Your Spark job finishes in 47 minutes. Exit code zero. No exceptions in the logs. The downstream BI report refreshes. Your on-call rotation moves on. Three days later, a data analyst notices that the revenue column for last Tuesday is exactly half what it should be. The pipeline read the partition twice — no, wait, it deduplicated when it shouldn't have — no, the timezone conversion was off. The job succeeded. The data was wrong. Nobody got paged.

This is the core problem with testing data pipelines. The failure mode is not a crash. It's a table full of plausible-looking wrong numbers.

---

## TL;DR

- Data pipeline failures are usually silent: the job exits zero but the output is wrong. Tests must actively check the data, not just that the job completed.
- There are four levels of testing: unit (fast, isolated, catches logic bugs), integration (real infrastructure, catches config and connector bugs), contract (validates the output data itself), and end-to-end (validates the full stack before a prod deploy).
- Unit test your transformation functions with a local SparkSession and tiny in-memory DataFrames — no cluster, runs in seconds.
- Contract tests — schema validation, null rate checks, row count ranges, duplicate detection on the primary key — are what catch the "job succeeded but data is wrong" class of failure.
- Test dbt models with schema tests (not_null, unique, accepted_values) and custom SQL tests. Source freshness checks catch upstream feed problems before they propagate.
- For Kafka consumers, use testcontainers (a library that spins up a real Kafka broker in Docker during your test run) instead of mocking — mocks lie about how Kafka actually behaves.
- Incremental pipelines need their own test pattern: set up a base state, run the job with new data, verify only the new rows changed, then run the job again and verify the result is identical (idempotency).
- In CI/CD: unit tests on every PR, integration tests on merge to main, end-to-end tests before prod deploy. A failing test must block the pipeline — never silently skip it.

---

## Why This Matters in Production

A payments team runs a nightly Spark job that aggregates transaction data by merchant. The job has been stable for eight months. A new engineer refactors the deduplication logic — a three-line change to remove what looked like dead code. Tests pass. The code ships. The job continues to succeed every night.

Two weeks later, during a quarterly business review, the finance team notices that merchant revenue numbers since the refactor are inflated by 15-20% for merchants with multiple payment terminals. The deduplication code was not dead — it was suppressing double-counted events from terminals that emit a retry on network timeout. Without it, every retry event counts as a second transaction.

The job never failed. The schema never changed. Row counts were in the expected range. The only way to catch this was a contract test checking for duplicate transaction IDs in the output, or a unit test explicitly covering the "terminal retry" case in the deduplication function.

This is not a contrived example. Silent data corruption caused by logic changes — not infrastructure failures — is the most common production data quality incident in analytical pipelines.

---

## How It Works

### Why Pipeline Testing Is Harder Than Application Testing

When you test application code, the function takes an input and returns a value. The test checks that value. If the value is wrong, the assertion fails. The failure is immediate and explicit.

Data pipelines do not work this way. The "return value" is a table with millions of rows distributed across dozens of partitions. Checking it requires reading all of it — or sampling it and hoping the sample is representative. A pipeline that writes 950,000 rows when it should write 1,000,000 rows will not throw an exception. Neither will a pipeline that writes 1,000,000 rows where 50,000 of them have a NULL in a column that should never be NULL.

Three properties make this worse:

**State.** Pipelines operate on mutable external state — tables, partitions, files. The output is not a return value; it is a side effect on a storage system. Testing side effects requires reading back what was written and inspecting it.

**Non-determinism.** Many pipelines are not fully deterministic. Spark shuffles can produce different row orderings. `current_timestamp()` returns different values on every run. Late-arriving data makes the output of an incremental job depend on when it ran, not just what data existed. Tests need to account for this — asserting on exact row order or exact timestamps will produce flaky tests.

**Silent failures.** A pipeline can succeed (exit code zero, no exceptions) and produce wrong data. This happens when the logic is wrong, when the input data has unexpected nulls or schema drift, or when an upstream feed silently stopped sending certain event types. There is no exception to catch. The only way to detect this class of failure is to actively validate the output.

### The Four Levels of Pipeline Testing

#### Level 1: Unit Tests

**What they test:** A single transformation function in isolation, using a small in-memory DataFrame.

**When to run them:** On every save, on every PR. They run in seconds.

**What they catch:** Logic bugs in transformation code — wrong join conditions, incorrect filter predicates, bad deduplication logic, off-by-one errors in window functions.

**What they do not catch:** Infrastructure issues, permissions, actual connector behavior, schema drift from real upstream data.

Unit tests are fast because they never touch a real cluster or real storage. The SparkSession runs locally. The data lives in memory. A typical unit test suite for a moderately complex PySpark job should run in under two minutes.

#### Level 2: Integration Tests

**What they test:** The full job running against real infrastructure — real S3 buckets, real Redshift clusters, real Kafka topics — with controlled test data you load before the test and clean up after.

**When to run them:** On merge to main (in CI), not on every PR save.

**What they catch:** IAM permission issues (your job can't read from that S3 prefix), connector bugs (the Redshift JDBC driver behaves differently than your mock), network issues, configuration bugs that only appear when the full job runs against real systems.

**What they do not catch:** Logic bugs in transformation functions (unit tests cover that) or downstream data quality issues in output (contract tests cover that).

Integration tests are slower — they spin up real connections to real systems, load test data, run the job, and clean up. A well-designed integration test suite runs in 10-30 minutes.

#### Level 3: Contract Tests

**What they test:** The output data itself — against the data contract (the agreed-upon specification of what the output table should look like).

**When to run them:** After every pipeline run, as part of the pipeline itself, before the output is served to downstream consumers.

**What they catch:** The "job succeeded but data is wrong" failure mode. Schema drift (a column changed type). Unacceptable null rates. Values outside expected ranges. Duplicate rows on the primary key. Row counts outside the expected range for the time period.

Contract tests are qualitatively different from unit and integration tests. They do not test the code — they test the output. A pipeline could have perfect unit test coverage and still produce bad data if the input data has unexpected characteristics that the code handles incorrectly. Contract tests are the last line of defense before bad data reaches downstream consumers.

#### Level 4: End-to-End Tests

**What they test:** The full pipeline stack, from the source system through every transformation to the final serving table, using a realistic sample dataset.

**When to run them:** Before production deployments, not on every PR or merge.

**What they catch:** Full integration failures — things that require every piece of the stack to be running together to surface. Cross-pipeline dependencies. Orchestration sequencing bugs.

**What they do not catch:** Edge cases in individual transformation functions (unit tests), infrastructure permission bugs (integration tests), or output data quality issues (contract tests). End-to-end tests validate the wiring, not the logic.

End-to-end tests are the most expensive to run. They require a realistic test environment, a realistic sample of data, and enough time for the full pipeline to complete. Reserve them for pre-deployment gates, not routine CI.

---

### Unit Testing PySpark Jobs with pytest

The key setup decision is the SparkSession. Creating a SparkSession is expensive (3-8 seconds). If each test creates its own session, a 50-test suite takes several minutes just on session startup. The fix is a session-scoped fixture in `conftest.py` (pytest's shared fixture file) that creates one SparkSession and shares it across the entire test run.

**conftest.py**

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped fixture: one SparkSession shared across all tests.
    'local[2]' means run locally with 2 threads — no cluster needed.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("pipeline-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")  # keeps tests fast
        .getOrCreate()
    )
    yield session
    session.stop()
```

**The transformation function under test (deduplicate.py)**

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def deduplicate_by_latest(df: DataFrame, id_col: str, ts_col: str) -> DataFrame:
    """
    Keep only the most recent row per id_col, ranked by ts_col.
    Used to collapse retry events from payment terminals.
    """
    window = Window.partitionBy(id_col).orderBy(F.col(ts_col).desc())
    return (
        df.withColumn("_rank", F.rank().over(window))
          .filter(F.col("_rank") == 1)
          .drop("_rank")
    )
```

**The unit test (test_deduplicate.py)**

```python
from datetime import datetime
from deduplicate import deduplicate_by_latest

def test_keeps_only_latest_event_per_id(spark):
    """
    Given two events for the same transaction_id, the function
    should keep only the one with the later timestamp.
    """
    input_data = [
        ("txn_001", datetime(2024, 1, 1, 10, 0, 0), 100.0),  # earlier
        ("txn_001", datetime(2024, 1, 1, 10, 0, 5), 100.0),  # later — keep this
        ("txn_002", datetime(2024, 1, 1, 11, 0, 0), 250.0),  # only one — keep it
    ]
    schema = ["transaction_id", "event_time", "amount"]
    df = spark.createDataFrame(input_data, schema)

    result = deduplicate_by_latest(df, id_col="transaction_id", ts_col="event_time")

    assert result.count() == 2

    txn_001_row = result.filter("transaction_id = 'txn_001'").first()
    assert txn_001_row["event_time"] == datetime(2024, 1, 1, 10, 0, 5)


def test_single_event_passes_through_unchanged(spark):
    """
    A DataFrame with no duplicates should come through unmodified.
    """
    input_data = [
        ("txn_003", datetime(2024, 1, 2, 9, 0, 0), 75.0),
    ]
    schema = ["transaction_id", "event_time", "amount"]
    df = spark.createDataFrame(input_data, schema)

    result = deduplicate_by_latest(df, id_col="transaction_id", ts_col="event_time")

    assert result.count() == 1
```

Two things to notice. First, the test DataFrames are tiny — three rows, two rows. They do not need to be large to test the logic. Second, the tests cover both the case that can go wrong (duplicates) and the case that should be unchanged (no duplicates). Only testing the happy path is an anti-pattern covered later in this chapter.

---

### Testing dbt Models

dbt (data build tool) has a built-in testing framework. There are two types of tests: schema tests and custom SQL tests.

**Schema tests** are declared in `schema.yml` files alongside your models. They run with `dbt test`.

```yaml
# models/marts/finance/schema.yml
models:
  - name: fct_merchant_revenue
    columns:
      - name: merchant_id
        tests:
          - not_null          # fails if any merchant_id is NULL
          - unique            # fails if any merchant_id appears more than once
      - name: revenue_usd
        tests:
          - not_null
      - name: payment_method
        tests:
          - accepted_values:
              values: ['credit_card', 'debit_card', 'ach', 'wire']
```

The four built-in schema tests cover the most common contract violations: `not_null`, `unique`, `accepted_values`, and `relationships` (referential integrity — every foreign key value exists in the referenced table).

**Custom SQL tests** are SQL files in the `tests/` directory. The convention is: the test fails if the SQL returns any rows. This lets you express arbitrary data quality conditions.

```sql
-- tests/assert_no_negative_revenue.sql
-- This test fails (returns rows) if any merchant has negative revenue.
-- Negative revenue indicates a refund processing bug.

select
    merchant_id,
    revenue_usd
from {{ ref('fct_merchant_revenue') }}
where revenue_usd < 0
```

**Source freshness checks** validate that upstream data feeds are arriving on schedule. They are declared in `sources.yml` and run with `dbt source freshness`.

```yaml
# models/staging/sources.yml
sources:
  - name: payments_raw
    freshness:
      warn_after: {count: 2, period: hour}
      error_after: {count: 6, period: hour}
    loaded_at_field: _ingested_at
    tables:
      - name: transactions
```

Source freshness checks catch the case where an upstream system stopped sending data. Without them, your pipeline succeeds, produces zero new rows, and nobody is paged — downstream consumers just stop seeing new data.

---

### Running Contract Tests with Great Expectations

Great Expectations is a library for defining and running data quality checks on DataFrames or tables. The core concept is an "expectation" — a single checkable assertion about a column or table. Expectations are grouped into "expectation suites" and run as "checkpoints" (a checkpoint connects a suite to a data source and runs the validation).

A minimal example: validating the output of a daily revenue aggregation job before it is served to downstream consumers.

```python
import great_expectations as gx

context = gx.get_context()

# Define what "correct" output looks like for the daily revenue table.
suite = context.add_expectation_suite("daily_revenue_output")

validator = context.get_validator(
    batch_request=...,  # points to your output table/partition
    expectation_suite_name="daily_revenue_output"
)

# Schema checks
validator.expect_column_to_exist("merchant_id")
validator.expect_column_to_exist("revenue_usd")
validator.expect_column_to_exist("transaction_date")

# Null checks
validator.expect_column_values_to_not_be_null("merchant_id")
validator.expect_column_values_to_not_be_null("revenue_usd")

# Value range checks — revenue should be non-negative
validator.expect_column_values_to_be_between(
    column="revenue_usd",
    min_value=0,
    max_value=None  # no upper bound
)

# Row count should be in a reasonable range for a daily partition
# (adjust based on your actual business volume)
validator.expect_table_row_count_to_be_between(
    min_value=10_000,
    max_value=5_000_000
)

# No duplicate merchants in the daily output
validator.expect_column_values_to_be_unique("merchant_id")

validator.save_expectation_suite()
```

Run this as a checkpoint at the end of your pipeline job. If any expectation fails, the checkpoint returns a failed result. Treat a failed checkpoint the same way you would treat a job failure: alert on-call, quarantine the output, do not serve the data downstream.

---

### Testing Kafka Consumers

The standard advice for testing code that talks to external systems is "mock the external system." For Kafka consumers, this advice is harmful. Kafka has complex behavior around consumer groups, partition assignment, offset commits, rebalancing, and message ordering that a mock will not replicate. Tests that mock Kafka tell you nothing about whether your consumer code actually works against a real broker.

The correct approach is testcontainers — a library that spins up a real Kafka broker in a Docker container at the start of your test run and tears it down at the end. Your tests connect to a real broker. The consumer behavior you observe in tests is the same behavior you will see in production.

```python
import pytest
from testcontainers.kafka import KafkaContainer
from kafka import KafkaProducer, KafkaConsumer
import json

@pytest.fixture(scope="session")
def kafka_container():
    """Spin up a real Kafka broker in Docker for the test session."""
    with KafkaContainer("confluentinc/cp-kafka:7.4.0") as kafka:
        yield kafka

def test_consumer_processes_payment_event(kafka_container):
    bootstrap_servers = kafka_container.get_bootstrap_server()
    topic = "payments.raw"

    # Publish a test event to the real Kafka broker
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer.send(topic, {
        "transaction_id": "txn_test_001",
        "amount": 99.99,
        "currency": "USD"
    })
    producer.flush()

    # Run your consumer logic against the real broker
    from your_pipeline.consumer import consume_and_process
    result = consume_and_process(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        max_messages=1
    )

    assert len(result) == 1
    assert result[0]["transaction_id"] == "txn_test_001"
    assert result[0]["amount"] == 99.99
```

The testcontainers approach requires Docker to be available in your CI environment, but this is true of most modern CI systems. The startup time for a Kafka container is 10-20 seconds — acceptable for an integration test, not acceptable for a unit test.

---

### Testing Incremental Pipelines

Incremental pipelines — jobs that process only new or changed data since the last run — are the hardest category to test correctly. They have state (the current contents of the target table), and the correct output depends on both the input data and the existing state.

A complete incremental pipeline test must verify three things:

1. **New rows are written.** After the incremental run, the new data is in the target table.
2. **Existing rows are untouched.** Data from previous runs is not modified or duplicated.
3. **Idempotency.** Running the job a second time with the same input produces the same output. This is the property that makes reruns safe after a failure.

```python
def test_incremental_load_pattern(spark, tmp_path):
    """
    Tests all three properties of a correct incremental load:
    1. New data is written
    2. Existing data is untouched
    3. Running twice produces the same result (idempotency)
    """
    target_path = str(tmp_path / "output")

    # --- Step 1: Set up the base state (data from previous runs) ---
    base_data = [
        ("merchant_A", "2024-01-01", 1000.0),
        ("merchant_B", "2024-01-01", 2500.0),
    ]
    base_df = spark.createDataFrame(base_data, ["merchant_id", "date", "revenue"])
    base_df.write.mode("overwrite").parquet(target_path)

    # --- Step 2: Run the incremental job with new data ---
    new_data = [
        ("merchant_A", "2024-01-02", 1100.0),  # new date for existing merchant
        ("merchant_C", "2024-01-02", 800.0),   # entirely new merchant
    ]
    new_df = spark.createDataFrame(new_data, ["merchant_id", "date", "revenue"])

    from your_pipeline.incremental import run_incremental_load
    run_incremental_load(new_df, target_path)

    result_after_first_run = spark.read.parquet(target_path)

    # New rows were written
    assert result_after_first_run.count() == 4

    # Existing rows are untouched
    merchant_a_jan1 = (
        result_after_first_run
        .filter("merchant_id = 'merchant_A' AND date = '2024-01-01'")
        .first()
    )
    assert merchant_a_jan1["revenue"] == 1000.0  # unchanged from base state

    # --- Step 3: Run the same incremental job again (idempotency check) ---
    run_incremental_load(new_df, target_path)

    result_after_second_run = spark.read.parquet(target_path)

    # Same number of rows — no duplicates introduced by the rerun
    assert result_after_second_run.count() == 4
```

The idempotency check (Step 3) is the one most teams skip. It is the property that makes your pipeline safe to rerun after a failure. If your pipeline is not idempotent, every failure forces a manual investigation to determine whether the partial run needs to be rolled back before the retry.

---

### What to Do When a Test Fails in Production

A contract test runs after your pipeline writes output to the target table and finds 8% NULL values in a column that should never be NULL. The job technically succeeded. What do you do?

**1. Quarantine the output.** Do not allow downstream consumers to read the new partition or table version. The specific mechanism depends on your architecture: swap back to the previous table version in Delta Lake, drop the bad partition, flip a feature flag that gates the data serving layer. The goal is to prevent bad data from reaching dashboards, ML models, or downstream pipelines.

**2. Alert on-call.** A failed contract test is a production incident, not a failed unit test to be retried. Page the on-call engineer. Include the failed expectation, the column, the partition, and the job run ID in the alert.

**3. Investigate and fix the root cause.** Check the input data for unexpected nulls or schema changes. Check the pipeline code for logic errors introduced in the most recent deploy. Check upstream systems for feed interruptions.

**4. Re-run after the fix.** Once the root cause is resolved, rerun the pipeline job for the affected partition. If your pipeline is idempotent (and it should be — see above), this is safe. The contract tests run again on the new output.

**The one thing you must never do** is suppress the failed test and keep serving the bad data downstream. This is worse than having no tests at all, because it creates false confidence. Downstream consumers do not know the data is wrong. They make decisions on it. The eventual discovery is more expensive and more damaging than a brief data outage.

---

### CI/CD Integration

A common mistake is running all test types in every CI stage. This produces a CI pipeline that takes 45 minutes on every PR, so engineers stop running it. The correct design runs the right tests at the right stage based on speed and cost.

**On every PR (fast feedback):**
- Unit tests only
- Target: under 5 minutes
- Fail the PR on any test failure
- No real infrastructure — local SparkSession, no real S3/Redshift/Kafka

**On merge to main (medium feedback):**
- Unit tests + integration tests
- Target: under 30 minutes
- Fail the merge on any test failure
- Runs against real infrastructure with isolated test environments (dedicated test S3 prefix, test Kafka topics, ephemeral Redshift schema)

**Before production deployment (pre-deploy gate):**
- Unit tests + integration tests + end-to-end tests
- Target: under 2 hours
- Fail the deployment on any test failure
- Runs against a staging environment with a realistic data sample

**After every pipeline run (contract validation):**
- Contract tests run as part of the pipeline itself, not in a separate CI stage
- If contract tests fail, the pipeline marks itself as failed, quarantines output, and pages on-call

---

## Decision Guide

| Situation | Test type to add |
|---|---|
| You changed a transformation function | Unit test covering the changed logic, including edge cases |
| You changed a pipeline's IAM role or S3 path configuration | Integration test that runs the job against real infra |
| Jobs succeed but downstream analysts report wrong numbers | Contract test on the output (nulls, ranges, duplicates, row counts) |
| You added a new upstream data source | Source freshness check + integration test loading from the new source |
| You are deploying a pipeline that has not run in production before | End-to-end test on staging with a realistic data sample |
| Your Kafka consumer behaves differently in production than in tests | Replace mocked Kafka with testcontainers |
| Your pipeline rerun after a failure produces duplicate rows | Idempotency test for the incremental load |
| A dbt model's output schema changed and broke a downstream model | dbt `relationships` and `not_null` schema tests on the downstream model |
| A job succeeded but wrote zero rows (upstream feed stopped) | Source freshness check or row count expectation in contract tests |

---

## Anti-Patterns

| Anti-pattern | What you observe | Fix |
|---|---|---|
| Only testing the happy path | Tests pass but production failures involve late data, NULLs, schema changes, or empty partitions — none of which appear in tests | Add explicit test cases for NULL inputs, empty DataFrames, duplicate keys, and out-of-order timestamps |
| Using row count as the only quality check | "We have 1M rows, so it must be right" — but 50K of those rows have wrong values in a critical column | Add column-level contract tests: null rates, value ranges, duplicate detection on the primary key |
| Not testing idempotency | After a pipeline failure and rerun, row counts double or aggregations are inflated | Add an idempotency test: run the job twice with the same input, assert the output is identical on both runs |
| Mocking the database or message broker in integration tests | Tests pass but production failures involve connector behavior, partition assignment, or offset behavior that the mock never replicated | Use testcontainers for Kafka; use real (isolated) infrastructure for database integration tests |
| Skipping schema change tests | A column is renamed or its type changes upstream; the pipeline succeeds but the output column is all NULLs or missing entirely | Add contract tests that check column names and types; add dbt `not_null` tests on all critical columns |
| No contract tests after pipeline writes | Pipeline code is well-tested but the output data degrades silently due to input data issues | Run a Great Expectations checkpoint or equivalent after every write, before marking the job as successful |
| Suppressing a failed test to avoid an outage | Bad data reaches downstream consumers; the incident is discovered days later by a data analyst | Treat a failed contract test as a production incident: quarantine, alert, investigate, rerun |
| Running all tests on every PR | CI takes 45 minutes; engineers skip it or push without waiting | Tier tests by speed: unit on every PR, integration on merge to main, end-to-end before deploy |

---

## Interview Talking Points

**Q: How is testing a data pipeline different from testing application code?**

The core difference is the failure mode. Application code either throws an exception or returns a wrong value — both are detectable. A data pipeline can exit zero and produce a table full of wrong numbers. No exception, no alert, no paging. You have to actively inspect the output to know if it is correct. That changes how you design tests: you cannot rely on the job succeeding to know the data is good.

**Q: What is a contract test and why does it matter?**

A contract test validates the output data against the specification of what it should contain — the right schema, acceptable null rates, values within expected ranges, no duplicates on the primary key, row counts in the expected range. Contract tests are what catch the "job succeeded but data is wrong" class of failure. They run after the pipeline writes output, before downstream consumers read it. If a contract test fails, you quarantine the output and page on-call — you do not serve bad data downstream.

**Q: How do you test a PySpark job without a cluster?**

You create a local SparkSession in `conftest.py` with `SparkSession.builder.master("local[2]")`. It runs in-process on your laptop or CI runner — no cluster needed. You create tiny test DataFrames with `spark.createDataFrame()`, call your transformation function, and assert on the result. The key optimization is making the SparkSession session-scoped so it is created once per test run, not once per test.

**Q: How would you test a Kafka consumer?**

Use testcontainers to spin up a real Kafka broker in Docker during the test run. Connect your consumer to that broker, publish test events to it, run the consumer logic, assert on what was processed. Mocking Kafka is unreliable because Kafka has complex behavior around consumer groups, partition rebalancing, and offset commits that mocks do not replicate. The testcontainers approach gives you a real broker with no production dependency.

**Q: What does it mean for a pipeline to be idempotent, and how do you test it?**

An idempotent pipeline produces the same output regardless of how many times it runs with the same input. This property makes reruns safe after failures. To test it: set up a base state, run the incremental job with new data, snapshot the output, run the same incremental job again with the same new data, and assert the output is identical to the snapshot. If row counts double or aggregations change on the second run, the pipeline is not idempotent.

**Q: How do you integrate tests into a CI/CD pipeline without slowing every PR to a crawl?**

Tier the tests by speed and cost. Unit tests on every PR — they run in under five minutes with no real infrastructure. Integration tests on merge to main — they hit real infrastructure but are isolated (dedicated test prefixes, ephemeral schemas). End-to-end tests as a pre-deployment gate — slow and expensive, but you only pay that cost before promoting to production. Contract tests are different: they are not a CI stage, they run as part of the pipeline itself after every write.
