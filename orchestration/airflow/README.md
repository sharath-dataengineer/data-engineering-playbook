# Airflow Architecture & Orchestration Patterns

> Chapter from the Data Engineering Playbook — orchestration

## About This Chapter

- **What this is.** A production-focused guide to Apache Airflow: what it is, how its components fit together, the design patterns that work at scale, and the pitfalls that cause on-call pages at 2 AM.
- **Who it's for.** Mid-level data engineers who have touched Airflow and want to use it correctly, and senior engineers evaluating orchestration choices or debugging production incidents.
- **What you'll take away.**
  - A clear mental model of Airflow's architecture and how each component affects reliability
  - Concrete patterns for writing DAGs that are safe to deploy and easy to operate
  - A decision guide for executor types, sensor modes, and when to choose Airflow vs alternatives

---

Picture this: your Spark job that transforms raw clickstream data into a clean events table runs fine in isolation. But it must not start until the upstream S3 dump has landed, it must finish before the dbt models downstream kick off, and if it fails it should retry twice before paging the on-call engineer. You also need to know when any of those steps took longer than usual. None of that logic lives inside Spark. Something outside has to coordinate the whole sequence. That something is Apache Airflow.

---

## TL;DR

- Airflow is a **workflow scheduler**, not a compute engine. It decides *when* to run your Spark job and *in what order* — it does not do the computation itself.
- A **DAG** is your workflow drawn as a diagram: tasks are nodes, dependency arrows are edges, and there are no loops.
- Airflow has five main components: Scheduler, Webserver, Workers, Metadata database, and (for distributed setups) a message queue.
- Pick your **executor** based on scale: LocalExecutor for small teams, CeleryExecutor for horizontal scale, KubernetesExecutor for full isolation and cloud-native environments.
- Use **reschedule mode** on all sensors in production. Poke mode holds a worker slot the entire time it waits — that silently starves the rest of your pipeline.
- Never put **database queries, API calls, or heavy imports at the top level of a DAG file**. That code runs on every scheduler heartbeat and will bring Airflow to its knees.
- **XComs** are for small values — a file path, a row count. Large data always goes to S3 or a staging table; XComs that carry large payloads bloat the metadata database until it falls over.
- One **broken DAG import** can stall the scheduler from parsing all other DAGs. Monitor DAG import errors as a first-class alert.

---

## Why This Matters in Production

A financial services team runs a nightly pipeline: ingest → validate → transform → load → report. Each step is a separate Spark job or dbt run, each owned by a different subteam. Without an orchestrator, teams chain jobs together with cron and Bash scripts. The result: a job runs before its input is ready, no one notices until a downstream table is wrong, and the on-call engineer has to manually backfill three days of data on a Friday afternoon.

Airflow solves this with explicit dependency declarations, automatic retries, centralized logging, a UI showing exactly which task failed and why, and the ability to re-run any individual step for any historical date without touching the others. When something breaks, you know within minutes. When a dataset needs to be rebuilt from scratch, you trigger a backfill from the UI rather than writing a one-off script.

That is the production value of an orchestrator. The rest of this chapter is about using Airflow correctly so it adds that value instead of becoming the failure point itself.

---

## How It Works

### Core Concepts

**DAG (Directed Acyclic Graph):** A DAG is your workflow expressed as a diagram. Each node in the diagram is a task. The arrows between nodes express "this must finish before that starts." *Acyclic* means the graph has no cycles — you cannot have task A depend on task B which depends back on task A. In code, a DAG is a Python file that Airflow's Scheduler reads and registers.

**Task:** One unit of work inside a DAG. Examples: run this SQL query, submit this Spark job, check whether a file exists in S3, send a Slack alert. A task has no knowledge of the other tasks around it — it just does its own job and reports success or failure.

**Operator:** The *type* of a task — the template that defines what kind of work is done. `BashOperator` runs a shell command. `PythonOperator` calls a Python function. `SparkSubmitOperator` submits a Spark job to a cluster. `S3KeySensor` waits until a file appears in S3. You pick the operator that matches the system you want to interact with.

**Sensor:** A special operator whose job is to wait. It keeps checking a condition — did this file land? did this external DAG finish? — and only marks itself successful once the condition is true. Sensors are how you express "do not start step 2 until step 1's output is confirmed to exist."

**Task Instance:** One actual execution of a task for a specific date. If your DAG runs daily, Monday's execution of `load_events_table` is one task instance, Tuesday's is another. Each task instance has its own logs and its own state (running, success, failed, skipped, etc.).

**DAG Run:** One execution of the complete DAG for a specific logical date (called the `logical_date` or `execution_date`). A daily DAG gets one DAG run per day. Each DAG run contains one task instance per task.

---

### Airflow Architecture

Airflow has five components. Understanding each one tells you where to look when something is wrong.

**Scheduler:** The brain. It continuously reads all DAG files, identifies which task instances are ready to run (all upstream dependencies met, no active concurrency limits hit), and sends them to Workers. It also handles retries when tasks fail. If the Scheduler is slow or dead, nothing runs.

**Webserver:** The UI. It reads from the Metadata database to show you DAG run history, task logs, task instance states, and the graph view of your DAG. It does not directly control execution — it's a read-heavy view over the database.

**Workers:** The machines that actually execute task code. When a Worker receives a task, it runs the operator, captures stdout/stderr as logs, and reports the result back. In a LocalExecutor setup, the Scheduler itself is also the Worker. In a Celery setup, Workers are separate processes.

**Metadata Database (Postgres or MySQL):** The source of truth for all Airflow state — every DAG run, every task instance state, every XCom value, every log pointer. If the metadata database is slow or full, the entire system degrades. This database must be treated as a critical production system: backed up, monitored, vacuumed (for Postgres), and not used as a data warehouse.

**Message Queue (Redis or RabbitMQ):** Used only with CeleryExecutor. The Scheduler puts task messages on the queue; Celery Workers pull from it. Redis is the more common choice. Without a healthy queue, tasks queue up and never execute.

---

### Executor Types — Choose Based on Scale

The executor controls *how* tasks are distributed to Workers. This is one of the most consequential configuration decisions you will make.

**LocalExecutor**

Runs tasks as subprocesses on the same machine as the Scheduler. Simple to operate — no queue, no separate Worker fleet. Good for teams with fewer than a few hundred DAG runs per day and pipelines that do not require resource isolation between tasks.

```
Scheduler process → spawns subprocesses → tasks run on the same host
```

Use it when: you are a small team, you want minimal infra overhead, and your workload fits on one machine.

**CeleryExecutor**

The Scheduler places tasks on a Redis or RabbitMQ queue. A fleet of Celery Worker processes (which can be on separate machines) picks them up. Workers can be scaled horizontally — add more Workers, handle more concurrent tasks. Workers can run on different hardware profiles.

```
Scheduler → Redis queue → Celery Workers (N machines)
```

Use it when: you need more concurrency than one machine can provide, or you want Workers on separate hosts from the Scheduler. Adds operational complexity: you now manage the queue, Worker health, and autoscaling.

**KubernetesExecutor**

Each task gets its own Kubernetes pod. The pod is created when the task starts and destroyed when it finishes. This means complete isolation between tasks — one task's heavy memory usage cannot starve another. It also means the Workers scale to zero when there is nothing to run, which matters for cost.

Each task can specify its own Docker image and resource requests (CPU, memory), which is powerful for pipelines that mix lightweight Python scripts with memory-intensive Spark jobs.

```
Scheduler → Kubernetes API → one pod per task → pod destroyed on completion
```

Use it when: you run on Kubernetes already, you need strong task isolation, or your tasks have wildly different resource requirements. The startup latency per pod (a few seconds to a minute depending on image size) can be a drawback for very short-lived tasks.

---

### DAG Design Patterns

**The TaskFlow API (`@task` decorator)**

The modern way to write Python-based DAGs. Instead of instantiating `PythonOperator` objects manually and wiring up dependencies with `>>` operators, you write plain Python functions decorated with `@task`. Airflow handles the wiring.

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def events_pipeline():

    @task
    def extract() -> dict:
        # fetch raw data, return a dict
        return {"row_count": 5000, "s3_path": "s3://bucket/raw/2024-01-01/"}

    @task
    def validate(metadata: dict) -> str:
        assert metadata["row_count"] > 0, "Empty extract"
        return metadata["s3_path"]

    @task
    def transform(s3_path: str):
        # submit Spark job using s3_path
        pass

    path = validate(extract())
    transform(path)

events_pipeline()
```

Return values flow directly between functions as XComs without you manually setting or fetching them.

**Dynamic Task Generation**

Create tasks programmatically from a list — useful when you process a variable set of tables, partitions, or environments.

```python
from airflow.decorators import dag, task
from datetime import datetime

TABLES = ["orders", "customers", "products"]

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def multi_table_load():

    @task
    def load_table(table_name: str):
        print(f"Loading {table_name}")

    for table in TABLES:
        load_table(table_name=table)

multi_table_load()
```

Airflow 2.x with dynamic task mapping lets this list come from a prior task's output, enabling fully runtime-driven task generation.

**Task Groups**

Task groups let you visually cluster related tasks in the Airflow UI without changing execution behavior. Useful when a DAG has 30+ tasks and the graph view becomes unreadable.

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("validation_checks") as validation:
    check_nulls = PythonOperator(...)
    check_row_counts = PythonOperator(...)
    check_schema = PythonOperator(...)
```

In the UI, these appear as a collapsible group labeled `validation_checks`.

---

### Dependency Patterns and Trigger Rules

By default, a task runs only when *all* upstream tasks have succeeded (`ALL_SUCCESS`). Trigger rules let you override this for specific use cases.

| Trigger Rule | When the task runs | Typical use case |
|---|---|---|
| `ALL_SUCCESS` | All upstream tasks succeeded | Default — normal pipeline flow |
| `ALL_FAILED` | All upstream tasks failed | Rarely used |
| `ALL_DONE` | All upstream tasks finished, regardless of status | Cleanup or teardown steps |
| `ONE_FAILED` | At least one upstream task failed | Alerting and notification tasks |
| `ONE_SUCCESS` | At least one upstream task succeeded | Fan-in where any success is enough |
| `SHORT_CIRCUIT` | Controlled by `ShortCircuitOperator` returning True/False | Conditionally skip a branch |

**Practical example — alerting on failure:**

```python
send_alert = SlackOperator(
    task_id="send_failure_alert",
    trigger_rule="ONE_FAILED",
    message="Pipeline failed — check Airflow UI",
    ...
)

[extract, transform, load] >> send_alert
```

`send_alert` runs only when at least one of the upstream tasks fails. If everything succeeds, the alert task is skipped.

---

### Sensors: Poke Mode vs Reschedule Mode

A sensor checks a condition on an interval until it is true. The critical production decision is *what happens to the worker slot while the sensor is waiting*.

**Poke mode (default — avoid in production):** The sensor holds a Worker slot the entire time it is waiting. If it waits for 4 hours, it occupies a Worker slot for 4 hours. With a limited Worker pool and multiple sensors running simultaneously, you silently exhaust all available slots and every other task in the system stops starting.

**Reschedule mode (use this):** The sensor checks the condition, and if it is false, it releases the Worker slot and reschedules itself to check again after `poke_interval` seconds. Between checks, the slot is free for other tasks. This is almost always the right mode for production sensors.

```python
from airflow.sensors.s3_key_sensor import S3KeySensor

wait_for_file = S3KeySensor(
    task_id="wait_for_upstream_file",
    bucket_name="my-data-bucket",
    bucket_key="raw/{{ ds }}/events.parquet",
    mode="reschedule",       # release the slot between checks
    poke_interval=60,        # check every 60 seconds
    timeout=4 * 3600,        # fail after 4 hours
)
```

Always set a `timeout`. A sensor without a timeout will wait forever if the condition never becomes true.

---

### Backfill

Backfill means running a DAG for historical `logical_date` values — for example, rebuilding the last 30 days of an events table after fixing a bug in the transform logic.

**Trigger a backfill from the CLI:**

```bash
airflow dags backfill \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  my_events_dag
```

Airflow creates one DAG run per scheduled interval between the start and end dates and runs them. By default it tries to run them concurrently.

**Avoiding overload on downstream systems during backfill:**

A 30-day backfill creates 30 simultaneous DAG runs by default. If each run submits a Spark job, you just submitted 30 Spark jobs at once. Control this with:

```python
dag = DAG(
    dag_id="my_events_dag",
    max_active_runs=3,          # at most 3 DAG runs executing simultaneously
    max_active_tasks=5,         # at most 5 tasks running across all active runs
    ...
)
```

Set `max_active_runs=1` during a backfill if your downstream system (a database, an API, a Spark cluster) cannot handle parallel load.

---

### SLA Monitoring

An SLA (Service Level Agreement) on a task means: "if this task has not finished by X minutes after the DAG's scheduled time, alert someone." This is how you catch slow pipelines before they breach a downstream deadline.

```python
from datetime import timedelta

load_final_table = SparkSubmitOperator(
    task_id="load_final_table",
    sla=timedelta(hours=2),     # alert if this task hasn't completed within 2 hours of dag start
    ...
)
```

Configure SLA miss callbacks at the DAG level to route alerts to email or Slack:

```python
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    # send Slack message or PagerDuty alert
    pass

dag = DAG(
    dag_id="my_events_dag",
    sla_miss_callback=sla_miss_callback,
    ...
)
```

---

### XComs — Passing Values Between Tasks

XCom (cross-communication) lets tasks share small values: a file path produced by step 1 that step 2 needs to read, a row count from a validation step, a job ID from a Spark submit that a status-check task needs to poll.

```python
# Push an XCom value
def push_file_path(**context):
    context["ti"].xcom_push(key="output_path", value="s3://bucket/processed/2024-01-01/")

# Pull it in another task
def consume_file_path(**context):
    path = context["ti"].xcom_pull(task_ids="produce_file", key="output_path")
    print(f"Reading from {path}")
```

**The hard rule:** XComs are stored in the metadata database. They are not a data transport layer. Do not push DataFrames, large JSON blobs, or anything beyond a few kilobytes through XComs. Large XComs fill the metadata database, slow down all database queries, and in extreme cases crash Airflow entirely. Large data always moves through S3, GCS, or a staging table — only the *pointer* (path or table name) moves through XComs.

---

### Common Pitfalls That Cause Production Incidents

**Top-level DAG file code**

The Scheduler parses every DAG file on a tight loop (every 30–60 seconds by default). Any code at the module level — outside of a function or a DAG context manager — runs on every parse. Database queries, API calls, and heavy imports at the top level make the Scheduler slow and unpredictable.

```python
# BAD — this runs on every scheduler heartbeat
import pandas as pd
conn = psycopg2.connect(...)
df = pd.read_sql("SELECT * FROM config_table", conn)

# GOOD — move side effects inside task functions
@task
def load_config():
    import psycopg2
    conn = psycopg2.connect(...)
    ...
```

Move all imports and IO operations inside task functions or into factory functions called at dag definition time with care.

**DAG import errors**

If a DAG file raises an exception during import (a missing module, a syntax error, a bad config value), Airflow marks it as a broken import. In some versions and configurations, broken DAG imports stall the Scheduler from processing other DAGs. One broken DAG in a shared deployment can freeze the entire system.

Monitor the DAG import errors metric. Fix broken DAG files as fast as you would fix a production outage — because effectively it is one.

**Too many tasks per DAG file**

Splitting many unrelated DAGs across many files is better than packing them all into one file. Fewer DAG files means faster Scheduler parse cycles.

---

## Decision Guide

| Scenario | Recommendation |
|---|---|
| Small team, < 200 DAG runs/day | LocalExecutor — minimal infra, easy to operate |
| Medium team, need horizontal scale | CeleryExecutor with Redis — add Workers to handle load |
| Running on Kubernetes, cloud-native | KubernetesExecutor — task isolation, scale to zero |
| Sensor waiting for a file or event | `mode="reschedule"` — always |
| Passing a file path between tasks | XCom — fine, it's small |
| Passing a DataFrame between tasks | Do not use XCom — write to S3, pass the path |
| Running same pipeline for 30 past days | Backfill with `max_active_runs=2` or `3` to control load |
| Need to alert when pipeline runs long | Set `sla` on tasks + `sla_miss_callback` on the DAG |
| Writing Python-heavy DAGs | TaskFlow API (`@task` decorator) — cleaner and testable |
| Conditional branching in a pipeline | `ShortCircuitOperator` with `trigger_rule="ALL_DONE"` on cleanup |

---

## Anti-Patterns

| Anti-pattern | What you observe | Fix |
|---|---|---|
| DB query or API call at the top level of a DAG file | Scheduler becomes slow, high CPU on Scheduler process, parse times spike | Move all IO and heavy imports inside task functions |
| Using poke mode for long-running sensors | All Worker slots are occupied, new tasks stop starting, pipeline stalls silently | Set `mode="reschedule"` on every sensor |
| Pushing large data through XComs | Metadata database grows rapidly, Scheduler and Webserver slow down, eventual DB crash | Write large data to S3; XCom only the path |
| No `timeout` on sensors | Sensors wait indefinitely if the expected file never arrives; Worker slots held forever | Always set `timeout` on every sensor |
| One giant DAG file with 50+ DAGs | Slow parse times, hard to debug, DAG changes affect unrelated pipelines | One DAG per file; group related DAGs in subfolders |
| No `max_active_runs` during backfill | Backfill submits 60 Spark jobs simultaneously, clusters fall over, backfill fails | Set `max_active_runs=2` or `3` on the DAG |
| Ignoring DAG import errors | A broken DAG file silently stalls the Scheduler for all other DAGs | Alert on DAG import error count > 0 |
| Hardcoding dates in DAG logic | Backfills produce incorrect results because the date never changes | Use Airflow's `{{ ds }}` or `{{ logical_date }}` templating variables |

---

## Interview Talking Points

**Q: What is Airflow and what does it actually do?**
Airflow is a workflow scheduler. It decides when to run your jobs, in what order, with what retry behavior, and it gives you visibility into whether they succeeded. It does not do the compute itself — it tells your Spark cluster, your dbt runner, your Python script to start.

**Q: What is a DAG? What does "acyclic" mean and why does it matter?**
A DAG is your workflow expressed as a graph where nodes are tasks and edges are dependencies. Acyclic means no loops — task A cannot eventually depend on itself. Without that constraint, the Scheduler could never determine which task to run first.

**Q: How do you choose between CeleryExecutor and KubernetesExecutor?**
Celery is simpler to reason about and good for teams with stable Worker fleets. Kubernetes gives you per-task isolation, per-task Docker images, and scale-to-zero, but adds pod startup latency and requires a Kubernetes cluster. If you are already on Kubernetes and your tasks vary in resource needs, KubernetesExecutor is the better fit.

**Q: What is the difference between poke mode and reschedule mode on a sensor?**
Poke mode holds a Worker slot the entire time the sensor waits. Reschedule mode releases the slot between checks. In production, always use reschedule mode, or you will silently exhaust your Worker pool on long-running sensors.

**Q: Why is code at the top level of a DAG file dangerous?**
The Scheduler parses every DAG file on a continuous loop. Any top-level code — database calls, API calls, heavy imports — runs every parse cycle. This makes the Scheduler slow and can cause cascading failures. All side effects belong inside task functions.

**Q: When would you use XComs and when would you not?**
XComs for small coordination values: a path, a count, a job ID. Never for actual data. XComs are stored in the metadata database; large XComs are a direct path to database exhaustion and Airflow downtime.

**Q: How would you safely backfill 90 days of data without overwhelming your systems?**
Set `max_active_runs` to a small number (2–3) on the DAG before triggering the backfill, so only a few historical runs execute concurrently. Monitor downstream system load and adjust if needed. Airflow will queue the remaining runs and process them as capacity frees up.

**Q: How does Airflow compare to Prefect or Dagster?**
Airflow has the largest operator ecosystem and the most widespread production adoption — if you need an operator for a specific system, Airflow probably has one. Prefect and Dagster have better local development experiences, more native data-awareness (Dagster's asset model, for example), and simpler testing stories. For teams starting fresh with greenfield cloud-native pipelines, Prefect or Dagster are worth evaluating. For most established data engineering orgs, Airflow is still the default choice because of ecosystem maturity and team familiarity.
