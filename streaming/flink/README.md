# Apache Flink for Data Engineers

> Chapter from the Data Engineering Playbook — streaming

## About This Chapter

- **What this is.** A production-focused guide to Apache Flink — how it works, how it differs from Spark Structured Streaming, and when to reach for it in your data platform.
- **Who it's for.** Mid-level data engineers who understand Kafka and batch pipelines, and senior DEs evaluating Flink vs Spark for new streaming workloads.
- **What you'll take away.**
  - A clear mental model of Flink's architecture and when it outperforms Spark Structured Streaming
  - Practical understanding of stateful processing, checkpointing, and exactly-once delivery
  - A decision guide for choosing between Flink and Spark based on your actual workload requirements

---

You have a fraud detection pipeline. A transaction arrives in Kafka. Within 200 milliseconds, your pipeline needs to look up the user's transaction history, apply a scoring model, and write a risk signal back to Kafka. Spark Structured Streaming collects events into micro-batches every few seconds — your p99 latency is 4 seconds. That's too slow for real-time fraud decisions. This is the problem Apache Flink was built to solve.

---

## TL;DR

- Flink is a true streaming engine: events are processed one at a time as they arrive, not collected into batches first. This gives sub-second (milliseconds) latency.
- Spark Structured Streaming is micro-batch: it collects events for a configurable interval (e.g., 1–5 seconds), then processes the batch. Simpler, but latency is bounded by the batch interval.
- Flink's core unit of work is the **job**: a directed graph of operators (sources, transformations, sinks) running in parallel across a cluster.
- Flink keeps **state** per key (per user, per session) durably in a state backend. For large state (TBs), RocksDB is the go-to choice.
- **Checkpointing** snapshots all state to S3 or HDFS periodically. On failure, Flink restarts from the checkpoint and replays Kafka from the saved offset — this is what enables exactly-once processing.
- Use **event time** (the timestamp in the event itself) for analytics, financial, and audit workloads. Processing time (when Flink received the event) is simpler but gives wrong answers for out-of-order data.
- **Watermarks** are Flink's mechanism for closing time windows despite late-arriving events. Without them, windows stay open forever and state grows unbounded.
- Choose Flink when: latency must be under 1 second, state is very large, you need session windows or complex event patterns, or your team already operates Flink.

---

## Why This Matters in Production

Consider a real-time session analytics pipeline for an e-commerce platform. Users generate click and purchase events. The business needs:

1. A session window that captures all events within 30 minutes of inactivity per user
2. A running count of revenue per region, updated every minute
3. An alert if a user's cart is abandoned for more than 10 minutes

With Spark Structured Streaming, the 30-minute session window and the sub-second cart abandonment alert are awkward. Spark's micro-batch model means you're already incurring multi-second latency before any processing starts. Session windows require stateful aggregation that scales poorly in Spark's memory model.

With Flink, each event is processed the moment it arrives. State is kept per `user_id` in RocksDB, so you handle millions of concurrent sessions without running out of JVM heap. The session window closes naturally when Flink's watermark advances past the 30-minute inactivity threshold. The cart abandonment alert fires within milliseconds of the condition being met.

The flip side: if your team knows Spark well and your SLA is "results within 10 seconds," Spark Structured Streaming is the lower-risk choice. Flink has a steeper operational learning curve.

---

## How It Works

### Architecture: JobManager and TaskManagers

Flink's cluster has two roles, analogous to Spark's Driver and Executors.

**JobManager** (the coordinator): accepts a submitted job, builds the execution plan, distributes work to TaskManagers, and monitors for failures. If a TaskManager dies, the JobManager triggers a restart from the last checkpoint. There is one active JobManager per Flink cluster (with optional standby for HA).

**TaskManagers** (the workers): actually process your data. Each TaskManager has a fixed number of **slots** — a slot is a unit of parallelism, roughly one CPU core's worth of processing capacity. A parallel operator instance (e.g., one of 16 parallel `map` operators) occupies one slot.

When you set `parallelism = 16` on your Flink job, Flink schedules 16 instances of each operator across the available slots. If your TaskManagers each have 4 slots and you have 4 TaskManagers, you have 16 total slots — a perfect fit.

```
JobManager
  └─ schedules and coordinates

TaskManager 1 (4 slots)    TaskManager 2 (4 slots)
  ├─ Slot 1: Source[0]       ├─ Slot 1: Source[2]
  ├─ Slot 2: Map[0]          ├─ Slot 2: Map[2]
  ├─ Slot 3: KeyBy→Agg[0]   ├─ Slot 3: KeyBy→Agg[2]
  └─ Slot 4: Sink[0]         └─ Slot 4: Sink[2]
```

### DataStream API

The DataStream API is Flink's Java and Python interface for writing streaming transformations. You work with a `DataStream<T>` object and chain operators:

```python
# Python DataStream API (PyFlink)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(16)

source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("transactions") \
    .set_group_id("flink-fraud-detector") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

# Transform: parse JSON, filter, key by user_id
result = (
    stream
    .map(lambda x: json.loads(x))                    # parse JSON string to dict
    .filter(lambda e: e["amount"] > 0)               # drop invalid records
    .key_by(lambda e: e["user_id"])                  # route same user_id to same slot
    .map(enrich_with_risk_score)                     # stateful per-user enrichment
)

result.sink_to(kafka_sink)
env.execute("fraud-detector")
```

`key_by` is the critical operator: it partitions the stream so that all events with the same key always go to the same slot. This is what makes per-key state coherent — all events for `user_id=42` land on the same TaskManager slot, which holds that user's state.

### Table API and Flink SQL

Flink SQL lets you write streaming queries using standard SQL syntax. Under the hood it compiles to the same operator graph as the DataStream API. For most analytics patterns, Flink SQL is faster to write and easier to maintain.

```sql
-- Flink SQL: count orders per region per minute (tumbling window)
-- This query runs continuously; results update every minute as new events arrive

CREATE TABLE orders (
    order_id     STRING,
    region       STRING,
    amount       DOUBLE,
    event_time   TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic'     = 'orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'    = 'json'
);

CREATE TABLE order_counts_per_minute (
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    region        STRING,
    order_count   BIGINT,
    total_revenue DOUBLE
) WITH (
    'connector' = 'iceberg',
    'catalog-name' = 'prod_catalog',
    'database-name' = 'streaming',
    'table-name'    = 'order_counts_per_minute'
);

INSERT INTO order_counts_per_minute
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE)  AS window_start,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE)    AS window_end,
    region,
    COUNT(*)                                        AS order_count,
    SUM(amount)                                     AS total_revenue
FROM orders
GROUP BY
    TUMBLE(event_time, INTERVAL '1' MINUTE),
    region;
```

This query runs indefinitely. Each time Flink's watermark advances past the end of a 1-minute window, results for that window are emitted and written to Iceberg. You never write a batch job, a scheduler, or a `GROUP BY ... WHERE event_time BETWEEN ...` query.

### Event Time vs Processing Time

This distinction determines whether your pipeline produces correct results.

**Processing time**: the wall-clock time when Flink's TaskManager processed the event. Simple — no timestamp parsing needed. But if Kafka has a backlog and events arrive 5 minutes late, your 1-minute windows will contain events that happened at different real-world times. For counting pageviews or debugging, this is fine. For financial reconciliation or SLA reporting, it gives wrong answers.

**Event time**: the timestamp embedded in the event itself — the time the event actually occurred. Correct even when events arrive out of order or late. Requires a timestamp field in your data and a watermark strategy.

Rule of thumb: use event time for any workload where the time something happened matters — financial transactions, clickstream analytics, audit logs, SLA measurement. Use processing time only for low-stakes monitoring where slight inaccuracy is acceptable.

### Watermarks: Handling Late Data

Watermarks are Flink's mechanism for deciding when a time window is complete.

A watermark at time `T` means: "I guarantee that all events with `event_time < T` have already arrived." Flink uses this to close windows. Without watermarks, a window from 12:00–12:01 would never close, because Flink can never be certain a late event won't arrive.

In the Flink SQL example above, `WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND` tells Flink: "Wait up to 10 seconds for late events before closing each window." Events arriving more than 10 seconds late will be dropped (or routed to a side output if configured).

Choosing the watermark lag is a production tradeoff:
- Too small (1 second): windows close fast, low latency, but late events are dropped and your counts are slightly off.
- Too large (5 minutes): high accuracy, but results are delayed by 5 minutes and state stays open longer, consuming more memory.

For most analytics workloads, 10–30 seconds is a reasonable default. For financial pipelines where every event matters, use a longer lag and route late events to a corrections topic.

### Stateful Processing and State Backends

Flink keeps state **per key** inside the TaskManager that owns that key partition. State is used for:
- Counting events per user over time
- Tracking session activity (last seen timestamp)
- Joining a stream with a lookup table
- Detecting patterns (e.g., 3 failed logins in 60 seconds)

Flink provides two state backends:

**HashMapStateBackend (memory)**: state is stored in the TaskManager's JVM heap. Fastest access — state reads and writes are simple HashMap lookups. Limited by the TaskManager's heap size. If you have 1 million active users each with 1 KB of state, that's 1 GB of heap pressure. Good for small-to-medium state or development environments.

**EmbeddedRocksDBStateBackend (disk-backed)**: state is stored in RocksDB, an embedded key-value store running on the TaskManager's local disk. Can handle TBs of state per TaskManager. State reads/writes involve a disk (or NVMe) read, so it's slower than memory — but it scales to state sizes that would OOM a JVM-based backend. This is the production choice for large stateful jobs: user sessions at scale, per-device counters, fraud detection models with large feature stores.

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend

env = StreamExecutionEnvironment.get_execution_environment()

# Use RocksDB for large stateful jobs
env.set_state_backend(EmbeddedRocksDBStateBackend())

# Enable incremental checkpoints — only changed RocksDB SST files are uploaded
# This dramatically reduces checkpoint size for large state
env.get_checkpoint_config().enable_unaligned_checkpoints()
```

### Checkpointing and Exactly-Once

Checkpointing is how Flink makes streaming jobs fault-tolerant and enables exactly-once processing.

Periodically (e.g., every 30 seconds), Flink pauses new event processing long enough to snapshot all in-flight state to a durable store (S3 or HDFS). The checkpoint includes:
- All operator state (counters, session data, aggregations in progress)
- The Kafka offset each source partition was at when the snapshot was taken

When a TaskManager fails, the JobManager restarts the job from the last successful checkpoint. Flink rewinds Kafka to the saved offsets and replays events from that point. Because state was fully captured in the checkpoint, no events are double-counted and no state is lost.

**Exactly-once end-to-end** requires two things: Flink checkpointing plus a transactional sink. Kafka transactions and Iceberg's atomic commit protocol both support this. With both in place, each event is reflected in the output exactly once — even across failures and restarts.

```python
# Enable checkpointing every 30 seconds
env.enable_checkpointing(30_000)  # milliseconds

# Set checkpoint storage to S3
env.get_checkpoint_config().set_checkpoint_storage("s3://my-bucket/flink-checkpoints/")

# Exactly-once checkpoint mode (default)
from pyflink.datastream import CheckpointingMode
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# Keep the last 3 checkpoints in case you need to restore from an older one
env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
env.get_checkpoint_config().set_min_pause_between_checkpoints(5_000)
```

Checkpoint overhead scales with state size. A job with 10 GB of RocksDB state and 30-second checkpoint intervals will upload incremental state diffs to S3 frequently — plan for the S3 write throughput and cost. Incremental checkpointing (RocksDB only uploads changed SST files) mitigates this significantly.

### Common Production Patterns

**Pattern 1: Kafka → Flink → Iceberg (real-time CDC ingestion)**

CDC (Change Data Capture) events from a database land in Kafka. Flink reads them, applies deduplication or schema normalization, and writes to an Iceberg table using Iceberg's Flink sink. Because Iceberg supports atomic commits and Flink supports exactly-once with transactional sinks, you get a consistent, query-able data lake table that is updated with sub-minute latency.

**Pattern 2: Kafka → Flink → Kafka (stream enrichment)**

Raw events arrive in a Kafka topic. Flink enriches each event by joining it with a broadcast state table (a lookup table loaded once and broadcast to all slots) — for example, joining a transaction event with a merchant metadata table. Enriched events are written back to a new Kafka topic for downstream consumers. This avoids hitting a database on every event.

**Pattern 3: Flink SQL for streaming analytics**

A business team wants a dashboard showing revenue by region updated in real time. Instead of a scheduled batch job running every 5 minutes, deploy the Flink SQL `TUMBLE` window query from the example above. The dashboard reads from the Iceberg output table. Results are available within seconds of events arriving.

---

## Decision Guide

| Criteria | Choose Flink | Choose Spark Structured Streaming |
|---|---|---|
| Latency requirement | Under 1 second (milliseconds) | 1–30 seconds is acceptable |
| State size | Very large (tens of GB to TBs per job) — RocksDB excels here | Small to medium state fits in executor memory |
| Window type | Session windows, complex event patterns (CEP), irregular windows | Tumbling and sliding windows, standard aggregations |
| Event processing model | True event-at-a-time processing required | Micro-batch is acceptable for your SLA |
| Team expertise | Team has Flink experience or is willing to invest | Team knows Spark and prefers a unified API |
| Ecosystem | Flink SQL, DataStream API, CEP library | Same API as Spark batch, large library ecosystem |
| Operational maturity | Kubernetes-native, dedicated stream infra | Already running Spark on Databricks, EMR, or Dataproc |
| Managed service preference | AWS Kinesis Data Analytics (MSF), Confluent Cloud Flink | Databricks Structured Streaming, AWS Glue Streaming |

---

## Anti-Patterns

| Anti-pattern | What you observe | Fix |
|---|---|---|
| Using processing time for analytics | Dashboard counts don't match source system counts; discrepancies grow larger during Kafka lag events | Switch to event time with a watermark; embed and parse event timestamps from the payload |
| No watermarks on event-time windows | Windows never emit results; state memory grows until OOM | Add a bounded watermark (e.g., `event_time - INTERVAL '30' SECOND`); route truly late events to a side output |
| HashMapStateBackend for large stateful jobs | TaskManagers run out of heap during peak traffic; jobs restart repeatedly with GC pressure | Switch to EmbeddedRocksDBStateBackend; increase TaskManager heap only for small-state jobs |
| Checkpointing to local disk | On TaskManager failure, checkpoint files are gone; job cannot recover | Always point checkpoint storage to S3 or HDFS — a durable, shared store all TaskManagers can read |
| Not enabling incremental checkpoints with RocksDB | Checkpoint uploads take minutes for large state; job spends more time checkpointing than processing | Enable `enableIncrementalCheckpointing()` on the RocksDB backend; only delta SST files are uploaded |
| Very low watermark lag in high-latency Kafka environments | Correct data is dropped as late; window aggregates are systematically undercounted | Measure your 95th-percentile Kafka consumer lag and set watermark lag at least that high |
| Parallelism higher than available slots | Job fails to deploy; operators queue waiting for slots | Set `parallelism <= total TaskManager slots`; scale TaskManagers first, then increase parallelism |
| Treating Flink like a batch system (running then stopping) | State is lost on each run; no benefit from stateful processing | Flink streaming jobs are long-running daemons; design for continuous operation and use savepoints for planned restarts |

---

## Interview Talking Points

**Q: How is Flink different from Spark Structured Streaming?**
Flink is a true streaming engine — every event is processed the instant it arrives. Spark Structured Streaming is micro-batch: it accumulates events for a short interval (typically 1–5 seconds), then processes them as a mini-batch. The practical difference is latency: Flink delivers milliseconds, Spark delivers seconds. Flink also has better support for very large state through RocksDB, and richer window semantics like session windows. The tradeoff is that Spark has a more mature ecosystem and a unified API with batch — if your team already runs Spark, the operational bar is much lower.

**Q: Explain event time vs processing time and when each matters.**
Processing time is when Flink's TaskManager received the event. It's simple but wrong when events arrive late or out of order — your 1-minute window might include events from 3 different real-world minutes if there was a Kafka lag spike. Event time is the timestamp embedded in the event itself — when the thing actually happened. For financial pipelines, SLA reporting, or any audit workload, you must use event time. Flink handles the out-of-order delivery problem through watermarks, which define how long to wait for late events before closing a window.

**Q: What are watermarks and why are they necessary?**
Watermarks are Flink's way of deciding when a time window is complete despite late data. A watermark at time T means "no events with event_time < T will arrive after this point." When the watermark advances past the end of a window, Flink closes that window and emits the result. Without watermarks, windows stay open indefinitely — Flink can never know if a late event is still coming. This causes state to grow unbounded and results to never be emitted. The watermark lag is a deliberate tradeoff: longer lag means more accuracy (fewer dropped late events) but higher end-to-end latency.

**Q: What is checkpointing and how does it enable exactly-once processing?**
Checkpointing is Flink periodically snapshotting all operator state plus the Kafka offsets of each source partition to durable storage (S3 or HDFS). On failure, Flink rewinds Kafka to the offsets saved in the last checkpoint and replays from there. Because state is fully captured, no events are double-counted. Exactly-once end-to-end also requires a transactional sink — Kafka transactions or Iceberg's atomic commit protocol — so that partially written output from a failed attempt is rolled back before the replay is committed.

**Q: When would you choose RocksDB over in-memory state?**
Use RocksDB (EmbeddedRocksDBStateBackend) whenever your per-job state exceeds what fits comfortably in TaskManager heap — typically anything above a few GB. RocksDB stores state on the TaskManager's local disk as a key-value store, so it scales to TBs of state per TaskManager. The cost is slightly higher read/write latency compared to heap. For session tracking across millions of users, per-device counters, or large feature stores for ML scoring, RocksDB is the only practical option. For development or small-state jobs (a few million keys, small payloads), HashMapStateBackend is simpler and faster.

**Q: What are common deployment options for Flink in production?**
The most common modern deployment is Flink on Kubernetes using the Flink Kubernetes Operator — the JobManager and TaskManagers run as pods, checkpoints go to S3, and the cluster scales via Kubernetes resource requests. For teams on legacy Hadoop infrastructure, Flink on YARN is still common. For managed options, AWS offers Managed Service for Apache Flink (formerly Kinesis Data Analytics), which handles cluster management and scaling. Confluent Cloud offers managed Flink tightly integrated with Confluent Kafka, which simplifies the Kafka-to-Flink connector configuration.
