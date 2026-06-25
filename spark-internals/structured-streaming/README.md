# Spark Structured Streaming

> Chapter from the Data Engineering Playbook — spark-internals

## About This Chapter

- **What this is.** A practical guide to Spark Structured Streaming: how it works under the hood, how to build reliable streaming pipelines with it, and where it fits in the broader streaming landscape.
- **Who it's for.** Mid-level data engineers who have used Spark for batch processing and want to extend those skills to streaming, and senior DEs who want a reference for production pitfalls and design decisions.
- **What you'll take away.**
  - A clear mental model of how Structured Streaming processes data (micro-batches, checkpointing, state)
  - Working knowledge of sources, sinks, output modes, watermarks, and triggers — with the tradeoffs that matter in production
  - A decision framework for when to use Structured Streaming versus Flink or Kafka Streams

---

You're landing clickstream events from your mobile app into S3 as JSON files every minute. The business wants a live dashboard showing rolling 5-minute session counts per user. You could write a batch job that runs every minute — but then you'd need a scheduler, deal with overlapping runs, handle late files, and build deduplication logic from scratch. Or you could write a single Structured Streaming query, point it at that S3 path, and let Spark handle all of that. Same DataFrame API you already know. Same cluster. No new infrastructure.

---

## TL;DR

- Structured Streaming lets you run DataFrame/SQL queries continuously on a stream of incoming data — you write batch-style code and Spark makes it streaming.
- Under the hood, Spark reads new data in small chunks called micro-batches (think: process every 30 seconds), runs your query, and writes results to a sink.
- The stream is modeled as an unbounded table (a table that never stops growing at the bottom) — your query always runs against the latest state of that table.
- Checkpointing is how Structured Streaming survives crashes — it saves your progress (which offsets were read, what state was accumulated) to a directory after every micro-batch. Without it, a restart means reprocessing everything from scratch.
- Watermarks are how you handle late data without running out of memory — you tell Spark "wait up to X minutes for late events, then drop state for older windows."
- Output mode controls what gets written on each trigger: only new rows (Append), only changed rows (Update), or the full result table (Complete). Most pipelines use Append.
- Exactly-once end-to-end is achievable: Kafka source reads from committed offsets idempotently, and Delta Lake or Iceberg sinks write transactionally.
- Use Structured Streaming when your team is already on Spark. Choose Flink for sub-second latency or massive stateful workloads. Choose Kafka Streams for simple Kafka-to-Kafka pipelines.

---

## Why This Matters in Production

Consider a real scenario: a payments team is aggregating transaction counts by merchant per 10-minute window. The pipeline must handle:
- Events that arrive 3–5 minutes late due to mobile network delays
- Exactly-once semantics so merchants are not double-charged
- A restart that happens mid-hour without reprocessing thousands of batches
- A growing number of merchants, meaning window state could grow large

Structured Streaming addresses all four. The watermark controls how long late-event state is retained. Delta Lake as the sink provides transactional writes. Checkpointing ensures the restart picks up exactly where it left off. And the watermark also bounds state size — once a window is past the watermark, Spark evicts it from memory.

Getting any of these wrong is expensive. Missing the watermark means OOM at 3 AM. Missing checkpointing means the on-call engineer reruns 6 hours of data and explains the duplicate counts to the merchant team.

---

## How It Works

### The Unbounded Table Mental Model

Spark models the incoming stream as a table that grows continuously. New rows arrive at the bottom. Your query — whether it's a filter, a join, or a GROUP BY — runs against that table. The output is also a table (the result table). The sink is how you materialize the result table to storage.

This mental model matters because it explains why batch-style code works in streaming: you're always reading from a table and writing to a table. The only difference is the table never stops arriving.

### Micro-Batch Execution

On each trigger:
1. Spark checks the source for new data (new Kafka offsets, new files in S3).
2. It reads only the new data since the last trigger.
3. It runs your query on that new data (plus any accumulated state for stateful operations).
4. It writes results to the sink.
5. It commits the checkpoint (saves which offsets were processed, updates state store).

Each step is atomic with respect to checkpointing. If a failure happens mid-write, Spark restarts from the last committed checkpoint, which means the previous write may get retried — this is why your sink needs to support idempotent or transactional writes for exactly-once behavior.

### Starting a Streaming Query

```python
# Read from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .load()
)

# Parse the value bytes into columns
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, LongType, DoubleType

schema = StructType() \
    .add("merchant_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("event_time", LongType())

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

# Write to Delta Lake with checkpoint
query = (
    parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/transactions/")
    .start("s3://my-bucket/delta/transactions/")
)
```

---

### Sources

**Kafka (most common in production)**

Kafka is the default choice for real-time pipelines. Spark tracks offsets per partition, so each restart continues from exactly where it left off.

```python
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "my-topic")          # single topic
    # .option("subscribePattern", "events-.*") # or regex for multiple topics
    .option("startingOffsets", "latest")       # "earliest" for backfill
    .option("maxOffsetsPerTrigger", 100000)    # cap throughput per micro-batch
    .load()
)
```

The key columns available after `.load()`: `key`, `value` (both binary — cast to string before parsing), `topic`, `partition`, `offset`, `timestamp`.

**Files in S3 / HDFS**

Spark polls the directory for new files. This works well for systems that drop files at a regular cadence (data lake ingestion, vendor feeds).

```python
df = (
    spark.readStream
    .format("parquet")           # or "json", "csv", "orc"
    .schema(schema)              # schema is required — readStream cannot infer
    .option("maxFilesPerTrigger", 10)  # process at most 10 new files per trigger
    .load("s3://my-bucket/incoming/events/")
)
```

Important: you must provide the schema explicitly. Structured Streaming cannot infer schema from files because it reads data incrementally and the first trigger may have only one file.

**Rate Source (testing only)**

Generates a synthetic stream of rows at a fixed rate. Useful for load testing a new pipeline before hooking up the real Kafka topic.

```python
df = spark.readStream.format("rate").option("rowsPerSecond", 100).load()
# Columns: timestamp (event time), value (incrementing long)
```

---

### Sinks

**Delta Lake / Iceberg (recommended for lake pipelines)**

Transactional writes. Delta's ACID guarantees mean a partial write from a failed micro-batch is either fully committed or fully rolled back — no partial files.

```python
query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/my-pipeline/")
    .start("s3://my-bucket/delta/my-table/")
)
```

**Kafka (stream-to-stream pipelines)**

Write transformed or enriched records back to a Kafka topic.

```python
query = (
    df.select(
        col("merchant_id").cast("string").alias("key"),
        to_json(struct("*")).alias("value")
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("topic", "enriched-transactions")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/enriched/")
    .start()
)
```

**Console (testing only)**

Prints results to stdout. Never use in production — it does not checkpoint and does not scale.

```python
query = df.writeStream.format("console").start()
```

**Foreach / ForeachBatch (custom sinks)**

`foreachBatch` gives you a micro-batch DataFrame and lets you write it anywhere — a Postgres table, an Elasticsearch index, a REST API.

```python
def write_to_postgres(batch_df, batch_id):
    (
        batch_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://host/db")
        .option("dbtable", "merchant_stats")
        .option("user", "etl_user")
        .option("password", "...")
        .mode("append")
        .save()
    )

query = (
    df.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "s3://my-bucket/checkpoints/postgres-sink/")
    .start()
)
```

The `batch_id` is monotonically increasing across triggers. You can use it for idempotency: if your write failed and the batch is retried, the same `batch_id` comes back, so you can deduplicate on it at the sink.

---

### Output Modes

Output mode tells Spark which rows to write to the sink on each trigger.

**Append** (default for most pipelines)

Only rows added since the last trigger are written. New rows are never updated after they are written.

Use Append for: raw data landing, filter/transform pipelines, windowed aggregations with watermarks (once a window is past the watermark, its final result is emitted and appended).

```python
.outputMode("append")
```

**Complete**

The entire result table is rewritten on every trigger. This only makes sense for aggregations — for example, keeping a running total per merchant where you want the current state of every row in the output.

Expensive for large tables because every trigger rewrites all rows. Works well for small dimension-style aggregations (e.g., top-10 merchants this hour).

```python
# This query keeps a running total per merchant across all time
query = (
    parsed
    .groupBy("merchant_id")
    .agg(sum("amount").alias("total_amount"))
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()
)
```

**Update**

Only rows that changed since the last trigger are written. More efficient than Complete for aggregations. Not supported by file sinks (files are immutable — you cannot update a row in a Parquet file). Works with Delta Lake (upserts) and console.

```python
.outputMode("update")
```

---

### Watermarking

Every stateful streaming operation (windowed aggregation, deduplication, stream-stream join) needs to retain some state in memory. The question is: how long do you keep state for a given event-time window before declaring it closed?

Without a watermark, Spark never discards state. If you're aggregating by event-time window and events can theoretically arrive from any point in the past, Spark holds state for every window it has ever seen. That is an unbounded memory leak that will OOM your executors.

A watermark is a threshold: "I accept late events up to X minutes after the window closes. Events older than that are dropped." Spark uses the max observed event time in the stream minus the threshold to determine which windows are still open.

```python
from pyspark.sql.functions import window, sum as _sum

query = (
    parsed
    .withWatermark("event_time", "10 minutes")   # accept events up to 10 min late
    .groupBy(
        window(col("event_time"), "5 minutes"),   # 5-minute tumbling window
        col("merchant_id")
    )
    .agg(_sum("amount").alias("total_amount"))
    .writeStream
    .outputMode("append")                         # emit window result only after it closes
    .format("delta")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/merchant-windows/")
    .start("s3://my-bucket/delta/merchant-windows/")
)
```

In this example: if the latest event time seen is 12:00, Spark will keep state open for windows that include events after 11:50. Any event with `event_time` before 11:50 is silently dropped.

With Append output mode and a watermark, Spark emits the window's result only after the window is definitively closed (latest event time > window end + watermark delay). This means results are delayed by the watermark duration — that is the tradeoff for bounded state.

---

### Triggers

Triggers control when Spark fires a micro-batch.

**Default (no trigger specified)** — process as fast as possible. Each micro-batch starts as soon as the previous one finishes. Maximizes throughput and minimizes latency. Can be overwhelming for downstream sinks if data arrives in bursts.

**Fixed interval** — process on a schedule, regardless of whether new data arrived.

```python
from pyspark.sql.streaming import Trigger

query = (
    df.writeStream
    .trigger(processingTime="30 seconds")   # fire every 30 seconds
    .format("delta")
    .option("checkpointLocation", "...")
    .start("...")
)
```

Use fixed interval when your sink (e.g., a JDBC database) cannot handle the throughput of continuous micro-batches, or when you want predictable write patterns.

**Once** — process all data available right now, then stop. Runs exactly like a batch job.

```python
query = (
    df.writeStream
    .trigger(once=True)
    .format("delta")
    .option("checkpointLocation", "...")
    .start("...")
)
query.awaitTermination()
```

Use Once for scheduled streaming: trigger the job every hour via Airflow, let it process the last hour of Kafka data, stop. Cheaper than a continuously running stream, and you get exactly-once semantics plus checkpointing for free.

**AvailableNow** — like Once, but breaks the backlog into multiple micro-batches instead of one giant batch. More memory-efficient when catching up after an outage.

```python
query = (
    df.writeStream
    .trigger(availableNow=True)
    .format("delta")
    .option("checkpointLocation", "...")
    .start("...")
)
query.awaitTermination()
```

Prefer AvailableNow over Once when your backlog could be large (e.g., the pipeline was down for 6 hours). Once tries to process all available data in one micro-batch; AvailableNow processes it in chunks at the configured `maxOffsetsPerTrigger`.

---

### Stateful Processing

**Windowed Aggregations**

Group events into time windows and compute aggregates. Spark maintains the running aggregate for each open window in a state store (an in-memory key-value store backed by the checkpoint directory).

```python
# Count events per 5-minute window per merchant
from pyspark.sql.functions import window, count

agg_df = (
    parsed
    .withWatermark("event_time", "5 minutes")
    .groupBy(
        window("event_time", "5 minutes"),    # tumbling window
        "merchant_id"
    )
    .agg(count("*").alias("event_count"))
)
```

Sliding windows (overlap between windows) are also supported:
```python
window("event_time", "10 minutes", "5 minutes")  # 10-min window, slides every 5 min
```

**Deduplication**

Remove duplicate events within a watermark window. Spark maintains a set of seen IDs in state and drops rows whose ID is already in the set.

```python
deduped = (
    parsed
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id", "event_time"])  # deduplicate on event_id within watermark
)
```

Without the watermark, the deduplication state set grows forever. With the watermark, Spark evicts IDs from windows older than the threshold.

**Stream-Stream Joins**

Join two Kafka streams — for example, joining a payment-initiated stream with a payment-completed stream to calculate authorization lag.

```python
initiated = spark.readStream.format("kafka") \
    .option("subscribe", "payment-initiated") \
    .load() \
    .withWatermark("event_time", "10 minutes")

completed = spark.readStream.format("kafka") \
    .option("subscribe", "payment-completed") \
    .load() \
    .withWatermark("event_time", "10 minutes")

joined = initiated.join(
    completed,
    expr("""
        initiated.payment_id = completed.payment_id AND
        completed.event_time BETWEEN initiated.event_time
            AND initiated.event_time + interval 10 minutes
    """),
    "leftOuter"
)
```

Both sides require watermarks. Without watermarks, Spark cannot determine when it is safe to drop the state for a given join key, and state grows unbounded.

---

### Checkpointing

Checkpointing is the mechanism that makes Structured Streaming fault-tolerant. After every micro-batch, Spark writes to the checkpoint directory:
- The source offsets that were processed in this batch
- The output of the batch (for Write-Ahead Log, used with non-idempotent sinks)
- The state store snapshot (for stateful operations)

On restart after a failure, Spark reads the checkpoint directory, determines what the last successfully committed batch was, and resumes from the next offset. This is what gives you at-least-once semantics — paired with an idempotent or transactional sink, it gives exactly-once.

```python
.option("checkpointLocation", "s3://my-bucket/checkpoints/pipeline-name/")
```

Rules in production:
- Each streaming query needs its own checkpoint directory. Sharing a checkpoint between two queries corrupts both.
- The checkpoint path must be persistent storage (S3, ADLS, HDFS). Local disk checkpoints are lost when the driver restarts.
- If you change the query's schema (add a column, rename a field) or change the source significantly, the existing checkpoint may be incompatible. Delete the checkpoint directory and restart — you will reprocess from the beginning (or from `startingOffsets: earliest`).
- Never delete a checkpoint while a query is running.

---

### Exactly-Once Semantics

Structured Streaming gives you exactly-once end-to-end when:
1. **Source is replayable**: Kafka, S3 files, and Delta change feeds are all replayable — if a batch fails partway through, Spark can re-read the same data.
2. **Sink is transactional or idempotent**: Delta Lake and Iceberg write transactionally (a partial write is rolled back). Kafka producers with idempotence enabled avoid duplicate messages.

The checkpoint provides the "at-least-once" guarantee (the offset is only committed after the write succeeds, so the same batch may be retried). The transactional sink upgrades this to exactly-once by making retried writes idempotent (Delta's transaction log deduplicates based on the epoch/batch ID).

If your sink is JDBC or another non-transactional store, you get at-least-once at best, unless you implement deduplication logic in `foreachBatch` using the `batch_id`.

---

## Decision Guide

| Situation | Recommendation |
|---|---|
| Your team already uses Spark for batch | Structured Streaming — same API, same cluster, minimal learning curve |
| You need sub-second end-to-end latency | Apache Flink — micro-batches introduce 100ms–2s latency by design |
| Massive stateful processing (billions of keys) | Apache Flink — more efficient state backend (RocksDB) |
| Simple Kafka topic transform (no joins, no state) | Kafka Streams — simpler to deploy, no Spark cluster needed |
| You need exactly-once to a data lake | Structured Streaming + Delta Lake or Iceberg |
| Scheduled hourly streaming job (not always-on) | Structured Streaming with `Trigger.AvailableNow()` |
| Prototyping / load testing a new pipeline | Structured Streaming with Rate source + console sink |
| Stream-to-stream join with complex state | Structured Streaming if already on Spark; Flink if latency or scale is the concern |
| Sink is a RDBMS (Postgres, MySQL) | Structured Streaming with `foreachBatch` + JDBC write |

---

## Anti-Patterns

| Anti-pattern | What you observe | Fix |
|---|---|---|
| No `checkpointLocation` set | On restart, pipeline reprocesses all data from Kafka `earliest` or S3 beginning — produces duplicates in the sink | Always set `checkpointLocation` to a persistent path. One directory per query. |
| Stateful operation (window, dedup, stream join) with no watermark | State store grows continuously. Driver or executor eventually OOMs, usually at peak hours. | Add `.withWatermark("event_time", "X minutes")` before the stateful operation. The watermark duration is your late-data tolerance. |
| `outputMode("complete")` without a GROUP BY | Query fails immediately with `AnalysisException: Complete output mode not supported when there are no streaming aggregations`. | Use `append` for non-aggregation queries. Use `complete` only with a `groupBy`. |
| Changing the schema and reusing the old checkpoint | Query fails on startup with a checkpoint compatibility error, or silently drops/misaligns columns. | Delete the checkpoint directory. The pipeline will reprocess from the configured `startingOffsets`. |
| Using `Trigger.Once()` for a large backlog | One giant micro-batch exhausts executor memory, job OOMs or times out. | Switch to `Trigger.AvailableNow()`, which chunks the backlog into multiple micro-batches at `maxOffsetsPerTrigger`. |
| Shared checkpoint directory across two queries | Both queries read conflicting state. One or both fail on startup or produce corrupted output. | Every streaming query gets its own unique checkpoint path. |
| Console sink in production | No durability. Data is printed and lost. No checkpointing, no fault tolerance. | Use a real sink: Delta, Iceberg, Kafka. Console is for local development only. |
| Not capping `maxOffsetsPerTrigger` on Kafka | After an outage, the first micro-batch reads millions of messages at once, causing GC pressure and slow processing time. | Set `.option("maxOffsetsPerTrigger", N)` to limit how many Kafka messages are consumed per trigger. |
| Using `startingOffsets: earliest` in production without understanding the backlog | First run processes months of historical data, overwhelming downstream sinks and taking hours. | Use `earliest` intentionally for backfills. Use `latest` for new production pipelines. |

---

## Interview Talking Points

**Q: What is Spark Structured Streaming and how does it differ from the older DStream API?**
Structured Streaming is built on top of Spark SQL and uses the DataFrame/Dataset API. You write the same queries you would for batch — filters, aggregations, joins — and Spark executes them continuously. The older DStream (Discretized Stream) API was lower-level and required you to think explicitly in terms of RDD batches. Structured Streaming gives you higher-level abstractions, better optimization through Catalyst (Spark's query optimizer), and stronger fault-tolerance guarantees.

**Q: How does Spark guarantee fault tolerance in Structured Streaming?**
Through checkpointing. After every micro-batch, Spark writes the source offsets it consumed and any accumulated state to a checkpoint directory. If the driver crashes, the next run reads the checkpoint, finds the last committed offset, and resumes from the next one. The write to the sink happens before the checkpoint is committed — if the write fails, the batch is retried from the same offset. With a transactional sink like Delta, this gives exactly-once semantics.

**Q: What is a watermark and why do you need it?**
A watermark is a threshold that tells Spark how long to wait for late-arriving events before closing a time window and evicting its state. Without a watermark, Spark holds state for every window indefinitely — because it cannot know whether a very late event might still arrive. That causes state to grow unbounded and eventually exhausts memory. The watermark bounds state size by saying: "events more than X minutes late are dropped; windows older than that can be evicted."

**Q: What is the difference between Append, Update, and Complete output modes?**
Append writes only the new rows added since the last trigger — rows are never updated after being written. It is the default and works for most pipelines. Complete rewrites the entire result table on every trigger — only valid for aggregations, expensive for large result sets. Update writes only the rows that changed since the last trigger — more efficient than Complete for aggregations, but not supported by all sinks (file sinks cannot update rows in place).

**Q: When would you use `Trigger.AvailableNow()` instead of `Trigger.Once()`?**
Both triggers process all available data and then stop, but AvailableNow breaks the work into multiple micro-batches (controlled by `maxOffsetsPerTrigger`), while Once processes all available data in a single micro-batch. If the backlog is large — say, the pipeline was down for several hours — Once can produce a single enormous batch that exhausts executor memory. AvailableNow is more memory-efficient and still terminates when the backlog is cleared.

**Q: How do you achieve exactly-once semantics end-to-end?**
You need both ends to cooperate. On the source side, Kafka tracks committed offsets so Spark can replay exactly the same records on a retry without missing or duplicating. On the sink side, you need a transactional writer — Delta Lake and Iceberg both write transactionally, so a partial write from a failed micro-batch is rolled back rather than partially committed. The checkpoint ties these together: offsets are only committed to the checkpoint after the sink write succeeds, so retries always re-attempt the same data against a transactionally-safe sink.

**Q: How does Structured Streaming compare to Apache Flink?**
Structured Streaming is the right choice if your team is already invested in Spark — same API, same cluster, lower operational overhead. Flink is purpose-built for streaming and offers true record-at-a-time processing with lower latency (single-digit milliseconds versus 100ms–2s micro-batch latency). Flink also handles massive stateful workloads more efficiently using RocksDB as the state backend. For most enterprise data lake pipelines where 30-second latency is acceptable and the team knows Spark, Structured Streaming is the practical choice. For real-time fraud detection or sub-second alerting, Flink is worth the operational complexity.

**Q: What happens if you change the schema of your streaming query and restart with the old checkpoint?**
The checkpoint encodes the schema of the state store and the query plan. A schema change — adding a column, renaming a field, changing a data type — can make the checkpoint incompatible. Spark will either fail on startup with an error or silently misalign columns. The fix is to delete the checkpoint directory and restart from scratch. Depending on the source, this means reprocessing from `startingOffsets: earliest`. This is a known operational constraint — it is worth versioning your checkpoint paths (e.g., include a schema version in the path) so you can do a clean cutover without losing the old checkpoint history.
