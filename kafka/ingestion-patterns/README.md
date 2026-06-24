# Kafka Ingestion Patterns

> Chapter from the Data Engineering Playbook — kafka.

## About This Chapter

**What this is.** A survey of the patterns for moving data out of Kafka — batch, micro-batch, near-real-time, and real-time — plus Lambda, Kappa, CDC, windowed aggregation, and fan-out. This chapter maps latency requirements to the right tool and operational trade-offs.

**Who it's for.** data engineers, analytics engineers, data/ML engineers, and platform/architecture leads.

**What you'll take away.** By the end you'll be able to:
- Match a downstream SLA to the right latency tier and tech (Kafka Connect, Spark Structured Streaming, Flink, Kafka Streams, ksqlDB) without over-engineering.
- Implement micro-batch and streaming ingestion with bounded triggers, checkpoints, and small-file compaction on Delta/Iceberg.
- Apply CDC (Debezium) change streams and windowed aggregations, and choose between Lambda and Kappa for reprocessing.

---

## TL;DR

- **Batch (hourly+):** Read committed offsets on a schedule, write a file or partition. Simplest. Highest latency. Use when downstream only refreshes hourly anyway.
- **Micro-batch (5–15 min):** Spark Structured Streaming with a fixed trigger interval. Streaming semantics, batch-like operations. The sweet spot for most analytics pipelines.
- **Near Real-Time (1–5 min):** Same stack, shorter trigger or lower checkpoint interval. Freshness in minutes at the cost of more small files and higher cluster pressure.
- **Real-Time (<1 min, sub-second):** Flink or Kafka Streams. Stateful, event-time-aware, millisecond latency. Required for fraud detection, live dashboards, alerting.
- **Other patterns:** Lambda (dual pipeline), Kappa (streaming-only with replay), CDC via Kafka (Debezium), Stateful Windowed Aggregation, Fan-out.
- **The hard truth:** latency requirements drive tech choice, and lower latency always means more operational complexity. Don't build a real-time pipeline when a micro-batch would satisfy the SLA.

---

## The Latency Spectrum

```
KAFKA TOPIC
    │
    ├── Batch (hourly+)
    │     Scheduled Spark job / Kafka Connect S3 Sink
    │     Latency: 1 hour+
    │     Complexity: Low
    │
    ├── Micro-Batch (5–15 min)
    │     Spark Structured Streaming + fixed trigger
    │     Latency: 5–15 min
    │     Complexity: Medium
    │
    ├── Near Real-Time (1–5 min)
    │     Spark Structured Streaming (short trigger) / Flink
    │     Latency: 1–5 min
    │     Complexity: Medium–High
    │
    └── Real-Time (<1 min / sub-second)
          Apache Flink / Kafka Streams / ksqlDB
          Latency: milliseconds–seconds
          Complexity: High
```

The right level is determined by one question: **how fresh does the data need to be, and who is consuming it?** A BI dashboard that refreshes every hour does not justify a real-time Flink pipeline.

---

## Pattern 1: Batch Ingestion (Hourly+)

### What it is

A scheduled job wakes up on a cron, reads all Kafka messages produced since the last run (using committed offsets or a watermark), writes them to a file, partition, or table, and exits.

### How it works

The job tracks where it left off — either via Kafka consumer group committed offsets or by storing the last processed offset/timestamp itself. On the next run, it picks up from that point.

### Latency

1 hour minimum (or whatever the cron interval is). Data is not visible to consumers until the batch completes.

### When to use

- Downstream BI tool refreshes hourly or less frequently — there is no value in more frequent ingestion
- Event volumes are low-to-moderate (millions of messages per hour, not billions)
- Schema validation, deduplication, or enrichment logic is complex and easier to express in batch SQL
- Team has Spark/SQL expertise but no streaming experience

### When to avoid

- Any downstream system needs data faster than the batch interval
- Event volume is very high — batch job runtime approaches the cron interval, leaving little margin

### Tech Options

| Tool | How | Fits when |
|---|---|---|
| **Kafka Connect S3 Sink** | Config-only connector; flushes files to S3 on schedule or file size | Simplest path to S3; no code; limited transformation |
| **Spark batch + Kafka source** | Spark reads a bounded offset range; writes Parquet/Delta/Iceberg | Full transformation logic; team already runs Spark |
| **Python consumer script** | Simple consumer loop; writes to file or DB | Low volume, simple schema, small team |
| **AWS Glue (Kafka source)** | Managed Spark with Kafka connector; serverless | AWS-first shop; no cluster management |

### Kafka Connect S3 Sink (config, no code)

```json
{
  "name": "kafka-s3-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "4",
    "topics": "order_events",
    "s3.region": "us-east-1",
    "s3.bucket.name": "data-lake-raw",
    "s3.part.size": "67108864",
    "flush.size": "100000",
    "rotate.interval.ms": "3600000",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "partitioner.class": "io.confluent.connect.s3.partitioner.TimeBasedPartitioner",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
    "locale": "US",
    "timezone": "UTC",
    "timestamp.extractor": "RecordField",
    "timestamp.field": "event_ts"
  }
}
```

`flush.size` triggers a file write after N records; `rotate.interval.ms` triggers it after N milliseconds — whichever comes first.

### Spark Batch (bounded offset read)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, LongType, TimestampType

spark = SparkSession.builder.appName("kafka-batch-ingest").getOrCreate()

schema = StructType() \
  .add("order_id", StringType()) \
  .add("customer_id", LongType()) \
  .add("amount", StringType()) \
  .add("event_ts", TimestampType())

# Read a bounded range of offsets — from last committed to latest
df = spark.read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:9092") \
  .option("subscribe", "order_events") \
  .option("startingOffsets", "earliest")  \  # or a stored offset JSON
  .option("endingOffsets", "latest") \
  .load()

parsed = df.select(
  from_json(col("value").cast("string"), schema).alias("data"),
  col("offset"),
  col("timestamp").alias("kafka_ts")
).select("data.*", "offset", "kafka_ts")

# Write as a dated partition
parsed.write \
  .mode("append") \
  .partitionBy("event_date") \
  .format("parquet") \
  .save("s3://data-lake-raw/order_events/")
```

---

## Pattern 2: Micro-Batch (5–15 Minutes)

### What it is

Spark Structured Streaming running continuously, but configured to produce an output batch every N minutes. The engine reads new Kafka messages, processes them, writes a mini-batch output, then waits for the next trigger. It is streaming infrastructure with batch-like predictability.

### How it works

Spark tracks Kafka offsets in a checkpoint directory. Each trigger interval it reads all new messages since the last checkpoint, runs the query, and commits the output + offset atomically. If the job crashes, it resumes from the last committed checkpoint — no data loss, no reprocessing already-written data.

### Latency

5–15 minutes (configurable). Data visible after the trigger fires and the write commits.

### When to use

- Dashboards or reports need data refreshed every 10–15 minutes
- ACID writes to Delta or Iceberg are required (Structured Streaming + Delta is the canonical CDC-to-lake pattern)
- Stateless or simple stateful operations (filter, map, window aggregation over short windows)
- Team runs Spark already — no new runtime to operate

### When to avoid

- Sub-minute latency is required — trigger overhead makes this unsuitable below ~2 minutes
- Very complex stateful logic (multi-stream joins with long windows) — Flink handles this more efficiently

### Tech Options

| Tool | Notes |
|---|---|
| **Spark Structured Streaming** | Native trigger API; excellent Delta/Iceberg integration; checkpoint-based recovery |
| **Apache Flink** | Lower overhead per micro-batch; better for high-volume or complex stateful logic |
| **Databricks Auto Loader** | Managed Structured Streaming with schema inference and file notification; Databricks-only |

### Spark Structured Streaming — Fixed Trigger

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StructType, StringType, LongType, TimestampType

spark = SparkSession.builder.appName("kafka-micro-batch").getOrCreate()

schema = StructType() \
  .add("order_id", StringType()) \
  .add("customer_id", LongType()) \
  .add("amount", StringType()) \
  .add("event_ts", TimestampType())

# Continuously read from Kafka
raw = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:9092") \
  .option("subscribe", "order_events") \
  .option("startingOffsets", "latest") \
  .option("maxOffsetsPerTrigger", 5_000_000) \   # cap records per trigger to bound latency
  .load()

parsed = raw.select(
  from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
  .withColumn("event_date", to_date("event_ts"))

# Write every 10 minutes to Delta
query = parsed.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "s3://checkpoints/order_events/") \
  .partitionBy("event_date") \
  .trigger(processingTime="10 minutes") \          # micro-batch trigger
  .start("s3://data-lake/order_events/")

query.awaitTermination()
```

`maxOffsetsPerTrigger` prevents a single trigger from reading a massive backlog and blowing the memory budget — important when the job restarts after a lag.

---

## Pattern 3: Near Real-Time (1–5 Minutes)

### What it is

The same as micro-batch but with a shorter trigger interval or smaller checkpoint interval. The engineering is identical; the operational demands are higher — more frequent file commits, more small files, more checkpoint pressure.

### Latency

1–5 minutes. Data visible within a few minutes of being produced to Kafka.

### When to use

- SLA is "data available within 5 minutes" — common for operational dashboards, fraud scoring input, near-real-time alerting
- Volume is moderate — the pipeline can fully process each micro-batch in well under the trigger interval

### The small file problem

A 1-minute trigger writing Parquet to S3 produces 1 file per partition per minute. At 24 partitions (hourly) × 60 triggers/hour = 1,440 files/hour. After a day: 34,560 files. Query performance degrades. A scheduled **compaction job** must consolidate small files.

```python
# Spark Structured Streaming — 1-minute trigger
query = parsed.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "s3://checkpoints/order_events_rt/") \
  .trigger(processingTime="1 minute") \
  .start("s3://data-lake/order_events/")
```

```python
# Separate compaction job — runs every few hours
from delta.tables import DeltaTable

DeltaTable.forPath(spark, "s3://data-lake/order_events/") \
  .optimize() \
  .executeCompaction()
```

Iceberg equivalent:

```python
spark.sql("""
  CALL analytics.system.rewrite_data_files(
    table => 'analytics.order_events',
    strategy => 'binpack',
    options => map('target-file-size-bytes', '134217728')
  )
""")
```

### Flink — Continuous Streaming (near-RT)

```python
# PyFlink — read from Kafka, write to Iceberg, checkpoint every 60s
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(60_000)   # checkpoint every 60 seconds → ~1 min latency

kafka_source = KafkaSource.builder() \
  .set_bootstrap_servers("broker:9092") \
  .set_topics("order_events") \
  .set_group_id("flink-order-ingestor") \
  .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets()) \
  .set_value_only_deserializer(JsonRowDeserializationSchema.builder()
    .type_info(order_type_info).build()) \
  .build()

stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")
# ... transform ...
stream.sink_to(iceberg_sink)
env.execute("order-events-near-rt")
```

The checkpoint interval in Flink directly controls end-to-end latency: with a 60-second checkpoint interval, output is visible within ~60–90 seconds of the event being produced.

---

## Pattern 4: Real-Time (<1 Minute, Sub-Second)

### What it is

Continuous, event-by-event processing with millisecond-to-second end-to-end latency. The pipeline processes each event as it arrives, with no batching delay. Stateful operations (windowed aggregations, joins across streams) are maintained in memory.

### Latency

Milliseconds to seconds. Sub-second is achievable with Flink or Kafka Streams for stateless pipelines; tens of seconds for stateful pipelines with watermarks.

### When to use

- Fraud detection: a payment event must be scored and acted on before it completes
- Live leaderboards, real-time recommendation updates, alerting on threshold breaches
- Operational dashboards where stale data causes business decisions to be wrong
- Stream-to-stream joins: enrich an event with another stream in near-real-time

### When to avoid

- Downstream is a columnar data store (Redshift, Snowflake, Athena) — these are designed for batch reads; writing one row at a time is extremely expensive
- The business SLA is 5+ minutes — real-time complexity is unwarranted
- Team has no streaming operations experience — real-time pipelines are hard to debug, reprocess, and backfill

### Tech Options

| Tool | Latency | Stateful | Best for |
|---|---|---|---|
| **Apache Flink** | Milliseconds | Yes — rich state backends (RocksDB) | Complex stateful, event-time, windowing, stream joins |
| **Kafka Streams** | Milliseconds | Yes — local RocksDB state | Stateful processing within Kafka ecosystem; no separate cluster |
| **ksqlDB** | Seconds | Yes — materialized views | SQL on streams; simpler than Flink; less flexible |
| **Spark Structured Streaming (continuous mode)** | ~100ms | Limited | Simple stateless; not production-grade for sub-second stateful |

### Apache Flink — Event-Time Windowed Aggregation

```python
# PyFlink — count orders per customer per 5-minute window, emit in real-time
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(10_000)   # 10-second checkpoint for low latency

# Watermark strategy: tolerate 30 seconds of late data
watermark_strategy = WatermarkStrategy \
  .for_bounded_out_of_orderness(Duration.of_seconds(30)) \
  .with_timestamp_assigner(OrderTimestampAssigner())

stream = env.from_source(kafka_source, watermark_strategy, "Kafka Source")

result = stream \
  .key_by(lambda e: e.customer_id) \
  .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
  .aggregate(OrderCountAggregator())

result.sink_to(alert_sink)   # downstream alert system or another Kafka topic
env.execute("order-rt-aggregation")
```

**Key Flink concepts:**
- **Event time vs processing time**: event time uses the timestamp embedded in the event; processing time uses the clock when the event arrives. Event time is correct for out-of-order data; processing time is simpler but wrong under lag.
- **Watermarks**: the engine's estimate of how far behind wall clock the event stream is. Events arriving after the watermark are considered late. The watermark advances as new events arrive; a 30-second bound means "I will wait 30 seconds for late events."
- **State backends**: RocksDB (default for large state) stores state on local disk, spills to memory — survives restarts via checkpoints.

### Kafka Streams — Stateful in-Process

```java
// Java — Kafka Streams: real-time count of orders per customer in a 5-min window
StreamsBuilder builder = new StreamsBuilder();

KStream<String, OrderEvent> orders = builder.stream(
  "order_events",
  Consumed.with(Serdes.String(), orderSerde)
);

KTable<Windowed<String>, Long> counts = orders
  .groupBy((key, order) -> order.getCustomerId(), Grouped.with(Serdes.String(), orderSerde))
  .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
  .count(Materialized.as("order-count-store"));

counts.toStream()
  .map((windowedKey, count) -> KeyValue.pair(windowedKey.key(), count))
  .to("order_counts_per_customer", Produced.with(Serdes.String(), Serdes.Long()));

KafkaStreams app = new KafkaStreams(builder.build(), config);
app.start();
```

Kafka Streams runs **inside your application process** — no separate cluster to operate. State is stored in local RocksDB, replicated to a Kafka changelog topic for recovery. This makes it operationally simpler than Flink for moderate workloads.

### ksqlDB — SQL on Streams

```sql
-- ksqlDB: create a stream over the Kafka topic
CREATE STREAM order_events (
  order_id VARCHAR,
  customer_id BIGINT,
  amount DOUBLE,
  event_ts BIGINT
) WITH (
  KAFKA_TOPIC = 'order_events',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'event_ts'
);

-- Materialized table: 5-minute windowed order count per customer
CREATE TABLE order_count_per_customer AS
SELECT
  customer_id,
  COUNT(*)           AS order_count,
  SUM(amount)        AS total_amount,
  WINDOWSTART        AS window_start,
  WINDOWEND          AS window_end
FROM order_events
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY customer_id
EMIT CHANGES;
```

ksqlDB continuously updates the materialized table as new events arrive. Results are stored in a Kafka topic and queryable via REST API or push queries.

---

## Pattern 5: Lambda Architecture

### What it is

Run two parallel pipelines from the same Kafka topic:
- **Speed layer**: real-time or near-real-time (Flink or Spark Streaming), produces approximate but fresh results
- **Batch layer**: hourly/daily batch job, produces accurate but delayed results

A **serving layer** merges results from both, returning the most recent batch result enriched with the speed layer's delta.

```
Kafka Topic
    ├── Speed Layer (Flink/SS) → Redis / serving store → "last 2 hours fresh"
    └── Batch Layer (Spark)   → Data Lake (Delta/Iceberg) → "hourly accurate"
                                          └── Serving Layer merges both
```

### When to use

- You need both freshness and accuracy, and a single pipeline cannot deliver both
- The speed layer can tolerate approximate results (estimates, rounded aggregations)
- Batch layer is the source of truth; speed layer fills the recency gap

### When to avoid

- Two pipelines means two codebases, two sets of bugs, two failure modes — complexity is high
- If Kappa (streaming only with replay) can satisfy the requirement, use it instead

---

## Pattern 6: Kappa Architecture

### What it is

A single streaming pipeline reads from Kafka for all processing — both real-time and historical. When a bug is fixed or logic changes, the pipeline is restarted from `offset=earliest` to reprocess the full history. Kafka retention is set long enough to replay the entire dataset.

```
Kafka Topic (long retention: 30–90 days)
    └── Single Flink / Spark SS pipeline
          ├── Writes to Data Lake
          └── On reprocessing: reset consumer group to earliest, replay
```

### When to use

- Streaming pipeline logic can correctly reprocess historical data (event time, not processing time)
- Kafka retention is long enough (weeks to months) to hold the full reprocess window
- Simplicity of a single codebase outweighs reprocessing cost

### When to avoid

- Historical data exceeds Kafka retention — you'd need to re-seed from the lake to Kafka
- Reprocessing takes longer than your SLA allows (replaying 30 days of events takes time)

---

## Pattern 7: CDC via Kafka (Debezium)

### What it is

A database change data capture (CDC) tool (Debezium is the standard) captures row-level inserts, updates, and deletes from a source database's transaction log and publishes them as structured events to Kafka. Downstream consumers treat the Kafka topic as a change stream and apply the changes to a target (data lake, warehouse, search index).

```
PostgreSQL → Debezium → Kafka topic (op: I/U/D, before/after image)
                              └── Consumer → apply MERGE to Delta/Iceberg/DynamoDB
```

### Event structure

```json
{
  "op": "u",
  "before": { "customer_id": 101, "status": "active" },
  "after":  { "customer_id": 101, "status": "churned" },
  "ts_ms": 1718620800000,
  "source": { "table": "customers", "lsn": 12345678 }
}
```

`op` values: `c` (create/insert), `u` (update), `d` (delete), `r` (snapshot read).

### Consumer applying CDC to Delta Lake

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, from_json

cdc_schema = "op STRING, after STRUCT<customer_id LONG, name STRING, status STRING, updated_ts TIMESTAMP>"

raw = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:9092") \
  .option("subscribe", "postgres.public.customers") \
  .load()

parsed = raw.select(from_json(col("value").cast("string"), cdc_schema).alias("cdc")) \
  .select("cdc.op", "cdc.after.*")

def apply_cdc(batch_df, batch_id):
  target = DeltaTable.forName(spark, "analytics.dim_customer")

  # Deletes first, then upserts — order matters
  deletes = batch_df.filter("op = 'd'").select("customer_id")
  target.alias("t").merge(deletes.alias("d"), "t.customer_id = d.customer_id") \
    .whenMatchedDelete().execute()

  upserts = batch_df.filter("op IN ('c', 'u')").drop("op")
  target.alias("t").merge(upserts.alias("s"), "t.customer_id = s.customer_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

parsed.writeStream \
  .foreachBatch(apply_cdc) \
  .option("checkpointLocation", "s3://checkpoints/cdc_customers/") \
  .trigger(processingTime="1 minute") \
  .start()
```

See [Merge Strategies](../../../pipeline-patterns/merge-strategies/README.md) for the full treatment of upsert, hard delete, and soft delete patterns applied to CDC streams.

---

## Pattern 8: Stateful Windowed Aggregation

### What it is

Rather than forwarding raw events to a lake, the streaming pipeline computes aggregations over time windows before writing. The result is a pre-aggregated metric table — smaller, faster to query, updated continuously.

### Window types

| Window type | Definition | Use case |
|---|---|---|
| **Tumbling** | Fixed, non-overlapping windows (00:00–00:05, 00:05–00:10) | Per-interval totals: "orders per 5-minute bucket" |
| **Sliding** | Fixed size, overlapping (window advances every N seconds) | Rolling averages: "avg order value in last 15 min, updated every 1 min" |
| **Session** | Variable size, closes after inactivity gap | User session analysis: "group clicks until 30-min gap" |

```sql
-- ksqlDB: tumbling 5-minute order count
SELECT
  customer_id,
  COUNT(*)  AS order_count,
  WINDOWSTART,
  WINDOWEND
FROM order_events
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY customer_id
EMIT CHANGES;

-- ksqlDB: hopping (sliding) 15-minute window, advancing every 1 minute
SELECT customer_id, AVG(amount) AS avg_amount
FROM order_events
WINDOW HOPPING (SIZE 15 MINUTES, ADVANCE BY 1 MINUTE)
GROUP BY customer_id
EMIT CHANGES;
```

---

## Technology Deep-Dive

### Spark Structured Streaming

- **Best for:** micro-batch to near-real-time; Delta/Iceberg writes; team already runs Spark
- **Trigger modes:**

| Trigger | Behavior | Use for |
|---|---|---|
| `processingTime="10 minutes"` | Fires every 10 min | Micro-batch |
| `processingTime="1 minute"` | Fires every 1 min | Near real-time |
| `once=True` | Runs one batch, stops | Scheduled batch-like execution |
| `availableNow=True` | Processes all backlog, stops | Efficient backfill |
| `continuous="1 second"` | Experimental; sub-second | Not production-ready for stateful |

- **Strengths:** familiar API, native Delta/Iceberg support, exactly-once with Delta, rich Python API
- **Weaknesses:** high latency floor (~30s minimum practical), heavy JVM overhead, limited stateful operation expressiveness vs Flink

### Apache Flink

- **Best for:** real-time (<1 min), complex stateful operations, event-time correctness, stream-to-stream joins
- **Checkpoint interval → latency:** the end-to-end latency is roughly `checkpoint interval + network latency`. 10-second checkpoint → ~15-second latency.
- **State backends:** HashMap (in-memory, fast, limited size), RocksDB (disk-backed, large state, slightly slower)
- **Strengths:** true event-time semantics, efficient stateful ops, sub-second latency, production-grade at massive scale
- **Weaknesses:** steeper learning curve, Java/Scala-first (PyFlink is usable but lags), harder to debug than batch

### Kafka Streams

- **Best for:** stateful in-process streaming within the Kafka ecosystem; no separate compute cluster
- **Strengths:** runs inside your application, exactly-once via Kafka transactions, low ops overhead, scales by adding application instances
- **Weaknesses:** Java-only, writing results outside Kafka requires a custom sink, no native Parquet/Delta/Iceberg output

### ksqlDB

- **Best for:** SQL-first streaming analytics; simpler use cases; teams that prefer SQL over code
- **Strengths:** declarative SQL, built-in materialized views, REST API queryable, push queries
- **Weaknesses:** less flexible than Flink for complex logic, results stored in Kafka topics (not directly in a lake), limited Python/Spark integration

### Kafka Connect (Sink Connectors)

- **Best for:** Kafka → storage without transformation; config-only, no code
- **Common connectors:** S3 Sink, JDBC Sink (Postgres, MySQL), Elasticsearch Sink, BigQuery Sink, Snowflake Sink
- **Strengths:** zero code, reliable, connector ecosystem is broad
- **Weaknesses:** limited transformation capability (SMTs are basic); schema evolution requires care; each connector has its own bugs and quirks

---

## Decision Matrix

| Requirement | Batch (hourly+) | Micro-Batch (5–15 min) | Near RT (1–5 min) | Real-Time (<1 min) |
|---|---|---|---|---|
| **Data freshness needed** | Hourly is fine | 5–15 min acceptable | < 5 min | Seconds or less |
| **Team streaming experience** | None needed | Basic | Intermediate | Advanced (Flink/Kafka Streams) |
| **Stateful operations** | Yes (batch SQL) | Simple | Moderate | Complex (windows, joins) |
| **Event volume** | Any | High | High | Very high |
| **Exactly-once writes** | Easy (truncate+load) | Yes (Delta + checkpoint) | Yes | Yes (Flink + Kafka transactions) |
| **Operational complexity** | Low | Medium | Medium–High | High |
| **Small file management** | No issue | Compaction needed (hours) | Compaction critical (minutes) | Depends on sink |
| **Backfill / reprocessing** | Simple | Restart from stored offsets | Same | Kappa: replay from earliest |
| **Cost** | Low (batch compute) | Medium | Medium–High | High (always-on cluster) |
| **Debugging ease** | Easy (batch logs) | Moderate | Moderate | Hard (state, watermarks, lag) |

---

## Choosing the Right Latency Tier

```
What is the downstream SLA?
  └── > 1 hour          → Batch. Scheduled Spark or Kafka Connect S3 Sink.
  └── 15–60 min         → Micro-batch. Spark SS, trigger 10–15 min.
  └── 5–15 min          → Micro-batch. Spark SS, trigger 5–10 min.
  └── 1–5 min           → Near RT. Spark SS short trigger or Flink. Add compaction job.
  └── < 1 min           → Real-time. Flink or Kafka Streams.
  └── Sub-second        → Kafka Streams (stateless) or Flink (stateful).

Is the result written to a data lake (S3/Parquet)?
  └── Yes → Spark Structured Streaming or Flink with Iceberg sink.
             Avoid real-time (small files); prefer micro-batch + compaction.

Is the result written to a serving store (Redis, DynamoDB, search)?
  └── Yes → Flink or Kafka Streams. Per-event writes are acceptable here.

Does processing require event-time correctness (out-of-order events)?
  └── Yes → Flink with watermarks. Spark SS handles event-time but less efficiently.

Is the team SQL-first with no JVM experience?
  └── Yes + freshness < 5 min → ksqlDB.
  └── Yes + freshness > 5 min → Spark SQL on Structured Streaming.
```

---

## Anti-Patterns

| Anti-pattern | What you'll observe | The fix |
|---|---|---|
| **Real-time pipeline writing directly to Snowflake/Redshift** | Per-row inserts crater warehouse performance; cost explodes | Write to S3 (micro-batch), COPY into warehouse periodically; or use a staging Kafka Connect JDBC sink with batching |
| **Micro-batch with no compaction** | S3 folder fills with millions of 1 KB files; Athena/Spark queries take 10× longer | Schedule `OPTIMIZE` (Delta) or `rewrite_data_files` (Iceberg) every few hours |
| **Using processing time instead of event time for windowed aggregations** | Late-arriving events (mobile app offline for 10 min) fall in the wrong window | Use event-time with watermarks; tolerate N seconds of late data |
| **No `maxOffsetsPerTrigger` cap on Structured Streaming** | After a lag (deploy, crash), the first trigger reads 50M events and OOMs | Set `maxOffsetsPerTrigger` to bound per-batch memory |
| **Kafka Connect S3 Sink with no schema registry** | Schema changes in the producer break the sink silently | Use Confluent Schema Registry + Avro/Protobuf; the connector enforces schema compatibility |
| **Lambda architecture with diverging logic** | Batch result and speed layer result disagree; analysts trust neither | Share logic (same JAR/library) between layers; or migrate to Kappa |
| **Setting Kafka retention too short for Kappa** | Reprocessing from `earliest` fails — messages expired | Increase retention on topics used for reprocessing (30–90 days); or use a tiered storage plugin |
| **Ignoring consumer group lag** | Kafka lag grows silently; micro-batch is hours behind without alerting | Monitor `kafka_consumer_group_lag` per partition; alert when lag exceeds 1 trigger interval |

---

## Monitoring Essentials

Regardless of pattern, these metrics must be monitored:

| Metric | What it signals | Alert threshold |
|---|---|---|
| **Consumer group lag** (messages behind) | Pipeline is falling behind; latency is increasing | > 1 trigger interval's worth of messages |
| **Processing time per trigger** | Trigger taking longer than interval — catching up or overloaded | > 80% of trigger interval |
| **Checkpoint duration** (Flink/SS) | Checkpoint backpressure; state too large | > 30% of checkpoint interval |
| **Records dropped / deserialization errors** | Bad messages arriving; schema mismatch | Any non-zero sustained rate |
| **Sink write latency** | Bottleneck at output (S3 throttle, DB lock) | P95 > 2× baseline |

---

## Interview & Architecture-Review Talking Points

- **"When would you use Flink over Spark Structured Streaming?"** — When you need sub-minute latency, complex stateful operations (multi-stream joins, long windows), or true event-time semantics with fine-grained watermark control. For micro-batch to near-RT writing to Delta or Iceberg, Structured Streaming is simpler and the team is often already running Spark.

- **"What is the small file problem in streaming, and how do you fix it?"** — Every micro-batch trigger creates N files (one per partition). Frequent triggers × many partitions = millions of small files that degrade query performance. Fix: schedule Delta `OPTIMIZE` or Iceberg `rewrite_data_files` every few hours to compact them, and set `target-file-size` to 128–256 MB.

- **"What's the difference between event time and processing time?"** — Processing time is when the event arrives at the engine. Event time is the timestamp embedded in the event itself. A mobile event produced at 10:00 but delivered at 10:45 (due to connectivity) has processing time 10:45 but event time 10:00. Windowed aggregations must use event time and watermarks to correctly handle out-of-order delivery.

- **"How do you handle exactly-once delivery from Kafka to a data lake?"** — Spark Structured Streaming + Delta Lake: the engine commits Kafka offsets and the Delta transaction atomically in the checkpoint. If the job crashes mid-write, the incomplete Delta transaction is rolled back on restart and the offsets are replayed. Flink achieves the same via two-phase commit with the Iceberg sink.

- **"When is Kafka Streams a better choice than Flink?"** — When the team is Java-first, the pipeline is stateful but not complex, and you want to avoid operating a separate Flink cluster. Kafka Streams runs in-process, scales by adding application replicas, and uses Kafka itself for state durability. The trade-off: no native lake sink, Java-only, less expressive than Flink for complex windowing.

---

## Further Reading

- [Exactly-Once Semantics in Kafka](../exactly-once/README.md) — transaction semantics, idempotent producers, consumer offset commits
- [Dead Letter Queues](../dlq/README.md) — handling deserialization failures, schema mismatches, poison pills in streaming pipelines
- [Idempotency in Pipelines](../../pipeline-patterns/idempotency/README.md) — safe re-run patterns when the streaming sink is Delta, Iceberg, or an RDBMS
- [Merge Strategies](../../pipeline-patterns/merge-strategies/README.md) — applying CDC change streams (insert/update/delete) to a target table
- [Choosing Your Data Platform](../../platform-engineering/choosing-your-data-platform/README.md) — Hudi is purpose-built for streaming upserts; when to choose it over Delta/Iceberg
