# Pipeline Compute Options

> Chapter from the Data Engineering Playbook — platform-engineering.

## About This Chapter

**What this is.** A comparison of the engines that run data pipelines — EMR Serverless, EMR on EC2/EKS, Glue, Lambda, Databricks, Dataproc/Synapse, Spark on Kubernetes (K8s), dbt, Flink, and Trino — and a framework for matching each to a workload based on data size, latency, and transformation complexity.

**Who it's for.** Mid-level data engineers, platform and architecture leads, engineering managers and tech leads, and engineers preparing for senior or staff data-engineering interviews.

**What you'll take away.** By the end you'll be able to:
- Place a workload on the serverless-vs-persistent and managed-vs-self-managed axes and pick a default (EMR Serverless on AWS) with clear signals for when to move off it.
- Decide when to use Spark and — just as important — when to avoid it in favor of dbt, Lambda, Trino/Athena, or Flink, using the volume/complexity crossover guide.
- Compare options on cost and capability (including the Glue and Databricks premiums) and recognize the common over-engineering mistakes.

---

## TL;DR

- **EMR Serverless** is the right default on AWS for batch Spark jobs: no cluster to manage, auto-scales, and you pay only for the seconds the job actually runs. Stay on it unless a specific signal forces a change.
- **Switch to EMR on EC2** when you need deep Spark tuning or cost savings from Reserved or Spot EC2 instances on predictable daily schedules.
- **Use Glue** when the pipeline is SQL-heavy, simpler, and the team does not need fine-grained Spark control.
- **Use Databricks** when Delta Lake is the table format, ML and data engineering share a platform, or the team wants Unity Catalog governance.
- **Use Flink** when the latency requirement is under 1 minute — Spark Structured Streaming cannot match Flink's efficiency at true real-time.
- **Avoid Spark entirely** when data is under 10 GB, logic is pure SQL, or the team is SQL-first — dbt plus a warehouse is faster to build, cheaper to run, and simpler to debug.
- **The most expensive mistake:** defaulting to Spark for every pipeline regardless of data size. A Lambda function or dbt model on a 5 GB table runs in 30 seconds and costs cents. An EMR Serverless job takes 2 minutes to cold-start (the delay before the job actually begins, while workers are being assigned) and 5 minutes to run the same logic.

---

## The Two Axes

Every compute option sits somewhere on two axes. Understanding these axes makes it easier to compare options at a glance.

```
                    SERVERLESS / ON-DEMAND
                           │
         AWS Glue ─────────┤──────── EMR Serverless
         AWS Lambda        │         Dataproc Serverless
                           │
FULLY ─────────────────────┼─────────────────────── SELF-
MANAGED                    │                         MANAGED
                           │
         Databricks ───────┤──────── EMR on EC2
         Azure Synapse      │         Spark on K8s
                           │         YARN / on-prem
                    ALWAYS-ON / PERSISTENT
```

**Serverless / on-demand** → no cluster to manage; you pay per job; there is a cold-start delay before each job begins.
**Always-on / persistent** → the cluster runs whether or not jobs are submitted; predictable startup time; higher idle cost when nothing is running.
**Fully managed** → the vendor handles upgrades, patching, and scaling; you have less control over internals.
**Self-managed** → you control the Spark version, JVM (Java Virtual Machine) flags, node types, and scaling policy; more operational burden on your team.

The right choice depends on where your workload sits on these two axes.

---

## When to Use Spark — and When to Avoid It

Many engineers default to Spark for every data problem. It is the wrong choice more often than you might expect.

### Use Spark when

| Signal | Why Spark fits |
|---|---|
| Data volume > 100 GB | Spark's distributed execution (splitting work across many machines) earns its overhead — single-node tools run out of memory or take hours |
| Semi-structured data (nested JSON, Parquet, Avro) | Native schema inference, `explode()`, struct types — no pre-processing step needed |
| Complex multi-join pipelines | Broadcast joins, shuffle joins, AQE (Adaptive Query Execution, which automatically optimizes the query plan at runtime) — the optimizer handles what SQL engines don't |
| Window functions over large partitions | Partition-local aggregation at scale |
| ML feature engineering | PySpark plus MLlib or pandas UDFs bring the Python ML ecosystem to distributed data |
| Writing Delta Lake or Iceberg | Spark is the primary write engine for both; MERGE, schema evolution, and OPTIMIZE all require it |
| Backfill / reprocessing historical S3 data | Bounded reads from S3 with offset control; parallelism over years of partitions |
| UDFs with complex business logic | Python/Scala/Java UDFs (User-Defined Functions, custom code you write and plug into the query engine) that SQL engines cannot express |

### Avoid Spark when

| Signal | Better alternative | Why |
|---|---|---|
| Data < 10 GB / < 10M rows | Python script, dbt, Lambda | Spark cold-start (1–3 min) plus shuffle overhead exceeds the job runtime itself |
| Pure SQL transformations on structured schema | dbt + Snowflake/Redshift | No JVM, no cluster, faster iteration, cheaper at small-medium scale |
| Latency < 1 minute required | Apache Flink | Spark Structured Streaming has a ~30s minimum practical latency floor; Flink achieves milliseconds |
| Ad-hoc interactive queries on lake tables | Trino / Athena | Query the files directly — no pipeline, no cluster warmup, pay per TB scanned |
| Row-level enrichment triggered by an event | AWS Lambda | Event-driven, sub-second, zero cluster cost — Spark is not an event handler |
| Team is SQL-only | dbt on warehouse | Forcing a SQL team to write PySpark introduces bugs and slows delivery for no gain |
| One-time migration or small daily reference table | Python + boto3 or Pandas | The overhead of a distributed job for a 50K-row table is wasteful |

### The volume / complexity crossover

Use this table as a starting point when deciding whether Spark is the right tool. Data size on the left, transformation complexity on the right.

```
Data size        │ Transformation complexity
─────────────────┼──────────────────────────────────────────────────
< 1 GB           │ Any complexity    → Python / dbt / Lambda
1 GB – 10 GB     │ SQL only          → dbt + warehouse
                 │ Complex / UDFs    → Spark (Glue or EMR Serverless)
10 GB – 1 TB     │ Any complexity    → Spark (EMR Serverless sweet spot)
> 1 TB           │ Any complexity    → Spark with tuning (EMR EC2 or Databricks)
Any size         │ < 1 min latency   → Flink
Any size         │ Streaming upserts → Flink or Spark Structured Streaming
```

---

## Baseline: EMR Serverless

### What it is

Amazon EMR Serverless runs Spark (or Hive) jobs without requiring you to provision or manage clusters. You submit a job via the EMR API or Step Functions, workers are assigned from a pre-initialized pool, the job runs, and workers are released when done. You pay per vCPU-second and GB-second of memory actually consumed — nothing more.

### How it works

The diagram below shows the lifecycle of a job submission on EMR Serverless.

```
You: spark-submit job.py → EMR Serverless Application
                                    │
                     Pre-initialized worker pool
                     (warm workers assigned in ~10–30s
                      if pre-init is configured;
                      cold workers take ~1–3 min)
                                    │
                         Job runs on S3 data
                                    │
                     Workers released on completion
                     You pay only for job duration
```

### Cost model

| Resource | Price (us-east-1, approximate) |
|---|---|
| vCPU | ~$0.052 per vCPU-hour |
| Memory | ~$0.0057 per GB-hour |
| Storage (shuffle) | ~$0.000111 per GB-hour |

A typical 10-executor job (4 vCPU × 16 GB each) running for 30 minutes:
- 40 vCPU × 0.5 hr = $1.04
- 160 GB × 0.5 hr = $0.46
- Total: ~$1.50 per run

### Strengths

- Zero cluster ops — no SSH access, no bootstrap scripts, no capacity planning
- Native AWS integration: S3, Glue Catalog, IAM roles, CloudWatch, Step Functions
- Pre-initialized worker pools eliminate cold-start for latency-sensitive pipelines
- Auto-scales within configured min/max worker bounds per job
- Supports Spark 3.x, Python 3.x, custom Docker images for dependencies

### Limits

| Limit | Impact |
|---|---|
| Cold-start without pre-init: 1–3 min | Not suitable for sub-5-minute triggered pipelines without pre-init enabled |
| Max job duration: 1000 hours (configurable) | Not a practical constraint for most batch jobs |
| Limited tuning vs EC2 | Cannot set `spark.executor.memoryOverhead` or tune YARN settings directly |
| No persistent cluster for interactive notebooks | Not a development environment; use EMR on EC2 or Databricks notebooks for that |
| No cross-job shared caching | Each job starts cold; no RDD/DataFrame cache (in-memory data structures that speed up repeated reads) shared across submissions |

### When to stay on EMR Serverless

- Batch jobs run on a schedule (daily, hourly) without constant tuning pressure
- Job durations are under 4 hours
- Team does not need to SSH into executors to debug out-of-memory (OOM) issues
- Burst workloads — job frequency varies unpredictably
- Cost optimization via per-second billing is preferred over reserved capacity

---

## AWS Alternatives

### EMR on EC2 — Full Control

**What it is:** Managed Spark clusters on EC2 instances (virtual machines in AWS). You provision a cluster (master plus core plus task nodes), bootstrap it with scripts, run jobs, and optionally auto-terminate. The cluster can be persistent (always-on) or transient (spun up per job, terminated after).

**Spark config control:** Full. Any `spark-defaults.conf`, JVM flags, executor memory overhead, YARN settings, and custom Hadoop config. This is the option when you need to tune out of a persistent OOM (out-of-memory crash) or shuffle bottleneck that EMR Serverless won't let you touch.

**Cost model:** Per EC2 instance-hour regardless of whether a job is running. Transient clusters (one cluster per job, terminated after) eliminate idle cost but add 5–10 min cluster provisioning time.

| Setup | Cost profile |
|---|---|
| Persistent cluster (always-on) | Pay 24/7; low latency; high idle waste |
| Transient cluster (per-job) | Pay only while running; 5–10 min startup overhead |
| Spot instances for task nodes | 50–80% cost reduction; risk of interruption mid-job (use checkpointing to save progress) |
| Reserved instances for core nodes | 30–60% savings for predictable daily workloads |

**Use when:**
- Jobs require deep Spark tuning that EMR Serverless does not expose
- Predictable daily batch schedule justifies Reserved Instances for 30–60% savings
- Job runs for 4+ hours where reserved capacity is more predictable than serverless billing
- Team needs an interactive development environment (JupyterHub on a persistent cluster)

**Avoid when:**
- Team cannot own cluster ops — node failures, AMI updates, bootstrap script maintenance
- Job frequency is unpredictable — idle persistent clusters waste money

---

### EMR on EKS — Spark on Kubernetes

**What it is:** Submit Spark jobs to an EKS (Elastic Kubernetes Service — AWS's managed Kubernetes platform) cluster. Each job runs as a set of Kubernetes pods (isolated containers). The data team gets Spark; the platform team gets a single Kubernetes control plane for ML, data, and applications.

**Use when:**
- Your organisation already runs EKS for applications and ML — one platform reduces operational overhead
- Data and ML teams want unified container image management
- You need fine-grained Kubernetes resource quotas and namespace isolation per team

**Avoid when:**
- No Kubernetes expertise on the team — the operational burden is substantially higher than EMR Serverless
- Benefit over EMR Serverless is marginal for a pure batch Spark shop

---

### AWS Glue — Serverless ETL for Simpler Pipelines

**What it is:** A fully managed serverless ETL (Extract, Transform, Load) service. Under the hood it is Spark, but abstracted behind DynamicFrames (Glue's own DataFrame-like API) with added services: Glue Crawlers (auto schema inference), Glue Catalog (a metadata store that acts like a Hive Metastore), a visual ETL editor, and native integration with every AWS data store.

**DynamicFrames vs DataFrames:**

DynamicFrames are Glue's alternative to Spark DataFrames. They handle messy source data (mixed types, extra columns) more gracefully, but at the cost of some performance.

```python
# Glue DynamicFrame (Glue-specific)
dynamic_df = glueContext.create_dynamic_frame.from_catalog(
    database="analytics", table_name="orders"
)
dynamic_df = dynamic_df.filter(lambda x: x["status"] == "shipped")
glueContext.write_dynamic_frame.from_options(dynamic_df, ...)

# You can convert to a Spark DataFrame for complex logic:
df = dynamic_df.toDF()
df = df.groupBy("customer_id").agg(...)
```

DynamicFrames have overhead (~20–30% slower than native DataFrames for large jobs) but handle schema inconsistencies gracefully — useful for messy source data.

**Cost model:** Glue charges in DPU (Data Processing Unit = 4 vCPU + 16 GB RAM) units, billed per second. ~$0.44 per DPU-hour vs EMR Serverless ~$0.27 per equivalent vCPU+memory hour. **Glue is ~30–60% more expensive than EMR Serverless for equivalent Spark compute.**

**Use when:**
- Pipeline is SQL-heavy and benefits from Glue's visual editor for less-technical builders
- Schema discovery is needed: Glue Crawlers automatically register Parquet/JSON schemas in Glue Catalog
- Team is small and values the fully-managed experience over cost optimization
- Integration with AWS services is the primary logic (Glue has native connectors for RDS, DynamoDB, Redshift, S3, Kafka)

**Avoid when:**
- Complex PySpark logic with large shuffles — DynamicFrame overhead adds up
- Tight cost control — EMR Serverless is materially cheaper
- Fine-grained Spark config tuning is needed

---

### AWS Lambda — Event-Driven Micro-Transforms

**What it is:** Serverless function execution. Max 15-minute runtime, up to 10 GB memory, 6 vCPU. Triggered by S3 events, SQS, Kafka, API Gateway, DynamoDB Streams, or EventBridge schedules. Think of it as running a short Python function in response to an event, with no server to manage.

**Not a Spark replacement** — Lambda cannot handle distributed shuffles, cannot read Parquet files at Spark-scale, and has no persistent state.

**Use when:**
- Small record-level transformation triggered by an event (new file lands in S3 → parse and write to DynamoDB)
- Lightweight enrichment (call an API, look up a value, route a message)
- Orchestration glue between data pipeline steps
- File validation, schema checks, routing before a Spark job

**Avoid when:**
- Processing more than a few million rows — memory and time limits are hit
- Joining datasets — there is no distributed execution model

---

## Databricks — The Premium Spark Platform

### What it is

A managed Spark platform running on your cloud (AWS, Azure, or GCP). Databricks adds a significant layer on top of open-source Spark: Delta Lake as the default table format, MLflow for experiment tracking, Unity Catalog for governance, collaborative notebooks, and Photon (a C++-based query engine that speeds up SQL 2–10x on certain workloads).

### Cluster types

| Type | Behavior | Use for |
|---|---|---|
| **Job Cluster** | Auto-created at job start, terminated on completion | Production batch pipelines — lowest cost, no idle |
| **All-Purpose Cluster** | Persistent; shared by notebooks and ad-hoc queries | Development, exploration, shared notebooks |
| **SQL Warehouse** | Optimized for SQL analytics; auto-suspend/resume | Analysts running SQL on Delta tables |

### Cost model

Databricks charges **DBUs (Databricks Units)** per hour, on top of the underlying cloud instance cost. You pay both the cloud provider for EC2 or VMs and Databricks separately for DBUs.

| Tier | DBU rate (approx.) | Typical total cost |
|---|---|---|
| Jobs Compute (batch) | ~$0.10/DBU | ~$0.30–0.50 per instance-hour above EC2 |
| All-Purpose Compute | ~$0.40/DBU | ~$1.00–1.50 per instance-hour above EC2 |
| Photon (SQL) | ~$0.20/DBU | Faster queries, higher DBU rate |

**Vs EMR Serverless:** For a job consuming 40 vCPU for 30 min, EMR Serverless costs ~$1.50. Databricks Jobs Compute for equivalent hardware costs ~$2.50–4.00. The premium pays for Delta, Unity Catalog, MLflow, and Photon.

### When Databricks earns its premium

- Delta Lake is your table format — Delta works on EMR too, but Databricks has first-class support: OPTIMIZE, VACUUM, Liquid Clustering, Delta Live Tables (a declarative pipeline framework built into Databricks)
- ML and data engineering share a platform — MLflow, feature store, and Spark all in one workspace
- Unity Catalog for cross-workspace governance — row/column-level security, data lineage, and audit logs across all Databricks workspaces
- Team needs collaborative development in notebooks with version control, code review, and live execution
- Photon speeds up SQL-heavy batch jobs enough to justify the cost

### When to stay on EMR Serverless

- Table format is Iceberg (not Delta) — EMR Serverless plus Iceberg is first-class on AWS
- AWS-committed shop with S3, Glue, and IAM already integrated
- Tight cost controls — the DBU premium is real
- No need for Databricks-specific features (MLflow, Unity Catalog, Delta Live Tables)

---

## Other Cloud Equivalents (Awareness)

### Google Cloud Dataproc

GCP's managed Spark service (Dataproc on VMs is equivalent to EMR on EC2; Dataproc Serverless is equivalent to EMR Serverless). Native integration with BigQuery, GCS (Google Cloud Storage), and Pub/Sub. Use when workloads are on GCP.

### Azure Synapse Analytics

A unified analytics workspace combining Spark pools (for data engineering) and SQL pools (for warehousing) in one service. Tight Power BI integration. Use when the org is Azure-committed and has mixed SQL plus Spark workloads.

### Azure HDInsight

Microsoft's older managed Hadoop/Spark service on Azure VMs. Being superseded by Synapse for new workloads. Use only if maintaining legacy HDInsight clusters.

---

## Self-Managed: Spark on Kubernetes

**What it is:** Submit Spark jobs directly to any Kubernetes cluster using `spark-submit --master k8s://`. Each job spins up driver and executor pods (isolated containers), runs, and pods are released when done.

```bash
spark-submit \
  --master k8s://https://<k8s-api-server>:443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=my-spark:3.5 \
  --conf spark.executor.instances=10 \
  s3://my-bucket/jobs/pipeline.py
```

**Use when:**
- On-premises environment where cloud managed services are not available
- An org-wide Kubernetes platform makes it operationally consistent with other services
- You need to run Spark on specific hardware (GPU nodes, high-memory instances not available on EMR)
- Strict container security requirements (custom hardened images)

**Ops overhead:** High. You manage Spark version upgrades, pod resource limits, cluster autoscaling, shuffle storage, and Kubernetes RBAC (Role-Based Access Control — the permission system for who can do what in Kubernetes). There are no pre-initialized workers, so cold-start is slower than EMR Serverless.

---

## Non-Spark Alternatives

### dbt + Data Warehouse

**What it is:** SQL `SELECT` statements compiled and executed inside Snowflake, Redshift, or BigQuery. dbt (data build tool) handles dependency resolution (figuring out the order to run models), incremental materialization (only processing new or changed data), testing, and documentation. No Spark, no cluster required.

**When it replaces Spark:**
- All transformations are expressible in SQL
- Schema is structured and well-defined
- Data volume fits warehouse economics (under 5 TB; beyond that, warehouse scan cost exceeds lake economics)
- Team is SQL-first with no PySpark experience

**When it cannot replace Spark:**
- Semi-structured source data (nested JSON, Avro)
- UDFs in Python or complex business logic beyond SQL
- ML feature engineering
- Writing directly to Iceberg or Delta on S3

---

### Apache Flink

**What it is:** A stream-first distributed compute engine. Flink processes events as they arrive with millisecond latency, rather than waiting to collect a batch. It also supports batch mode.

**When it replaces Spark:**
- Latency under 1 minute — Flink achieves sub-second; Spark's minimum practical micro-batch interval is ~30 seconds
- Complex stateful streaming (where the engine must remember past events): multi-stream joins, long windows, incremental aggregation
- CDC (Change Data Capture — reading a stream of database row changes) ingestion at high frequency (Kafka to lake every minute)

**When it does not replace Spark:**
- Batch-only workloads — Spark has a larger ecosystem and more mature batch optimizations (AQE, Catalyst query optimizer)
- Team is Python/PySpark-first — PyFlink lags behind Java/Scala Flink APIs

---

### Trino / Presto

**What it is:** A distributed query engine that reads data directly from S3 (Parquet, Iceberg, Hive tables) without moving data into a warehouse. It is query-only; it has no write or ETL capability.

**When it supplements Spark:**
- Ad-hoc interactive queries on lake tables (faster and cheaper than spinning up a Spark job)
- Federated queries across S3, PostgreSQL, Redshift, and Elasticsearch in a single SQL statement
- Amazon Athena is AWS's managed Trino — same query model, serverless, pay per TB scanned

**When it does not replace Spark:**
- Writing transformed output back to S3 or Iceberg — Trino can write but is not optimized for ETL at scale
- Complex transformations with UDFs or window functions over billions of rows

---

## Decision Matrix

| Option | Workload | Managed | Serverless | Spark native | Multi-cloud | Approx cost (10 vCPU × 1 hr) |
|---|---|---|---|---|---|---|
| **EMR Serverless** | Batch | Fully | Yes | Yes | AWS only | ~$3 |
| **EMR on EC2** | Batch / interactive | Partially | No | Yes | AWS only | ~$2–4 (+ EC2) |
| **EMR on EKS** | Batch | Partially | No | Yes | AWS only | ~$2–3 (+ EKS) |
| **AWS Glue** | Batch / ETL | Fully | Yes | Yes (abstracted) | AWS only | ~$4–5 |
| **Databricks** | Batch / RT / ML | Fully | No (job clusters) | Yes + Delta | AWS/GCP/Azure | ~$4–7 |
| **Dataproc** | Batch | Fully | Yes (Serverless) | Yes | GCP only | ~$2–4 |
| **Azure Synapse** | Batch / SQL | Fully | Yes | Yes | Azure only | ~$3–5 |
| **Spark on K8s** | Batch / streaming | Self | No | Yes | Any | ~$1–2 (+ K8s ops) |
| **dbt + warehouse** | Batch (SQL) | Fully | Yes | No (SQL) | Multi | Per query (varies) |
| **Apache Flink** | Streaming + batch | Self / managed | No | No (own engine) | Any | ~$2–4 (+ ops) |
| **Trino / Athena** | Query only | Fully (Athena) | Yes | No | AWS (Athena) | Per TB scanned |
| **AWS Lambda** | Micro-transform | Fully | Yes | No | AWS only | ~$0.001/invocation |

---

## Migration Signals: When to Move Off EMR Serverless

| Signal | Recommended move | Reason |
|---|---|---|
| Jobs consistently OOM (out of memory) despite adding memory | EMR on EC2 | Exposes `spark.executor.memoryOverhead`, YARN tuning, and custom AMI options |
| Same job runs daily on a predictable schedule and costs are growing | EMR on EC2 with Reserved Instances | Reserved EC2 for core nodes saves 30–60% over on-demand serverless billing |
| Team wants Delta Live Tables, Unity Catalog, or MLflow | Databricks | These are Databricks-native features; not available on EMR |
| Latency must be under 5 minutes | Flink or Spark Structured Streaming with short trigger | EMR Serverless cold-start makes sub-5-min impractical without pre-init |
| All logic is SQL, data under 5 TB | dbt + Snowflake/Redshift | Eliminate Spark entirely; faster dev cycle, lower cost |
| Org migrates to GCP or Azure | Dataproc Serverless / Azure Synapse | EMR does not run on other clouds |
| Complex ad-hoc exploration needed alongside pipelines | Databricks or EMR on EC2 with JupyterHub | EMR Serverless has no interactive notebook environment |

---

## Anti-Patterns

| Anti-pattern | What you'll observe | The fix |
|---|---|---|
| **Spark for a 500 MB table** | 3-min cold-start + 2-min job = 5 min for logic that runs in 10s on a laptop | Python script or dbt model |
| **Persistent EMR on EC2 running 24/7 for jobs that run hourly** | Cluster idle 23 hours/day; $500+/mo wasted | Switch to transient clusters or EMR Serverless |
| **Glue for a 1 TB complex join pipeline** | DynamicFrame overhead + Glue DPU pricing = slow and expensive | EMR Serverless with native DataFrames |
| **Databricks All-Purpose cluster left running over the weekend** | $300–500 in idle DBU + EC2 charges | Set auto-terminate after 30 min idle; use Job Clusters for production |
| **Flink for a daily batch pipeline** | Operational complexity (state backends, checkpoints, watermarks) with no latency benefit | Spark batch or dbt |
| **Lambda for a 100M-row transformation** | Function OOMs or times out after 15 min | EMR Serverless |
| **Treating EMR Serverless and EMR on EC2 as equivalent for cost** | Serverless at high steady volume costs more than Reserved EC2 | Model total monthly cost; Reserved EC2 often wins for predictable daily pipelines above a certain volume |

---

## Interview & Architecture-Review Talking Points

- **"Why would you choose EMR Serverless over EMR on EC2?"** — No cluster ops: no provisioning, no capacity planning, no AMI maintenance. Pay per second of job runtime rather than per cluster-hour whether or not jobs are running. The trade-off is less Spark tuning control and a cold-start latency floor. For a team running daily batch jobs without tight latency requirements, the ops savings outweigh the loss of fine-grained control.

- **"When would Databricks be worth the premium over EMR?"** — When Delta Live Tables, Unity Catalog, or MLflow are needed. When data and ML engineering share a platform and the cross-team productivity gain justifies the DBU cost. When Photon speeds up SQL-heavy workloads enough to recover the cost difference. For a pure Spark batch shop on Iceberg, the premium is hard to justify.

- **"When would you not use Spark at all?"** — Small data (under 10 GB): the startup cost exceeds the job runtime. Pure SQL transformations on structured data: dbt on Snowflake runs faster and is cheaper. Sub-minute latency: Flink is the purpose-built tool. Ad-hoc exploration: Trino/Athena queries the lake directly without a pipeline. Defaulting to Spark for every problem is the most common over-engineering mistake in data engineering.

- **"What's the difference between AWS Glue and EMR Serverless?"** — Both are serverless Spark on AWS. Glue abstracts Spark behind DynamicFrames with added services (Crawlers, visual editor, native connectors). EMR Serverless gives you native PySpark/Spark SQL with full DataFrame control and is ~30–60% cheaper per compute unit. Use Glue when you want catalog integration and a simpler managed experience; use EMR Serverless when you need PySpark control or cost efficiency.

- **"How would you decide between Flink and Spark Structured Streaming?"** — Latency requirement is the primary signal. Spark Structured Streaming is practical down to ~1-minute trigger intervals; below that, Flink is the right tool. For stateful operations (complex joins, long windows), Flink's RocksDB state backend (a key-value store used to keep track of streaming state across events) is more efficient. For teams already running Spark batch, Structured Streaming reuses the same codebase and Delta/Iceberg integration — the barrier to entry is lower than standing up a Flink cluster.

---

## Further Reading

- [Choosing Your Data Platform](../choosing-your-data-platform/README.md) — EMR Serverless is the compute; this chapter decides what format and storage layer sits underneath it
- [Spark Performance Playbook](../../../spark-performance-playbook/) — tuning the Spark jobs once you have chosen the compute platform
- [FinOps: Cost Optimization](../../finops/cost-optimization/README.md) — Reserved Instance strategy, Spot usage, right-sizing executors
