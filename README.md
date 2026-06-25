<div align="center">

<h1>Data Engineering Playbook</h1>

<p><em>Book-chapter-depth notes on the systems, patterns, and tradeoffs behind production data platforms.</em></p>

<p>
  <img src="https://img.shields.io/badge/Chapters-62-0A66C2?style=flat-square" alt="Chapters"/>
  <img src="https://img.shields.io/badge/Spark-Internals%20%7C%20Tuning-E25A1C?style=flat-square&logo=apachespark&logoColor=white" alt="Spark"/>
  <img src="https://img.shields.io/badge/Kafka-Streaming-231F20?style=flat-square&logo=apachekafka&logoColor=white" alt="Kafka"/>
  <img src="https://img.shields.io/badge/Lakehouse-Iceberg%20%7C%20Delta%20%7C%20Hudi-22C55E?style=flat-square" alt="Lakehouse"/>
  <img src="https://img.shields.io/badge/GenAI%20for%20Data-Intelligent%20Platform-8B5CF6?style=flat-square" alt="GenAI"/>
</p>

</div>

---

## What This Is

This is the onboarding resource I wish had existed — the reference I write for engineers joining a data platform team, and the depth I want in front of me before an architecture review. Every chapter is grounded in production: real failure modes, real tradeoffs, real code, and the decision matrices that tell you *when* to reach for each pattern, not just *how*.

Each chapter follows the same shape: a **TL;DR** you can read in 60 seconds, a **production scenario** that motivates the topic, **how it works** with diagrams, **code** you can adapt, a **decision guide**, **anti-patterns** that will burn you, and **interview / architecture-review talking points**.

**New here? Three good entry points:**

- **Building or modernizing a platform?** → [Choosing Your Data Platform](platform-engineering/choosing-your-data-platform/README.md) → [Pipeline Compute Options](platform-engineering/pipeline-compute-options/README.md) → [The Intelligent Data Platform](platform-engineering/intelligent-data-platform/README.md)
- **Making pipelines reliable?** → [Idempotency](pipeline-patterns/idempotency/README.md) → [Merge Strategies](pipeline-patterns/merge-strategies/README.md) → [Monitoring](observability/monitoring/README.md)
- **Tuning Spark?** → [Partitioning](spark-internals/partitioning/README.md) → [Skew Handling](spark-internals/skew-handling/README.md) → [AQE](spark-internals/aqe/README.md)

---

## Table of Contents

### 🏗️ Platform Engineering
*How to build a platform that scales with the org, not just the data.*

| Chapter | What it covers |
|---|---|
| [Choosing Your Data Platform](platform-engineering/choosing-your-data-platform/README.md) | Warehouse vs lake vs lakehouse; when each is the right call, and the migration traps |
| [Pipeline Compute Options](platform-engineering/pipeline-compute-options/README.md) | EMR Serverless, EMR, Glue, Databricks, Flink — cost models and when to use (or avoid) Spark |
| [Self-Service Data Platforms](platform-engineering/self-service-platforms/README.md) | The API-surface discipline that lets many teams ship without a ticket queue |
| [Golden Paths](platform-engineering/golden-paths/README.md) | Paving the road so the right thing is the easy thing |
| [Developer Experience](platform-engineering/developer-experience/README.md) | Time-to-first-pipeline as the metric that matters |
| [The Intelligent Data Platform](platform-engineering/intelligent-data-platform/README.md) | The metadata substrate, GenAI surfaces, and agentic data engineering — and how to migrate a lake into one |
| [Feature Stores](platform-engineering/feature-stores/README.md) | Online vs offline store, point-in-time correctness, training-serving skew, the DE's role in ML platforms |

### 🔄 Orchestration
*Scheduling and coordinating data pipeline execution.*

| Chapter | What it covers |
|---|---|
| [Airflow Architecture & Patterns](orchestration/airflow/README.md) | DAGs, executor types, sensors, backfill, SLA monitoring, and the pitfalls that take down production |

### 🔧 Transformation
*Turning raw data into business-ready form.*

| Chapter | What it covers |
|---|---|
| [dbt for Data Engineers](transformation/dbt/README.md) | Models, tests, incremental builds, the staging→mart pattern, semantic layer, and CI/CD integration |

### 🏗️ Infrastructure & Deployment
*Provisioning, deploying, and operating data infrastructure reliably.*

| Chapter | What it covers |
|---|---|
| [Kubernetes for Data Engineering](infrastructure/kubernetes/README.md) | Running Spark and Airflow on K8s, pod sizing, resource quotas, debugging, vs managed services |
| [Terraform for Data Infrastructure](infrastructure/terraform/README.md) | IaC for S3, EMR, Glue, Kafka, Redshift — versioned, repeatable data stack provisioning |
| [CI/CD for Data Pipelines](infrastructure/ci-cd/README.md) | Testing pyramid, automated deployment, rollback strategies, schema checks in CI |

### 🔒 Governance & Security
*Keeping data safe, trustworthy, and compliant.*

| Chapter | What it covers |
|---|---|
| [Data Security & Governance](governance/data-security/README.md) | PII handling, column masking, RBAC, GDPR/CCPA right-to-erasure, audit logging |
| [Schema Evolution Patterns](governance/schema-evolution/README.md) | Backward/forward compatibility, Avro vs Protobuf, Schema Registry, safe vs breaking changes |

### 🧪 Testing
*Catching data bugs before the business does.*

| Chapter | What it covers |
|---|---|
| [Testing Data Pipelines](testing/pipeline-testing/README.md) | Unit, integration, contract, and end-to-end testing for Spark, dbt, and Kafka pipelines |

### ⚙️ Pipeline Patterns
*The patterns that decide whether a re-run heals or corrupts your data.*

| Chapter | What it covers |
|---|---|
| [Idempotency in Data Pipelines](pipeline-patterns/idempotency/README.md) | Safe re-runs across RDBMS, Hive, Delta, and Iceberg |
| [Merge Strategies](pipeline-patterns/merge-strategies/README.md) | Truncate-load, upsert, SCD2, full sync with hard delete |
| [Data Landing Table Patterns](pipeline-patterns/landing-table-patterns/README.md) | Event log, snapshot, current-state, CDC changelog, accumulating snapshot |

### 🔥 Spark Internals
*Why your job is slow, and what the engine is actually doing.*

| Chapter | What it covers |
|---|---|
| [Partitioning in Spark](spark-internals/partitioning/README.md) | Partition sizing, the small-file problem, repartition vs coalesce |
| [Data Skew Handling](spark-internals/skew-handling/README.md) | Salting, AQE skew join, isolating hot keys |
| [Adaptive Query Execution (AQE)](spark-internals/aqe/README.md) | Runtime re-optimization: coalescing, skew joins, dynamic join selection |
| [The Catalyst Optimizer](spark-internals/catalyst/README.md) | Logical → physical plan, predicate pushdown, how to read a plan |
| [Broadcast Join](spark-internals/broadcast-join/README.md) | When to broadcast, thresholds, and the OOM failure mode |
| [Project Tungsten](spark-internals/tungsten/README.md) | Binary memory, whole-stage codegen, the CPU-bound era of Spark |
| [Spark Structured Streaming](spark-internals/structured-streaming/README.md) | Micro-batch streaming on Kafka and file sources, watermarking, stateful aggregations, checkpointing, exactly-once |

### 🌊 Kafka & Streaming
*Getting data off the bus correctly, at latency.*

| Chapter | What it covers |
|---|---|
| [Kafka Ingestion Patterns](kafka/ingestion-patterns/README.md) | Batch → micro-batch → near-real-time → real-time, with the tech-stack tradeoffs |
| [Exactly-Once Semantics](kafka/exactly-once/README.md) | Idempotent producers, transactions, and what "exactly once" really means |
| [Consumer Groups](kafka/consumer-groups/README.md) | Partition assignment, rebalancing, parallelism |
| [Offsets](kafka/offsets/README.md) | Commit strategies and the at-least-once / at-most-once tradeoff |
| [Event Design](kafka/event-design/README.md) | Schema, keys, versioning, and contracts on the wire |
| [Dead Letter Queues](kafka/dlq/README.md) | Quarantining bad records without crashing the stream |

### ⚡ Real-Time Streaming
*Low-latency event processing at scale.*

| Chapter | What it covers |
|---|---|
| [Apache Flink for Data Engineers](streaming/flink/README.md) | True streaming vs micro-batch, stateful processing, watermarks, RocksDB state, Kafka→Flink→Iceberg patterns |

### 🧊 Lakehouse
*Open table formats and the metadata that makes them work.*

| Chapter | What it covers |
|---|---|
| [Apache Iceberg](lakehouse/iceberg/README.md) | Snapshots, hidden partitioning, partition evolution, multi-engine |
| [Delta Lake](lakehouse/delta/README.md) | Transaction log, MERGE, Z-ordering, ACID on S3 |
| [Apache Hudi](lakehouse/hudi/README.md) | Record-level upserts and incremental pipelines on the lake |
| [Metadata Layers](lakehouse/metadata-layers/README.md) | Catalogs, commits, and the engine contract |

### 📐 Data Modeling
*Shaping data so the questions are cheap to answer.*

| Chapter | What it covers |
|---|---|
| [Star Schema](data-modeling/star-schema/README.md) | Facts, dimensions, grain — the workhorse of analytics |
| [Snowflake Schema](data-modeling/snowflake-schema/README.md) | Normalized dimensions and when the joins are worth it |
| [Slowly Changing Dimensions](data-modeling/scd-types/README.md) | SCD Types 1–6, with the history-tracking tradeoffs |
| [Data Vault 2.0](data-modeling/data-vault/README.md) | Hubs, links, satellites — auditability at enterprise scale |
| [Customer 360](data-modeling/customer-360/README.md) | Identity resolution and unifying behavioral + CRM + transactional data |

### ✅ Data Quality
*Catching bad data before the business does.*

| Chapter | What it covers |
|---|---|
| [Accuracy](data-quality/accuracy/README.md) | Correctness checks, contracts, and validation gates |
| [Completeness](data-quality/completeness/README.md) | Null-rate, row-count, and coverage checks |
| [Freshness](data-quality/freshness/README.md) | Freshness SLAs and the staleness detection that backs them |
| [Reconciliation](data-quality/reconciliation/README.md) | Source-to-target reconciliation and drift detection |

### 🔭 Observability
*Knowing what your platform is doing.*

| Chapter | What it covers |
|---|---|
| [Monitoring Data Pipelines](observability/monitoring/README.md) | What to alert on, and how to avoid alert fatigue |
| [Data Lineage](observability/lineage/README.md) | OpenLineage, column-level lineage, and impact analysis |
| [Metrics](observability/metrics/README.md) | The signals that describe platform health |
| [Logging](observability/logging/README.md) | Structured logging for distributed pipelines |

### 💰 FinOps
*Treating cloud cost as an engineering responsibility.*

| Chapter | What it covers |
|---|---|
| [Cost Optimization](finops/cost-optimization/README.md) | Right-sizing, shuffle reduction, compaction economics |
| [Cost Attribution](finops/cost-attribution/README.md) | Per-job and per-partition cost, attributed to a team and an outcome |
| [Capacity Planning](finops/capacity-planning/README.md) | Sizing for peak vs average, and the headroom question |

### 🧩 Distributed Systems
*The fundamentals under every data system.*

| Chapter | What it covers |
|---|---|
| [Database Types](distributed-systems/database-types/README.md) | RDBMS, columnar, document, KV, wide-column, graph, time-series, search, NewSQL, vector |
| [CAP Theorem](distributed-systems/cap-theorem/README.md) | What it actually constrains — and what it doesn't |
| [Consistency Models](distributed-systems/consistency-models/README.md) | Strong, eventual, causal — and what your store really gives you |
| [Consensus](distributed-systems/consensus/README.md) | Paxos/Raft intuition and where it shows up in data infra |
| [Event-Driven Systems](distributed-systems/event-driven-systems/README.md) | Choreography vs orchestration, idempotency, ordering |

### 🧭 Engineering Leadership
*The work that scales beyond the pipelines you personally write.*

| Chapter | What it covers |
|---|---|
| [Technical Leadership](engineering-leadership/leadership/README.md) | Force-multiplying through standards, not just shipping |
| [Technical Strategy](engineering-leadership/technical-strategy/README.md) | Setting multi-quarter technical direction |
| [Technical Roadmaps](engineering-leadership/roadmaps/README.md) | Sequencing platform work against business need |
| [Architecture Reviews](engineering-leadership/architecture-reviews/README.md) | Structured reviews that produce decisions, not just discussion |
| [Architecture Decision Records](engineering-leadership/decision-records/README.md) | Documenting *why*, so the next engineer doesn't re-litigate it |

---

## How to Use This

- **As a reference** — jump to the chapter for the problem in front of you; each is self-contained.
- **As an onboarding path** — follow the entry points above in order.
- **As interview prep** — every chapter ends with talking points calibrated to senior/staff data-engineering interviews.

Chapters cross-link heavily — idempotency ↔ merge strategies ↔ landing patterns ↔ Kafka ingestion ↔ the intelligent-platform substrate — so following the links is often the fastest way to build a complete mental model.

---

<div align="center">
<sub>Part of a broader data-engineering portfolio · <a href="https://github.com/sharath-dataengineer">github.com/sharath-dataengineer</a></sub>
</div>
