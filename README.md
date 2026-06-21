<div align="center">

<h1>Sharath Rama</h1>

<h3>Data Engineer &nbsp;|&nbsp; Platform Architecture &nbsp;|&nbsp; Lakehouse &nbsp;|&nbsp; Streaming &nbsp;|&nbsp; GenAI for Data</h3>

<p><em>I build data platforms that operate themselves — not just pipelines that run on schedules.</em></p>

<p>
  <img src="https://img.shields.io/badge/Experience-11%2B%20Years-0A66C2?style=flat-square" alt="Experience"/>
  <img src="https://img.shields.io/badge/Spark%20Pipelines-150%2B%20Production-E25A1C?style=flat-square&logo=apachespark&logoColor=white" alt="Spark"/>
  <img src="https://img.shields.io/badge/Pipeline%20Runtime%20Improvement-40%25-22C55E?style=flat-square" alt="ETL"/>
  <img src="https://img.shields.io/badge/Cloud%20Cost%20Savings-%2450K%2B%2Fyear-F59E0B?style=flat-square" alt="Cost"/>
  <img src="https://img.shields.io/badge/Incident%20Resolution-70%25%20Faster-8B5CF6?style=flat-square" alt="Incident"/>
</p>

</div>

---

## Professional Summary

Data Engineer with 11+ years architecting 60+ terabyte lakehouse platforms on AWS and Apache Spark — across financial services, insurance, telecoms, and financial-software SaaS. I set technical direction, define org-wide engineering standards, and drive adoption across data engineering teams. I have built and owned 150+ production Spark pipelines, architected CDC-aware medallion lakehouses, delivered real-time Kafka pipelines serving 50+ stakeholders at sub-5-minute latency, and built an LLM-powered observability tool that cut incident resolution time by 70%. Track record of 30%+ cost takeouts and 30–40% performance gains from architecture-level intervention — not just code tuning.

---

## Technical Expertise

| Domain | Technologies & Patterns |
|---|---|
| **Distributed Processing** | Apache Spark (Structured Streaming, Spark SQL, RDD), Kafka, Apache Flink |
| **Lakehouse & Storage** | Apache Iceberg, Delta Lake, Amazon Redshift, Amazon S3, Parquet, ORC |
| **Orchestration** | Apache Airflow, Kubernetes (K8s), Amazon EMR |
| **Cloud Infrastructure** | AWS (EMR, Redshift, S3, Glue, Lambda, IAM, VPC), Terraform |
| **Data Modeling** | Dimensional modeling, medallion architecture, wide tables |
| **Data Quality & Governance** | Data contracts, schema evolution, lineage tracking, OpenMetadata, SLAs |
| **GenAI for Data Engineering** | LLM-powered metadata assistants, NL-to-SQL, automated RCA, drift detection |
| **Languages** | Python, SQL, Bash |
| **Developer Tooling** | dbt, Great Expectations, Pytest, Docker |

---

## Stack Notes — What I've Operated vs. What I've Studied

The production systems in this portfolio ran on **Spark on EMR, HOCON-configured declarative pipelines, Parquet/Hive-partitioned S3, and Airflow** — the enterprise default at Intuit at the time. That is the stack I have real incident war stories for, real performance numbers from, and real architectural opinions about.

The **Iceberg** work here represents genuine operational knowledge (the health monitor, the compaction economics work, the small-file and manifest-bloat patterns) but I was not running Iceberg as the primary production table format — that migration was designed and in progress. The `iceberg-health-monitor` tool was built specifically because operating Iceberg at scale surfaces gaps that the documentation does not cover.

**dbt** — I have built with it for smaller-scale marts and know the ecosystem well. It was not the primary transformation layer in the large-scale production system documented here, where the config-driven Spark framework served that role. I would reach for dbt on a fresh build today.

**Kubernetes-native Spark** — I understand the architecture (Spark Operator, node selectors, pod templating), have worked with K8s for other services, and the autonomous-data-platform ADRs reference it. Production Spark in the case studies ran on EMR. I have not run Spark-on-K8s in production at the scale described here.

**Apache Flink** — listed in the Technical Expertise table because I understand the architecture (stateful streaming, exactly-once checkpointing, watermark semantics) and have evaluated it for streaming use cases. I have not operated a Flink cluster in production. The streaming work I've done ran on Spark Structured Streaming and Kafka. Flink is on the learning roadmap, not the war stories list.

**Delta Lake** — I have worked with Delta Lake for schema evolution and ACID writes on S3 across multiple roles. The production systems documented here used Parquet with Hive metastore partitioning rather than Delta — the org was already invested in that stack before I joined. I understand the Delta protocol (transaction log, checkpointing, Z-ordering) and would evaluate Delta or Iceberg depending on the compute engine and catalog ecosystem.

**Databricks** — I understand the platform deeply: Delta Live Tables, Unity Catalog, Databricks Runtime optimizations, cluster policies. The production systems here ran on EMR. Given a greenfield today, Databricks would be a serious candidate depending on team size, catalog requirements, and the org's cloud investment.

**Monte Carlo** — I have evaluated it for data observability and understand the platform (freshness/volume monitors, field-level lineage, circuit breakers). The observability in this portfolio was built custom — the [pipeline-health-monitor](https://github.com/sharath-dataengineer/pipeline-health-monitor) and the LLM-augmented DQ system — because the org's requirements and scale made custom tooling the right call. Monte Carlo would be my first consideration for a team that wants production-grade observability without building it.

I write this here because hiring managers can tell when a resume is padded with "familiar with" versions of every trending technology. The stack I have operated at scale is the stack worth interrogating me on.

---

## Architecture Expertise

I split this into two tiers — what I have owned in production with real incidents and real metrics, and what I can design and defend in an architecture review but have not operated at scale.

### Built and Operated in Production

| Architecture | What I Have Actually Owned |
|---|---|
| **Medallion Lakehouse (CDC/SCD2)** | Bronze/Silver/Gold on S3 + EMR Serverless; SCD2 star-schema marts; advanced partitioning and merge strategies — 40% faster runtimes, 30% cost reduction at Intuit scale |
| **Declarative Config-Driven Pipelines** | 74 logical Spark pipelines as HOCON config; zero bespoke Spark code per table; multi-region/multi-environment parameterization — see [enterprise-data-foundation](https://github.com/sharath-dataengineer/enterprise-data-foundation) |
| **Real-Time Kafka Ingestion** | Streaming pipelines processing millions of daily transactions at peak load; sub-5-minute latency; 50+ stakeholders across product, analytics, and leadership |
| **Multi-System Funnel Attribution** | 7-day first-touch attribution across 5 source systems with no shared key — contact center → CRM → sales booking |
| **Data Quality as a Gate** | Golden-check DQ gates blocking publish (not just alerting); 50% reduction in data incidents; test coverage lifted from ~30% to >90% of active pipelines |
| **LLM-Augmented Observability** | Event-driven failure classification with LLM-generated root cause advisory — 70% faster incident resolution, eliminating manual log triage |
| **LLM-Augmented DQ** | Auto-calibrated statistical thresholds (mean ± 2σ from 30-day partition history); LLM-generated anomaly explanations with upstream context; 60% DQ triage time reduction, 50% incident reduction — documented in [pipeline-health-monitor](https://github.com/sharath-dataengineer/pipeline-health-monitor) |
| **FinOps & Cost Attribution** | EMR FinOps dashboards with per-job and per-partition cost attribution; $50K+ annual cloud savings |

### Design-Level Expertise — Can Architect and Defend, Not Production-Owned at This Scale

| Architecture | Depth |
|---|---|
| **Data Mesh** | Domain ownership model, federated governance, data product contracts, self-serve infrastructure — documented in playbook, not operated |
| **ML Platform / Feature Store** | Feature store design, training/serving skew prevention, feature freshness SLAs — design knowledge |
| **Customer 360** | Identity graph design, behavioral + CRM + transactional unification — adjacent to production work; not owned end to end |
| **Security** | Column-level encryption, row-level access policies, audit logging, VPC-isolated compute — implemented at component level, not platform-wide ownership |

---

## Platform Engineering Expertise

A data platform is not a collection of pipelines — it is a product with internal customers, SLAs, and a developer experience that either accelerates or blocks the engineering organization.

**Golden Paths:** I design self-service golden paths that let domain teams onboard data products without requiring platform team involvement for every deployment. This means standardized templates, automated validation, and contract-based interfaces.

**Observability by Default:** Every pipeline I design emits freshness, volume, schema, and lineage metadata from day one. Observability is not a layer added afterward — it is the scaffolding the pipeline is built on.

**Cost Attribution:** Cloud cost is not a platform concern — it is an organizational one. I build FinOps-aware platforms where every compute dollar is attributed to a domain, a team, and a business outcome.

**Architecture Reviews:** I run structured architecture reviews that produce Architectural Decision Records (ADRs), not just verbal sign-offs. Decisions are documented, dated, and linked to the systems they govern.

**Developer Experience:** I measure platform success by time-to-first-pipeline for new data engineers, not by the number of jobs running. Friction in the platform is a bug, not a feature request.

### Where This Shows Up in Practice

The config-driven pipeline framework in [`enterprise-data-foundation`](https://github.com/sharath-dataengineer/enterprise-data-foundation) is the clearest example: a single framework definition powers 70+ production jobs. A new domain table is a new config file and a new DQ spec — no bespoke Spark code, no platform team involvement.

Architecture reviews I run produce ADRs, not verbal agreements. Every design decision for the ERC Data Foundation is documented with context, decision, and consequences — so engineers who join two years later understand *why* the merge key is what it is, not just *what* it is. This directly reduces the time I spend re-explaining old decisions and increases the team's ability to evolve the system confidently.

The `pipeline-health-monitor` LightGBM risk predictor was a design I handed off to a junior engineer to implement after writing the feature extraction spec and the baseline management design. The handoff artifact — a design doc with the feature table, the training approach, and the policy tier thresholds — was detailed enough that the implementation proceeded without daily check-ins. That is the kind of documentation I try to write by default.

---

## Impact Metrics

| Metric | Result | Context |
|---|---|---|
| Production Spark pipelines owned | **150+** | Across financial, CRM, clickstream, contact center, and billing domains |
| Pipeline failure reduction | **−40%** | Defined and drove org-wide adoption of partitioning, merge, and SCD2 standards |
| Pipeline runtime improvement | **40% (most recent role)** | CDC-aware medallion lakehouse + Spark execution tuning + partition strategy on EMR Serverless |
| Compute cost reduction | **30%** | EMR Serverless right-sizing, shuffle reduction, partition strategy |
| Annual cloud savings | **$50K+** | FinOps dashboards with per-job cost attribution enabling targeted spend reduction |
| Incident resolution speed | **70% faster** | LLM-powered observability tool eliminating manual log triage |
| Data incident reduction | **−50%** | SLA-driven DQ gates across batch and streaming layers |
| DQ triage time | **−60%** | LLM-generated anomaly explanations + auto-calibrated statistical thresholds |
| Pipeline test coverage | **30% → 90%+** | Test-driven pipeline design rolled out org-wide |
| Stakeholders served | **50+** | Near-real-time Kafka dashboards across product, analytics, and leadership |
| Engineering scope | Architecture reviews, org-wide standards, mentorship | Standards now adopted across multiple data engineering teams |

---

## Production Work

Three repositories documenting systems I built and operated at Intuit. Real incidents, real metrics, real architectural decisions.

<table>
<tr>
<td width="50%" valign="top">

### [enterprise-data-foundation](https://github.com/sharath-dataengineer/enterprise-data-foundation)

![Spark](https://img.shields.io/badge/Spark%20on%20EMR-Config--Driven-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Attribution](https://img.shields.io/badge/7--Day%20Attribution-Funnel%20Analytics-0A66C2?style=flat-square)
![DQ](https://img.shields.io/badge/DQ%20Gate%20Pattern-50%25%20Incident%20Reduction-22C55E?style=flat-square)

A config-driven Spark-on-EMR analytics foundation stitching contact-center, CRM, clickstream, and sales-booking data into a governed recommendation→revenue funnel. 74 logical pipelines as HOCON config (zero bespoke Spark code per table), 7-day first-touch attribution across 5 source systems with no shared key, DQ gates that block publishing to executive dashboards. Forked across 5–6 environments × 11 regional variants.

</td>
<td width="50%" valign="top">

### [pipeline-health-monitor](https://github.com/sharath-dataengineer/pipeline-health-monitor)

![SparkListener](https://img.shields.io/badge/SparkListener-Event--Driven-E25A1C?style=flat-square)
![LightGBM](https://img.shields.io/badge/LightGBM-Risk%20Prediction-F59E0B?style=flat-square)
![LLM](https://img.shields.io/badge/Claude%20via%20Bedrock-LLM%20Advisory-8B5CF6?style=flat-square)

An event-driven pipeline health monitor with two paths: immediate failure alerting, and an AI path extracting 8 features from completed stages to predict failure risk before it happens. LightGBM risk predictor → policy tier engine → Claude-powered root cause advisory for P1/P2 alerts. Adaptive EMA baseline prevents false positives on anomalous runs. Result: **70% faster incident resolution**, manual log triage eliminated.

</td>
</tr>
<tr>
<td colspan="2" valign="top">

### [kafka-streaming-case-study](https://github.com/sharath-dataengineer/kafka-streaming-case-study)

![Kafka](https://img.shields.io/badge/Apache%20Kafka-Peak%20Load-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Streaming](https://img.shields.io/badge/Spark%20Structured%20Streaming-Sub--5min%20Latency-E25A1C?style=flat-square)
![DQ](https://img.shields.io/badge/Inline%20DQ-50%25%20Incident%20Reduction-22C55E?style=flat-square)

Real-time Kafka streaming pipelines processing millions of transactions per day at peak-season load — with sub-5-minute end-to-end latency serving 50+ stakeholders. The defining challenge: peak volume spikes 5–8× daily average, and the 5-minute SLA cannot slip when leadership is watching live dashboards during the highest-stakes window of the year. Covers partition sizing for peak, micro-batch tuning, exactly-once semantics via idempotent upsert, and inline DQ that routes bad rows to quarantine rather than crashing the stream.

</td>
</tr>
</table>

---

## Reference Architecture & Knowledge Base

Design thinking, architecture patterns, and depth knowledge — demonstrating how I approach problems I haven't yet been handed.

<table>
<tr>
<td width="50%" valign="top">

### [spark-performance-playbook](https://github.com/sharath-dataengineer/spark-performance-playbook)

![Spark](https://img.shields.io/badge/Apache%20Spark-Performance-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Benchmarks](https://img.shields.io/badge/Production%20Benchmarks-Included-F59E0B?style=flat-square)

A production-grounded Spark performance playbook covering shuffle tuning, join strategy selection, AQE configuration, memory management, partition sizing, and EMR cluster configuration. Every technique applied on real workloads: 40% faster pipelines and 30% compute cost reduction at peak Intuit scale; comparable gains at earlier roles across insurance, finance data, and legal/financial data platforms.

</td>
<td width="50%" valign="top">

### [autonomous-data-platform](https://github.com/sharath-dataengineer/autonomous-data-platform)

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Iceberg](https://img.shields.io/badge/Iceberg-0A66C2?style=flat-square)

Reference architecture for a self-operating data platform — extending the production monitoring patterns from `pipeline-health-monitor` into a fuller vision: schema drift detection, AI-powered RCA, cost attribution, and an LLM copilot. The monitoring core is derived from the production system at Intuit; the broader integrated platform is the architecture I would design given the mandate and a greenfield.

</td>
</tr>
<tr>
<td width="50%" valign="top">

### [data-engineering-playbook](https://github.com/sharath-dataengineer/data-engineering-playbook)

![Knowledge](https://img.shields.io/badge/Book--Chapter%20Depth-Deep%20Dives-E25A1C?style=flat-square)
![Topics](https://img.shields.io/badge/Spark%20%7C%20Kafka%20%7C%20Lakehouse-Topics%20Covered-F59E0B?style=flat-square)

Book-chapter-depth knowledge base: data modeling theory, Spark internals (shuffle, catalyst, memory), Kafka architecture, lakehouse design, distributed systems fundamentals, and FinOps. Written as the onboarding resource I wish existed — used for mentoring engineers on Spark internals, lakehouse design, and cloud-native patterns.

</td>
<td width="50%" valign="top">

### [iceberg-health-monitor](https://github.com/sharath-dataengineer/iceberg-health-monitor)

![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-Open%20Source-22C55E?style=flat-square)
![Health](https://img.shields.io/badge/Table%20Health%20Scoring-Optimization-E25A1C?style=flat-square)

Open-source Iceberg table health tooling: composite health score per table based on snapshot age, small file ratio, manifest bloat, compaction lag, and partition skew. Built because Iceberg tables degrade silently in production — the operational gap the documentation does not cover.

</td>
</tr>
<tr>
<td width="50%" valign="top">

### [data-platform-reference-architecture](https://github.com/sharath-dataengineer/data-platform-reference-architecture)

![Architecture](https://img.shields.io/badge/12%20Architecture%20Patterns-Documented-8B5CF6?style=flat-square)
![Patterns](https://img.shields.io/badge/Trade--off%20Analysis-ADR%20Format-0A66C2?style=flat-square)

Architecture decision guides for 12 platform patterns: Customer 360, Lakehouse, Streaming, CDC, Data Mesh, ML Platform, Feature Store, Real-Time Analytics, Governance, Observability, FinOps, and Security. Each pattern covers failure modes and implementation constraints — written as the reference I'd want before an architecture review, not a tutorial.

</td>
<td width="50%" valign="top">

### [metadata-copilot](https://github.com/sharath-dataengineer/metadata-copilot)

![LLM](https://img.shields.io/badge/LLM%20Powered-Metadata%20Assistant-8B5CF6?style=flat-square)
![NL2SQL](https://img.shields.io/badge/NL--to--SQL-Lineage%20Discovery-0A66C2?style=flat-square)

An LLM-powered metadata assistant: automated lineage discovery from code and query history, natural language to SQL with schema awareness, ownership resolution, freshness analysis, and impact analysis for schema changes. Represents the applied GenAI direction where I see the most real leverage for data platform operators.

</td>
</tr>
</table>

---

## Technology Stack

<p>
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/SQL-4479A1?style=flat-square&logo=postgresql&logoColor=white" alt="SQL"/>
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white" alt="Spark"/>
  <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white" alt="Kafka"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white" alt="Airflow"/>
  <img src="https://img.shields.io/badge/Apache%20Iceberg-0A66C2?style=flat-square" alt="Iceberg"/>
  <img src="https://img.shields.io/badge/Delta%20Lake-003366?style=flat-square" alt="Delta Lake"/>
  <img src="https://img.shields.io/badge/Amazon%20EMR-FF9900?style=flat-square&logo=amazonaws&logoColor=white" alt="EMR"/>
  <img src="https://img.shields.io/badge/Amazon%20Redshift-8C4FFF?style=flat-square&logo=amazonredshift&logoColor=white" alt="Redshift"/>
  <img src="https://img.shields.io/badge/Amazon%20S3-569A31?style=flat-square&logo=amazons3&logoColor=white" alt="S3"/>
  <img src="https://img.shields.io/badge/Kubernetes-326CE5?style=flat-square&logo=kubernetes&logoColor=white" alt="Kubernetes"/>
  <img src="https://img.shields.io/badge/Terraform-7B42BC?style=flat-square&logo=terraform&logoColor=white" alt="Terraform"/>
  <img src="https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white" alt="dbt"/>
  <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white" alt="Docker"/>
</p>

**Data Architecture Patterns:**
<p>
  <img src="https://img.shields.io/badge/Lakehouse%20Architecture-Medallion%20%7C%20Data%20Vault-0A66C2?style=flat-square" alt="Lakehouse"/>
  <img src="https://img.shields.io/badge/Streaming-Exactly%20Once%20%7C%20CDC-E25A1C?style=flat-square" alt="Streaming"/>
  <img src="https://img.shields.io/badge/Data%20Mesh-Federated%20Governance-8B5CF6?style=flat-square" alt="Data Mesh"/>
  <img src="https://img.shields.io/badge/Customer%20360-Identity%20Resolution-22C55E?style=flat-square" alt="Customer 360"/>
  <img src="https://img.shields.io/badge/GenAI%20for%20Data-LLM%20%7C%20NL--to--SQL%20%7C%20RCA-F59E0B?style=flat-square" alt="GenAI"/>
</p>

---

## Engineering Leadership

The work that had the highest organizational leverage was not the pipelines I built — it was the standards I defined that other teams adopted, and the engineers I worked with who became more capable because of shared patterns.

### Org-Wide Data Engineering Standards — 40% Pipeline Failure Reduction

At Intuit, each data engineering team was solving the same problems independently: how to partition a table, how to handle SCD2 merges, how to structure incremental loads, how to write DQ checks. The result was fragmentation: teams with different merge strategies, different partitioning patterns, and different DQ approaches — all producing pipelines with different failure modes.

I defined and drove adoption of org-wide standards covering:

- **Partitioning strategy** — standard partition key selection guidelines for batch and streaming layers, with documented tradeoffs between query performance and write efficiency
- **Merge and SCD2 patterns** — a reference implementation for CDC-style merge/upsert (idempotent, business-key-keyed, dynamic partition overwrite) that became the default pattern for new pipelines
- **Incremental load design** — the watermark-based incremental pattern (self-watermarking on `updated_ts`) documented as a reusable pattern, not just used in one mart
- **DQ gate standards** — the golden-check pattern (dup/null/reconciliation, blocking on failure) standardized as the minimum bar for any pipeline feeding a downstream consumer

**Outcome:** 40% reduction in pipeline failures across the teams that adopted the standards. The consistency patterns are now used across multiple data engineering teams. New pipelines built to the standard fail in predictable, diagnosable ways — not in unique, one-off modes that require tribal knowledge to debug.

This is the difference between being a strong individual contributor and being a force multiplier. The enterprise-data-foundation was the reference implementation. The standards are what scaled it.

### Mentorship and Technical Enablement

I wrote the `data-engineering-playbook` as the onboarding resource I wished had existed — covering Spark internals, lakehouse design, Kafka architecture, and data modeling theory at book-chapter depth. The goal was to make knowledge that lived in my head transferable to engineers on the team.

Concrete outcomes:
- Three engineers with no prior Spark experience were deploying production pipelines within their first month using the config-driven framework and documentation
- Engineers mentored on Spark internals (shuffle, catalyst, memory management) and lakehouse design — topics that typically take 12–18 months of production experience to develop intuition for
- The LightGBM risk predictor in the pipeline health monitor was designed by me and implemented by a junior engineer working from the feature spec and baseline management design doc — no daily check-ins required

---

## Engineering Principles

These are not aspirational values. They are constraints I design systems against and positions I defend in architecture reviews.

**1. Observability is not a feature — it is the load-bearing wall.**
A pipeline without freshness and volume signals is not a pipeline. It is a batch job with no on-call. Every system I design emits operational metadata before it emits business data. If you cannot answer "is this data correct, complete, and fresh?" without querying the data itself, the platform is not done.

**2. Schema evolution is a first-class problem, not a migration script.**
Schema changes break downstream consumers in ways that don't surface until 3 AM. I design for schema evolution from the start: format-level schema evolution (Iceberg, Avro), contract-based interfaces between producers and consumers, and automated drift detection. The platform should catch schema drift before the business does.

**3. Cost is an engineering responsibility, not a finance report.**
Cloud costs that cannot be attributed to a team, a pipeline, and a business outcome are unmanaged risk. I build FinOps instrumentation into platforms so cost is visible at the point of architectural decision — not discovered in a monthly invoice. Every join strategy, compaction schedule, and cluster configuration has an economic consequence.

**4. High-leverage engineers are force multipliers, not just strong individual contributors.**
The highest-leverage work I do is not writing the most performant Spark job — it is designing the system that makes the next 10 engineers more productive. That means golden paths, documented decisions, reusable patterns, and mentorship that transfers architectural thinking, not just syntax.

**5. Data quality failures are architecture failures.**
Bad data downstream is almost always caused by a missing constraint upstream. I treat data quality as an architectural concern: schema enforcement at ingestion, data contracts between producers and consumers, automated quality gates in CI/CD, and quality SLAs with the same rigor as latency SLAs. Retrofitting quality into an existing system costs 10x what designing it in from the start.

---

## Current Focus Areas

**Autonomous Data Platform Operations**
The next frontier in data engineering is not faster pipelines — it is platforms that monitor, diagnose, and remediate themselves. I invest time in patterns for AI-assisted root cause analysis, automated anomaly response, and self-healing pipeline orchestration — extending the observability work in the production case studies toward fully autonomous platform operations.

**Iceberg at Production Scale**
Apache Iceberg is the right table format for the modern lakehouse, but operational excellence at scale requires tooling that does not yet exist as a standard product. The [`iceberg-health-monitor`](https://github.com/sharath-dataengineer/iceberg-health-monitor) in this portfolio addresses the operational gaps: compaction economics, manifest bloat, snapshot lifecycle management, and partition evolution at scale.

**LLM-Powered Data Infrastructure**
Metadata management, lineage discovery, and NL-to-SQL are problems where LLMs provide genuine leverage — not just a demo. The [`metadata-copilot`](https://github.com/sharath-dataengineer/metadata-copilot) and the LLM advisory patterns in the pipeline health monitor represent my applied work in this area — making data infrastructure more accessible to analysts and surfacing institutional knowledge that currently lives in Slack threads.

---

## Career Arc

11+ years across five industries and four platform generations. Each role tackled a harder problem than the last — from ETL framework reuse at a large telco, to CDC-aware lakehouses and LLM-augmented observability at Intuit.

| Period | Industry | Stack | Key Outcome |
|---|---|---|---|
| 2014–2019 | Telecoms | Informatica + SSIS + S3 | 30% dev effort reduction; on-prem → AWS S3 migration |
| 2019–2021 | Insurance | SQL + EDW + AWS | 40% incident turnaround reduction; regulatory EDW |
| 2021–2022 | Finance data | AWS Glue + EMR + Spark | 30% faster execution; 40% pipeline reusability gain |
| 2022–2023 | Legal & financial data | Redshift + EMR + Spark | 35% faster pipelines; near-real-time financial datasets |
| 2023–present | Financial-Software SaaS | Medallion lakehouse + Kafka + LLM tooling | 40% runtime reduction; 70% faster incident resolution |

### What Each Stage Built

**Telecoms — 2014–2019 (Virgin Media)**
Built reusable ETL frameworks in Informatica PowerCenter and SSIS, integrating billing, customer, and network domains across a large telco. The defining project: migrating legacy Informatica workflows to AWS S3 — my first exposure to cloud-native data movement at scale. Designed relational database models and warehouse schemas supporting BI teams. Reduced development effort by 30% through framework reuse. This is where I developed discipline around parameterized, reusable ETL design — a principle that shows up in every subsequent role.

**Insurance — 2019–2021 (Hanover Insurance)**
Built and maintained scalable ETL pipelines integrating actuarial, claims, and finance source systems into an enterprise data warehouse. The work that defined this role: writing complex SQL logic to proactively detect and resolve production data discrepancies — before they surfaced in regulatory reports. Reduced incident turnaround time by 40%. This is where I developed the instinct for treating data quality as an operational concern, not a post-hoc cleanup job.

**Finance Data — 2021–2022 (Hewlett Packard Enterprise)**
Modernized legacy finance data ingestion processes with parameterized, reusable ETL components on AWS — Glue, EMR, Spark. Led data migration and integration efforts across finance systems with zero data loss. Achieved 30% faster execution and 40% improvement in pipeline reusability. This was my first large-scale Spark work and my first experience designing for high-volume loads with explicit partitioning strategies.

**Legal & Financial Data — 2022–2023 (Thomson Reuters)**
Architected a cloud-native data warehouse on AWS Redshift; built ETL pipelines using Glue, EMR, and Spark for financial and legal datasets previously running on nightly batch cycles. Delivered near-real-time reporting with 35% faster pipeline runtimes and 25% compute cost reduction.

**Financial-Software SaaS — 2023–present (Intuit)**
The most technically demanding role: CDC-aware medallion lakehouse, real-time Kafka pipelines, LLM-augmented observability, org-wide standards definition. The production case studies in this portfolio are from this role. Full narrative in [enterprise-data-foundation](https://github.com/sharath-dataengineer/enterprise-data-foundation), [kafka-streaming-case-study](https://github.com/sharath-dataengineer/kafka-streaming-case-study), and [pipeline-health-monitor](https://github.com/sharath-dataengineer/pipeline-health-monitor).

---

## Contact

For architecture discussions, technical collaboration, or Data Engineering opportunities:

- **Email:** sharathrama.de@gmail.com
- **LinkedIn:** [linkedin.com/in/sharathde](https://www.linkedin.com/in/sharathde)
- **GitHub:** Open an issue or start a discussion on any repository above.

---

<div align="center">

<sub>Data Engineer &nbsp;|&nbsp; 11+ years &nbsp;|&nbsp; Platform Architecture &nbsp;|&nbsp; Lakehouse &nbsp;|&nbsp; Streaming &nbsp;|&nbsp; GenAI for Data</sub>

</div>
