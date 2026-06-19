# Data Engineering Playbook

> Public knowledge base written at **book-chapter depth** for senior and staff-level Data Engineers.

## Sections

- **Data Modeling** — Star/snowflake schema, SCD, Data Vault, Customer 360
- **Distributed Systems** — CAP, consistency, consensus, events
- **Spark Internals** — Catalyst, Tungsten, AQE, skew, partitioning
- **Kafka** — EOS, consumer groups, offsets, DLQ, event design
- **Lakehouse** — Iceberg, Delta, Hudi, metadata layers
- **Data Quality** — Freshness, completeness, accuracy, reconciliation
- **Observability** — Monitoring, logging, metrics, lineage
- **FinOps** — Cost optimization, capacity, attribution
- **Platform Engineering** — Golden paths, DX, self-service
- **Engineering Leadership** — Architecture reviews, strategy, roadmaps, ADRs, mentorship

Browse each topic folder for standalone chapters.

## Why This Exists

This playbook is the knowledge I wish I'd had earlier — written for engineers who want to understand *why*, not just *how*. It is drawn from 11+ years building and operating production data platforms, most recently at Intuit spanning CRM, clickstream, contact-center, and billing/analytics domains.

The chapters lean on lessons that only show up at scale:

- **Spark Internals & Data Quality** — from operating **60+ production Spark pipelines** and delivering a **40% runtime improvement** and **10–20× query speedups** through architecture-level intervention, not just config tuning.
- **FinOps** — from cutting **$50K+/year** in cloud spend via compaction economics, EMR right-sizing, and cost attribution by domain.
- **Observability, Lineage & Engineering Leadership** — from incident postmortems, architecture reviews, and mentoring engineers from IC to senior level.

Examples and numbers are representative of real workloads, generalized to protect confidentiality.
