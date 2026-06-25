# dbt for Data Engineers

> Chapter from the Data Engineering Playbook — transformation

## About This Chapter

- **What this is.** A production-focused guide to dbt (data build tool) — what it does, how to structure your models, how to test and deploy them, and when to reach for each feature.
- **Who it's for.** Mid-level to senior data engineers who already know SQL and understand the basics of a data warehouse or lake, and want to use dbt with confidence in a production environment.
- **What you'll take away.**
  - A clear mental model of the staging → intermediate → mart layer pattern and why it pays off at scale.
  - Practical guidance on choosing materializations and writing incremental models for large tables.
  - Ready-to-use talking points for system design interviews and code reviews.

Picture this: your ingestion pipeline lands raw Salesforce, Stripe, and event-tracking data into your warehouse every hour. Analysts are writing ad hoc queries directly against those raw tables — with different column aliases, incompatible date formats, and no agreed-on definition of "active customer." Three dashboards show three different revenue numbers. dbt is the layer that fixes this. It gives your SQL a software engineering backbone: version control, automated testing, documented lineage, and a single place where "net revenue" means one thing to everyone.

---

## TL;DR

- dbt transforms data **already in your warehouse** — it does not move or load data. Think of it as a build system for SQL.
- You write plain SELECT statements. dbt compiles them into DDL (CREATE TABLE / CREATE VIEW) and runs them in the right order.
- The three-layer pattern — staging, intermediate, mart — keeps your SQL organized and maintainable as the codebase grows.
- `ref()` and `source()` are how dbt tracks dependencies and builds the lineage graph automatically.
- Materializations (table, view, incremental, ephemeral) control how dbt physically writes each model — picking the wrong one is a common performance mistake.
- Incremental models are essential for large tables; they process only new or changed rows instead of rebuilding everything from scratch.
- Built-in tests (`not_null`, `unique`, `accepted_values`, `relationships`) run in CI and catch data quality issues before they reach analysts.
- dbt Core is free and runs anywhere; dbt Cloud adds scheduling, a hosted IDE, and Semantic Layer serving for teams that need them.

---

## Why This Matters in Production

Suppose your company processes 50 million orders per day. Your `orders` table in the warehouse grows by hundreds of millions of rows per month. A naive transformation job that rebuilds the final aggregation table from scratch every run costs you 20 minutes and significant compute spend on every execution.

With dbt's incremental materialization, you process only the rows that arrived since the last successful run. A run that once took 20 minutes now takes 90 seconds. At the same time, your analytics engineering team and the DE team both work in the same dbt project, see each other's models in the lineage graph, and run the same tests in CI before any change ships to production.

That combination — performance at scale, shared ownership, and automated quality gates — is why dbt has become the standard transformation layer in modern data stacks.

---

## How It Works

### What dbt Is (and Is Not)

dbt is a transformation tool. Its job is to take raw data that has already been loaded into your warehouse or lakehouse and turn it into clean, tested, documented tables and views.

**dbt does not move data.** It does not replace your ingestion tool (Fivetran, Airbyte, Kafka, custom pipelines). Those tools get the data into the warehouse. dbt takes over from there.

When you run `dbt run`, dbt:
1. Reads your `.sql` model files (each one is a SELECT statement).
2. Resolves the dependency graph (which model depends on which).
3. Compiles each model into a `CREATE TABLE AS` or `CREATE VIEW AS` statement.
4. Runs those statements against your warehouse in dependency order.

```
Ingestion Layer         dbt Layer              Serving Layer
─────────────────  →    ─────────────────  →   ─────────────────
Fivetran, Airbyte        Staging models         BI tools
Kafka + Spark            Intermediate models    Analysts
Custom pipelines         Mart models            ML features
```

### Where dbt Fits in the Stack

dbt sits between ingestion and serving:

- **Before dbt:** raw tables land in a "raw" or "landing" schema. Column names are whatever the source system used. Types may be wrong. Nulls are everywhere.
- **dbt's job:** clean, rename, test, document, and join those raw tables into business-ready datasets.
- **After dbt:** analysts, dashboards, and ML pipelines query from dbt models — clean, tested, with agreed-on definitions.

### The Staging → Intermediate → Mart Pattern

This three-layer architecture is dbt's most important convention. It keeps large projects navigable and prevents logic from getting tangled.

#### Staging (`stg_`)

One model per source table. The goal is clean, raw-faithful data — not business logic.

What staging does:
- Renames columns to your team's conventions (`ACCT_NUM` → `account_id`)
- Casts types (`STRING` → `DATE`, `VARCHAR` → `NUMERIC`)
- Applies simple filters (exclude test accounts, deleted records)
- Standardizes null handling

What staging never does:
- **No joins.** Each staging model touches exactly one source table. If you join in staging, you create a dependency that makes the model harder to reuse and test.

```sql
-- models/staging/salesforce/stg_salesforce__accounts.sql
with source as (
    select * from {{ source('salesforce', 'accounts') }}
),

renamed as (
    select
        id                              as account_id,
        name                            as account_name,
        type                            as account_type,
        cast(created_date as date)      as created_date,
        isdeleted                       as is_deleted
    from source
    where isdeleted = false
)

select * from renamed
```

#### Intermediate (`int_`)

Complex business logic lives here. Intermediate models join staging models together, apply business rules, and create reusable building blocks for the mart layer.

Intermediate models are **not exposed to analysts or BI tools**. They are internal scaffolding.

```sql
-- models/intermediate/orders/int_orders__with_customer.sql
with orders as (
    select * from {{ ref('stg_stripe__orders') }}
),

customers as (
    select * from {{ ref('stg_salesforce__accounts') }}
),

joined as (
    select
        orders.order_id,
        orders.order_date,
        orders.gross_amount,
        customers.account_name,
        customers.account_type
    from orders
    left join customers
        on orders.account_id = customers.account_id
)

select * from joined
```

#### Mart (`fct_`, `dim_`)

Business-ready tables that analysts and BI tools query directly. Two types:

- **Fact tables (`fct_`):** Events and transactions. One row per event. Include measures (amounts, counts) and foreign keys to dimensions. Example: `fct_orders`, `fct_payments`.
- **Dimension tables (`dim_`):** Descriptive context. One row per entity. Example: `dim_customers`, `dim_products`.

```sql
-- models/marts/finance/fct_daily_revenue.sql
with orders as (
    select * from {{ ref('int_orders__with_customer') }}
),

daily as (
    select
        date_trunc('day', order_date)   as revenue_date,
        account_type,
        sum(gross_amount)               as gross_revenue,
        count(distinct order_id)        as order_count
    from orders
    group by 1, 2
)

select * from daily
```

### ref() and source()

These two functions are the backbone of dbt's lineage tracking.

**`ref('model_name')`** — use this when your model depends on another dbt model. dbt parses every `ref()` call in your project, builds a directed acyclic graph (DAG) of dependencies, and runs models in the correct order. It also automatically points `ref()` to the right schema (dev vs. prod) based on your target.

```sql
-- This tells dbt: "I depend on stg_stripe__orders. Run that first."
select * from {{ ref('stg_stripe__orders') }}
```

**`source('source_name', 'table_name')`** — use this when your model reads from a raw table that was loaded by your ingestion pipeline, not produced by dbt. Declaring sources in `sources.yml` enables source freshness checks: dbt can alert you if Fivetran hasn't updated the table in the last 6 hours.

```yaml
# models/staging/salesforce/sources.yml
sources:
  - name: salesforce
    database: raw
    schema: salesforce
    tables:
      - name: accounts
        loaded_at_field: _fivetran_synced
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 12, period: hour}
```

### Materializations

A materialization controls how dbt writes your model to the warehouse. Choosing the right one is a performance and cost decision.

#### `view` — No Data Stored

dbt creates a SQL view. No data is written. Every query against this model re-runs the underlying SQL at query time.

- Fast to build (just CREATE VIEW)
- Can be slow to query on large underlying data
- Good for: staging models, rarely-queried intermediate logic

#### `table` — Full Rebuild Every Run

dbt drops and recreates the table on every run. Simple and predictable.

- Slow for large tables (scans everything every time)
- Good for: small mart tables, reference data, anything under ~10M rows

```yaml
-- dbt_project.yml
models:
  my_project:
    staging:
      +materialized: view
    marts:
      +materialized: table
```

#### `incremental` — Only New Rows

dbt adds or updates only rows that are new or changed since the last run. This is essential for large tables.

- Much faster than rebuilding a full table
- Requires you to define how to filter "new" rows
- Requires `unique_key` if you want upsert behavior (update existing rows), or `partition_by` for partition-level inserts

```sql
-- models/marts/finance/fct_events.sql
{{
  config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge'
  )
}}

select
    event_id,
    user_id,
    event_type,
    event_timestamp,
    properties
from {{ ref('stg_events__raw') }}

{% if is_incremental() %}
  -- On incremental runs, only process events from the last 3 days
  -- (3-day overlap handles late-arriving data)
  where event_timestamp >= dateadd('day', -3, current_timestamp())
{% endif %}
```

The `is_incremental()` macro evaluates to `true` only when the target table already exists and you are not running with `--full-refresh`. On the first run, dbt builds the full table. On subsequent runs, it processes only the filtered rows and merges them in.

**`partition_by` for BigQuery and Spark:** Instead of row-level merges, dbt can insert whole partitions. This is far more efficient on BigQuery and Spark-based warehouses.

```sql
{{
  config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    },
    incremental_strategy='insert_overwrite'
  )
}}
```

#### `ephemeral` — Inline CTE

The model is never stored. dbt injects it as a Common Table Expression (CTE) into any model that references it. Use this for small intermediate logic you want to encapsulate without creating a physical object in the warehouse.

### Built-in Tests

dbt ships with four generic tests you declare in YAML. These run during `dbt test` and in CI.

```yaml
# models/marts/finance/schema.yml
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'completed', 'refunded', 'cancelled']
      - name: account_id
        tests:
          - relationships:
              to: ref('dim_customers')
              field: account_id
```

- **`not_null`** — fails if any row has a null in this column
- **`unique`** — fails if any value appears more than once
- **`accepted_values`** — fails if any value is outside the declared list (catches schema changes in the source)
- **`relationships`** — fails if any value in this column does not exist in the referenced table (foreign key check)

### Custom Data Tests

For business rules that go beyond the four built-in tests, write a SQL query in the `tests/` folder. If the query returns any rows, the test fails.

```sql
-- tests/assert_revenue_is_non_negative.sql
-- Fails if any order has negative gross_amount
select
    order_id,
    gross_amount
from {{ ref('fct_orders') }}
where gross_amount < 0
```

```sql
-- tests/assert_no_duplicate_daily_revenue.sql
-- Fails if the same date + account_type combination appears more than once
select
    revenue_date,
    account_type,
    count(*) as row_count
from {{ ref('fct_daily_revenue') }}
group by 1, 2
having count(*) > 1
```

Custom tests are the right tool for data quality checks that encode business knowledge: "refunds should never exceed the original order amount," "active users must have at least one event in the last 90 days."

### Seeds

Seeds are small CSV files you commit to your dbt project. dbt loads them as tables in your warehouse. Use them for static reference data: country codes, product category mappings, cost center lists.

```
data/
  country_codes.csv
  product_categories.csv
```

```bash
dbt seed  # loads all CSVs in data/ as tables
```

Do not use seeds for data that changes frequently or is large. Seeds are version-controlled data, not an ingestion mechanism.

### Snapshots

Snapshots implement SCD Type 2 (Slowly Changing Dimension Type 2 — a pattern for tracking historical changes to a record while preserving the history). dbt checks your source table for changed rows and writes new history records with `dbt_valid_from` and `dbt_valid_to` timestamps.

```sql
-- snapshots/salesforce_accounts_snapshot.sql
{% snapshot salesforce_accounts_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('salesforce', 'accounts') }}

{% endsnapshot %}
```

When a customer's account type changes from "SMB" to "Enterprise," dbt:
1. Closes the existing record by setting `dbt_valid_to` to the current timestamp.
2. Inserts a new record with the new account type and `dbt_valid_from` set to now.

Snapshots are your source of truth for "what did this record look like on a given date."

### Macros

Macros are reusable SQL snippets written in Jinja (dbt's templating language). Think of them as functions in your SQL codebase.

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name) %}
    ({{ column_name }} / 100.0)::numeric(18, 2)
{% endmacro %}
```

```sql
-- Usage in a model
select
    order_id,
    {{ cents_to_dollars('amount_cents') }} as amount_dollars
from {{ ref('stg_stripe__charges') }}
```

Common macro use cases:
- Date spine generation (create a row for every date in a range)
- Currency conversion with a rate table lookup
- Generating surrogate keys (hashed combination of columns)
- Dynamic pivots

### dbt Semantic Layer and MetricFlow

The dbt Semantic Layer lets you define metrics once in YAML, tied to your dbt models. BI tools and AI assistants query these metric definitions instead of writing raw SQL.

```yaml
# models/marts/metrics.yml
metrics:
  - name: net_revenue
    label: Net Revenue
    model: ref('fct_orders')
    description: "Gross revenue minus refunds"
    type: simple
    type_params:
      measure:
        name: net_revenue_amount
        agg: sum
        expr: gross_amount - refund_amount
    dimensions:
      - name: revenue_date
        type: time
        type_params:
          time_granularity: day
      - name: account_type
        type: categorical
```

Without the Semantic Layer, every analyst and BI tool writes their own `sum(gross_amount - refund_amount)` with their own date filters and joins. With it, "net revenue" means the same thing everywhere. This is especially important when AI-powered tools are generating SQL against your warehouse — they use the metric definitions instead of guessing from column names.

### dbt in CI/CD

**Slim CI** — running tests only on changed models — is the standard approach for teams with large dbt projects. Testing every model on every PR takes too long.

```bash
# In your CI pipeline (GitHub Actions, GitLab CI, etc.)
# 1. Get the manifest from your last production run
dbt ls --select state:modified+  # list changed models and their downstream dependencies

# 2. Run only changed models and their dependents
dbt run --select state:modified+
dbt test --select state:modified+
```

The `state:modified+` selector tells dbt: "run the models that changed in this PR, plus everything downstream of them." A change to `stg_stripe__orders` triggers a rebuild of `int_orders__with_customer` and `fct_daily_revenue` — but not unrelated models.

**Blue-green deployment with dbt:** Build into a staging schema first, validate with tests, then swap. This eliminates downtime during large model rebuilds.

```bash
# Build into a staging schema
dbt run --target prod_staging --full-refresh --select fct_orders

# Run tests against the staging schema
dbt test --target prod_staging --select fct_orders

# If tests pass, swap schemas (warehouse-specific DDL)
ALTER TABLE prod_staging.fct_orders RENAME TO prod.fct_orders;
```

### dbt Core vs. dbt Cloud

| | dbt Core | dbt Cloud |
|---|---|---|
| Cost | Free, open source | Paid (free tier available) |
| Runs | Anywhere (CLI, CI, Airflow) | Hosted scheduler |
| IDE | Your editor | Browser-based IDE |
| Lineage UI | None built-in | Included |
| Semantic Layer serving | Not included | Included |
| Best for | Any team; embed in existing orchestration | Teams wanting managed scheduling and collaboration |

Most production setups use dbt Core triggered by Airflow, Prefect, or Dagster. dbt Cloud makes sense when your team wants a managed scheduler, the browser IDE for less technical collaborators, or the hosted Semantic Layer endpoint.

---

## Decision Guide

| Scenario | Recommended approach |
|---|---|
| Raw source table — first model to touch it | Staging model (`stg_`) + `source()` declaration + freshness check |
| Business logic joining 3+ staging models | Intermediate model (`int_`) + `view` or `table` materialization |
| Analyst-facing aggregation, under 10M rows | Mart model (`fct_`) + `table` materialization |
| High-volume event or transaction table (100M+ rows) | `incremental` materialization + `unique_key` for upsert |
| BigQuery or Spark, partition-based pipeline | `incremental` + `partition_by` + `insert_overwrite` strategy |
| Small intermediate logic, no need for a physical table | `ephemeral` materialization |
| Track history of slowly changing source records | Snapshot with `timestamp` or `check` strategy |
| Static reference data (country codes, categories) | Seed (CSV in the dbt project) |
| Repeated SQL pattern used across many models | Macro |
| Consistent metric definition across BI tools and AI | dbt Semantic Layer metric definition |
| CI: only test what changed | Slim CI with `state:modified+` selector |

---

## Anti-Patterns

| Anti-pattern | What you observe | Fix |
|---|---|---|
| Joining in staging models | `stg_orders` joins `stg_customers` | Move joins to an intermediate model. Each staging model should touch exactly one source table. |
| All models materialized as `table` | Every `dbt run` is slow; large tables rebuild from scratch each time | Audit materializations. Large tables should be `incremental`. Small helper models should be `view` or `ephemeral`. |
| Incremental model without a late-data window | Events that arrive late (e.g., mobile events with delayed delivery) are silently dropped | Add a lookback window in `is_incremental()`: `where event_timestamp >= dateadd('day', -3, current_timestamp())` |
| No tests on mart models | Duplicate `order_id` values appear in production; analysts notice the wrong numbers | Add `unique` and `not_null` tests to every primary key in your mart layer. Run `dbt test` in CI. |
| Business logic buried in staging | `stg_orders` contains complex revenue calculations | Move business logic to intermediate or mart models. Staging is for cleaning and renaming only. |
| `ref()` used to point at a raw source table | Model breaks when schema changes; no freshness monitoring | Use `source()` for raw tables. Declare them in `sources.yml` with freshness thresholds. |
| Skipping slim CI — running `dbt test` on all models for every PR | CI takes 30+ minutes; engineers skip it | Use `state:modified+` to test only changed models and their downstream dependents. |
| Defining metrics differently across models | `fct_orders` and `fct_revenue` return different totals for "net revenue" | Define the metric once in the Semantic Layer and have all models reference the same definition. |

---

## Interview Talking Points

**Q: What does dbt actually do, and what doesn't it do?**
dbt is a transformation tool. It compiles SELECT statements into DDL and runs them in dependency order against your warehouse. It adds testing, documentation, and lineage on top of SQL. It does not ingest or move data — your ingestion pipeline (Fivetran, Airbyte, Spark, etc.) has to load data into the warehouse before dbt can touch it.

**Q: Walk me through how you'd structure a dbt project for a new data domain.**
I start with staging models — one per source table, using `source()`, with simple column renaming and type casting, no joins. Then intermediate models for joins and business logic that isn't ready to expose to analysts. Then mart models — `fct_` for events and transactions, `dim_` for entities — which is what BI tools and analysts query. Each layer has tests in the corresponding `schema.yml`.

**Q: How do you handle large tables in dbt?**
Incremental models. I use `is_incremental()` to filter to only new or changed rows, with a lookback window of a few days to catch late-arriving data. I define `unique_key` so dbt can merge (upsert) on the target rather than duplicating rows. For BigQuery or Spark, I use `partition_by` with `insert_overwrite` to replace whole partitions, which is much faster than row-level merges on large datasets.

**Q: How do you use dbt in CI/CD?**
Slim CI. On every pull request, I run `dbt run --select state:modified+` and `dbt test --select state:modified+`. This builds and tests only the changed models and everything downstream of them. Testing the full project on every PR is too slow for large projects. For production deployments of large models, I'll build into a staging schema, run tests, and then swap — a blue-green pattern that avoids downtime.

**Q: What's the difference between `ref()` and `source()`?**
`ref()` is for models produced by dbt. `source()` is for raw tables loaded by your ingestion pipeline. Using `source()` lets you declare freshness thresholds — dbt can alert you if a source table hasn't been updated in the expected window. Using `ref()` gives you automatic lineage: dbt knows model A depends on model B and will run them in order.

**Q: How do you test data quality in dbt?**
Two layers. Built-in generic tests in `schema.yml` — `not_null`, `unique`, `accepted_values`, `relationships` — cover the standard cases. For business rules, I write custom singular tests: SQL queries in the `tests/` folder where any returned row means the test failed. Examples: "refunds should not exceed the original order amount," "active seller count should never decrease by more than 10% day-over-day."

**Q: When would you use a snapshot vs. an incremental model?**
Snapshots are for tracking history on slowly changing source records — customer account type changes, product price changes. dbt manages the SCD Type 2 logic automatically with `dbt_valid_from` and `dbt_valid_to` columns. Incremental models are for high-volume append or upsert workloads — new events, new transactions — where you want to process only the new rows on each run without tracking history.

**Q: What is the dbt Semantic Layer and why does it matter?**
The Semantic Layer lets you define metrics — `net_revenue`, `weekly_active_sellers` — once in YAML, tied to your dbt models. BI tools and AI-powered query tools consume these definitions instead of writing raw SQL against columns. This means "net revenue" is calculated the same way everywhere, nobody is guessing at column names, and when the underlying model changes you update the definition in one place.
