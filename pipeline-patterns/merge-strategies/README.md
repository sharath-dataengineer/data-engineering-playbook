# Merge Strategies

> Chapter from the Data Engineering Playbook — pipeline-patterns.

---

## TL;DR

- **Truncate and Load** — drop everything, reload from source. Simple, correct, expensive at scale.
- **Append-Only** — insert new rows, never touch existing ones. Right for immutable event tables; wrong for anything that updates.
- **Upsert** — insert new rows, update changed rows, leave everything else alone. The default for dimensions and slowly-changing entities.
- **Full Sync (Upsert + Hard Delete)** — upsert, then delete rows that no longer exist in source. Use when your target must be a mirror of the source.
- **Soft Delete** — mark deleted rows with a flag instead of removing them. Keeps history; requires all consumers to filter on the flag.
- **SCD Type 2** — close the old row with an end date, insert a new row. The history-preserving standard for dimension tables that analysts query point-in-time.

The wrong strategy silently corrupts data. Truncate+Load on a 500M-row table will run for hours. Append-only on a customer table will accumulate duplicates. Upsert without delete will leave ghost rows. Choose deliberately.

---

## Shared Scenario

All examples use two tables:

**`dim_customer`** — non-partitioned dimension (customers can update their profile)

| customer_id | name | email | status | updated_ts |
|---|---|---|---|---|
| 101 | Alice | alice@example.com | active | 2024-06-01 |
| 102 | Bob | bob@example.com | active | 2024-06-01 |
| 103 | Carol | carol@example.com | active | 2024-06-01 |

**`fact_orders`** — partitioned by `order_date` (orders are immutable once placed)

Source delivers a daily extract. The extract contains new customers, updated customers, and — depending on the strategy — signals for deleted customers.

---

## Strategy 1: Truncate and Load

### What it is

Truncate the target table, then load the full source extract from scratch. The target is always a point-in-time snapshot of the source.

### When to use

- Source delivers a full extract every run (no delta, no watermark)
- Table is small enough that full reload completes within SLA (rule of thumb: < 5M rows, or < 5 minutes runtime)
- Simplicity matters more than efficiency — no merge logic to debug

### When to avoid

- Large tables: truncating 500M rows and reloading them every hour is expensive and slow
- When downstream tables have foreign keys — truncate breaks referential integrity mid-load
- When audit history of changes is needed

### RDBMS (PostgreSQL)

```sql
-- Option A: direct truncate + insert
TRUNCATE TABLE dim_customer;

INSERT INTO dim_customer (customer_id, name, email, status, updated_ts)
SELECT customer_id, name, email, status, updated_ts
FROM stg_customer;
```

```sql
-- Option B: swap pattern — zero downtime, atomic swap
CREATE TABLE dim_customer_new AS
SELECT customer_id, name, email, status, updated_ts
FROM stg_customer;

ALTER TABLE dim_customer RENAME TO dim_customer_old;
ALTER TABLE dim_customer_new RENAME TO dim_customer;
DROP TABLE dim_customer_old;
```

The swap pattern (Option B) is preferable in production: the old table is readable until the instant of the swap, so dashboards don't see an empty table mid-load.

### PySpark / Hive

```python
# Full overwrite — equivalent to truncate + load
df = spark.table("staging.stg_customer")

df.write \
  .mode("overwrite") \
  .format("parquet") \
  .saveAsTable("analytics.dim_customer")
```

For partitioned tables, overwrite only the partitions present in the source:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df = spark.table("staging.stg_orders").filter("order_date = '2024-06-18'")

df.write \
  .mode("overwrite") \
  .partitionBy("order_date") \
  .format("parquet") \
  .saveAsTable("analytics.fact_orders")
```

Dynamic partition overwrite rewrites only the partitions present in `df`. Without it, the default (static) mode overwrites the entire table.

---

## Strategy 2: Append-Only

### What it is

Insert new rows. Never update or delete existing ones. The table grows monotonically.

### When to use

- Data is **immutable by design**: event logs, audit trails, click events, sensor readings
- Downstream queries always filter by a time range — nobody queries "the current state" of an event
- Source never re-sends or corrects historical records

### When to avoid

- Entity tables (customers, orders, products) — source will re-send updated versions; append creates duplicates
- When you need to query "the latest record per customer" efficiently — that requires dedup on every read

### RDBMS

```sql
-- Simple insert — only new rows based on watermark
INSERT INTO event_log (event_id, user_id, event_type, event_ts)
SELECT event_id, user_id, event_type, event_ts
FROM stg_events
WHERE event_ts > (SELECT MAX(event_ts) FROM event_log);
```

### PySpark

```python
df = spark.table("staging.stg_events") \
  .filter("event_ts > '2024-06-17 23:59:59'")

df.write \
  .mode("append") \
  .partitionBy("event_date") \
  .format("parquet") \
  .saveAsTable("analytics.event_log")
```

---

## Strategy 3: Upsert (Insert + Update, No Delete)

### What it is

For each row in the source:
- If a row with the same business key already exists in the target → **update** it
- If no matching row exists → **insert** it
- Rows in the target that have no matching row in the source → **left untouched**

This is the most common strategy for dimension tables and entity tables with incremental loads.

### When to use

- Source delivers a delta extract: only new and changed rows since the last run
- Rows in the target represent the latest known state of an entity
- Deletes are rare, handled manually, or not required (ghost rows are acceptable)

### When to avoid

- When the target must mirror the source exactly — ghost rows (customers that were deleted in source) will accumulate
- When source can re-send rows with stale `updated_ts` — a guard condition is needed to prevent overwriting newer data with older data

### RDBMS (PostgreSQL — `ON CONFLICT`)

```sql
INSERT INTO dim_customer (customer_id, name, email, status, updated_ts)
SELECT customer_id, name, email, status, updated_ts
FROM stg_customer
ON CONFLICT (customer_id) DO UPDATE SET
  name       = EXCLUDED.name,
  email      = EXCLUDED.email,
  status     = EXCLUDED.status,
  updated_ts = EXCLUDED.updated_ts
WHERE dim_customer.updated_ts < EXCLUDED.updated_ts;  -- guard: don't overwrite newer data
```

The `WHERE` clause on the `DO UPDATE` is important: it prevents a late-arriving extract (with an older `updated_ts`) from overwriting a more recent value already in the target.

### Delta Lake (PySpark — `DeltaTable.merge`)

```python
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "analytics.dim_customer")
staged = spark.table("staging.stg_customer")

target.alias("t") \
  .merge(staged.alias("s"), "t.customer_id = s.customer_id") \
  .whenMatchedUpdate(
    condition="t.updated_ts < s.updated_ts",  # guard: only update if source is newer
    set={
      "name":       "s.name",
      "email":      "s.email",
      "status":     "s.status",
      "updated_ts": "s.updated_ts"
    }
  ) \
  .whenNotMatchedInsertAll() \
  .execute()
```

### Apache Iceberg (Spark SQL — `MERGE INTO`)

```sql
MERGE INTO analytics.dim_customer AS t
USING staging.stg_customer AS s
ON t.customer_id = s.customer_id

WHEN MATCHED AND t.updated_ts < s.updated_ts THEN
  UPDATE SET
    t.name       = s.name,
    t.email      = s.email,
    t.status     = s.status,
    t.updated_ts = s.updated_ts

WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, status, updated_ts)
  VALUES (s.customer_id, s.name, s.email, s.status, s.updated_ts);
```

---

## Strategy 4: Full Sync — Upsert + Hard Delete

### What it is

Same as Upsert, but also **deletes rows** from the target that no longer exist in the source. After every run, the target is an exact mirror of the source.

This requires the source to deliver a **full extract** (not just changed rows), or a separate **delete feed** listing the IDs that were deleted.

### When to use

- Target must exactly mirror source — referential integrity, compliance, downstream systems assume no ghost rows
- Source provides a full snapshot (e.g., a CRM export of all active customers)
- Or: source provides a CDC delete feed (e.g., Debezium `op=d` records)

### When to avoid

- Source delivers only incremental changes with no delete signal — you cannot tell a "not in this batch" from a "deleted"
- Historical analysis requires keeping old rows — use Soft Delete instead

### RDBMS (PostgreSQL — full snapshot approach)

```sql
-- Step 1: load source snapshot into a staging table
-- (stg_customer_snapshot contains ALL current rows from source)

-- Step 2: delete rows from target that are not in source
DELETE FROM dim_customer
WHERE customer_id NOT IN (
  SELECT customer_id FROM stg_customer_snapshot
);

-- Step 3: upsert the rest
INSERT INTO dim_customer (customer_id, name, email, status, updated_ts)
SELECT customer_id, name, email, status, updated_ts
FROM stg_customer_snapshot
ON CONFLICT (customer_id) DO UPDATE SET
  name       = EXCLUDED.name,
  email      = EXCLUDED.email,
  status     = EXCLUDED.status,
  updated_ts = EXCLUDED.updated_ts
WHERE dim_customer.updated_ts < EXCLUDED.updated_ts;
```

**Caution:** `DELETE ... WHERE NOT IN (subquery)` is slow on large tables. A join-based delete scales better:

```sql
DELETE FROM dim_customer tgt
WHERE NOT EXISTS (
  SELECT 1 FROM stg_customer_snapshot stg
  WHERE stg.customer_id = tgt.customer_id
);
```

### RDBMS (PostgreSQL — CDC delete feed approach)

When source provides explicit delete records (e.g., a change log with `operation = 'D'`):

```sql
-- Apply deletes from the CDC feed
DELETE FROM dim_customer
WHERE customer_id IN (
  SELECT customer_id
  FROM stg_customer_cdc
  WHERE operation = 'D'
);

-- Apply upserts from the CDC feed (inserts and updates)
INSERT INTO dim_customer (customer_id, name, email, status, updated_ts)
SELECT customer_id, name, email, status, updated_ts
FROM stg_customer_cdc
WHERE operation IN ('I', 'U')
ON CONFLICT (customer_id) DO UPDATE SET
  name       = EXCLUDED.name,
  email      = EXCLUDED.email,
  status     = EXCLUDED.status,
  updated_ts = EXCLUDED.updated_ts;
```

### Delta Lake (PySpark — full snapshot approach)

```python
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "analytics.dim_customer")
snapshot = spark.table("staging.stg_customer_snapshot")

# MERGE handles insert + update + delete in one atomic operation
target.alias("t") \
  .merge(snapshot.alias("s"), "t.customer_id = s.customer_id") \
  .whenMatchedUpdate(
    condition="t.updated_ts < s.updated_ts",
    set={
      "name":       "s.name",
      "email":      "s.email",
      "status":     "s.status",
      "updated_ts": "s.updated_ts"
    }
  ) \
  .whenNotMatchedInsertAll() \
  .whenNotMatchedBySourceDelete() \   # rows in target but not in snapshot → delete
  .execute()
```

`whenNotMatchedBySourceDelete()` requires Delta Lake 2.3+ (Databricks Runtime 12.2+).

### Apache Iceberg (Spark SQL — CDC delete feed approach)

```sql
-- Delete rows from target that appear as deletes in the CDC feed
DELETE FROM analytics.dim_customer
WHERE customer_id IN (
  SELECT customer_id FROM staging.stg_customer_cdc WHERE operation = 'D'
);

-- Upsert the inserts and updates
MERGE INTO analytics.dim_customer AS t
USING (SELECT * FROM staging.stg_customer_cdc WHERE operation IN ('I', 'U')) AS s
ON t.customer_id = s.customer_id

WHEN MATCHED AND t.updated_ts < s.updated_ts THEN
  UPDATE SET
    t.name       = s.name,
    t.email      = s.email,
    t.status     = s.status,
    t.updated_ts = s.updated_ts

WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, status, updated_ts)
  VALUES (s.customer_id, s.name, s.email, s.status, s.updated_ts);
```

---

## Strategy 5: Soft Delete

### What it is

Instead of physically removing a row, mark it as deleted with a flag or timestamp. The row stays in the table; consumers filter it out.

```sql
-- Physical delete (hard): row is gone
DELETE FROM dim_customer WHERE customer_id = 101;

-- Soft delete: row stays, but is flagged
UPDATE dim_customer
SET is_deleted = TRUE, deleted_ts = NOW()
WHERE customer_id = 101;
```

### When to use

- Audit, compliance, or regulatory requirements demand that deleted records be retained
- Downstream ML models or analytics need historical data including records that were later deleted
- Recovery from accidental deletes must be instant (just flip the flag)

### When to avoid

- All consumers must remember to add `WHERE is_deleted = FALSE` — easy to forget and causes data quality issues
- Table grows indefinitely — physical storage for logically deleted rows is never reclaimed without archival
- When the deleted rows are PII and must be erased (e.g., GDPR right to erasure) — a soft delete is not erasure

### RDBMS

```sql
-- Add soft delete columns to the table
ALTER TABLE dim_customer
  ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE,
  ADD COLUMN deleted_ts TIMESTAMP;

-- Apply soft deletes from CDC feed
UPDATE dim_customer
SET
  is_deleted = TRUE,
  deleted_ts = NOW()
WHERE customer_id IN (
  SELECT customer_id FROM stg_customer_cdc WHERE operation = 'D'
);

-- Every read must filter:
SELECT * FROM dim_customer WHERE is_deleted = FALSE;
```

### Delta Lake / Iceberg

Both Delta and Iceberg support `UPDATE` statements natively:

```sql
-- Delta Lake
UPDATE analytics.dim_customer
SET is_deleted = TRUE, deleted_ts = current_timestamp()
WHERE customer_id IN (
  SELECT customer_id FROM staging.stg_customer_cdc WHERE operation = 'D'
);
```

**Tip:** Create a view that hides deleted rows so consumers never have to remember the filter:

```sql
CREATE OR REPLACE VIEW analytics.dim_customer_active AS
SELECT * FROM analytics.dim_customer
WHERE is_deleted = FALSE;
```

---

## Strategy 6: SCD Type 2 — History-Preserving Updates

### What it is

Slowly Changing Dimension Type 2 (SCD2) preserves full change history for a dimension entity. When a row changes:
- The **existing row** is closed: `effective_end_ts` is set to now, `is_current` is set to FALSE
- A **new row** is inserted with the new values, `effective_start_ts` = now, `is_current` = TRUE

The table contains one row per customer **per version of their data**, not one row per customer.

### When to use

- Analytical queries need point-in-time correctness: "What was this customer's status when they placed this order?"
- Dimension attributes change over time and those changes are analytically meaningful (region, tier, segment)
- Fact tables reference dimension keys — with SCD2, a fact row keeps the key for the version of the dimension that was current at transaction time

### When to avoid

- The dimension has very high cardinality and changes frequently — the table can grow extremely large
- Analysts never need historical values — SCD2 adds complexity for no benefit; use Upsert (SCD1) instead
- Consumers aren't built for it — most BI tools need explicit training or modeling to handle multiple rows per entity

### RDBMS

```sql
-- Close the old row for customers that have changed
UPDATE dim_customer_scd2
SET
  effective_end_ts = NOW(),
  is_current = FALSE
WHERE customer_id IN (
  SELECT s.customer_id
  FROM stg_customer s
  JOIN dim_customer_scd2 t ON t.customer_id = s.customer_id AND t.is_current = TRUE
  WHERE s.name <> t.name OR s.email <> t.email OR s.status <> t.status
);

-- Insert new current rows for changed and new customers
INSERT INTO dim_customer_scd2
  (customer_id, name, email, status, effective_start_ts, effective_end_ts, is_current)
SELECT
  s.customer_id,
  s.name,
  s.email,
  s.status,
  NOW()                       AS effective_start_ts,
  '9999-12-31 23:59:59'::timestamp AS effective_end_ts,
  TRUE                        AS is_current
FROM stg_customer s
WHERE
  -- new customers not yet in target
  NOT EXISTS (SELECT 1 FROM dim_customer_scd2 t WHERE t.customer_id = s.customer_id)
  OR
  -- changed customers whose old row was just closed
  EXISTS (
    SELECT 1 FROM dim_customer_scd2 t
    WHERE t.customer_id = s.customer_id
      AND t.is_current = FALSE
      AND t.effective_end_ts >= NOW() - INTERVAL '1 minute'
  );
```

### Delta Lake (PySpark)

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F

target = DeltaTable.forName(spark, "analytics.dim_customer_scd2")
staged = spark.table("staging.stg_customer")
now = "current_timestamp()"

# Identify changed rows
changed = (
  target.toDF().alias("t")
  .join(staged.alias("s"), "customer_id")
  .filter("t.is_current = true AND (t.name <> s.name OR t.email <> s.email OR t.status <> s.status)")
  .select("t.customer_id")
)

# Step 1: close old rows for changed customers
target.alias("t") \
  .merge(changed.alias("c"), "t.customer_id = c.customer_id AND t.is_current = true") \
  .whenMatchedUpdate(set={
    "is_current":        "false",
    "effective_end_ts":  now
  }) \
  .execute()

# Step 2: insert new rows for changed + new customers
new_rows = staged.alias("s").join(
  target.toDF().filter("is_current = true").alias("t"),
  "customer_id", "left_anti"   # new customers with no current row
).union(
  staged.alias("s").join(changed.alias("c"), "customer_id")  # changed customers
).select(
  "customer_id", "name", "email", "status",
  F.current_timestamp().alias("effective_start_ts"),
  F.lit("9999-12-31 23:59:59").cast("timestamp").alias("effective_end_ts"),
  F.lit(True).alias("is_current")
)

new_rows.write \
  .format("delta") \
  .mode("append") \
  .saveAsTable("analytics.dim_customer_scd2")
```

**Point-in-time query** — "What was this customer's status on the order date?":

```sql
SELECT
  o.order_id,
  o.order_date,
  c.name,
  c.status
FROM analytics.fact_orders o
JOIN analytics.dim_customer_scd2 c
  ON  o.customer_id = c.customer_id
  AND o.order_date BETWEEN DATE(c.effective_start_ts) AND DATE(c.effective_end_ts)
WHERE o.order_date = '2024-06-15';
```

---

## Head-to-Head Comparison

| Strategy | Deletes propagated | History preserved | Source requirement | Target size | Complexity | Best for |
|---|---|---|---|---|---|---|
| **Truncate + Load** | Yes (full reload) | No | Full extract every run | Predictable (= source) | Low | Small reference tables |
| **Append-Only** | No (never deletes) | Yes (all versions appended) | Delta or full; ignore updates | Grows forever | Low | Immutable event/fact tables |
| **Upsert** | No | No (overwrites) | Delta (new + changed) | ≈ source current state | Medium | Dimensions, slowly changing entities |
| **Full Sync (Upsert + Hard Delete)** | Yes | No | Full snapshot or CDC with deletes | = source current state | Medium–High | Mirror tables, reference data |
| **Soft Delete** | Logically (flag) | Yes (deleted rows kept) | Delta or CDC with delete signal | Grows (deleted rows retained) | Medium | Audit, compliance, recovery |
| **SCD Type 2** | No (row version closed) | Yes (full version history) | Delta with change detection | Grows (one row per version) | High | Time-travel dimensions, analytics |

---

## Decision Guide

```
Does source deliver a full extract every run?
  └─ Yes, and table is small (< 5M rows)  → Truncate and Load
  └─ Yes, and table is large               → Full Sync (Upsert + Hard Delete)

Data is immutable — events, logs, click streams?
  └─ Yes  → Append-Only

Do you need to propagate deletes to target?
  └─ No   → Upsert
  └─ Yes, physically remove them  → Full Sync (Upsert + Hard Delete)
  └─ Yes, but keep the row for audit / GDPR  → Soft Delete

Do analysts need point-in-time queries?
  └─ Yes  → SCD Type 2
  └─ No   → Upsert (SCD Type 1)
```

---

## Anti-Patterns

| Anti-pattern | What you'll observe | The fix |
|---|---|---|
| **Append-Only on an entity table** | `dim_customer` has 10 rows for customer 101 — one per daily load | Use Upsert; entity tables need a business key merge |
| **Upsert without a change guard (`updated_ts` check)** | Late-arriving batch overwrites newer data already in target | Add `WHERE target.updated_ts < source.updated_ts` to the MATCH condition |
| **Full Sync with `NOT IN` on a large table** | `DELETE ... WHERE customer_id NOT IN (SELECT ...)` runs for hours | Use `NOT EXISTS` with an indexed join, or stage deletes explicitly into a delete feed |
| **Soft Delete without a view** | Every query team must remember `WHERE is_deleted = FALSE`; someone forgets | Create `dim_customer_active` view; point all consumers at the view |
| **SCD2 with no `is_current` index** | Point-in-time join scans millions of historical rows for each fact | Index on `(customer_id, is_current)` and `(customer_id, effective_start_ts, effective_end_ts)` |
| **Truncate + Load on a 500M row table hourly** | Load window keeps growing; SLA missed by hour 3 | Switch to incremental strategy (Upsert or Full Sync with delete feed) |
| **Mixing SCD1 and SCD2 on the same table** | Some columns overwritten, some versioned — analyst confusion | Separate tables or use SCD1 for fast-changing low-value fields, SCD2 only for analytically meaningful attributes |

---

## Interview & Architecture-Review Talking Points

- **"When would you use Truncate and Load over Upsert?"** — When the source delivers a full extract and the table is small enough to reload within SLA. The simplicity is the benefit: no merge logic, no drift between source and target, no ghost rows. The cost is the reload time — at scale, it becomes the bottleneck.

- **"What's the difference between Upsert and Full Sync?"** — Upsert handles inserts and updates but ignores rows deleted from source — ghost rows accumulate. Full Sync additionally propagates deletes, so the target is always a mirror of the source. Full Sync requires either a full snapshot or an explicit delete feed from the source.

- **"How do you handle deletes when the source only delivers a delta?"** — You can't detect a delete from a delta alone — a missing row in the delta could mean "no change" or "deleted." You need either: (a) the source to provide a CDC delete signal, or (b) the source to occasionally deliver a full snapshot for reconciliation.

- **"Why not just always use SCD2?"** — SCD2 adds two join conditions to every query (the date range filter), doubles or triples table size over time, and confuses analysts who expect one row per customer. Use it only when historical point-in-time accuracy is a genuine analytical requirement.

- **"What's the risk of soft delete?"** — Every consumer must filter on `is_deleted = FALSE`. In practice, someone forgets, and you get phantom rows in a report. Mitigate with a view that hides deleted rows as the contract surface.

- **"Which merge strategy is idempotent?"** — All of them, if implemented correctly. Truncate+Load is trivially idempotent. Upsert and Full Sync are idempotent with a proper business key and a `updated_ts` guard. See the [Idempotency chapter](../idempotency/README.md) for the per-storage-engine patterns.

---

## Further Reading

- [Idempotency in Pipelines](../idempotency/README.md) — safe re-run patterns for each of these strategies across RDBMS, Hive, Delta, and Iceberg
- [Delta Lake deep dive](../../lakehouse/delta/README.md)
- [Apache Iceberg deep dive](../../lakehouse/iceberg/README.md)
- [Enterprise Data Foundation — source-to-target design](../../../enterprise-data-foundation/design/source-to-target.md)
