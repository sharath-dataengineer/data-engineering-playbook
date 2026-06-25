# Schema Evolution Patterns

> Chapter from the Data Engineering Playbook — governance

## About This Chapter

**What this is.** A practical guide to managing schema changes — adding columns, removing fields, renaming attributes, changing data types — without breaking the pipelines, consumers, and downstream models that already depend on your data.

**Who it's for.** Mid-level data engineers who have felt the pain of a "quick schema change" that turned into an all-hands incident, and senior engineers who want a systematic vocabulary and decision framework for schema governance.

**What you'll take away.**
- A clear mental model of backward, forward, and full compatibility — and which one to require for each system you run.
- A practical decision guide for choosing the right schema format and enforcement strategy.
- A repeatable migration playbook (add-then-remove, dual-write, contract testing) that you can apply to your next schema change today.

---

You changed a column name in a Kafka topic. It seemed safe — you updated the producer and ran a quick smoke test. By morning, three downstream jobs were producing NULLs silently, two Spark jobs had thrown `AnalysisException: cannot resolve 'old_column_name'`, and one dbt model had passed CI but was serving wrong numbers to a finance dashboard. Nobody had broken a deployment rule. The schema just changed under everyone's feet.

Schema evolution is the practice of changing your data's structure over time in a controlled, safe way. Done right, it is invisible to most consumers. Done wrong, it is the kind of incident that earns you a postmortem.

---

## TL;DR

- Schema evolution means changing the shape of your data (columns, types, names) without breaking things that already read it.
- There are three levels of compatibility: backward (new readers can read old data), forward (old readers can read new data), full (both). Know which level applies to each system before you make a change.
- Safe changes: adding a nullable field, widening a type (int to long), adding an enum value at the end.
- Breaking changes: removing a field, renaming a field, changing a type (string to int), making an optional field required.
- Schema Registry (Confluent) enforces compatibility rules at the Kafka producer — it rejects writes that would violate the registered schema. This is your first line of defense.
- Iceberg handles schema evolution natively using column IDs under the hood, so renaming or dropping a column does not rewrite data files.
- Always migrate using add-then-remove: add the new column first, migrate all consumers, then drop the old one after a safe window.
- Consumer-driven contract testing catches breaking changes in CI before they ever reach production.

---

## Why This Matters in Production

Picture a mid-size company running a real-time order pipeline. The `orders` Kafka topic feeds 50 consumers: fraud detection, inventory updates, revenue reporting, customer notifications, and more. The dbt layer on top of the warehouse has 200 models that join and transform orders data.

A product engineer renames `order_status` to `status` in the producer. In their service, that is a one-line change. In the data platform, that rename silently breaks every consumer that reads `order_status` by name. Avro consumers throw deserialization errors. Spark jobs produce NULL columns. dbt models compile successfully but compute wrong aggregates because `order_status` now returns NULL everywhere.

The blast radius of a single field rename in a high-fan-out system is enormous. Schema evolution gives you the tools to make that rename safely — or to enforce a policy that prevents the rename until all consumers are ready.

---

## How It Works

### Compatibility Levels

Compatibility is a contract: it describes which combinations of old and new schema versions can coexist safely. You choose the compatibility level per schema, per topic, or per table depending on your system.

**Backward compatibility**

A consumer upgraded to the new schema can still read messages or rows that were written with the old schema. The new schema is backward-compatible with the old one.

- Safe change: add a new nullable column `discount_amount` to a table. Consumers reading old rows get NULL for that column — that is fine.
- Breaking change: remove `customer_email` from the schema. Old rows in storage still have that field physically, but the new schema has no definition for it. Consumers that expect `customer_email` will fail.

Most data warehouse tables and Kafka topics enforce at minimum backward compatibility. It protects you from reading historical data with a schema that no longer understands it.

**Forward compatibility**

A consumer still running the old schema can read messages written by a producer using the new schema. The old consumer simply ignores fields it does not recognize.

- Safe change: the producer adds a new field `referral_code`. Old consumers that do not know about `referral_code` skip it and process the rest of the message normally.
- Breaking change: the producer removes `payment_method`, which the old consumer expects. Now the old consumer reads a message that is missing a field it depends on. At best it defaults to NULL; at worst it throws an exception.

Forward compatibility matters when you cannot coordinate upgrades across all consumers simultaneously — for example, a mobile SDK that might lag a few versions behind your backend.

**Full compatibility**

The schema is both backward and forward compatible. Any reader on any version of the schema can read any message from any version. This is the most restrictive level and requires that you never remove or rename fields, only add new optional ones.

Full compatibility is the right default for long-lived shared schemas — a canonical `user` event schema used across dozens of teams, or a core dimension table queried by hundreds of models. It is too restrictive for fast-moving schemas owned by a single team.

---

### Safe Changes vs. Breaking Changes

Understanding which changes are safe is more useful than memorizing compatibility definitions. Run through this checklist before any schema change.

**Safe changes — will not break existing consumers**

| Change | Why it is safe |
|---|---|
| Add a new nullable (optional) field | Old readers ignore it; new readers get NULL for old records |
| Add a new enum value at the end of the list | Old readers that encounter it will either skip it or use a default |
| Widen a numeric type (int → long, float → double) | The broader type can represent all values the old type could |
| Add a new table column with a DEFAULT | Existing rows get the default; existing queries still work |

**Breaking changes — will break existing consumers**

| Change | Why it breaks |
|---|---|
| Remove a field | Any reader that expects the field gets NULL or an error |
| Rename a field | The old name no longer exists; old readers cannot find it |
| Change a type incompatibly (string → int) | Old data cannot be cast; deserialization fails |
| Make an optional field required | Old records that omit the field now fail validation |
| Add a required field with no default | Old records that do not have the field fail validation |

---

### Schema Formats

The format you use determines how schema evolution works in practice. Each format makes different tradeoffs.

**Avro**

Avro is a binary serialization format where the schema is stored separately from the data — typically in a Schema Registry. On every producer write to Kafka, the registry compares the new schema against the registered version and rejects the write if it violates the configured compatibility level.

The key implication for pipelines: schema validation happens at write time, not read time. You catch the problem before it reaches your consumers.

Avro identifies fields by name, which means renaming a field is a breaking change. If you must rename, you use Avro's `aliases` feature to declare the old name as an alias, then migrate readers, then drop the alias.

**Protobuf**

Protobuf is a binary format developed by Google. Unlike Avro, Protobuf identifies fields by number, not by name. Each field has a tag number (`field_name = 3`), and that number is what gets encoded in the binary message.

This means renaming a field is safe in Protobuf as long as you do not change its tag number. The encoded binary does not contain the name at all — only the number. Removing a tag number is still a breaking change; the recommendation is to mark the number as `reserved` so it is never reused.

Protobuf is more flexible for schema evolution than Avro at the cost of slightly more complex schema files.

**JSON Schema**

JSON Schema is human-readable and requires no binary tooling to debug. It is well-suited to REST APIs where you want to validate request and response bodies, and where human-readable payloads matter more than message size.

Schema evolution with JSON Schema is less automated than Avro or Protobuf. You version your schema files manually and must coordinate consumers yourself. The tradeoff is simplicity: any engineer can open the schema file and understand it without specialized tools.

**Parquet / Iceberg column metadata**

In columnar file formats like Parquet, the schema is embedded in the file footer. Parquet itself has no enforcement — if you write a file with a different schema and read it with the old schema, the behavior is undefined (schema-on-read). This is risky for shared tables.

Iceberg wraps Parquet files in a metadata layer that provides first-class schema evolution. Each column has a stable numeric ID in the metadata, separate from its name and position. Renaming a column updates the name in the metadata but does not touch the data files — the column ID is unchanged, so readers still find the right physical data. Adding, dropping, and reordering columns are all first-class operations that update only the metadata.

---

### Schema Registry (Confluent)

Confluent Schema Registry is a service that stores all schema versions for all your Kafka topics. Every Kafka producer that uses the Avro (or Protobuf or JSON Schema) serializer sends the schema to the registry on the first write and gets back a schema ID. That ID is prepended to every message.

When a producer tries to register a new schema version, the registry checks it against the most recent registered version using the configured compatibility level. If the change is incompatible, the registry rejects the registration and the producer fails — before a single bad message reaches the topic.

This is schema validation at the source. For a Kafka topic with 50 consumers, catching the breaking change at the producer is infinitely better than discovering it after 50 consumer jobs have been running for hours on bad data.

A typical setup:

```
# On topic creation, set the compatibility level
curl -X PUT http://schema-registry:8081/config/orders-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'

# Register a new schema version — registry will check compatibility
curl -X POST http://schema-registry:8081/subjects/orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\": \"record\", \"name\": \"Order\", ...}"}'
```

If the new schema removes a required field, the registry returns a `409 Conflict` and the schema is not registered.

---

### Schema Evolution in Table Formats

**Apache Iceberg**

Iceberg's schema evolution is the most production-ready of the open table formats. Every column has a stable integer column ID assigned at creation. The name and position you see in SQL are just metadata. When you run:

```sql
ALTER TABLE orders ADD COLUMN discount_amount DECIMAL(10,2);
ALTER TABLE orders RENAME COLUMN cust_id TO customer_id;
ALTER TABLE orders DROP COLUMN legacy_flag;
```

None of these operations rewrite data files. Iceberg updates a JSON metadata file that maps column IDs to names, types, and positions. Historical data files still exist unchanged. Readers use the column ID mapping to find the right data regardless of the current column order.

The practical result: schema evolution in Iceberg is cheap, safe, and reversible (for adds). You can add and rename columns on a petabyte table in seconds.

**Delta Lake**

Delta Lake offers two modes:

- **Schema enforcement** (default): Delta rejects any write that does not match the current table schema. A Spark job trying to write an extra column will fail with a `AnalysisException`. This is the safe default.
- **Schema evolution** (opt-in): Setting `mergeSchema=true` on a write tells Delta to add any new columns from the DataFrame to the table schema automatically.

```python
df.write \
  .format("delta") \
  .option("mergeSchema", "true") \
  .mode("append") \
  .save("/data/orders")
```

Use `mergeSchema` only when you have reviewed what new columns the DataFrame contains. A typo in a column name will silently add a new `custmer_id` column while your original `customer_id` is also still present.

**Hive Metastore / Plain Parquet**

Hive Metastore with plain Parquet is schema-on-read: there is no enforcement when data files are written. If a job writes a Parquet file with a different schema than what the Hive table definition expects, Hive will attempt to read it and either return NULLs or throw a runtime error.

This is the most fragile setup. If you are using Hive-managed tables as a shared resource across multiple teams, add a validation step in your pipeline that checks the Parquet file schema against the Hive table schema before registering the partition.

---

### Practical Migration Strategies

**Add-then-remove (the default playbook)**

The safest way to rename or restructure a field without breaking consumers:

1. Add the new column (`new_name`) alongside the old one (`old_name`).
2. Update the producer to write to both columns.
3. Migrate each consumer to read from `new_name`. Track which consumers have been updated.
4. After all consumers have been updated and a safe deprecation window has passed (one sprint, one month — whatever matches your SLA), drop `old_name`.

This approach is slower but it is the only approach that gives you a clean rollback path at every step.

**Dual-write (for format migrations)**

When migrating between schema formats — for example from a plain JSON Kafka topic to an Avro topic with Schema Registry — run the producer writing to both topics simultaneously for a migration window. Consumers migrate at their own pace. When the old topic has no active consumers, decommission it.

**API versioning (for REST services feeding pipelines)**

When a REST API is the source system, run versioned endpoints simultaneously:

```
GET /api/v1/orders  → old schema (maintained for existing consumers)
GET /api/v2/orders  → new schema (new consumers use this)
```

Set a deprecation date for v1. Give downstream teams enough runway to migrate. Hard-remove v1 only after you have confirmed zero traffic.

---

### Consumer-Driven Contract Testing

Schema Registry catches incompatible producer changes. But it cannot catch the case where the producer is technically compatible — no fields removed — but a field the consumer depends on has changed semantics or a new required business rule has been added.

Consumer-driven contract testing fills this gap. Each consumer writes a contract — a test that describes exactly which fields it needs and what types those fields must have:

```python
# In the fraud_detection service test suite
def test_orders_contract():
    sample_message = fetch_sample_from_topic("orders")
    assert "order_id" in sample_message, "order_id is required"
    assert isinstance(sample_message["order_id"], str), "order_id must be a string"
    assert "total_amount" in sample_message, "total_amount is required"
    assert isinstance(sample_message["total_amount"], (int, float)), "total_amount must be numeric"
    assert "customer_id" in sample_message, "customer_id is required"
```

The producer's CI pipeline runs all consumer contracts before any schema change is deployed. If the fraud detection team's contract fails against the new schema, the producer's CI fails — and the engineer who proposed the schema change sees exactly which consumer will break and why.

Tools like Pact formalize this pattern for HTTP APIs. For Kafka and data pipelines, a lighter-weight approach is to maintain contract test files in a shared repository that each producer's CI references.

---

## Decision Guide

| Situation | Recommended approach |
|---|---|
| Kafka topic with many consumers | Avro + Schema Registry, enforce BACKWARD or FULL compatibility |
| Fast-moving internal Kafka topic, single team | Protobuf with field number discipline |
| REST API source system | JSON Schema + API versioning (v1/v2) |
| Data lake table, Spark workloads | Iceberg with ALTER TABLE for schema changes |
| Delta Lake, adding new columns regularly | `mergeSchema=true` on writes, but audit new columns in CI |
| Hive Metastore shared table | Add Parquet schema validation before partition registration |
| Renaming a field anywhere | Add-then-remove migration; never rename in place |
| Breaking change unavoidable | Dual-write + deprecation window + consumer-driven contracts |
| Long-lived canonical schema (user, order) | Full compatibility in Schema Registry; require review board approval for any change |
| Single-team, fast-iteration schema | Backward compatibility; relax to allow adds freely |

---

## Anti-Patterns

| Anti-pattern | What you observe | Fix |
|---|---|---|
| Rename-in-place on a Kafka topic | Consumers start returning NULLs or throwing deserialization errors; no pre-production warning | Use add-then-remove; never rename a field without a migration window |
| Relying on schema-on-read for shared tables | A new Parquet partition silently has different column order; queries return wrong values or NULLs | Migrate shared tables to Iceberg or Delta; add schema validation before partition registration |
| Setting Schema Registry to NONE compatibility | Any schema change is accepted; a producer accidentally removes a field and 50 consumers fail overnight | Set BACKWARD or FULL at minimum; reserve NONE only for isolated development topics |
| Using `mergeSchema=true` without auditing | A typo creates a second column (`custmer_id`); the correct column now has NULLs for new rows | Always print the new columns being added before merge; assert no unrecognized column names in CI |
| Adding a required field to an Avro schema | Old records without that field fail deserialization; batch replay jobs break | Always add fields as optional with a default value; migrate the default to required only after all historical data has been backfilled |
| Changing an enum without appending at the end | Old Avro or Protobuf consumers encounter an unknown value and throw an error | Append new enum values at the end; never reorder or remove existing values |
| No deprecation window before dropping a column | A quarterly report job that runs once a month is broken and nobody notices until month-end | Enforce a minimum deprecation window (e.g., 30 days) tracked in a changelog before any drop |
| Assuming a "backwards compatible" change is risk-free | A field is technically still present but its semantics changed; downstream models compute wrong KPIs silently | Consumer-driven contracts test field presence AND semantic validity, not just schema structure |

---

## Interview Talking Points

**Q: What is schema evolution and why do data engineers care about it?**
Schema evolution is the practice of changing the structure of your data — adding columns, removing fields, renaming attributes — without breaking the systems that already read that data. Data engineers care about it because a single schema change in a high-fan-out system (a Kafka topic with 50 consumers, a warehouse table used by 200 dbt models) can cause a cascading failure across the entire data platform if it is not managed carefully.

**Q: What is the difference between backward and forward compatibility?**
Backward compatibility means a consumer using the new schema can still read data written with the old schema — critical for data lakes where you replay historical data. Forward compatibility means a consumer using the old schema can still read data written with the new schema — critical when you cannot upgrade all consumers at once. Full compatibility requires both, which is the safest but most restrictive option.

**Q: Why is renaming a field a breaking change in Avro but not in Protobuf?**
Avro identifies fields by name in its schema definition. If you rename a field, any consumer looking for the old name will not find it. Protobuf identifies fields by their tag number, which is encoded in the binary message. The name is just a label in the `.proto` file. You can rename a Protobuf field freely as long as you keep the same tag number — the binary encoding is unchanged.

**Q: How does Confluent Schema Registry prevent bad schema changes in production?**
When a Kafka producer tries to register a new schema version, the Schema Registry compares it against the current registered version using the configured compatibility level. If the change violates that level — for example, removing a field under BACKWARD compatibility — the registry rejects the registration with a 409 error. The producer fails before writing a single message. This moves schema validation to the source rather than discovering the problem hours later in consumer logs.

**Q: How does Iceberg handle schema changes differently from plain Parquet?**
Iceberg assigns a stable numeric ID to every column at creation time. Renaming or reordering a column only updates the metadata JSON — the data files are untouched. Plain Parquet has no such metadata layer. Schema-on-read means Hive or Spark will attempt to reconcile the schema at query time with no guarantees, which is risky for shared production tables.

**Q: Walk me through the add-then-remove migration pattern.**
Add the new column alongside the old one. Update the producer to write to both. Migrate each consumer to read from the new column — tracking migration status per consumer. After a defined deprecation window with confirmed zero reads on the old column, drop it. This gives you a rollback option at every step and avoids a big-bang migration that requires coordinating all consumers simultaneously.

**Q: What is consumer-driven contract testing and how does it fit into a schema governance strategy?**
Consumer-driven contracts let each downstream consumer declare which fields it depends on and what types those fields must be. The producer's CI pipeline runs these contracts before deploying any schema change. If a change breaks a consumer's contract, the producer's CI fails — catching the breaking change before it reaches production, rather than at 2am when a job fails. It complements Schema Registry by catching semantic and business-logic breaking changes that a structural schema comparison alone would miss.
