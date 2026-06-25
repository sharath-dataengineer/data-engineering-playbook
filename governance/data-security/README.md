# Data Security & Governance for Data Engineers

> Chapter from the Data Engineering Playbook — governance

## About This Chapter

**What this is.** A production-focused guide to data security and governance from the data engineer's perspective — covering how to protect PII in pipelines, enforce access controls on data platforms, meet regulatory requirements like GDPR and CCPA, and audit who touched what data.

**Who it's for.** Mid-level to senior data engineers who build and maintain pipelines, own data platform access configurations, or are asked to make their systems "compliant" without being given a clear definition of what that means.

**What you'll take away.**
- A concrete toolkit for classifying PII, applying column masking, and enforcing row-level security across Snowflake, BigQuery, and AWS.
- Practical, step-by-step approaches to the three compliance operations DEs actually implement: right-to-erasure, data minimization, and consent filtering.
- A checklist of the most common security anti-patterns in data pipelines and how to fix them before they become incidents.

---

Your team ships a new marketing attribution pipeline. It reads from the CRM, joins user profiles, and writes a wide denormalized table to S3. Six weeks later, a security audit finds that the table contains unmasked Social Security Numbers, is readable by every analyst in the company, and lands in a bucket with no encryption. The data has been sitting there for six weeks. This chapter is about not being that team.

---

## TL;DR

- Data engineers own security at the pipeline layer. If your pipeline writes PII somewhere it shouldn't, that's a pipeline bug — not a security team problem.
- Tag every PII column in your data catalog when you create the table. Downstream teams should never have to guess what's sensitive.
- Column masking lets analysts query a table without seeing raw values — implement it on the platform (Snowflake, BigQuery, Lake Formation) so it's enforced regardless of which tool runs the query.
- Row-level security restricts which rows a user sees — implement it with policies or filtered views tied to the user's identity, not in application code.
- Encryption at rest and in transit is mostly platform-handled, but DEs must not disable it and must configure it explicitly in pipeline code for Kafka and database connections.
- For GDPR/CCPA: right-to-erasure, data minimization, and consent filtering are operational requirements you build into ingestion and storage — not one-time cleanup tasks.
- Never put raw PII rows into a vector store or embedding index. Those embeddings can surface PII in AI-generated responses in ways that are very hard to audit.
- Audit logs (who queried what, when) are a compliance requirement — enable them at the platform level and make sure they're retained.

---

## Why This Matters in Production

A user submits a GDPR deletion request. Your company's legal team forwards it to the data team. The request names a specific user ID and asks that all personal data be removed within 30 days (the GDPR requirement).

What actually has to happen now? The user's records exist in: your Kafka topic (event stream), an S3 raw landing zone (ingested JSON), an Iceberg table in the data lake, a Redshift analytics table, and two downstream dbt models. Every one of those systems has a different deletion mechanism. You have 30 days.

If you designed the pipeline with this scenario in mind — consistent user ID as a partition key, Iceberg with merge-on-read for row deletes, Kafka topic configured for compaction with tombstone support — you execute the deletion in a few hours. If you didn't, you're spending the next three weeks writing one-off scripts and hoping you didn't miss anything.

Security and governance are not features you add later. They're constraints you design around from the beginning.

---

## How It Works

### PII Identification and Catalog Tagging

PII (Personally Identifiable Information) is any data that can identify a specific person, either directly or in combination with other fields. The most common categories DEs encounter:

| PII Type | Examples |
|---|---|
| Direct identifiers | Full name, SSN, passport number, driver's license |
| Contact information | Email address, phone number, mailing address |
| Digital identifiers | IP address, device ID, cookie ID, advertising ID |
| Location data | Precise GPS coordinates, home address |
| Financial | Credit card number, bank account number, transaction ID tied to a person |
| Health | Diagnosis codes, prescription records, biometric data |

**What this means for your pipeline:** When you create a table or add a column, tag it in your data catalog. In practice this means:

- In Snowflake: apply object tags (`ALTER TABLE orders MODIFY COLUMN ssn SET TAG pii_type = 'government_id'`).
- In AWS Glue Data Catalog: add column-level metadata using `PII_TYPE` as a key in column parameters.
- In dbt: add `meta: {pii: true, pii_type: 'email'}` to column YAML definitions.

Tagging is not optional housekeeping. It's the signal that downstream masking policies, access controls, and audit rules depend on. A column that isn't tagged as PII won't be caught by automated controls.

---

### Column Masking

Column masking means a user running `SELECT * FROM customers` sees `****-****-1234` in the credit card column instead of the real number. The real value is stored in the database — the mask is applied at query time based on who is asking.

This is different from encryption. The data is readable to authorized users without any extra steps. Unauthorized users get the masked version automatically.

**Snowflake dynamic data masking:**

```sql
-- Create the masking policy
CREATE OR REPLACE MASKING POLICY mask_credit_card AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ANALYST_FULL_ACCESS') THEN val
    ELSE '****-****-****-' || RIGHT(val, 4)
  END;

-- Apply it to a column
ALTER TABLE payments MODIFY COLUMN credit_card_number
  SET MASKING POLICY mask_credit_card;
```

Any role not in `ANALYST_FULL_ACCESS` will see the masked version. No application code changes required.

**BigQuery column-level security:**

BigQuery uses policy tags (labels applied to columns in the schema) combined with IAM to control access. Users without the `datacatalog.categoryFineGrainedReader` permission on the policy tag taxonomy will see an error when they try to select that column.

```sql
-- In the table schema, assign a policy tag to the column
-- Then grant access to the taxonomy node for authorized groups only
GRANT `roles/datacatalog.categoryFineGrainedReader`
  ON TAG `projects/my-project/locations/us/taxonomies/123/policyTags/456`
  TO 'group:analysts-pii@example.com';
```

**AWS Lake Formation column masking:**

Lake Formation supports column-level permissions natively. You grant `SELECT` on specific columns and exclude others:

```bash
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::123456789:role/AnalystRole \
  --permissions SELECT \
  --resource '{"TableWithColumns": {"DatabaseName": "prod", "Name": "customers",
    "ColumnNames": ["customer_id", "signup_date", "tier"]}}'
```

The analyst role can only select those three columns. Attempting to select `ssn` or `date_of_birth` returns a permission denied.

**Delta Lake with custom views:**

If your platform doesn't have native masking, create a view layer:

```sql
-- analysts query this view, not the underlying table
CREATE OR REPLACE VIEW customers_analyst_view AS
SELECT
  customer_id,
  signup_date,
  CONCAT('****@', SPLIT_PART(email, '@', 2)) AS email_domain_only,
  CONCAT('***-**-', RIGHT(ssn, 4))           AS ssn_last4
FROM customers_raw;

-- Grant SELECT on the view, not the table
GRANT SELECT ON customers_analyst_view TO ROLE analyst_role;
REVOKE SELECT ON customers_raw FROM ROLE analyst_role;
```

---

### Row-Level Security

Row-level security (RLS) means that when two users query the same table, they get different rows back based on who they are. A regional manager running `SELECT * FROM sales` should see only their region's rows.

**Using views with current user identity:**

```sql
-- Snowflake: CURRENT_USER() returns the logged-in username
CREATE OR REPLACE VIEW sales_by_owner AS
SELECT * FROM sales_raw
WHERE sales_rep_email = CURRENT_USER();
```

This is simple but requires that usernames in the database match the `sales_rep_email` values in the table — which isn't always the case.

**Snowflake row access policies:**

Row access policies are more robust because they use a mapping table to translate user identity to allowed row sets:

```sql
-- Mapping table: user -> list of regions they can see
CREATE TABLE user_region_access (username STRING, region STRING);

-- Row access policy
CREATE OR REPLACE ROW ACCESS POLICY region_policy AS (region STRING) RETURNS BOOLEAN ->
  EXISTS (
    SELECT 1 FROM user_region_access
    WHERE username = CURRENT_USER()
      AND user_region_access.region = region
  );

-- Apply to the table
ALTER TABLE sales ADD ROW ACCESS POLICY region_policy ON (region);
```

**BigQuery row-level security:**

BigQuery uses row access policies with filter conditions:

```sql
CREATE ROW ACCESS POLICY analyst_region_filter
ON sales_table
GRANT TO ('group:us-analysts@example.com')
FILTER USING (region = 'US');
```

**Lake Formation tag-based row filtering:**

Lake Formation can filter rows based on column values using data filter resources, similar to the above patterns. Tag the table with the filter configuration in the console or via CLI.

---

### Encryption

**At rest:** S3 server-side encryption (SSE-S3 uses AWS-managed keys; SSE-KMS uses a customer-managed key in AWS Key Management Service). EBS volume encryption for compute nodes. These are usually platform defaults that DEs must not turn off. When creating S3 buckets in Terraform or CloudFormation, always include:

```hcl
resource "aws_s3_bucket_server_side_encryption_configuration" "default" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
  }
}
```

**In transit:** Data moving between systems must use TLS (Transport Layer Security — the protocol that encrypts data over a network). DEs configure this in pipeline code:

```python
# Kafka producer — TLS configuration
producer = KafkaProducer(
    bootstrap_servers='broker:9093',
    security_protocol='SSL',
    ssl_cafile='/etc/kafka/ca.crt',
    ssl_certfile='/etc/kafka/client.crt',
    ssl_keyfile='/etc/kafka/client.key'
)

# PostgreSQL connection with SSL required
conn = psycopg2.connect(
    host='db.example.com',
    database='prod',
    user='pipeline_svc',
    password=os.environ['DB_PASSWORD'],
    sslmode='require'   # 'disable' or 'allow' are not acceptable
)
```

Never use `sslmode='disable'` in production. Never accept self-signed certificates without pinning them explicitly.

---

### IAM and RBAC for Data Platforms

IAM (Identity and Access Management) controls who can do what. RBAC (Role-Based Access Control) groups permissions into roles that are assigned to users or service accounts.

**Least-privilege service accounts:** Your Spark ETL job needs a service account. That account should have exactly:
- Read access to the source S3 prefix (e.g., `s3://raw-data/crm/events/*`)
- Write access to the output S3 prefix (e.g., `s3://processed/crm/events/*`)
- Nothing else

Not read access to the entire bucket. Not write access to other teams' paths. Not ability to create or delete buckets.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::raw-data",
        "arn:aws:s3:::raw-data/crm/events/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::processed/crm/events/*"
    }
  ]
}
```

**Role hierarchy for analysts:**

| Role | Access |
|---|---|
| `analyst_marts_read` | SELECT on `prod.marts.*` only |
| `analyst_staging_read` | SELECT on `prod.staging.*`, no PII columns |
| `data_engineer_rw` | Read raw, read/write staging and marts |
| `data_engineer_admin` | All of the above plus DDL on dev/staging schemas |
| `pipeline_svc_account` | Purpose-specific least-privilege (per pipeline) |

Analysts should never have direct access to raw landing zones. Raw data often contains unmasked PII before transformation. The marts layer is where masking and access policies should be enforced.

---

### Data Access Patterns: Purpose Limitation and Need-to-Know

**Purpose limitation** means data collected for one purpose should not be used for another without explicit approval. In practice: if you collected a user's location to show them nearby stores, you should not join that location data with their health records to build a targeting model — unless there's a specific documented, approved use case.

As a DE, this translates to pipeline design: don't build a single wide denormalized table that joins every dimension together "for convenience." Build purpose-specific marts. A marketing analytics mart should not contain health fields. An ops dashboard should not contain financial account numbers.

**Need-to-know** means access to data should be based on whether someone requires it to do their job — not whether they could technically query it. An analyst building a revenue dashboard does not need access to raw user PII rows. They need aggregated or masked representations.

Enforce this through roles and views, not trust. "I don't think anyone would query that" is not a control.

---

### GDPR and CCPA: What DEs Actually Implement

#### Right to Erasure (the "Right to Be Forgotten")

A user submits a deletion request. You have 30 days (GDPR) or 45 days (CCPA) to delete their personal data from all systems.

**Iceberg row-level deletes:**

```sql
-- Positional deletes with merge-on-read (no immediate rewrite)
DELETE FROM events WHERE user_id = '550e8400-e29b-41d4-a716-446655440000';

-- To physically remove the data, rewrite affected files
CALL system.rewrite_data_files(
  table => 'prod.events',
  where => 'user_id = ''550e8400-e29b-41d4-a716-446655440000'''
);
```

**Kafka tombstone for compacted topics:**

```python
# Send a tombstone record (null value) with the user's key
# Kafka compaction will remove all records with this key after the next compaction
producer.send(
    topic='user-events',
    key=b'550e8400-e29b-41d4-a716-446655440000',
    value=None  # null value = tombstone
)
```

Note: tombstones only work on topics configured with `cleanup.policy=compact`. For non-compacted topics, you cannot delete individual records — you must expire the entire topic or re-emit without the deleted user.

**Redshift:**

```sql
DELETE FROM user_profiles WHERE user_id = '550e8400-e29b-41d4-a716-446655440000';
VACUUM user_profiles; -- physically removes the deleted rows
```

**dbt models:**

If a dbt model materializes from a source table that already has the user removed, re-running the model will exclude the user from the output. The key requirement: your dbt model must not hard-code or cache user data in seed files or snapshots. Snapshots that captured a user's historical state need to be manually cleaned — identify affected snapshot tables and run DELETE + dbt snapshot run.

#### Data Minimization

Don't ingest fields you don't use. This is enforced at the ingestion layer:

```python
# At ingestion: project only the fields you need
FIELDS_TO_INGEST = {
    'user_id', 'event_type', 'event_timestamp', 'product_id'
    # NOT: date_of_birth, ssn, raw_ip_address
}

def transform_event(raw_event: dict) -> dict:
    return {k: v for k, v in raw_event.items() if k in FIELDS_TO_INGEST}
```

If a field isn't in `FIELDS_TO_INGEST`, it never lands in your storage. You can't have a breach of data you don't hold.

#### Consent Management

If a user did not consent to analytics use, their events should not flow into your analytics pipeline. Implement this as a filter at ingestion time:

```python
def should_ingest_for_analytics(user_id: str, consent_store: ConsentStore) -> bool:
    consent = consent_store.get(user_id)
    return consent is not None and consent.analytics_allowed

# In your Kafka consumer / ingestion loop
for event in consumer:
    if should_ingest_for_analytics(event['user_id'], consent_store):
        write_to_analytics_landing(event)
    else:
        write_to_ops_only_landing(event)  # or discard
```

The consent store must be queryable in real time (or near-real-time) and must be updated within minutes of a user changing their preference. A consent withdrawal that takes 24 hours to propagate to your pipeline is a compliance risk.

---

### PII in AI/ML Embeddings

A vector embedding is a numerical representation of text (or other data) that captures semantic meaning. Vector stores (like Pinecone, Weaviate, or pgvector) index these embeddings so AI systems can do similarity search.

The problem: if you embed raw PII rows (e.g., `{"name": "Jane Smith", "email": "jane@example.com", "diagnosis": "Type 2 Diabetes"}`), the embedding model encodes that information into the vector. When a user asks the AI a question, the system retrieves similar embeddings and often includes the source text in the response. Raw PII surfaces in AI answers, potentially to users who should not see it.

**What to embed instead:**

```python
# Bad: embedding the raw record
text_to_embed = f"User {user['name']} ({user['email']}) diagnosed with {user['diagnosis']}"

# Good: embed anonymized, metadata-only descriptions
text_to_embed = f"Clinical record: {condition_category}, age_bracket={user['age_bracket']}, region={user['region']}"
```

For RAG (Retrieval-Augmented Generation) pipelines over internal data:
- Embed document summaries or anonymized descriptions, not raw records.
- Apply the same column-masking logic to any text that will be embedded.
- Log every retrieval so you can audit what the AI surfaced and to whom.

---

### Audit Logging

Audit logging means recording who accessed which data, when, and what query they ran. This is required for SOC 2, HIPAA, GDPR, and most enterprise security frameworks.

**AWS CloudTrail for S3:** CloudTrail logs S3 object-level API calls (GetObject, PutObject, DeleteObject) per bucket. Enable data event logging explicitly — it's off by default:

```bash
aws cloudtrail put-event-selectors \
  --trail-name my-trail \
  --event-selectors '[{
    "ReadWriteType": "All",
    "IncludeManagementEvents": true,
    "DataResources": [{"Type": "AWS::S3::Object",
      "Values": ["arn:aws:s3:::prod-data-lake/"]}]
  }]'
```

**Snowflake query history:**

```sql
-- Who queried the customers table in the last 7 days?
SELECT query_text, user_name, start_time, rows_produced
FROM snowflake.account_usage.query_history
WHERE query_text ILIKE '%customers%'
  AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;
```

**BigQuery audit logs:** BigQuery writes data access logs to Cloud Audit Logs automatically. Query them via Log Explorer or export to BigQuery for analysis. Set log retention to at least 90 days (1 year recommended for regulated industries).

**Lake Formation audit logs:** Lake Formation logs all data access decisions (allows and denies) to CloudTrail. Useful for proving that a particular role was denied access to a particular column at a specific time.

Retain audit logs for at least the duration required by your regulatory framework. GDPR doesn't specify a minimum, but most legal teams require 1 year. HIPAA requires 6 years.

---

### Data Contracts as Governance

A data contract is a schema agreement between a data producer (the team that writes the table or topic) and data consumers (the teams that read it). It specifies: what columns exist, what their types are, what nullability is allowed, and what the SLA for freshness is.

From a security and governance perspective, contracts serve a critical function: they prevent a producer from silently adding a new PII column, renaming a column in a way that bypasses masking policies, or changing a data type in a way that breaks downstream anonymization logic.

```yaml
# data_contract.yaml for the orders table
schema:
  - name: order_id
    type: STRING
    nullable: false
    pii: false
  - name: user_id
    type: STRING
    nullable: false
    pii: true
    pii_type: pseudonymous_id
  - name: order_total
    type: DECIMAL(18,2)
    nullable: false
    pii: false
  - name: shipping_address
    type: STRING
    nullable: true
    pii: true
    pii_type: address
    masking_policy: mask_address

evolution_rules:
  - allow: add_optional_column
  - deny: remove_column
  - deny: change_column_type
  - deny: change_pii_classification
```

When a producer violates the contract (adds a column marked `pii: true` without going through the approval workflow), the contract validation step in CI fails and the change is blocked. This is governance enforced at the code level, not the process level.

---

## Decision Guide

| Situation | Recommended Approach |
|---|---|
| Analysts need to query a table with SSNs | Column masking policy (Snowflake / BigQuery / Lake Formation). Analysts see masked values automatically. |
| Regional managers should only see their region's data | Row-level security via row access policy (Snowflake) or row access policy (BigQuery). |
| A user requests GDPR deletion | Iceberg row delete + file rewrite, Kafka tombstone, Redshift DELETE + VACUUM, dbt model re-run. |
| New pipeline ingesting user data | Apply data minimization at ingestion — ingest only required fields. Tag PII columns in catalog on first write. |
| Building a RAG pipeline over internal data | Embed anonymized descriptions only. Log all retrievals. Apply masking before embedding. |
| Service account for a Spark job | Create a dedicated service account with read-only on source path, write-only on output path. No wildcards. |
| Analyst asking for raw access to land zone | Deny. Create a masked, filtered view on the mart layer instead. |
| Schema change to a table with masking policies | Validate through data contract CI check. Changing PII classification requires explicit approval step. |
| Kafka topic with PII events | Enable TLS for all producers/consumers. Use compacted topic for deletion support. Apply consent filter at consumer. |
| Compliance team asking "who accessed the customers table?" | Query Snowflake account_usage.query_history or BigQuery audit log export. CloudTrail for S3 access. |

---

## Anti-Patterns

| Anti-pattern | What you observe | Fix |
|---|---|---|
| Writing PII to S3 with no encryption or access controls | Bucket is public or accessible by all roles; no SSE-KMS; SSNs visible in raw JSON files | Enable SSE-KMS on bucket creation. Apply Lake Formation grants. Never use `public-read` ACLs on data buckets. |
| Granting analysts SELECT on the raw landing zone | Analysts can query unmasked PII in the ingestion layer | Create a mart layer with column masking applied. Grant SELECT on the mart view, not the raw table. |
| Pipeline service account with `s3:*` on `*` | One compromised credential gives full read/write access to all data in the account | Scope IAM policy to specific bucket prefixes and specific actions. Review permissions quarterly. |
| `sslmode=disable` or `security_protocol=PLAINTEXT` in pipeline config | Data in transit is unencrypted; credentials can be intercepted on the network | Set `sslmode=require` for DB connections. Set `security_protocol=SSL` for Kafka. Enforce this in code review. |
| No consent filter in the analytics ingestion pipeline | Users who opted out of analytics still have their events in the analytics warehouse | Add a consent lookup at the ingestion step. Filter events where `analytics_allowed = false`. |
| Embedding raw PII rows into a vector store | AI assistant responses include names, emails, or health data from retrieved embeddings | Embed anonymized descriptions only. Apply masking logic before generating embeddings. |
| Single shared service account used by all pipelines | Can't audit which pipeline accessed what; one compromised credential exposes everything | Create a dedicated service account per pipeline with purpose-specific permissions. |
| Deletion handling not designed into the pipeline | GDPR deletion request takes weeks and requires manual data archaeology | Design for deletion from day one: consistent user ID as partition key, Iceberg for row deletes, compacted Kafka topics. |
| No audit logging on S3 data lake | Cannot answer "did anyone access the user profiles table on March 15?" | Enable CloudTrail data event logging for S3. Enable Snowflake/BigQuery audit logs. Set retention >= 1 year. |
| Data contract not enforced in CI | Producer adds a PII column silently; downstream masking policies don't cover it | Add schema validation against the data contract in the CI pipeline. Block merges that violate PII classification rules. |

---

## Interview Talking Points

**Q: How do you handle PII in a data pipeline?**
Start at ingestion: apply data minimization (don't collect fields you don't need), tag PII columns in the catalog on first write, and apply column masking policies on the platform layer so analysts see masked values regardless of which tool they use. For pipelines that must process raw PII, use dedicated service accounts with narrow IAM permissions and ensure audit logging is enabled.

**Q: What's the difference between column masking and encryption?**
Encryption transforms the value — you need the key to read it. Column masking transforms the display of the value at query time based on who's asking, while the underlying data remains readable to authorized users without extra steps. Masking is appropriate for controlling analyst access within a platform. Encryption is appropriate for protecting data at rest and in transit.

**Q: How would you implement a GDPR right-to-erasure request in a data lake?**
Walk through each storage layer: Iceberg supports positional row deletes followed by a file rewrite to physically remove the data. Kafka compacted topics support tombstone records (null value keyed on the user ID) which remove the user after the next compaction. Redshift uses DELETE plus VACUUM. dbt models re-run from the already-cleaned source. The key design requirement is a consistent user ID across all systems so the deletion can be coordinated programmatically.

**Q: What does "least privilege" mean for a data pipeline service account?**
The service account should have exactly the permissions needed to run the pipeline and nothing more. Read access to the source path, write access to the output path, no wildcards, no access to other teams' buckets. If the account is compromised, the blast radius is limited to what that pipeline touches.

**Q: How do you prevent a schema change from breaking your security policies?**
Data contracts enforced in CI. The contract specifies PII classification for every column. A change that alters PII classification or removes a column triggers a validation failure in CI and blocks the merge. This catches the case where a producer renames a masked column — the new column name won't have the masking policy applied until it goes through the approval workflow.

**Q: What are the governance risks of building a RAG system over internal data?**
The main risk is embedding raw PII into the vector store. When the AI retrieves similar embeddings and includes the source text in its response, that PII surfaces to whoever is talking to the AI — regardless of whether they're authorized to see it. The fix is to embed only anonymized descriptions and metadata, apply masking logic before generating any embeddings, and log every retrieval for audit purposes.

**Q: How do you handle a user who opted out of analytics but their events are still arriving?**
Implement a consent filter at the ingestion layer. On each event, look up the user's consent state and route non-consenting users' events away from the analytics pipeline — either to a separate ops-only store or discard them. The consent store must be updated in near-real-time so that a withdrawal propagates within minutes, not hours.
