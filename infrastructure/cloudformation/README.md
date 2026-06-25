# AWS CloudFormation for Data Engineering

> Chapter from the Data Engineering Playbook — infrastructure

## About This Chapter

**What this is.** A practical guide to provisioning and managing AWS data infrastructure — S3, EMR, Glue, MSK, Redshift, IAM — using AWS CloudFormation, the native AWS Infrastructure as Code tool. Covers templates, stacks, change sets, reusable nested stacks, and the patterns that matter most for data engineering teams running on AWS.

**Who it's for.** Mid-level to senior data engineers working on AWS who currently click through the console to create resources, or who want to codify and version their data infrastructure. No CloudFormation or DevOps background required.

**What you'll take away.** By the end you'll be able to:
- Read and write a CloudFormation template for common data engineering resources — S3 buckets, EMR clusters, Glue jobs, MSK topics, Redshift clusters.
- Use change sets to preview infrastructure changes safely before applying them, and understand how rollback works when something goes wrong.
- Structure templates for reuse with nested stacks and parameters, and know when to pick CloudFormation over Terraform or CDK.

---

Your team wants to replicate the production data lake setup in a new AWS account for a project. The production environment was built six months ago by clicking through the AWS console. Nobody wrote anything down. Recreating it takes two engineers three days of archaeology — checking IAM roles, S3 bucket policies, Glue crawlers, EMR configurations — and the result still doesn't quite match production.

CloudFormation solves this. Every resource you create is declared in a template file. The template is the truth. Spinning up a new environment is one command.

---

## TL;DR

- CloudFormation is AWS's native Infrastructure as Code tool. You write a template (YAML or JSON) describing what you want, and AWS creates it. The template is checked into version control like any other code.
- A **stack** is a collection of AWS resources created and managed together from one template. Create the stack once; update it by deploying a new version of the template.
- **Change sets** are CloudFormation's equivalent of `terraform plan` — preview exactly what will be created, modified, or deleted before committing the change.
- CloudFormation handles rollback automatically: if a resource fails to create during a stack update, it rolls back the entire change to the last known good state.
- **Nested stacks** let you split large templates into reusable modules — a "data lake bucket" nested stack used by 10 different pipeline team stacks.
- **StackSets** deploy the same template to multiple AWS accounts or regions simultaneously — essential for enterprise data platforms running across accounts.
- `DeletionPolicy: Retain` on S3 buckets and RDS instances prevents CloudFormation from deleting your data when a stack is deleted. Always set this on data resources.
- CloudFormation is AWS-native and requires no extra tooling; Terraform is multi-cloud and has a larger module ecosystem. For an AWS-only org, CloudFormation is a strong default.

---

## Why This Matters in Production

A large data platform team at an enterprise org manages infrastructure across 5 AWS accounts (dev, staging, prod, analytics-sandbox, DR) and 3 regions. They have 40+ S3 buckets, a dozen EMR clusters, 20 Glue jobs, and 3 MSK clusters. They manage all of this by clicking through the console.

The problems:

- A new data engineer accidentally deletes a prod S3 bucket lifecycle policy while exploring the console. Nobody notices for two weeks — until storage costs spike 40%.
- The DR account's EMR cluster configuration is 3 months out of sync with production. When they fail over for a DR test, pipelines behave differently.
- Onboarding a new pipeline team requires a senior engineer to manually recreate 12 resources in a new account. Half a day of work, every time.
- An audit finds that IAM roles have accumulated permissions over time — nobody knows which are still needed.

CloudFormation addresses all of these: configurations are in version control (every change is a PR), environments are replicated from the same template (no drift), new accounts are provisioned with a StackSet in minutes, and IAM roles are explicitly declared and audited in the template.

---

## How It Works

### Template structure

A CloudFormation template is a YAML (or JSON) file with these sections:

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: Data lake landing zone for the orders pipeline

# Parameters let callers customize the template without editing it
Parameters:
  Environment:
    Type: String
    AllowedValues: [dev, staging, prod]
    Default: dev
  TeamName:
    Type: String

# Conditions let you turn resources on/off based on parameters
Conditions:
  IsProd: !Equals [!Ref Environment, prod]

# Resources is the only required section — what you're creating
Resources:
  LandingBucket:
    Type: AWS::S3::Bucket
    DeletionPolicy: Retain          # DO NOT delete this bucket when the stack is deleted
    UpdateReplacePolicy: Retain
    Properties:
      BucketName: !Sub "${TeamName}-landing-${Environment}"
      VersioningConfiguration:
        Status: !If [IsProd, Enabled, Suspended]
      LifecycleConfiguration:
        Rules:
          - Id: MoveToIA
            Status: Enabled
            Transitions:
              - TransitionInDays: 30
                StorageClass: STANDARD_IA
          - Id: MoveToGlacier
            Status: Enabled
            Transitions:
              - TransitionInDays: 90
                StorageClass: GLACIER

# Outputs expose values other stacks or humans can use
Outputs:
  LandingBucketArn:
    Value: !GetAtt LandingBucket.Arn
    Export:
      Name: !Sub "${TeamName}-${Environment}-LandingBucketArn"
```

Key built-in functions:
- `!Ref` — reference a parameter or resource by its logical name
- `!Sub` — string substitution (like an f-string: `!Sub "${Name}-${Env}"`)
- `!GetAtt` — get an attribute of a resource (like a bucket's ARN)
- `!If` — conditional value based on a Condition
- `!ImportValue` — pull an exported Output from another stack

---

### Common data engineering resources

**EMR cluster:**

```yaml
EMRCluster:
  Type: AWS::EMR::Cluster
  Properties:
    Name: !Sub "${TeamName}-${Environment}-spark"
    ReleaseLabel: emr-7.0.0
    Applications:
      - Name: Spark
      - Name: Hive
    Instances:
      MasterInstanceGroup:
        InstanceCount: 1
        InstanceType: m5.xlarge
      CoreInstanceGroup:
        InstanceCount: 2
        InstanceType: m5.2xlarge
        Market: ON_DEMAND
      TaskInstanceGroups:
        - InstanceCount: 4
          InstanceType: m5.2xlarge
          Market: SPOT
          BidPrice: "0.15"
    JobFlowRole: !Ref EMRInstanceProfile
    ServiceRole: !Ref EMRServiceRole
    LogUri: !Sub "s3://${LogsBucket}/emr-logs/"
    Configurations:
      - Classification: spark-defaults
        ConfigurationProperties:
          spark.executor.memory: "8g"
          spark.driver.memory: "4g"
```

**Glue job:**

```yaml
GlueJob:
  Type: AWS::Glue::Job
  Properties:
    Name: !Sub "${TeamName}-orders-transform-${Environment}"
    Role: !GetAtt GlueRole.Arn
    Command:
      Name: glueetl
      ScriptLocation: !Sub "s3://${ScriptsBucket}/jobs/orders_transform.py"
      PythonVersion: "3"
    DefaultArguments:
      "--job-bookmark-option": "job-bookmark-enable"
      "--enable-metrics": "true"
      "--enable-continuous-cloudwatch-log": "true"
      "--TempDir": !Sub "s3://${TempBucket}/glue-temp/"
    MaxRetries: 1
    Timeout: 120
    GlueVersion: "4.0"
    NumberOfWorkers: 10
    WorkerType: G.1X
```

**MSK (Managed Kafka) cluster:**

```yaml
MSKCluster:
  Type: AWS::MSK::Cluster
  Properties:
    ClusterName: !Sub "${TeamName}-kafka-${Environment}"
    KafkaVersion: "3.5.1"
    NumberOfBrokerNodes: 3
    BrokerNodeGroupInfo:
      InstanceType: kafka.m5.large
      StorageInfo:
        EBSStorageInfo:
          VolumeSize: 500
      ClientSubnets:
        - !Ref PrivateSubnet1
        - !Ref PrivateSubnet2
        - !Ref PrivateSubnet3
    EncryptionInfo:
      EncryptionInTransit:
        ClientBroker: TLS
        InCluster: true
    EnhancedMonitoring: PER_BROKER
```

---

### Change sets — preview before you change

A change set lets you see exactly what CloudFormation will do before you commit. This is the equivalent of `terraform plan`.

```bash
# 1. Create a change set (does NOT apply changes yet)
aws cloudformation create-change-set \
  --stack-name orders-pipeline-prod \
  --template-body file://template.yaml \
  --change-set-name add-glue-job-v2 \
  --parameters ParameterKey=Environment,ParameterValue=prod \
               ParameterKey=TeamName,ParameterValue=orders

# 2. Review what will change
aws cloudformation describe-change-set \
  --stack-name orders-pipeline-prod \
  --change-set-name add-glue-job-v2

# 3. Apply only after you've reviewed and approved
aws cloudformation execute-change-set \
  --stack-name orders-pipeline-prod \
  --change-set-name add-glue-job-v2
```

The describe output tells you for each resource: `Action` (Add/Modify/Remove), `Replacement` (whether the resource will be recreated — important for stateful resources like RDS or MSK), and which properties are changing.

**Always use change sets for production stacks.** Direct updates skip the review step.

---

### Rollback behavior

CloudFormation rolls back automatically if a resource fails to create or update during a stack operation. When rollback happens:
- All changes from the failed operation are reversed
- The stack returns to its last stable state
- CloudFormation emits events showing exactly which resource caused the failure and why

You can disable rollback temporarily for debugging (so the partially-created state persists and you can inspect it), but re-enable it before running in production.

---

### Nested stacks — reusable building blocks

A nested stack is a CloudFormation stack that another stack creates as a resource. This is how you build reusable infrastructure modules.

```yaml
# parent-stack.yaml — the pipeline team's template
Resources:

  # Reuse the standard data lake bucket pattern
  LandingZone:
    Type: AWS::CloudFormation::Stack
    Properties:
      TemplateURL: https://s3.amazonaws.com/infra-templates/data-lake-bucket.yaml
      Parameters:
        TeamName: orders
        Environment: !Ref Environment
        BucketPurpose: landing

  ProcessedZone:
    Type: AWS::CloudFormation::Stack
    Properties:
      TemplateURL: https://s3.amazonaws.com/infra-templates/data-lake-bucket.yaml
      Parameters:
        TeamName: orders
        Environment: !Ref Environment
        BucketPurpose: processed

  # Use the output from the nested stack
  GlueJob:
    Type: AWS::Glue::Job
    Properties:
      DefaultArguments:
        "--input-bucket": !GetAtt LandingZone.Outputs.BucketName
        "--output-bucket": !GetAtt ProcessedZone.Outputs.BucketName
```

The `data-lake-bucket.yaml` template is maintained by the platform team. Every pipeline team uses the same hardened, audited pattern — consistent encryption, lifecycle policies, tagging — without copying and pasting it.

---

### StackSets — deploy to multiple accounts at once

StackSets let you deploy the same template to multiple AWS accounts or regions simultaneously. This is essential for enterprise data platforms where each team or business unit has its own AWS account.

```bash
# Deploy the standard data platform baseline to 10 team accounts at once
aws cloudformation create-stack-set \
  --stack-set-name data-platform-baseline \
  --template-body file://baseline.yaml \
  --capabilities CAPABILITY_NAMED_IAM

aws cloudformation create-stack-instances \
  --stack-set-name data-platform-baseline \
  --accounts 111111111111 222222222222 333333333333 \
  --regions us-east-1 \
  --parameter-overrides ParameterKey=Environment,ParameterValue=prod
```

Use cases for data engineering:
- Provision the same logging, monitoring, and IAM baseline across all data team accounts
- Deploy a new data governance policy (Lake Formation tag, S3 bucket policy) to all accounts at once
- Ensure all accounts have the same EMR security configuration

---

### Parameters from SSM Parameter Store

For sensitive values (passwords, connection strings, KMS key ARNs) that you don't want hardcoded in templates, reference AWS Systems Manager (SSM) Parameter Store:

```yaml
Parameters:
  RedshiftPassword:
    Type: AWS::SSM::Parameter::Value<String>
    Default: /data-platform/prod/redshift/master-password

Resources:
  RedshiftCluster:
    Type: AWS::Redshift::Cluster
    Properties:
      MasterUsername: admin
      MasterUserPassword: !Ref RedshiftPassword
```

The value is resolved at deploy time from SSM — the template itself never contains the secret.

---

### Drift detection

Drift happens when someone manually changes a resource in the AWS console after CloudFormation created it. CloudFormation can detect this:

```bash
# Detect drift on a stack
aws cloudformation detect-stack-drift --stack-name orders-pipeline-prod

# Check the results
aws cloudformation describe-stack-resource-drifts \
  --stack-name orders-pipeline-prod \
  --stack-resource-drift-status-filters MODIFIED DELETED
```

The output shows which properties were changed and what the expected vs actual values are. CloudFormation will want to revert these on the next update. For data engineering, set up a scheduled drift detection Lambda and alert when drift is found — it usually means someone "fixed" something in the console that needs to be properly codified.

---

## CloudFormation vs Terraform vs AWS CDK

| | CloudFormation | Terraform | AWS CDK |
|---|---|---|---|
| **What it is** | AWS-native IaC | Multi-cloud IaC | Write infrastructure in Python/TypeScript, generates CloudFormation |
| **Language** | YAML / JSON | HCL (HashiCorp config language) | Python, TypeScript, Java, Go |
| **AWS coverage** | 100% — new services same day | Usually lags AWS by days/weeks | 100% (generates CloudFormation) |
| **State management** | AWS manages it for you | You manage state file (S3 + DynamoDB) | AWS manages it (via CloudFormation) |
| **Multi-cloud** | No — AWS only | Yes | No — AWS only |
| **Module ecosystem** | Nested stacks, Service Catalog | Terraform Registry (huge) | CDK Constructs library |
| **When to choose** | AWS-only shop, want zero extra tooling | Multi-cloud or large Terraform ecosystem already in use | Prefer writing Python/TypeScript over YAML, want L2/L3 abstractions |

**Practical rule for an AWS-focused team:**
- Already using Terraform across the org → keep using Terraform
- AWS-only, no existing IaC → CloudFormation is the path of least resistance (no extra tooling, 100% AWS coverage, AWS support)
- Large org with platform engineering team → CDK for the platform team (higher-level abstractions), CloudFormation for simple team-level stacks

---

## Decision Guide

| Situation | Recommendation |
|---|---|
| Creating S3 buckets, IAM roles, Glue jobs for a new pipeline | Single CloudFormation template with parameters for env/team |
| Multiple teams need the same base infrastructure pattern | Nested stack in a central S3 bucket; teams reference it |
| Need to deploy the same setup to 5 AWS accounts | StackSet — one command, all accounts |
| Sensitive config (passwords, keys) needed in templates | SSM Parameter Store references — never hardcode secrets |
| Someone modified a resource in the console | Run drift detection; codify the change in the template |
| Deleting a stack that contains S3 or RDS | Set `DeletionPolicy: Retain` — or you will lose data |
| Stack update failed and left partial resources | Check stack events for the root cause; fix the template; retry |
| Want to preview changes before applying | Always use a change set on production stacks |

---

## Anti-Patterns

| Anti-pattern | What you observe | Fix |
|---|---|---|
| **No `DeletionPolicy: Retain` on data resources** | Stack delete wipes your S3 bucket and all its data permanently | Set `DeletionPolicy: Retain` on every S3 bucket, RDS instance, and DynamoDB table in your templates |
| **Hardcoding account IDs and ARNs** | Template breaks when deployed to a different account | Use `!Sub "arn:aws:s3:::${BucketName}"` and `!Sub "arn:aws:iam::${AWS::AccountId}:role/..."` |
| **One giant monolithic template** | Template hits the 500-resource limit; updates are slow and risky; one broken resource blocks everything | Split into nested stacks by logical boundary (networking, storage, compute) |
| **Direct stack updates in production** | A bad change rolls out immediately with no review step | Always use change sets for prod stacks — create, review, execute |
| **Storing state in one account** | StackSet deploys fail because the admin account can't assume roles in target accounts | Set up StackSet trust relationships (AWSCloudFormationStackSetAdministrationRole and AWSCloudFormationStackSetExecutionRole) properly |
| **Not tagging resources** | S3 cost attribution is impossible; security audits can't identify resource owners | Add a `Tags` block to every resource with team, environment, and pipeline name |
| **Ignoring drift** | Console changes accumulate; the template no longer reflects reality; next deployment reverts "fixes" | Run drift detection on a schedule; treat drift alerts like code review failures |
| **Circular dependencies** | Stack creation fails with `circular dependency` error | Use `DependsOn` explicitly only when needed; break circular deps by splitting into separate stacks with Outputs/Imports |

---

## Interview Talking Points

- **"What's the difference between CloudFormation and Terraform?"** — CloudFormation is AWS-native: zero extra tooling, 100% AWS service coverage on day one, state managed by AWS. Terraform is multi-cloud with a large module ecosystem but you manage state yourself. For an AWS-only team, CloudFormation is the simpler default. For multi-cloud or teams heavily invested in Terraform, stick with Terraform.

- **"How do you safely update a CloudFormation stack in production?"** — Always use a change set. Create it, review the action for each resource (especially any showing `Replacement: true` — that resource will be deleted and recreated, which is destructive for stateful resources), then execute. Never do a direct update on production.

- **"What happens if a CloudFormation stack update fails halfway through?"** — CloudFormation automatically rolls back the entire operation to the last stable state. It reverses all changes made during the failed update. You can inspect the stack events to find the root cause — usually a resource that failed to create or a permission that's missing.

- **"How would you deploy the same data platform setup to 10 different AWS accounts?"** — StackSets. Define the template once, create a StackSet, then create stack instances pointing at all 10 account IDs and the target region. One command deploys to all of them simultaneously.

- **"What's `DeletionPolicy: Retain` and when would you use it?"** — It tells CloudFormation to leave a resource in place when the stack is deleted, instead of deleting it. Use it on any resource that holds data — S3 buckets, RDS instances, DynamoDB tables. Without it, deleting a stack deletes your data permanently.

- **"How does CloudFormation compare to CDK?"** — CDK lets you write infrastructure in Python or TypeScript. Under the hood it generates CloudFormation templates — so you get the full CloudFormation feature set plus higher-level abstractions (for example, one CDK construct creates an EMR cluster, its IAM roles, its security groups, and its logging config all at once). For teams comfortable with Python, CDK reduces the YAML boilerplate significantly.

---

## Further Reading

- [Terraform for Data Infrastructure](../terraform/README.md) — same concepts, different tool; useful if your org uses both
- [CI/CD for Data Pipelines](../ci-cd/README.md) — how to deploy CloudFormation templates automatically as part of your pipeline CI/CD
- [Data Security & Governance](../../governance/data-security/README.md) — IAM roles and Lake Formation policies you'll provision with CloudFormation
