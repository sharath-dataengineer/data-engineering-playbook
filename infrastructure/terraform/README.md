# Terraform for Data Infrastructure

> Chapter from the Data Engineering Playbook — infrastructure

## About This Chapter

- **What this is.** A practical guide to managing data infrastructure — S3 buckets, EMR clusters, Glue jobs, MSK, Redshift, IAM roles — using Terraform, the most widely adopted Infrastructure as Code tool in the industry.
- **Who it's for.** Mid-level to senior data engineers who currently provision infrastructure by clicking through the AWS console, or who inherit infrastructure they cannot reproduce. No prior DevOps background required.
- **What you'll take away.**
  - A mental model for why data engineers, not just platform teams, should own infrastructure code.
  - Practical Terraform patterns for the data resources you actually use every day.
  - A decision framework for choosing between Terraform, AWS CDK, and CloudFormation.

---

Picture this: your Spark job has been running fine for six months. One Tuesday it fails. You dig in and find that someone increased the EMR cluster's bootstrap action memory flag directly in the AWS console two weeks ago to fix a different problem — and now a routine cluster replacement rolled those changes back. Nobody documented it. Nobody told you. The cluster that exists no longer matches the cluster anyone thinks exists. Reproducing your production environment from scratch would take days of archaeology through AWS console history.

This is the problem Infrastructure as Code solves. Everything that exists in your cloud account is declared in a file. The file is the truth.

---

## TL;DR

- Infrastructure as Code means your S3 buckets, EMR clusters, and Glue jobs are defined in text files checked into version control — just like your pipeline code.
- Terraform is provider-agnostic (works with AWS, GCP, Azure, Snowflake, and more) and is the most widely used IaC tool in data platform work.
- Terraform tracks what it created in a state file; storing that state file in S3 with DynamoDB locking is mandatory when multiple engineers share an environment.
- The `terraform plan` command shows you exactly what will change before you change it — this is your safety net.
- Drift (when someone manually edits infrastructure in the console) is automatically detected the next time you run plan, and Terraform will want to revert those changes.
- Reusable modules let you define a "data lake bucket" once and have 10 pipeline teams use the same hardened pattern.
- Never delete your state file. Losing state means Terraform no longer knows what it created and will try to recreate everything, creating duplicates or conflicts.
- Terraform is the right default for data infrastructure; CDK makes sense when your team is deeply Python/TypeScript-first; CloudFormation is a last resort.

---

## Why This Matters in Production

A mid-size company runs 15 data pipelines. Each pipeline team provisioned its own S3 buckets, EMR clusters, and IAM roles — mostly by hand, months or years ago. When a compliance audit asks "which S3 buckets hold PII?" nobody can answer reliably. When the platform team tries to enforce encryption at rest across all buckets, they have to hunt through the console. When a pipeline team tries to replicate production in a staging environment to debug a data quality issue, they spend a week recreating configs from memory and screenshots.

Now imagine the same 15 pipelines, but every resource is defined in `.tf` files in a shared repository. The compliance answer is a grep. Enforcing encryption is a one-line change to a shared module plus a PR. Staging is `terraform workspace select staging && terraform apply`. The difference is not theoretical — it is months of accumulated debugging time and incident risk.

---

## How It Works

### Core Concepts

**Providers** are plugins that teach Terraform how to talk to a specific cloud or service. Before you can create an AWS resource, you declare the AWS provider and give it your region. Terraform downloads the provider plugin and uses it for all API calls. You can use multiple providers in a single project — for example, the AWS provider to create an S3 bucket and the Snowflake provider to create a warehouse in the same `terraform apply`.

```hcl
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}
```

**Resources** are the actual things you create — an S3 bucket, an EMR cluster, a Glue database. Each resource block has a type (like `aws_s3_bucket`) and a name you choose for reference within your code. Terraform translates the block into API calls to create, update, or delete the real-world object.

```hcl
resource "aws_s3_bucket" "raw_data" {
  bucket = "mycompany-data-lake-raw"
}
```

**Variables** parameterize your configuration so the same code works across environments. Instead of hardcoding `"prod"` everywhere, you define a variable and pass different values for dev, staging, and prod.

```hcl
variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

resource "aws_s3_bucket" "raw_data" {
  bucket = "mycompany-data-lake-raw-${var.environment}"
}
```

**Outputs** expose values from your Terraform configuration so other systems or modules can consume them — for example, the ARN (Amazon Resource Name, a unique identifier for any AWS resource) of a newly created bucket so your pipeline code can reference it without hardcoding.

```hcl
output "raw_bucket_arn" {
  description = "ARN of the raw data lake bucket"
  value       = aws_s3_bucket.raw_data.arn
}
```

**State** is how Terraform tracks the real-world resources it manages. After you run `terraform apply`, Terraform writes a file called `terraform.tfstate` that maps each resource in your code to the actual resource ID in AWS. The next time you run plan, Terraform compares your code against this state file to calculate what needs to change. Without state, Terraform would try to create everything from scratch on every run.

---

### Remote State: Why It Matters

By default, the state file lives on your laptop. That is fine for solo projects and catastrophic for teams. If two engineers apply at the same time, they overwrite each other's state and corrupt it. If your laptop dies, the state is gone.

**Remote state** stores the state file in a shared location — S3 is the standard AWS choice. **State locking** via DynamoDB (a NoSQL database AWS offers) ensures only one engineer can run `apply` at a time. If you start an apply and it fails halfway through, the lock stays on until you manually release it.

```hcl
terraform {
  backend "s3" {
    bucket         = "mycompany-terraform-state"
    key            = "data-platform/pipeline-infra/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-state-lock"
    encrypt        = true
  }
}
```

Set this up before any other engineer touches the project. Migrating state later is possible but painful.

---

### Common Data Engineering Resources

**S3 buckets with lifecycle policies.** Lifecycle policies automatically move or delete objects based on age. This is how you avoid paying for five years of raw landing data that your pipeline already processed and archived.

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "raw_data_lifecycle" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "expire-old-raw-files"
    status = "Enabled"

    expiration {
      days = 90
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"  # cheaper storage tier after 30 days
    }
  }
}
```

**EMR clusters.** Defining your Spark cluster in Terraform means every engineer on the team provisions the same cluster configuration. No more "it worked on my EMR cluster" incidents caused by different bootstrap scripts.

```hcl
resource "aws_emr_cluster" "spark_cluster" {
  name          = "data-platform-spark-${var.environment}"
  release_label = "emr-6.13.0"
  applications  = ["Spark", "Hadoop"]

  ec2_attributes {
    instance_profile = aws_iam_instance_profile.emr_profile.arn
    subnet_id        = var.private_subnet_id
  }

  master_instance_group {
    instance_type = "m5.xlarge"
  }

  core_instance_group {
    instance_type  = "m5.2xlarge"
    instance_count = var.core_node_count
  }

  tags = {
    Environment = var.environment
    Team        = "data-platform"
    CostCenter  = var.cost_center
  }
}
```

**AWS Glue resources.** Glue crawlers discover schema automatically; Glue jobs run your ETL scripts. Both should be version-controlled.

```hcl
resource "aws_glue_catalog_database" "analytics" {
  name = "analytics_${var.environment}"
}

resource "aws_glue_crawler" "raw_data_crawler" {
  database_name = aws_glue_catalog_database.analytics.name
  name          = "raw-data-crawler-${var.environment}"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.raw_data.bucket}/events/"
  }

  schedule = "cron(0 6 * * ? *)"  # daily at 6am
}
```

**MSK (Managed Streaming for Kafka).** Kafka clusters are expensive to get right manually. Terraform ensures your broker count, instance type, and storage are consistent across environments.

**Redshift clusters.** Node type, number of nodes, and VPC placement — defined once, never drifted.

**IAM roles and policies.** This is where most data teams take shortcuts that become security incidents. Every Glue job, EMR cluster, and Lambda function should have a dedicated IAM role with the minimum permissions it needs. Terraform makes it practical to define fine-grained roles consistently.

```hcl
resource "aws_iam_role" "glue_job_role" {
  name = "glue-etl-job-${var.pipeline_name}-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "s3-read-write"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${aws_s3_bucket.raw_data.arn}/*"
      }
    ]
  })
}
```

---

### Managing Multiple Environments

Two common approaches:

**Directory-per-environment.** Each environment (`dev/`, `staging/`, `prod/`) is a separate Terraform root with its own state file. You copy or symlink shared modules in. Changes to prod require a deliberate directory switch. This is the safest pattern for mature teams because dev and prod state are completely isolated.

```
infra/
  modules/
    data-lake-bucket/
    emr-cluster/
  environments/
    dev/
      main.tf
      variables.tf
      terraform.tfvars
    staging/
      main.tf
      variables.tf
      terraform.tfvars
    prod/
      main.tf
      variables.tf
      terraform.tfvars
```

**Terraform workspaces.** A single configuration uses `terraform workspace select dev` or `terraform workspace select prod` to switch contexts. Each workspace gets its own state. This is simpler to set up but carries more risk — one `apply` in the wrong workspace affects the wrong environment. Use it for smaller, lower-stakes projects.

---

### Reusable Modules

A module is a folder of `.tf` files that you call from other configurations. The payoff is consistency at scale: your platform team writes a hardened `data-lake-bucket` module that enforces encryption, versioning, and lifecycle policies. Every pipeline team calls it instead of writing their own bucket code. When you need to add an S3 access logging requirement, you update the module once and every bucket inherits the change on the next apply.

```hcl
# modules/data-lake-bucket/main.tf
resource "aws_s3_bucket" "this" {
  bucket = var.bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
```

Calling the module from a pipeline's configuration:

```hcl
module "clickstream_raw" {
  source      = "../../modules/data-lake-bucket"
  bucket_name = "mycompany-clickstream-raw-${var.environment}"
  tags = {
    Pipeline    = "clickstream"
    Environment = var.environment
  }
}
```

---

### Drift Detection

Drift happens when someone bypasses Terraform — they log into the AWS console and manually change a security group rule, resize a Redshift cluster, or edit a Glue crawler's schedule. The real-world resource no longer matches what Terraform's state file says it should be.

The next time anyone runs `terraform plan`, Terraform calls the AWS APIs, compares the live state to your code, and flags every difference. If you defined the crawler schedule as `cron(0 6 * * ? *)` but someone changed it to `cron(0 8 * * ? *)` in the console, the plan will show a change to revert it.

This is powerful but requires discipline: if the manual change was intentional, update the Terraform code to match before applying, or Terraform will revert the change on the next apply.

---

### Plan Before You Apply

`terraform plan` is a dry run (a simulation that shows what would happen without actually doing it). It lists every resource that will be created, changed, or destroyed. Always read the plan output before running `terraform apply`. Pay particular attention to the destruction count — an unexpected `destroy` is usually a sign that a resource name changed or a dependency was removed accidentally.

```
Plan: 3 to add, 1 to change, 0 to destroy.
```

In CI/CD pipelines, post the plan output as a PR comment so reviewers can see the infrastructure impact alongside the code change.

---

## Decision Guide

| Scenario | Recommended Tool | Reason |
|---|---|---|
| Multi-cloud or cloud-agnostic data platform | Terraform | Single tool works across AWS, GCP, Azure, and SaaS providers like Snowflake |
| Team is primarily Python engineers who dislike HCL syntax | AWS CDK (Python) | CDK lets you write infrastructure in Python with full IDE support and type checking |
| AWS-only, existing heavy CloudFormation investment | CloudFormation | Switching costs outweigh benefits if the team already has templates and runbooks |
| Rapidly prototyping new pipeline infra | Terraform with workspaces | Fast iteration without committing to a full directory structure |
| Enterprise with strict change-management auditing | Terraform + Atlantis or Terraform Cloud | Provides PR-based approval workflows and audit logs for every apply |
| Data infra that needs to react to application events | AWS CDK | CDK integrates more naturally with Lambda, Step Functions, and event-driven patterns |
| Team inherits infrastructure created entirely by hand | Terraform with `import` blocks | Import existing resources into state without recreating them |

---

## Anti-Patterns

| Anti-pattern | What you observe | Fix |
|---|---|---|
| State file stored locally or in a shared drive | Team members overwrite each other's state; state is lost when a laptop is replaced | Migrate to remote state in S3 with DynamoDB locking immediately — do this before the first team apply |
| No environment separation | A `terraform apply` during testing destroys a production Glue job | Use separate state files per environment via directory structure or workspaces; never share state between prod and non-prod |
| Manually editing resources in the AWS console | Terraform plan shows unexpected diffs; manual changes get silently reverted on next apply | Enforce a policy: if it exists in Terraform, it is only changed through Terraform; use AWS Config rules to alert on console changes |
| Hardcoding credentials in `.tf` files | AWS keys appear in git history; security incident | Use IAM roles and instance profiles; pass secrets via environment variables or AWS Secrets Manager, never in code |
| Committing the state file to git | Sensitive resource IDs and outputs (including secrets) are visible in git history | Add `*.tfstate` and `*.tfstate.backup` to `.gitignore`; use remote state only |
| Deleting the state file to "start fresh" | Terraform recreates all resources, creating duplicates; old resources are orphaned and continue incurring cost | Never delete state; if you must start over, use `terraform destroy` first, then remove state |
| One giant `main.tf` file with 500 resources | Runs take forever; a single change requires reviewing hundreds of unrelated resources | Break infrastructure into focused modules; split environments into separate state roots |
| Not pinning provider versions | A provider upgrade changes behavior silently; `apply` in prod behaves differently than in dev | Always pin provider versions with `~>` constraints; update versions intentionally after testing |
| Forgetting to release a stuck state lock | Team is blocked from running any Terraform commands after a failed apply | Run `terraform force-unlock <lock-id>` after confirming no apply is actually running; document this procedure |
| Importing resources without updating code first | Imported resource immediately shows a diff and tries to change itself | Write the matching Terraform resource block before importing; run plan after import and reconcile all diffs before committing |

---

## Interview Talking Points

**Q: What is Infrastructure as Code and why should data engineers care about it?**
Infrastructure as Code means defining cloud resources — buckets, clusters, databases — in version-controlled text files rather than clicking through the console. Data engineers should care because their pipelines depend directly on infrastructure: if your EMR cluster was provisioned by hand, a rebuild after a failure is guesswork, and no two environments will be identical. IaC makes environments reproducible and auditable.

**Q: How does Terraform know what it already created?**
Terraform maintains a state file that maps every resource in your configuration to its real-world identifier in AWS. When you run plan, Terraform calls the AWS APIs to fetch the current state of each resource, compares that against the state file and your code, and computes the diff. Without state, Terraform would try to create everything from scratch on every run.

**Q: What happens when an engineer manually changes infrastructure in the AWS console?**
Terraform detects this as drift the next time someone runs plan. The plan output will show the resource as needing a change — either reverting the manual edit or updating the code to match it. If you apply without updating the code, Terraform reverts the manual change. This is why the discipline rule is important: anything managed by Terraform is only changed through Terraform.

**Q: Why store Terraform state in S3 with DynamoDB? Why not just keep it locally?**
Local state breaks down the moment more than one engineer touches the same infrastructure. Two simultaneous applies can corrupt the state file. S3 gives everyone a single shared source of truth; DynamoDB adds locking so only one apply can run at a time. The DynamoDB lock record is released when apply completes; if a run crashes mid-apply, you may need to manually release the lock with `terraform force-unlock`.

**Q: How do you handle dev, staging, and prod with Terraform?**
The two main approaches are separate directories per environment (each with its own state file, which gives complete isolation) and Terraform workspaces (a single configuration with per-workspace state). I prefer separate directories for anything that touches production — the explicit switch required to get to the prod directory is a useful forcing function that prevents accidental applies.

**Q: What is a Terraform module and when would you build one?**
A module is a reusable block of Terraform code in its own directory that accepts input variables and exposes outputs. I would build a module when multiple teams need the same type of resource with consistent configuration — for example, a data lake S3 bucket that always has versioning, encryption, and lifecycle policies enabled. Writing the module once and having all teams call it means a compliance requirement can be enforced in one place and inherited everywhere.

**Q: What is `terraform plan` and why is it important?**
Plan is a dry run that shows every resource Terraform would create, change, or destroy if you ran apply — without actually doing anything. It is the safety check that prevents surprises. I make it a rule to always read the full plan output before applying, and in CI/CD pipelines I post the plan as a PR comment so the team can review infrastructure changes the same way they review code changes.

**Q: How do you bring existing resources created outside Terraform under Terraform management?**
Using `terraform import`. You write the Terraform resource block that describes the existing resource, then run `terraform import <resource_address> <real_aws_id>` to add it to the state file. After importing, run plan — there will almost always be diffs where the real resource configuration does not match your code. Fix the code until plan shows no changes before committing. In Terraform 1.5+, import blocks in configuration files make this process more repeatable.

**Q: When would you choose AWS CDK over Terraform?**
CDK is compelling when the team is primarily Python or TypeScript engineers who want full IDE support, type checking, and the ability to use loops and conditionals natively without learning HCL syntax. CDK also integrates more naturally with event-driven AWS patterns like Lambda and Step Functions. I would choose CDK for AWS-only greenfield projects with a strong software engineering culture. For multi-cloud setups or when Snowflake, Databricks, or other SaaS providers need to be provisioned alongside AWS resources, Terraform wins because CDK has no native support for non-AWS providers.
