# Kubernetes for Data Engineering

> Chapter from the Data Engineering Playbook — infrastructure

## About This Chapter

- **What this is.** A practical guide to Kubernetes from a data engineering perspective — how it works, how Spark and Airflow run on it, how to debug failures, and when it is the right tool versus a managed service.
- **Who it's for.** Mid-level to senior data engineers who encounter K8s in their platform stack and want to go beyond "it works somehow" to a working mental model they can use in production.
- **What you'll take away.**
  - A clear understanding of the K8s primitives that directly affect how your pipelines run, fail, and get resource allocation.
  - Practical patterns for running Spark and Airflow on K8s, including how to size pods and configure resource limits correctly.
  - A decision framework for choosing between K8s and managed services, plus a table of anti-patterns to avoid.

---

Your Spark job has been sitting in a `Pending` state for 20 minutes. No errors, no logs — just waiting. A teammate's batch job was submitted around the same time and is consuming every available node. Your job cannot start because there is no room in the cluster. This is a resource starvation problem, and it happens every week on teams that run workloads on Kubernetes without setting up proper guardrails. Understanding what Kubernetes is doing under the hood — and how to configure it correctly — is the difference between a platform that scales gracefully and one that turns into a daily firefight.

---

## TL;DR

- Kubernetes (K8s) is a system that runs and manages containers across a cluster of machines — think of it as an operating system for your data platform infrastructure.
- Data engineers need to understand K8s because modern data tools like Spark, Airflow, and Flink run natively on it; your job sizing, failures, and scaling behavior all happen at the K8s layer.
- Every running container lives inside a **pod** (the smallest deployable unit in K8s); pods live on **nodes** (physical or virtual machines in the cluster).
- **Resource requests** tell K8s how much CPU and memory a pod needs to be scheduled; **limits** are the hard ceiling — hit the memory limit and the pod is killed with `OOMKilled`.
- Spark on K8s creates driver and executor pods dynamically; right-sizing these is the primary tuning lever for batch job performance and cost.
- Airflow's KubernetesExecutor runs each task in its own pod, which gives complete isolation and eliminates the "one bad task kills the worker" problem common with Celery workers.
- Namespace-based resource quotas are the main mechanism for preventing one team's heavy jobs from starving another team's pipelines on a shared cluster.
- K8s gives you control and portability; managed services (EMR, Glue, Dataproc) give you simplicity. Neither is universally better — pick based on your team's ops capacity and workload profile.

---

## Why This Matters in Production

Imagine a shared data platform serving three teams: a reporting team running dozens of small Airflow DAGs every hour, an ML team running large Spark training jobs overnight, and a product analytics team doing ad-hoc heavy queries. All three share the same K8s cluster.

Without proper configuration:
- The ML team submits a Spark job that requests 200 CPU cores and 2 TB of memory. The cluster is not large enough, so the job sits `Pending` while also blocking the scheduler from placing other pods.
- One of the reporting team's Airflow tasks has a memory leak. It keeps consuming memory until it exceeds the node's physical limit, triggering the OS to kill other pods on the same node — taking down unrelated pipelines.
- A developer pushes an Airflow DAG referencing a Docker image that does not exist. Instead of a clear error, the DAG's pod is stuck in `ImagePullBackOff` for 30 minutes before anyone notices.

With correct K8s configuration — resource requests, limits, namespace quotas, and proper image management — each of these becomes a contained, diagnosable, fast-to-recover event rather than a cluster-wide incident. That is the payoff for understanding this material.

---

## How It Works

### Core K8s Concepts for Data Engineers

**Pods** are the smallest unit K8s works with. A pod runs one or more containers together on the same machine with shared networking. For data engineering: each Spark executor is a pod, each Airflow task (with KubernetesExecutor) is a pod, and each Flink task manager is a pod. When you see a job fail, the first thing you look at is the pod.

**Nodes** are the machines in the cluster — physical servers or cloud VMs. The K8s scheduler decides which node a pod runs on, based on available resources and any constraints you specify. From a DE perspective: node sizing determines how large a single Spark executor can be (you cannot schedule a 128 GB memory pod on a node with 64 GB available).

**Namespaces** are logical divisions within a cluster. Teams or environments (dev, staging, prod) get their own namespace. Namespaces allow resource quotas, access controls, and network policies to be applied independently. A pod in the `team-analytics` namespace cannot by default reach a pod in the `team-ml` namespace.

**Resource requests vs. limits** is one of the most important concepts for DEs to internalize:

- **Request**: what the pod says it will need. K8s uses this number to decide whether a node has room to schedule the pod. If you request 4 CPU cores and 16 GB memory, K8s will only place the pod on a node that has 4 unallocated cores and 16 GB unallocated memory.
- **Limit**: the hard ceiling. If the pod tries to use more CPU than its limit, it is throttled (slowed down). If it tries to use more memory than its limit, it is killed immediately with an `OOMKilled` error.

Setting requests too low means your pod gets scheduled on an already-busy node and runs slowly. Setting limits too low means your pod gets killed during normal operation. Setting no limits at all means one runaway job can consume the entire cluster.

**Persistent Volumes (PVs)** are storage that survives pod restarts. Pods are ephemeral — if a pod is killed, any data written to the pod's local filesystem is gone. PVs are how you attach durable disk storage. For data engineering: Airflow logs, intermediate Spark shuffle data, and checkpoints for streaming jobs often need PVs so they survive pod failures and restarts.

---

### Spark on Kubernetes

When you run Spark on K8s, the Spark driver (the process that orchestrates the job) runs in one pod and each executor (the workers that actually process data) runs in its own pod.

The **Spark Kubernetes Operator** is a K8s extension (called a custom resource, or CRD) that manages Spark application lifecycle. You define a `SparkApplication` YAML object describing your job, and the operator handles creating driver and executor pods, monitoring them, and cleaning up when the job finishes.

A minimal Spark job pod spec looks like this:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: etl-customer-metrics
  namespace: team-analytics
spec:
  type: Scala
  mode: cluster
  image: company-registry/spark:3.5.0
  mainClass: com.company.CustomerMetricsJob
  mainApplicationFile: s3://company-artifacts/jobs/customer-metrics.jar
  driver:
    cores: 2
    coreLimit: "2000m"
    memory: "4g"
    serviceAccount: spark-driver-sa
  executor:
    cores: 4
    coreLimit: "4000m"
    memory: "16g"
    memoryOverhead: "2g"
    instances: 10
```

Key points on Spark pod sizing:

- **`memory` vs `memoryOverhead`**: Spark's `memory` setting controls the JVM heap. `memoryOverhead` covers off-heap usage (Python worker processes, native libraries, shuffle buffers). For PySpark jobs, set `memoryOverhead` to at least 20% of `memory`, often more. Many `OOMKilled` failures on Spark pods come from forgetting to set `memoryOverhead`.
- **`cores` vs `coreLimit`**: `cores` is what Spark uses for task parallelism calculations. `coreLimit` is the K8s CPU limit. Set them to the same value to avoid confusion.
- **Dynamic allocation**: Spark can automatically scale the number of executor pods up and down based on pending tasks. Enable it with `spark.dynamicAllocation.enabled=true` and the shuffle tracking feature. This lets a job start small and scale out only if it needs more parallelism, which is much more cost-efficient on a shared cluster.

For batch jobs, a practical starting point: 4 cores and 16 GB memory per executor, driver at 2 cores and 4 GB. Tune from there based on observed CPU utilization and GC pressure in Spark UI.

---

### Airflow on Kubernetes

Airflow has two K8s-native execution modes.

**KubernetesExecutor** replaces the traditional Celery worker pool. Instead of long-running worker processes, each Airflow task gets its own pod. The pod starts when the task is queued, runs the task, and is deleted when the task finishes (success or failure). This means:
- Complete isolation between tasks — a memory-leaking task cannot affect other tasks.
- Each task can have its own resource spec and Docker image.
- No idle worker processes consuming resources between task runs.
- Cold start latency (typically 15-60 seconds for a pod to start) — not suitable for very high-frequency tasks.

**KubernetesPodOperator** (KPO) lets you run any Docker image as an Airflow task, regardless of what is installed in the Airflow environment. This is how you run tasks with specialized dependencies (custom ML libraries, proprietary tools) without polluting the main Airflow image.

```python
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

customer_metrics_task = KubernetesPodOperator(
    task_id="run_customer_metrics",
    namespace="team-analytics",
    image="company-registry/analytics-tools:2.1.0",
    cmds=["python", "-m", "jobs.customer_metrics"],
    env_vars={"ENV": "production", "DATE": "{{ ds }}"},
    container_resources=k8s.V1ResourceRequirements(
        requests={"cpu": "2", "memory": "8Gi"},
        limits={"cpu": "2", "memory": "8Gi"},
    ),
    is_delete_operator_pod=True,  # clean up the pod after it finishes
    get_logs=True,
    dag=dag,
)
```

**Pod templates** let you define a default pod spec for all KubernetesExecutor tasks, which you can override per task. Use pod templates to set standard labels, node affinity (run jobs on a specific node pool), or mount secrets. A pod template is just a Kubernetes Pod YAML file referenced in `airflow.cfg` under `pod_template_file`.

---

### Namespace-Based Resource Quotas for Multi-Team Platforms

A `ResourceQuota` object in K8s caps the total resources a namespace can consume. This is the primary mechanism for preventing one team from starving another.

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: team-analytics-quota
  namespace: team-analytics
spec:
  hard:
    requests.cpu: "40"
    requests.memory: "160Gi"
    limits.cpu: "80"
    limits.memory: "320Gi"
    pods: "50"
```

With this quota, the `team-analytics` namespace can never consume more than 40 CPU cores in requests across all its pods, regardless of what individual pods request. If a job tries to schedule pods that would exceed the quota, K8s rejects the pod creation — the job fails fast with a clear error rather than silently waiting.

Pair quotas with `LimitRange` objects, which set default requests and limits for pods that do not specify their own. This catches cases where a developer forgets to set resource specs entirely.

---

### Debugging Common DE Failures on K8s

**OOMKilled** — the pod was using more memory than its limit and was terminated by K8s. How to diagnose:

```bash
kubectl describe pod <pod-name> -n <namespace>
# Look for: Last State: Terminated, Reason: OOMKilled
```

Fix: raise the memory limit, or raise `memoryOverhead` for Spark pods. Also check whether the job is loading more data into memory than expected (full-table scans when a filter should have been pushed down).

**Pending pods** — the pod has been created but K8s cannot find a node to place it on. Common causes:
- The cluster has no nodes with enough available CPU/memory to satisfy the pod's request. Fix: scale the node pool up, or reduce the pod's request if it was set too conservatively.
- The pod has a `nodeSelector` or affinity rule that no current node satisfies. Fix: check pod spec and node labels.
- The namespace quota is exhausted. Fix: reduce concurrent workloads or raise the quota.

```bash
kubectl describe pod <pod-name> -n <namespace>
# Look for: Events section — it will say why scheduling failed
```

**ImagePullBackOff / ErrImagePull** — K8s cannot pull the container image. Common causes:
- Typo in the image name or tag.
- The image tag does not exist in the registry.
- Missing image pull credentials (K8s needs a `Secret` of type `kubernetes.io/dockerconfigjson` attached to the pod's service account).

```bash
kubectl describe pod <pod-name> -n <namespace>
# Events section will show the exact pull error and the registry response
```

**Getting logs from a failed pod:**

```bash
# Logs from the current run (if the pod is still around)
kubectl logs <pod-name> -n <namespace>

# Logs from a previous run of the same pod (if the pod restarted)
kubectl logs <pod-name> -n <namespace> --previous

# For Spark: get driver logs specifically
kubectl logs <spark-app-name>-driver -n <namespace>
```

For pods that have already been deleted (common with KubernetesExecutor tasks), configure log forwarding to an external system (S3, GCS, Elasticsearch) before the pod is removed. Airflow's remote logging feature handles this automatically when configured.

---

## Decision Guide

| Workload type | Best compute option | Reason |
|---|---|---|
| Large Spark batch jobs, custom images, multi-team platform | Spark on K8s with operator | Full resource control, dynamic allocation, portable across clouds |
| Spark on AWS, need managed autoscaling, team has limited K8s expertise | EMR on EC2 or EMR Serverless | AWS manages node lifecycle; simpler ops |
| Spark on GCP, existing GCP stack | Dataproc | Native GCS integration, managed autoscaling, simple cluster creation |
| Airflow tasks with heavy dependencies or custom environments | KubernetesPodOperator | Task isolation, custom image per task, no dependency conflicts |
| Airflow tasks with standard Python libraries | CeleryExecutor or LocalExecutor | Lower cold-start latency, simpler setup |
| Short-lived glue/transform jobs (< 5 min, simple logic) | AWS Glue or Cloud Run | No cluster to maintain, pay-per-execution, zero ops |
| Real-time streaming (Flink, Kafka Streams) | K8s | Long-running pods map well to stream processors; fine-grained scaling |
| Ad-hoc interactive SQL | Databricks SQL Warehouse or Redshift Serverless | Serverless; no pod management needed |

---

## Anti-Patterns

| Anti-pattern | What you observe | Fix |
|---|---|---|
| No resource limits on pods | One runaway job consumes all cluster CPU/memory; other teams' jobs go `Pending` or get evicted | Set explicit `limits` on every pod spec; enforce via `LimitRange` in each namespace |
| No resource requests set | K8s places pods on already-saturated nodes; jobs run extremely slowly or fail mid-run from resource contention | Always set `requests` equal to expected usage; treat it as a contract with the scheduler |
| Using K8s for simple jobs that a managed service handles fine | Small, low-frequency ETL jobs now require pod spec YAML, image builds, and K8s operations knowledge | Use Glue or Cloud Run for simple scheduled transforms; reserve K8s for jobs that need its control |
| Ignoring pod evictions | Jobs fail mid-run with no obvious error; logs show `Evicted` status | Set `requests` accurately so pods are scheduled on nodes with genuine headroom; use PriorityClasses to protect critical jobs |
| Single Airflow image with all dependencies | Image is 10+ GB; cold start is slow; one library upgrade breaks everything | Use KubernetesPodOperator with task-specific images; keep the base Airflow image minimal |
| Not setting `memoryOverhead` for Spark | Spark executor pods hit `OOMKilled` even though `memory` seems sufficient | Add `memoryOverhead` (at least 20% of heap for JVM, often 40%+ for PySpark) to every Spark executor spec |
| Shared cluster with no namespace quotas | One team's large job exhausts cluster capacity; other teams experience cascading pipeline failures | Create a `ResourceQuota` per team namespace during cluster setup, not after the first incident |
| Hardcoded image tags (`:latest`) in production | Deploying the same DAG picks up a new image version unexpectedly; jobs break due to dependency changes | Always pin to an explicit image digest or version tag in production specs |

---

## Interview Talking Points

**Q: What is Kubernetes and why does a data engineer need to understand it?**
A: K8s is a container orchestration system — it runs containers across a cluster and manages scheduling, scaling, and recovery. DEs need to understand it because tools like Spark, Airflow, and Flink run natively on K8s. Your pod sizing, resource limits, and namespace configuration directly determine whether your pipelines run reliably or fail under load.

**Q: What is the difference between a resource request and a resource limit in K8s?**
A: A request is what the pod needs to be scheduled — K8s only places the pod on a node that has that much capacity free. A limit is the hard ceiling — if the pod exceeds its memory limit, K8s kills it with OOMKilled. Setting requests too low leads to noisy-neighbor problems; setting limits too low causes job failures during normal operation.

**Q: How does Spark run on Kubernetes?**
A: The driver runs in one pod and each executor runs in its own pod. The Spark K8s operator manages pod lifecycle. Key tuning parameters are executor memory (heap), memoryOverhead (off-heap), core count, and whether dynamic allocation is enabled. Forgetting memoryOverhead is the most common cause of OOMKilled on Spark executor pods.

**Q: What is the KubernetesExecutor in Airflow and when would you use it?**
A: KubernetesExecutor runs each Airflow task in its own short-lived pod. It gives complete task isolation — a failing task cannot affect other tasks. It is ideal when you have tasks with different dependency requirements, when you want to right-size resources per task, or when you need to ensure a runaway task cannot take down other pipelines. The trade-off is pod cold-start latency of 15-60 seconds, so it is not ideal for very high-frequency tasks.

**Q: How do you prevent one team's jobs from starving another team's pipelines on a shared K8s cluster?**
A: By creating a `ResourceQuota` in each team's namespace that caps total CPU requests, memory requests, and pod count. Pair that with a `LimitRange` to set defaults for pods that do not specify their own resource specs. This way, even if one team submits a massive job, it will fail fast with a quota-exceeded error rather than monopolizing the cluster silently.

**Q: How do you debug an OOMKilled pod?**
A: Run `kubectl describe pod <name>` and look at the `Last State` section — it will say `Reason: OOMKilled`. Then raise the memory limit (and for Spark, raise memoryOverhead). If it keeps OOMKilling after increases, investigate whether the job is loading unexpectedly large datasets, often caused by a missing filter or a broadcast join on a large table.

**Q: When would you choose EMR or Dataproc over Spark on K8s?**
A: If the team has limited K8s operations expertise, if you are already deeply integrated with AWS or GCP managed services, or if the primary goal is simplicity over fine-grained control. Managed services handle node lifecycle, autoscaling, and patching. K8s is the right choice when you need portability across clouds, custom pod configurations, or you are running a multi-tool data platform where K8s is already the common substrate.
