# Capacity Planning for Data Platforms

> Chapter from the Data Engineering Playbook — finops.

## About This Chapter

**What this is.** Capacity planning means choosing the right amount of compute, storage, and concurrency to meet your service-level agreements (SLAs — the promises you make about latency and availability) at peak load, without spending more than necessary. This chapter treats SLA and cost as a single problem: model how much resource demand varies over time, lock in a stable minimum (the "baseline floor"), and let elastic burst capacity handle the peaks.

**Who it's for.** Mid-level data engineers, platform and architecture leads, and engineering managers or tech leads.

**What you'll take away.** By the end you'll be able to:
- Run the measure → forecast → provision → reconcile loop, breaking demand into trend, seasonality, and explicitly budgeted events (backfills, product launches).
- Apply queueing reality (`ResponseTime ≈ ServiceTime/(1−ρ)`) and plan to the binding constraint — memory, shuffle, slots, or partitions — instead of vCPU alone.
- Compute a commit floor (the p5–p10 low-end of demand) for Savings Plans (discounted long-term cloud compute contracts), size additive recovery, burst, and maintenance headroom, and verify a backfill converges with `P·R > λ` before launch.

---

Capacity planning is the discipline of buying the *right* amount of compute, storage, and concurrency *just before* you need it — not after a pager goes off, and not eighteen months early into a reserved-instance commitment you can't unwind. For a data platform this is harder than for a stateless web service: workloads are bursty (backfills, month-end close, holiday traffic), the unit of capacity is heterogeneous (vCPU, GB, slots, partitions, throughput-units), and the cost of getting it wrong shows up on two different ledgers — the AWS bill and the SLA dashboard.

## TL;DR

- Capacity planning answers two coupled questions: *will the platform meet its SLA at peak?* and *how much will that cost?* Treat them as one optimization, not two.
- Model demand as a **distribution** (a range of possible values with associated probabilities), not a single point estimate. Plan to a percentile (p95/p99 of demand), size headroom to your recovery objective, and let autoscaling absorb the variance you didn't forecast.
- The governing equation is **Little's Law** and **queueing theory** (the math of waiting lines applied to compute): utilization above ~70-80% on a shared cluster means latency explodes non-linearly. "We're only at 85% CPU" is a warning, not a victory.
- Separate **baseline** (steady, predictable work — suited to Reserved/Savings Plans for discounts) from **burst** (backfills, reprocessing — suited to cheaper spot or on-demand capacity). Committing to your peak is how you set money on fire.
- The most expensive capacity mistake in data is **the backfill you didn't budget for**. A backfill is when you reprocess historical data, often due to a bug fix or schema change. Reserve explicit headroom for it; it is not an edge case, it is a quarterly event.
- Forecasts decay. Re-forecast on a cadence (monthly) and on triggers (new tenant, schema explosion, a 2× data-source) — a 12-month-old growth curve is fiction.

## Why this matters in production

Concrete scenario. You run a Spark-on-EMR (Amazon's managed Hadoop/Spark service) plus Iceberg lakehouse (an open table format for large analytic datasets) feeding a Databricks SQL serving layer. The platform has hummed along at ~400 EMR core-hours/day for a year. Then three things happen in the same quarter:

1. A new product team onboards and lands a clickstream source that triples raw ingest volume.
2. Finance asks for a 36-month historical restatement — a backfill over 2.1 TB of compacted Parquet.
3. The holiday season doubles transaction volume for six weeks.

If you sized your Reserved Instances (pre-purchased cloud compute at a discount) to last year's steady state, the backfill and the new tenant now run on on-demand pricing at 3-4× the rate, and your month-end SQL warehouse queues up because the concurrency limit (how many queries can run at once) was set for the old user count. The first signal isn't a cost alert — it's the data freshness SLO (Service Level Objective, the target you've set for how fresh your data must be) breaching because the 02:00 batch is now finishing at 09:30, after the dashboards have already refreshed.

Capacity planning turns all three of those into *line items on a forecast* instead of *incidents*. It sits upstream of cost optimization (you can't right-size what you haven't sized) and it consumes the per-team signal produced by cost attribution.

## How it works

Capacity planning has four stages that form a closed loop: **measure → forecast → provision → reconcile.**

```mermaid
flowchart LR
    A[Historical telemetry<br/>vCPU-h, GB, slots, lag] --> B[Demand model<br/>trend + seasonality + events]
    B --> C[Capacity target<br/>percentile + headroom]
    C --> D[Provisioning plan<br/>baseline RI/SP + burst spot/on-demand]
    D --> E[Run]
    E --> F[Reconcile<br/>forecast vs actual vs SLA]
    F -->|drift / triggers| B
    F -->|waste| C
```

**The demand model.** Break each workload's resource usage over time into four components: trend (long-term growth), seasonality (regular weekly or monthly patterns), event spikes (one-off surges you can schedule for), and noise (random variation):

```
demand(t) = baseline + g·t           (linear/compound growth)
          + S(t)                     (weekly/monthly seasonality)
          + Σ eventᵢ(t)              (known one-offs: backfills, launches)
          + ε                        (noise → drives headroom)
```

You forecast `baseline + g·t + S(t)`, you *budget* the events explicitly, and you size headroom against `ε` (noise) plus your failure-recovery requirement.

**The queueing reality.** Capacity is not "does average demand fit average supply." Think of it like a checkout line: when everyone arrives at the same time (variable arrivals), wait times rise much faster than you'd expect. Under variable arrivals, latency follows the M/M/1 response curve (a standard queueing model where jobs arrive randomly and are served one at a time):

```
ResponseTime ≈ ServiceTime / (1 − ρ)        where ρ = utilization
```

At ρ=0.5 (50% utilized), response time is 2× service time. At ρ=0.9 it's 10×. At ρ=0.95 it's 20×. This is *why* you can't run a shared cluster at 95% CPU and expect predictable SLAs — and why "headroom" is not waste, it's the price of bounded latency. Little's Law (`L = λ·W`, meaning the number of concurrent jobs in the system equals the arrival rate times the average time each job spends) closes the loop: a slowdown (rising W, time in system) silently inflates concurrency (rising L, jobs in flight) until you hit a hard limit (executor slots, warehouse max clusters, Kafka consumer lag).

**Sizing to a percentile.** Pick the demand percentile you provision the baseline for and the mechanism that covers the rest. A percentile tells you what fraction of the time demand falls below a given level — provisioning to p95 means you cover 95% of demand from committed capacity, and let burst handle the top 5%:

| Layer | Provision baseline to | Covers the rest with |
|---|---|---|
| Steady batch (EMR/Spark) | p50-p75 of daily core-hours | Managed scaling, spot task nodes |
| Interactive SQL (Databricks/BigQuery) | p90 of concurrent queries | Multi-cluster autoscaling, query queueing |
| Streaming (Kafka/Flink) | p99 of sustained throughput | Partition headroom + standby capacity |
| Storage (S3/Iceberg) | trend + 90 days | Elastic; budget, don't pre-provision |

Streaming sits at p99 because you cannot "queue" a firehose without growing lag, and lag is unbounded — once a streaming consumer falls behind, it keeps falling further behind until you add capacity.

## Deep dive

This is where engineers get it wrong. The mechanics that don't fit on a slide.

### 1. The unit of capacity is not vCPU — it's the bottleneck resource

A Spark job that runs out of memory (OOMs) at 60% CPU is *memory-bound* — adding cores does nothing. A shuffle-heavy job (one that moves large amounts of data between nodes) is *network/disk-bound*. A SQL warehouse with 200 idle connections waiting for a query slot is *concurrency-slot-bound*. Planning capacity based only on CPU is the classic failure mode. Profile each workload class for its **binding constraint** (the resource that limits throughput first) and plan that dimension:

- **Memory-bound Spark**: plan executor count from `peak shuffle/spill + cached RDD bytes`, not vCPU. `spark.executor.memory` × executors must exceed the peak working set (the total data actively used during the job's most memory-intensive stage), or you pay in spill I/O (writing temporary data to disk because memory is full, which then makes you *also* disk-bound). Skew — when one data key has far more records than others — can make a job that "should" need 50 executors need 200.
- **Concurrency-bound SQL**: capacity = max concurrent queries, not data scanned. Databricks SQL serverless scales clusters on queue depth (how many queries are waiting); the planning variable is `max_num_clusters`, and the cost knob is how aggressively you let it scale down.
- **Throughput-bound streaming**: capacity = partition count × per-partition consumer throughput. You cannot add consumers beyond partition count (each Kafka partition can only be read by one consumer in a group), so partition count is a *capacity-planning decision made at topic-creation time* and is expensive to change later.

### 2. Headroom math: don't confuse "headroom" with "slack"

Headroom is the extra capacity you keep in reserve. It must cover three distinct things, and they add together — you can't double-count a single pool of spare nodes:

```
headroom = recovery_headroom        # absorb a lost AZ / node group while staying under SLA
         + burst_headroom           # the demand variance ε you chose not to forecast
         + maintenance_headroom      # compaction, vacuum, OPTIMIZE running concurrently
```

A common error: sizing for N-1 node failure (losing one node while the rest keep running) but forgetting that nightly Iceberg compaction (merging small files into larger ones for efficiency) and the freshness backfill run in the *same window*. Two independent "spare" pools that turn out to be the same pool. When you plan a 99.9% monthly availability target, that's 43 minutes of downtime budget — your recovery headroom has to bring a replacement node group up *within* that budget. For EMR (Amazon's managed Spark service) that means warm capacity or a pre-scaled instance fleet, not "the autoscaler will figure it out in 12 minutes."

### 3. Backfills are the dominant non-steady cost, and they're forecastable

A backfill of D days of history at throughput R, parallelized over P workers, against a steady stream still arriving at rate λ (the Greek letter lambda, used here for the live arrival rate of new data):

```
wall_clock ≈ (D · daily_volume) / (P · R − λ)
```

Two traps fall out of this formula:

- If `P·R ≤ λ` you **never catch up** — the backfill diverges (falls further and further behind). Engineers discover this at hour 14 of a backfill that's losing ground instead of gaining it. Always check that effective throughput exceeds live arrival rate before launching.
- The `P` that minimizes wall-clock often *maximizes* cost (more spot reclaims, more shuffle, diminishing returns past the shuffle-partition sweet spot). Plan backfills to a *deadline*, then choose the smallest `P` that hits it.

### 4. Reserved commitments and the lock-in trap

Savings Plans and Reserved Instances trade flexibility for ~30-60% discount compared to on-demand pricing. The planning rule: **commit only to the floor of your demand distribution — the capacity you are statistically certain to use 24/7 for the commitment term.** Compute the floor as roughly the p5-p10 of hourly demand (meaning demand is above this level 90-95% of all hours) over a representative period, *not* the average and never the peak. Everything above the floor rides on-demand or spot. A 3-year commit to your *average* means you pay for capacity you don't use at night and still burst onto on-demand at peak — the worst of both. This is the seam where capacity planning hands off to cost optimization.

### 5. Forecast decay and the re-forecast cadence

A growth curve fit six months ago is wrong today because: a new tenant changed the slope, a schema added 40 columns and doubled bytes-per-row, or a deprecation removed a workload. Re-forecast monthly *and* on triggers:

| Trigger | Why it invalidates the forecast |
|---|---|
| New tenant / data product onboarded | Step change in baseline, not captured by trend |
| 2× source volume or new high-cardinality dimension | Bytes-per-row and shuffle volume jump |
| SLA tightened (e.g., freshness 6h→1h) | Higher percentile target → more baseline |
| Engine/format migration (Hive→Iceberg, EMR→EKS) | Throughput-per-dollar changes; old R is invalid |
| Incident post-mortem | Real peak was higher than the planned percentile |

## Worked example

End-to-end: forecast next-quarter EMR core-hours from telemetry, decide the commit floor, and size backfill headroom. Telemetry (the raw usage metrics) comes from Spark event logs and CloudWatch; this runs as a PySpark job.

```python
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

# 1) Load 180 days of daily core-hours per workload class from the metering table
daily = (
    spark.read.table("platform.metering.emr_core_hours")
    .where(F.col("ds") >= F.date_sub(F.current_date(), 180))
    .groupBy("ds")
    .agg(F.sum("core_hours").alias("core_hours"))
    .orderBy("ds")
    .toPandas()
)

# 2) Decompose: linear trend + day-of-week seasonality
daily["t"] = np.arange(len(daily))
daily["dow"] = pd.to_datetime(daily["ds"]).dt.dayofweek
trend = np.polyfit(daily["t"], daily["core_hours"], deg=1)        # [slope, intercept]
g, b0 = trend
seasonal = daily.groupby("dow")["core_hours"].mean()
seasonal = seasonal - seasonal.mean()                            # additive seasonal offsets

# residual ε after removing trend + seasonality -> drives headroom
fitted = b0 + g * daily["t"] + daily["dow"].map(seasonal).values
resid_std = (daily["core_hours"] - fitted).std()

# 3) Forecast the next 90 days, then take demand percentiles
future = np.arange(len(daily), len(daily) + 90)
future_dow = (pd.to_datetime(daily["ds"].iloc[-1]) + pd.to_timedelta(np.arange(1, 91), "D")).dayofweek
forecast = b0 + g * future + future_dow.map(seasonal).values

p50 = np.percentile(forecast, 50)
p95 = np.percentile(forecast, 95)

# 4) The commit floor = the capacity we use ~every hour, all term long.
#    Approx as p10 of the forecast minus a safety margin (never commit to the average).
commit_floor = max(0.0, np.percentile(forecast, 10) - 1.0 * resid_std)

# 5) Headroom = recovery (N-1 of a 10-node baseline) + burst (1.65σ ≈ p95 of noise) + maintenance
recovery_headroom   = p50 * (1 / 10)        # tolerate losing 1 of 10 baseline nodes
burst_headroom      = 1.65 * resid_std      # one-sided 95% of residual noise
maintenance_headroom = p50 * 0.08           # compaction/OPTIMIZE overlap
planned_peak = p95 + recovery_headroom + burst_headroom + maintenance_headroom

print(f"daily growth slope (core-h/day): {g:6.1f}")
print(f"forecast p50 / p95 core-h:       {p50:8.0f} / {p95:8.0f}")
print(f"commit floor (Savings Plan):     {commit_floor:8.0f}")
print(f"planned peak (provision to):     {planned_peak:8.0f}")
print(f" -> burst above floor on spot:   {planned_peak - commit_floor:8.0f}")
```

Backfill headroom check — *before* launching the 36-month restatement, verify it converges (catches up with live data) and hits the deadline:

```python
D_days        = 36 * 30          # history to reprocess
daily_volume  = 0.60             # TB/day of source landed historically
live_lambda   = 0.18             # TB/day still arriving on the stream
per_worker_R  = 0.05             # TB/day throughput per Spark task node (measured!)
deadline_days = 5

# smallest P that meets the deadline AND beats live arrival rate
for P in range(1, 400):
    eff = P * per_worker_R - live_lambda
    if eff <= 0:
        continue                                  # diverges: never catches up
    wall = (D_days * daily_volume) / eff
    if wall <= deadline_days:
        print(f"backfill: {P} task nodes -> {wall:.1f} days (eff {eff:.2f} TB/day)")
        break
else:
    print("No worker count meets the deadline — extend deadline or raise per-worker R")
```

The matching EMR managed-scaling config keeps the steady baseline on-demand and lets the backfill burst onto spot task nodes (cheaper interruptible instances), with a hard ceiling so the plan is enforced, not just hoped for:

```json
{
  "ManagedScalingPolicy": {
    "ComputeLimits": {
      "UnitType": "InstanceFleetUnits",
      "MinimumCapacityUnits": 10,
      "MaximumCapacityUnits": 120,
      "MaximumOnDemandCapacityUnits": 10,
      "MaximumCoreCapacityUnits": 20
    }
  }
}
```

`MinimumCapacityUnits=10` is the committed baseline (covered by Savings Plan), `MaximumOnDemandCapacityUnits=10` pins on-demand to the baseline only, and units 11-120 are spot task nodes for burst and backfill. The cap of 120 is the *enforced* planned-peak — a runaway query can't silently quadruple the bill.

## Production patterns

- **Two-tier provisioning by default.** Baseline pool (Reserved Instances or Savings Plans, on-demand, long-lived) + burst pool (spot instances, ephemeral, interruptible). Tag them distinctly so cost attribution can show "baseline vs burst" per team — that split is the single most useful FinOps chart.
- **Capacity as a budgeted line item per workload class.** Don't forecast "the platform." Forecast ingest, transform, serving, and ML-feature workloads separately; they have different growth slopes and binding constraints.
- **Scheduled pre-scaling for known events.** Month-end close, Black Friday, and quarterly restatements are on the calendar. Pre-warm the cluster on a cron (scheduled job) 30-60 min ahead rather than letting cold autoscaling eat into the SLA window.
- **A standing "backfill budget."** Reserve an explicit, named slice of capacity (and dollars) for reprocessing every quarter. Treat its *absence* as the anomaly.
- **Saturation SLOs, not just utilization dashboards.** Alert on queue depth (how many jobs are waiting to run), Spark task wait time, Kafka consumer lag derivative (the rate at which lag is growing), and SQL warehouse queued-query count — these lead the latency breach. CPU% lags it. Wire these into your monitoring and metrics dashboards.
- **Partition headroom up front for Kafka.** Over-partition modestly at topic creation (a capacity decision you can't cheaply reverse) so you can scale consumers later without a repartition migration.

## Anti-patterns & failure modes

| Anti-pattern | Symptom you'd observe | Fix |
|---|---|---|
| Plan to average demand | Nightly idle waste *and* daytime on-demand burst on the same cluster | Commit to the floor (p5-p10), burst above it on spot |
| Size to CPU only | Job OOMs / spills at 55% CPU; adding cores doesn't help | Plan to the binding constraint (memory, shuffle, slots) |
| Run shared cluster at 90%+ | p99 latency 10×+ service time; "random" SLA breaches | Cap steady utilization ~70%; headroom is not waste |
| No backfill budget | Backfill pre-empts batch; freshness SLO breaches; cost spikes 3× | Standing backfill capacity + convergence check (`P·R > λ`) |
| Commit 3yr to peak | RI utilization < 40%; locked-in waste you can't unwind | Commit floor only; layer Savings Plans incrementally |
| Set-and-forget forecast | Forecast off 50%+ within a quarter after a new tenant | Monthly re-forecast + trigger-based re-forecast |
| Under-partition Kafka | Can't scale consumers; lag grows; partition migration outage | Over-partition modestly at creation time |
| Backfill that diverges | Lag *grows* during backfill; ETA recedes | Verify `P·per_worker_R − λ > 0` before launch |

## Decision guidance

**Forecasting method — pick by data maturity:**

| Situation | Method | Why |
|---|---|---|
| <6 months telemetry, stable | Linear trend + DoW (day-of-week) seasonality (as above) | Simple, transparent, defensible in review |
| Strong weekly/yearly seasonality | Holt-Winters / Prophet | Captures multiplicative seasonality (patterns that grow proportionally with the trend) |
| Spiky, event-driven (launches) | Trend baseline + explicit event budget | ML smears spikes; budget them manually |
| Hard real-time SLA (streaming) | Queueing model to p99 + standby | Latency is non-linear; averages lie |

**Provisioning model — pick by workload shape:**

| Workload | Model | Rationale |
|---|---|---|
| Steady 24/7 transform | Savings Plan / RI to the floor | Maximize discount on certain capacity |
| Bursty batch / backfill | Spot task nodes, capped | Cheapest, interruption-tolerant |
| Interactive SQL | Serverless autoscaling, queue-based | Demand is concurrency, not bytes |
| Streaming | Provisioned to p99 + partition headroom | Can't queue a firehose |

Use capacity *planning* (this chapter) to decide *how much*; use cost optimization to make each unit cheaper; use cost attribution to know *whose* growth is driving the curve.

## Interview & architecture-review talking points

- "We provision the baseline to the p5-p10 floor on a Savings Plan and burst everything above it on capped spot. Committing to the average is how teams end up paying for night-time idle *and* daytime on-demand simultaneously."
- "Headroom isn't slack — it's three additive budgets: recovery, burst, and maintenance. The classic miss is assuming N-1 spare and compaction headroom are the same pool. They overlap in the nightly window."
- "We plan to the binding constraint per workload class, not vCPU. A memory-bound Spark stage and a concurrency-bound SQL warehouse have nothing in common except that planning them on CPU% is wrong for both."
- "Before any large backfill we check `P·R > λ` — effective throughput must exceed live arrival or it diverges. Then we pick the *smallest* worker count that hits the deadline, because the parallelism that minimizes wall-clock usually maximizes cost."
- "We alert on saturation signals — queue depth, Spark task wait, consumer-lag derivative — because those lead the latency breach. Utilization dashboards lag the incident."
- "Forecasts decay. We re-forecast monthly and on triggers: new tenant, 2× source, tightened SLA, engine migration. A point estimate from last quarter is fiction."

## Further reading

- Cost Optimization — making each provisioned unit cheaper once you've sized it
- Cost Attribution — per-team metering that tells you whose growth bends the curve
- Skew Handling — why a single hot key blows up your executor count estimate
- Kafka Consumer Groups and Offsets — throughput-bound capacity and lag as the streaming saturation signal
- Data Freshness — the SLO that breaks first when capacity falls short
- Monitoring — wiring saturation SLOs that lead latency breaches
- Gunther, N. *Guerrilla Capacity Planning* — the queueing-theory foundation (Universal Scalability Law)
- AWS Well-Architected Framework, Cost Optimization Pillar — Right Sizing and Demand-Based Supply
