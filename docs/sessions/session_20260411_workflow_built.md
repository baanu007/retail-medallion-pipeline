# Session Report — April 11, 2026 (Workflow)
## Glue Workflow + Daily Schedule: Built and End-to-End Run SUCCEEDED

---

## TL;DR

Built the `retail-daily-pipeline` Glue Workflow chaining all **18
pipeline jobs** in dependency order. Scheduled trigger fires daily at
**02:00 UTC**. Manually started a test run — **the workflow completed
end-to-end with all 18 jobs SUCCEEDED on the first attempt** in
**13 minutes 21 seconds**.

This brings consecutive Glue successes to **35+ jobs with zero debugging**
after the initial Bronze marathon.

---

## 1. Architecture

### 1.1 Workflow graph (5 levels, 18 jobs, 5 triggers)

```
                  ┌────────────────────────────┐
                  │ T0 SCHEDULED cron(0 2 * *) │
                  │   (daily at 02:00 UTC)     │
                  └────────────┬───────────────┘
                               ▼
                  ┌────────────────────────────┐
                  │  bronze-ingest-all-tables  │  (Level 0)
                  └────────────┬───────────────┘
                               │ SUCCEEDED
                               ▼
                  ┌────────────────────────────┐
                  │  T1 CONDITIONAL: bronze OK │
                  └────────────┬───────────────┘
                               ▼
       ┌───────────────────────┼───────────────────────┐
       ▼                       ▼                       ▼
 clean-order-items   clean-order-item-options   clean-date-dim    (Level 1: silver)
       │                       │                       │
       └───────────────────────┼───────────────────────┘
                               │ ALL 3 SUCCEEDED
                               ▼
                  ┌────────────────────────────┐
                  │  T2 CONDITIONAL: silver OK │
                  └────────────┬───────────────┘
                               ▼
       ┌───────────┬───────────┼───────────┬───────────┐
       ▼           ▼           ▼           ▼
 build-dim-   build-dim-  build-dim-  build-dim-                  (Level 2: dims)
   date        customer    restaurant   menu-item
       │           │           │           │
       └───────────┴───────────┼───────────┘
                               │ ALL 4 SUCCEEDED
                               ▼
                  ┌────────────────────────────┐
                  │  T3 CONDITIONAL: dims OK   │
                  └────────────┬───────────────┘
                               ▼
                  ┌────────────────────────────┐
                  │     build-fact-orders      │  (Level 3: fact)
                  └────────────┬───────────────┘
                               │ SUCCEEDED
                               ▼
                  ┌────────────────────────────┐
                  │  T4 CONDITIONAL: fact OK   │
                  └────────────┬───────────────┘
                               ▼
    ┌────────┬────────┬────────┼────────┬────────┬────────┐
    ▼        ▼        ▼        ▼        ▼        ▼        ▼
  clv      rfm     churn    sales    sales    sales   loyalty   (Level 4: metrics)
 snapshot         indicat.  daily    weekly   monthly            (9 jobs parallel)
    │        │        │        │        │        │        │
    └────────┴────────┴────────┼────────┴────────┴────────┘
                               │
                      ┌────────┴────────┐
                      ▼                 ▼
                   location           upsell
                     perf
```

### 1.2 Triggers (5 total)

| # | Name | Type | Predicate | Action(s) |
|---|---|---|---|---|
| 0 | `t0-schedule` | SCHEDULED | `cron(0 2 * * ? *)` | bronze-ingest-all-tables |
| 1 | `t1-bronze-to-silver` | CONDITIONAL | bronze = SUCCEEDED | 3 silver jobs |
| 2 | `t2-silver-to-dims` | CONDITIONAL | all 3 silver = SUCCEEDED | 4 dim jobs |
| 3 | `t3-dims-to-fact` | CONDITIONAL | all 4 dims = SUCCEEDED | fact_orders |
| 4 | `t4-fact-to-metrics` | CONDITIONAL | fact_orders = SUCCEEDED | 9 metric jobs |

Glue Workflows allow only **one starting trigger** per workflow. I used a
SCHEDULED trigger (not an ON_DEMAND) — the `aws glue start-workflow-run` API
can still kick off the workflow manually regardless of the trigger type, so
this covers both daily production runs and ad-hoc testing from a single
trigger.

---

## 2. Deployment: `glue_jobs/workflow/create_workflow.py`

Idempotent Python script using boto3. Creates the workflow if it doesn't
exist, then **deletes and recreates every trigger** on each run so the final
state is fully deterministic regardless of prior state.

```bash
python glue_jobs/workflow/create_workflow.py
```

Output (after small fixes — see Section 3):
```
  EXISTS: workflow retail-daily-pipeline
  DELETED: legacy gp-t0-ondemand trigger
  CREATED: trigger t0-schedule (SCHEDULED) -> ['bronze-ingest-all-tables']
  CREATED: trigger t1-bronze-to-silver (CONDITIONAL) -> ['clean-order-items', 'clean-order-item-options', 'clean-date-dim']
  CREATED: trigger t2-silver-to-dims (CONDITIONAL) -> ['build-dim-date', 'build-dim-customer', 'build-dim-restaurant', 'build-dim-menu-item']
  CREATED: trigger t3-dims-to-fact (CONDITIONAL) -> ['build-fact-orders']
  CREATED: trigger t4-fact-to-metrics (CONDITIONAL) -> ['build-gold-clv-snapshot', 'build-gold-rfm', 'build-gold-churn', ...]

Workflow ready. Manually run with:
  aws glue start-workflow-run --name retail-daily-pipeline --profile retail-chain

The SCHEDULED trigger is ACTIVATED and will fire daily at 02:00 UTC.
```

---

## 3. Two Gotchas Hit During Creation

### 3.1 ON_DEMAND cannot have `StartOnCreation=True`
First attempt tried to create both an ON_DEMAND and a SCHEDULED trigger as
entry points, with `StartOnCreation=True` for both. AWS rejected:
```
InvalidInputException: Starting trigger on create is not supported for
ON_DEMAND trigger type.
```
**Fix**: pass `StartOnCreation=True` only for non-ON_DEMAND triggers.

### 3.2 Glue allows only ONE starting trigger per workflow
After fixing #1, next attempt hit:
```
InvalidInputException: Workflow retail-daily-pipeline already has a
starting trigger: gp-t0-ondemand
```
Glue Workflows enforce a single starting trigger. Can't have both ON_DEMAND
and SCHEDULED as entry points.

**Fix**: use only the SCHEDULED trigger as the entry point. `start-workflow-run`
API can still kick off a manual test run — it doesn't require an ON_DEMAND
trigger to be present.

### 3.3 Windows cp1252 Unicode on `→` arrow
Print statement had a `→` that crashed on Windows cp1252 encoding. Replaced
with `->` (ASCII) to stay portable.

---

## 4. End-to-End Test Run — RESULTS

### 4.1 How it was triggered
```bash
aws glue start-workflow-run --name retail-daily-pipeline --profile retail-chain
# → RunId: wr_74fe399f505aa79fe069cdb10f3b8aa6969303b04eda0ca3569f61b6dce0f00d
```

### 4.2 Timing
| Metric | Value |
|---|---|
| Start | 2026-04-11 22:39:51 UTC |
| End | 2026-04-11 22:53:12 UTC |
| **Wall-clock total** | **801 seconds (13 min 21 sec)** |
| Sum of individual job times | 2,010 seconds (~33.5 min) |
| Time saved by parallelism | 1,209 seconds (**20 min / 60%**) |
| TotalActions / Succeeded / Failed | 18 / 18 / 0 |

### 4.3 Per-job execution times
```
Level 0 (Bronze):
  bronze-ingest-all-tables         SUCCEEDED   138s

Level 1 (Silver, 3 parallel — wall-time ≈ max = 123s):
  clean-order-items                SUCCEEDED   107s
  clean-order-item-options         SUCCEEDED   123s   ← slowest
  clean-date-dim                   SUCCEEDED    81s

Level 2 (Dims, 4 parallel — wall-time ≈ max = 99s):
  build-dim-date                   SUCCEEDED    88s
  build-dim-customer               SUCCEEDED    88s
  build-dim-restaurant             SUCCEEDED    99s   ← slowest
  build-dim-menu-item              SUCCEEDED    74s

Level 3 (Fact):
  build-fact-orders                SUCCEEDED    95s

Level 4 (Metrics, 9 parallel — wall-time ≈ max = 157s):
  build-gold-clv-snapshot          SUCCEEDED   123s
  build-gold-rfm                   SUCCEEDED   117s
  build-gold-churn                 SUCCEEDED   107s
  build-gold-sales-daily           SUCCEEDED   120s
  build-gold-sales-weekly          SUCCEEDED   121s
  build-gold-sales-monthly         SUCCEEDED   126s
  build-gold-loyalty               SUCCEEDED   120s
  build-gold-location-perf         SUCCEEDED   126s
  build-gold-upsell                SUCCEEDED   157s   ← slowest
```

Total wall-clock = ~138 + 123 + 99 + 95 + 157 + overhead = ~800s, matching
observed 801s. The 60% time reduction from parallelism confirms the
conditional triggers fired correctly at each layer boundary.

### 4.4 Progression observed via monitor events

```
status=RUNNING succ=0  fail=0 running=0  ← workflow started
status=RUNNING succ=0  fail=0 running=1  ← bronze starts
status=RUNNING succ=1  fail=0 running=0  ← bronze SUCCEEDED, T1 firing
status=RUNNING succ=1  fail=0 running=3  ← 3 silvers parallel
status=RUNNING succ=3  fail=0 running=1  ← 2 silvers done
status=RUNNING succ=4  fail=0 running=0  ← all silvers done, T2 firing
status=RUNNING succ=4  fail=0 running=4  ← 4 dims parallel
status=RUNNING succ=7  fail=0 running=1  ← 3 dims done
status=RUNNING succ=8  fail=0 running=0  ← all dims done, T3 firing
status=RUNNING succ=8  fail=0 running=1  ← fact_orders running
status=RUNNING succ=9  fail=0 running=0  ← fact done, T4 firing
status=RUNNING succ=9  fail=0 running=9  ← 9 metrics parallel!
status=RUNNING succ=15 fail=0 running=3  ← 6 metrics done
status=RUNNING succ=17 fail=0 running=1  ← 8 metrics done
status=COMPLETED succ=18 fail=0 running=0  ← DONE ✓
```

Every conditional trigger fired exactly when expected.

---

## 5. How It Works (For Future Reference)

### Manual run (ad-hoc)
```bash
aws glue start-workflow-run \
    --name retail-daily-pipeline \
    --profile retail-chain
```

### Check status
```bash
aws glue get-workflow-run \
    --name retail-daily-pipeline \
    --run-id <RUN_ID> \
    --profile retail-chain \
    --query "Run.{Status:Status,Stats:Statistics}"
```

### View graph
```bash
aws glue get-workflow-run \
    --name retail-daily-pipeline \
    --run-id <RUN_ID> \
    --include-graph \
    --profile retail-chain
```

### Disable scheduled runs (if needed for cost control)
```bash
aws glue stop-trigger --name t0-schedule --profile retail-chain
```

### Re-enable scheduled runs
```bash
aws glue start-trigger --name t0-schedule --profile retail-chain
```

### Recreate everything (idempotent)
```bash
python glue_jobs/workflow/create_workflow.py
```

---

## 6. Current Project Status

| Step | Status |
|---|---|
| 1. Data exploration | DONE |
| 2. Data quality report | DONE |
| 3. Architecture v3 (SDD + draw.io) | DONE |
| 4. Bronze ETL (1 job) | DONE + running in workflow |
| 4. Silver ETL (3 jobs) | DONE + running in workflow |
| 5. Gold layer (14 jobs) | DONE + running in workflow |
| 6. Streamlit dashboard (8 pages) | DONE + AppTest verified |
| 6a. **Glue Workflow** | DONE + end-to-end run SUCCEEDED |
| 6b. **Daily 02:00 UTC trigger** | DONE (Glue SCHEDULED trigger) |
| 7. CI/CD + final submission | PENDING |

---

## 7. Files Added

```
glue_jobs/workflow/
└── create_workflow.py    (205 lines — idempotent workflow + 5 trigger setup)
```

---

## 8. What Remains — Step 7: CI/CD + Submission

1. Final documentation polish — update SDD docx to match the actual v3
   implementation (currently has some v1 Aurora/EventBridge references)
2. Video / presentation walk-through of the complete pipeline + dashboard
3. Git commit history + README at project root
4. Submission package

---

## 9. Running Total: 35+ Consecutive Glue Job Successes

Counting jobs that ran inside today's test of this workflow run (18 jobs)
plus the earlier manual runs of all the same jobs (which also all succeeded
first try after the Bronze debug marathon), we have:

- Bronze: 1 job × 2 runs = 2
- Silver: 3 jobs × 2 runs = 6
- Gold dims: 4 jobs × 2 runs = 8
- Gold fact: 1 job × 2 runs = 2
- Gold metrics: 9 jobs × 2 runs = 18

**Total: 36 successful Glue job executions**, zero failures, after the
initial 9-error Bronze marathon. The pattern-based approach (self-contained
scripts, DeltaTable API, defensive dedup, proper exit pattern, manifests +
alerts) is fully validated.
