# Session Report — April 11, 2026
## Bronze Ingestion: Debug Marathon → Full Success

---

## TL;DR

After ~2 hours of debugging through 9 sequential errors, the Bronze Glue job
`bronze-ingest-all-tables` is now running **SUCCEEDED** end-to-end. All 3 source
tables land cleanly in S3 as Delta Lake tables with idempotent MERGE/UPSERT
semantics. Full ELI6 walkthrough document also produced.

**Current state**: Bronze layer is DONE. Next session = Glue Workflow + EventBridge schedule, then Silver jobs.

---

## 1. What We Set Out To Do

Continue from April 9 session. Goal: deploy and run the Bronze Glue job built
in the previous session, verify data lands in S3, get the job showing SUCCEEDED
in the Glue console.

---

## 2. The 9 Errors — Full Debug Log

Every error below was hit, diagnosed, and fixed in sequence. They are
documented here because each one is a lesson that will apply to Silver and Gold
jobs as well.

### Error #1 — `ModuleNotFoundError: No module named 'delta'`

**Symptom**: Job failed at line 27 (`from delta.tables import DeltaTable`).

**Root cause**: Glue 4.0 does NOT include Delta Lake libraries by default. They
are an opt-in via a job parameter.

**Fix**: Added `--datalake-formats delta` to Job parameters in Glue console.
Also removed any manual `spark.conf.set(...)` lines (Glue handles this
automatically once the parameter is set).

---

### Error #2 — `ModuleNotFoundError: No module named 'utils'`

**Symptom**: Job failed at line 33/35 on `from utils import get_logger, ...`.
We had originally built `glue_jobs/common/utils.py` as a shared helpers file
and referenced it via `--extra-py-files`.

**Root cause**: Glue 4.0's `--extra-py-files` with individual `.py` files is
unreliable — the file doesn't always land on the Python path. Also tried
`sys.path.insert(0, "/tmp/glue_jobs/common")` — that path does not exist on
Glue worker nodes.

**Fix**: Merged ALL functions from `utils.py` directly into
`bronze-ingest-all-tables.py`. One self-contained script, zero external imports.
**This is now the project's standard pattern for Glue jobs** — inline helpers,
no shared modules, full self-containment per script.

---

### Error #3 — `LAUNCH ERROR: Error downloading from S3`

**Symptom**: Glue couldn't find the script file — `The specified key does not
exist`.

**Root cause**: The uploaded filename in S3 didn't match what the Glue job was
pointing to (rename mismatch during manual upload).

**Fix**: Re-uploaded via AWS CLI with exact filename:
```bash
aws s3 cp bronze-ingest-all-tables.py \
    s3://<BUCKET_NAME>/scripts/bronze-ingest-all-tables.py \
    --profile retail-chain
```

**Lesson**: Never trust manual console uploads for script name precision. Use
the CLI, and verify with `aws s3 ls`.

---

### Error #4 — `GlueArgumentError: --REGION, --SNS_TOPIC_ARN required`

**Symptom**: These parameters WERE set in the Glue console, but Glue said they
were missing.

**Root cause**: The AWS console had silently inserted hidden TAB characters
into parameter keys when we pressed Tab to navigate between fields. The keys
were literally `"--REGION\t"` and `"--SNS_TOPIC_ARN\t"` — invisible to human
eyes in the UI.

**Discovery**: Ran `aws glue get-job --query Job.DefaultArguments` from CLI and
saw the escaped `\t` characters in the JSON output.

**Fix**: Used `aws glue update-job` with a clean JSON payload replacing all
parameters with correct keys (no tabs).

**Lesson**: When a Glue parameter error makes zero sense, inspect the raw JSON
via CLI. The console UI hides invisible characters.

---

### Error #5 — `KeyError: 'dbname'`

**Symptom**: `creds['dbname']` failed inside `build_jdbc_url()`.

**Root cause**: The Secrets Manager secret (`retail-chain/aurora/credentials`)
had `username`, `password`, `host`, `port`, `dbInstanceIdentifier` — but no
`dbname` key.

**Fix**: Updated the secret via CLI to add the missing key:
```bash
aws secretsmanager update-secret \
    --secret-id retail-chain/aurora/credentials \
    --secret-string '{"username":"...","password":"...","host":"...","port":"1433","dbname":"retail_chain"}'
```

**Lesson**: When writing code that reads secrets, document the expected schema
at the top of the file so secret/code drift is obvious.

---

### Error #6 — `SYSTEM_EXIT_ERROR: SystemExit: 0`

**Symptom**: Data landed successfully in S3, manifest showed SUCCESS, but the
Glue console showed the job as FAILED in red.

**Root cause**: The script ended with `sys.exit(0)`. Glue interprets ANY
`sys.exit()` call as abnormal termination, even exit code 0. This is a known
Glue quirk.

**Fix**: Removed both `sys.exit(0)` and `sys.exit(1)`.
- On success: just call `job.commit()` and let the script end naturally.
- On failure: `job.commit()` followed by `raise Exception("...")` — Glue
  correctly marks this as FAILED with a clear error message.

**This is the standard Glue job exit pattern going forward**:
```python
if any_failed:
    send_failure_alert(...)
    job.commit()
    raise Exception("One or more tables failed — check manifest and CloudWatch logs.")
else:
    job.commit()
    # let script end naturally, no sys.exit()
```

---

### Error #7 — `AccessDeniedException: glue:GetDatabase`

**Symptom**: `order_items` table failed with IAM access denied when Delta Lake
tried to verify the default Glue catalog database exists.

**Root cause**: The IAM role `AWSGlueServiceRole-retail-chain` had
`SecretsManagerReadWrite`, `CloudWatchFullAccess`, `AmazonSNSFullAccess`,
`AmazonS3FullAccess` — but no Glue catalog permissions. Delta Lake on Glue
always checks the catalog during write operations, even if you don't use
catalog tables directly.

**Fix**: Attached the AWS-managed `AWSGlueServiceRole` policy:
```bash
aws iam attach-role-policy \
    --role-name AWSGlueServiceRole-retail-chain \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
```

**Lesson**: Any Glue job touching Delta Lake MUST have the `AWSGlueServiceRole`
managed policy attached. This is a hard requirement, not optional.

---

### Error #8 — `Unsupported data source type for direct query on files: delta; line 2 pos 19`

**Symptom**: Even with all IAM and module issues resolved, the SQL MERGE
statement failed with "Unsupported data source type: delta" at the exact
position of `delta.\`path\`` in the MERGE SQL.

**Root cause**: The `MERGE INTO delta.\`s3://...\`` SQL syntax requires the
Delta SQL parser extension to be registered in the Spark session. In Glue 4.0,
even with `--datalake-formats delta` set, the SQL parser doesn't reliably
recognize the `delta.\`path\`` prefix for MERGE statements. DataFrame API
(`spark.read.format("delta")`) works fine, but SQL `MERGE INTO delta.` does not.

**Fix**: Switched entirely from SQL MERGE to the Python DeltaTable API:

Before (broken):
```python
merge_sql = f"""
    MERGE INTO delta.`{s3_path}` AS target
    USING {view_name} AS source
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
"""
spark.sql(merge_sql)
```

After (working):
```python
from delta.tables import DeltaTable

on_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
(
    DeltaTable.forPath(spark, s3_path)
    .alias("target")
    .merge(df.alias("source"), on_condition)
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

**Lesson**: For Delta MERGE in Glue 4.0, ALWAYS use `DeltaTable.forPath().merge()`
— never `spark.sql("MERGE INTO delta.\`path\`")`. The Python API is
self-sufficient and doesn't depend on SQL parser extensions.

---

### Error #9 — `multipleSourceRowMatchingTargetRowInMergeException`

**Symptom**: Delta complained that multiple source rows matched the same target
row during MERGE.

**Root cause**: **WRONG MERGE KEYS.** I had originally set:
- `order_items` merge key = `["ORDER_ID"]`
- `order_item_options` merge key = `["ORDER_ID", "LINEITEM_ID"]`

But these are NOT the true primary keys:
- `order_items` grain is line item, so PK = `ORDER_ID + LINEITEM_ID`
  (one order has many line items)
- `order_item_options` grain is individual options, so PK = `ORDER_ID +
  LINEITEM_ID + OPTION_GROUP_NAME + OPTION_NAME` (one line item has many
  options)

With the wrong (too-narrow) keys, multiple source rows had the same
merge-key value, and Delta refused to guess which one should win.

**Fix** — two parts:

1. **Corrected merge keys** in `TABLES` config:
   ```python
   TABLES = [
       {
           "source_table": "dbo.order_items",
           "merge_keys":   ["ORDER_ID", "LINEITEM_ID"],
           ...
       },
       {
           "source_table": "dbo.order_item_options",
           "merge_keys":   ["ORDER_ID", "LINEITEM_ID", "OPTION_GROUP_NAME", "OPTION_NAME"],
           ...
       },
       ...
   ]
   ```

2. **Added defensive dedup** before merging:
   ```python
   before = df.count()
   df = df.dropDuplicates(merge_keys)
   after = df.count()
   if before != after:
       logger.info(f"  {table_name}: deduped {before - after} duplicate rows ({before} → {after}).")
   ```

   This is a safety net — if the source data has true exact duplicates on the
   key, we silently collapse them and log the count rather than crashing. This
   should be standard in ALL Bronze jobs going forward.

**Lesson**: Merge keys must be the TRUE composite primary key, verified via
`SELECT COUNT(*), COUNT(DISTINCT key_cols) FROM source`. If the counts don't
match, the key is wrong. Always add `dropDuplicates` before merging as a
safety net.

---

## 3. Final State of `bronze-ingest-all-tables.py`

Location: `glue_jobs/bronze/bronze-ingest-all-tables.py`

Key characteristics of the final working version:
- **Self-contained**: all helper functions inlined (no `common/utils.py` dependency)
- **Uses `DeltaTable.forPath().merge()`** Python API (NOT SQL MERGE)
- **Dedupes source** on merge keys before merging
- **Correct merge keys**: composite PKs for all tables
- **Clean exit**: `job.commit()` on success, `raise Exception()` on failure, NO `sys.exit()`
- **Per-table try/except**: one table's failure doesn't block others
- **Status manifest**: written to S3 at `manifests/bronze-ingest-all-tables/{date}/status.json`
- **SNS alert**: fires on any failure

Final merge keys:
| Table | Merge Keys | Strategy |
|---|---|---|
| dbo.order_items | ORDER_ID + LINEITEM_ID | merge |
| dbo.order_item_options | ORDER_ID + LINEITEM_ID + OPTION_GROUP_NAME + OPTION_NAME | merge |
| dbo.date_dim | (none — full overwrite) | overwrite |

---

## 4. AWS Infrastructure — Updated Status

| Component | Status |
|---|---|
| S3 bucket `<BUCKET_NAME>` with bronze/silver/gold folders | DONE |
| Secrets Manager `retail-chain/aurora/credentials` (with `dbname` key) | DONE |
| IAM Role `AWSGlueServiceRole-retail-chain` | DONE |
| IAM Role has `AWSGlueServiceRole` managed policy | DONE (added today) |
| IAM Role has S3, Secrets, SNS, CloudWatch full access | DONE |
| SNS Topic `pipeline-failure-alerts` | DONE |
| Scripts in S3: `s3://<BUCKET_NAME>/scripts/bronze-ingest-all-tables.py` | DONE |
| Glue Job `bronze-ingest-all-tables` configured and running | DONE |
| Glue Job parameters: S3_BUCKET, SECRET_NAME, REGION, SNS_TOPIC_ARN, `--datalake-formats delta` | DONE |
| Bronze run verified — all 3 tables in S3 | DONE |
| Glue Job shows SUCCEEDED status | DONE |
| Glue Workflow | PENDING |
| EventBridge daily 2AM UTC schedule | PENDING |
| Silver jobs (x3) | PENDING |
| Gold jobs (x13) | PENDING |
| Streamlit dashboard | PENDING |

---

## 5. Data Landed in S3 (Verified)

```
s3://<BUCKET_NAME>/bronze/
├── order_items/
│   ├── _delta_log/00000000000000000000.json      (3.6 KB)
│   └── ingestion_date=2026-04-11/
│       └── part-00000-xxxx.snappy.parquet         (11.2 MiB)
├── order_item_options/
│   ├── _delta_log/00000000000000000000.json      (2.4 KB)
│   └── ingestion_date=2026-04-11/
│       └── part-00000-xxxx.snappy.parquet         (5.5 MiB)
└── date_dim/
    ├── _delta_log/00000000000000000000.json
    └── ingestion_date=2026-04-11/
        └── part-00000-xxxx.snappy.parquet         (5.0 KB)

s3://<BUCKET_NAME>/manifests/bronze-ingest-all-tables/2026-04-11/status.json
(overall_status: SUCCESS — all 3 tables)
```

---

## 6. Deliverables Produced Today

1. **`glue_jobs/bronze/bronze-ingest-all-tables.py`** — final working Bronze
   ingestion script (self-contained, all fixes applied)

2. **`glue_jobs/bronze/bronze-ingest-all-tables-EXPLAINED.docx`** — 5-section
   ELI6 walkthrough:
   - Section 1: The Big Picture (toy box → warehouse analogy)
   - Section 2: Block-by-block code walkthrough with analogies
   - Section 3: All 9 errors with root cause + fix + analogy
   - Section 4: What happens when it runs (15-step table + S3 layout + manifest)
   - Section 5: The 7 Key Takeaways

3. **`glue_jobs/bronze/_generate_explained_doc.py`** — generator script for
   the .docx (kept in case we want to tweak/regenerate)

4. **This session file** (`session_20260411_bronze_debug_success.md`)

---

## 7. Key Patterns Established for All Future Glue Jobs

These are now the project-wide standards, validated through today's debugging:

### 7.1 Script Structure
- **Self-contained**: inline all helper functions, no shared `common/utils.py`
  imports
- **Single file per job** — one `.py` uploaded to `s3://bucket/scripts/`

### 7.2 Glue Job Parameters (Required for Every Job)
```
--JOB_NAME          (auto)
--S3_BUCKET         <BUCKET_NAME>
--SECRET_NAME       retail-chain/aurora/credentials
--REGION            us-east-1
--SNS_TOPIC_ARN     arn:aws:sns:us-east-1:<AWS_ACCOUNT_ID>:pipeline-failure-alerts
--datalake-formats  delta    ← REQUIRED for any Delta operations
```

### 7.3 IAM Role Must Have
- `AWSGlueServiceRole` (managed) — REQUIRED for Delta
- `AmazonS3FullAccess`
- `SecretsManagerReadWrite`
- `CloudWatchFullAccess`
- `AmazonSNSFullAccess`

### 7.4 Delta MERGE Pattern
ALWAYS use the Python DeltaTable API, NEVER SQL MERGE:
```python
from delta.tables import DeltaTable

df = df.dropDuplicates(merge_keys)  # safety net

on_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
(
    DeltaTable.forPath(spark, s3_path)
    .alias("target")
    .merge(df.alias("source"), on_condition)
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

### 7.5 Exit Pattern
```python
if any_failed:
    send_failure_alert(...)
    job.commit()
    raise Exception("One or more tables failed — check manifest and CloudWatch logs.")
else:
    job.commit()
    # no sys.exit() — let script end naturally
```

### 7.6 Merge Keys Verification
Before using ANY merge key, verify it's actually the primary key:
```sql
SELECT COUNT(*), COUNT(DISTINCT concat(key_col1, key_col2, ...)) FROM source;
```
If counts don't match, add more columns to the key. Always dedupe the source
DataFrame on the merge keys as a safety net before calling merge.

### 7.7 CLI Debugging Commands (Worth Memorizing)
```bash
# Profile is "retail-chain" (account <AWS_ACCOUNT_ID>)

# Inspect Glue job config (catches hidden-char bugs)
aws glue get-job --job-name <name> --profile retail-chain \
    --query Job.DefaultArguments

# Update Glue job params cleanly
aws glue update-job --job-name <name> --job-update file://clean.json \
    --profile retail-chain

# Verify script in S3
aws s3 ls s3://<BUCKET_NAME>/scripts/ --profile retail-chain
aws s3 cp s3://<BUCKET_NAME>/scripts/<file> - --profile retail-chain | tail -20

# Read manifest after run
aws s3 cp s3://<BUCKET_NAME>/manifests/<job>/<date>/status.json - \
    --profile retail-chain

# Check data landed
aws s3 ls s3://<BUCKET_NAME>/bronze/<table>/ --recursive --profile retail-chain

# Check IAM role policies
aws iam list-attached-role-policies --role-name <role> --profile retail-chain
```

---

## 8. Source Data Primary Keys (Memorize These)

Based on grain analysis during error #9 debugging:

| Table | Row Count | True Primary Key | Grain |
|---|---|---|---|
| dbo.order_items | 203,519 | ORDER_ID + LINEITEM_ID | One row per line item per order |
| dbo.order_item_options | 193,017 | ORDER_ID + LINEITEM_ID + OPTION_GROUP_NAME + OPTION_NAME | One row per option per line item |
| dbo.date_dim | 365 | date_key | One row per calendar day |

Silver and Gold jobs MUST respect these grains when joining/aggregating.

---

## 9. Next Session — Resume Here

### Immediate Next Steps
1. **Set up Glue Workflow** — string Bronze → Silver → Gold → Alert together
2. **Set up EventBridge rule** — daily 2AM UTC trigger → Glue Workflow
3. **Test the workflow** — manual trigger, verify Bronze runs, confirm
   downstream blocking works

### Then Build Silver Jobs (x3)
Apply DQ rules, cast types, clean nulls. Each is a separate Glue job for
surgical debugging. Use all patterns established above:

- `glue_jobs/silver/clean-order-items.py`
  - Cast ITEM_PRICE to DECIMAL, ITEM_QUANTITY to INT
  - Parse CREATION_TIME_UTC to timestamp
  - Reject rows with NULL ORDER_ID or LINEITEM_ID
  - Merge key: ORDER_ID + LINEITEM_ID
- `glue_jobs/silver/clean-order-item-options.py`
  - Cast OPTION_PRICE to DECIMAL, OPTION_QUANTITY to INT
  - Merge key: ORDER_ID + LINEITEM_ID + OPTION_GROUP_NAME + OPTION_NAME
- `glue_jobs/silver/clean-date-dim.py`
  - Cast is_weekend/is_holiday to BOOLEAN
  - Strategy: overwrite

### Then Gold Jobs (x13) — dims, fact, metrics
Defined in April 9 session file under "Final Glue Job File Structure".

### Then Streamlit Dashboard + CI/CD (Step 7)

---

## 10. Meta-Lessons from Today

1. **Error messages in Glue can be misleading.** `SYSTEM_EXIT_ERROR` with code 0
   means success — the error is in the exit mechanism, not the logic.
2. **Hidden characters kill you.** Tab characters from console UIs, whitespace
   in secret values — always inspect raw JSON via CLI.
3. **Delta Lake on Glue has quirks.** SQL `MERGE INTO delta.\`path\`` doesn't
   work reliably. Python API always does. Pick the Python API and stop trying.
4. **Merge keys must match data grain.** This is a data-modeling concern, not a
   code concern. Verify via `COUNT(*) vs COUNT(DISTINCT key)` BEFORE setting
   them in code.
5. **Defensive dedup is cheap insurance.** `df.dropDuplicates(merge_keys)`
   before merge = zero downside, catches dirty data silently.
6. **The AWS CLI is faster than the console.** For any non-trivial debugging,
   drop to CLI. It shows you what the console hides.

Every error today = permanent knowledge. Silver and Gold will go WAY faster
because all of these patterns are now established and tested.
