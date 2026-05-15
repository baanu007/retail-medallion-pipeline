"""
clean-order-item-options.py
Silver layer ETL — cleans and conforms order_item_options from Bronze → Silver.

Architecture:
    S3 Bronze (raw Delta) ──read──▶ PySpark cleaning ──Delta MERGE──▶ S3 Silver

Source:  s3://{bucket}/bronze/order_item_options/
Target:  s3://{bucket}/silver/order_item_options/
Merge key: ORDER_ID + LINEITEM_ID + OPTION_GROUP_NAME + OPTION_NAME
           (true composite PK — verified in Bronze debug)

Business context:
    This table feeds the UPSELL ANALYSIS metric in Gold.
    Original requirements said: "Use option_price < 0 to detect discounts."
    Data exploration confirmed ZERO negative prices exist in the dataset.
    We PIVOTED to upsell analysis: compare FREE ($0, 66.3% of options) vs
    PAID add-ons (33.7%) and identify top-grossing paid options.

Operations performed (the "what Silver does" for this table):
    1. Read Bronze Delta table
    2. Drop rows with NULL ORDER_ID or NULL LINEITEM_ID (unjoinable)
    3. Type casting:
         - OPTION_PRICE    → DECIMAL(10,2)
         - OPTION_QUANTITY → INT   (always 1 — zero variance — still kept for formula)
    4. Standardize OPTION_GROUP_NAME — reduces 133 groups to ~100 by fixing
       case inconsistency + "CT " prefix duplicates + whitespace:
         - "BOTTLE DEPOSIT" and "Bottle Deposit" → "Bottle Deposit"
         - "CT Bacon Egg And Cheese Options" → "Bacon Egg And Cheese Options"
         - "add" and "Add" → "Add"
    5. Strip whitespace on OPTION_NAME
    6. Filter obvious junk: negative prices (shouldn't exist, but defensive)
         - OPTION_PRICE    >= 0
         - OPTION_QUANTITY >= 1
    7. Derived columns for downstream Gold jobs:
         - is_paid_option = OPTION_PRICE > 0        (FREE vs PAID classification)
         - option_revenue = OPTION_PRICE * OPTION_QUANTITY
    8. Log row count delta (Bronze → Silver) to detect silent data loss
    9. Dedupe on merge keys (safety net), then MERGE into Silver Delta
   10. Write manifest + SNS alert on failure

NOTE on orphan options:
    Data exploration found 28 option rows (0.01%) that reference ORDER_IDs not
    present in order_items. We KEEP these in Silver — filtering them would
    hide source-system bugs. They'll be naturally excluded when Gold JOINs
    options to line items to build fact_orders.

Job parameters required:
    --S3_BUCKET       <BUCKET_NAME>
    --REGION          us-east-1
    --SNS_TOPIC_ARN   arn:aws:sns:us-east-1:...:pipeline-failure-alerts
    --datalake-formats delta
"""

import json
import logging
import sys
from datetime import date, datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType


# ─────────────────────────────────────────────
# HELPER FUNCTIONS (self-contained)
# ─────────────────────────────────────────────

def get_logger(job_name):
    logger = logging.getLogger(job_name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def write_manifest(s3_client, bucket, job_name, table_results, run_date):
    overall = (
        "SUCCESS" if all(v == "SUCCESS" for v in table_results.values())
        else "PARTIAL_FAILURE" if any(v == "SUCCESS" for v in table_results.values())
        else "FAILED"
    )
    manifest = {
        "job": job_name,
        "run_date": run_date,
        "run_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "overall_status": overall,
        "tables": table_results
    }
    key = f"manifests/{job_name}/{run_date}/status.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json"
    )
    return key


def send_failure_alert(sns_topic_arn, region, job_name, run_date, table_results):
    failed = {k: v for k, v in table_results.items() if v != "SUCCESS"}
    message = (
        f"PIPELINE FAILURE — {job_name}\n"
        f"Run date: {run_date}\n\n"
        f"Failed tables:\n"
        + "\n".join(f"  • {tbl}: {err}" for tbl, err in failed.items())
        + "\n\nCheck CloudWatch logs and S3 manifest for details."
    )
    sns = boto3.client("sns", region_name=region)
    sns.publish(
        TopicArn=sns_topic_arn,
        Subject=f"[ALERT] Pipeline failure: {job_name} — {run_date}",
        Message=message
    )


def delta_table_exists(spark, s3_path):
    try:
        spark.read.format("delta").load(s3_path).limit(1).count()
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────
# JOB ARGUMENTS
# ─────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "S3_BUCKET",
    "REGION",
    "SNS_TOPIC_ARN",
])

JOB_NAME    = args["JOB_NAME"]
S3_BUCKET   = args["S3_BUCKET"]
REGION      = args["REGION"]
SNS_TOPIC   = args["SNS_TOPIC_ARN"]
RUN_DATE    = date.today().isoformat()
RUN_TS      = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/order_item_options"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/order_item_options"
MERGE_KEYS  = ["ORDER_ID", "LINEITEM_ID", "OPTION_GROUP_NAME", "OPTION_NAME"]
TABLE_LABEL = "order_item_options"


# ─────────────────────────────────────────────
# SPARK + GLUE INITIALISATION
# ─────────────────────────────────────────────

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(JOB_NAME, args)

logger = get_logger(JOB_NAME)
logger.info(f"Job started — run_date={RUN_DATE}, bucket={S3_BUCKET}")
logger.info(f"Source: {BRONZE_PATH}")
logger.info(f"Target: {SILVER_PATH}")


# ─────────────────────────────────────────────
# OPTION_GROUP_NAME STANDARDIZATION
# ─────────────────────────────────────────────

def standardize_option_group(df):
    """
    Cleans OPTION_GROUP_NAME in three passes:
      1. Trim whitespace.
      2. Strip leading 'CT ' prefix (CT-variant duplicates).
      3. Title-case so "BOTTLE DEPOSIT" / "Bottle Deposit" / "bottle deposit"
         all collapse to "Bottle Deposit".
    """
    col = F.col("OPTION_GROUP_NAME")
    cleaned = F.trim(col)
    # Remove leading "CT " (case-insensitive)
    cleaned = F.regexp_replace(cleaned, r"(?i)^CT\s+", "")
    # Title-case — Spark's initcap lower-cases then capitalizes first letter
    cleaned = F.initcap(cleaned)
    return df.withColumn("OPTION_GROUP_NAME", cleaned)


# ─────────────────────────────────────────────
# MAIN CLEANING LOGIC
# ─────────────────────────────────────────────

table_results = {}

try:
    logger.info(f"━━━ Processing table: {TABLE_LABEL} ━━━")

    # ── Step 1: Read Bronze ──────────────────────────────
    logger.info(f"Reading Bronze Delta from {BRONZE_PATH}")
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    bronze_count = bronze_df.count()
    logger.info(f"  Bronze row count: {bronze_count:,}")

    # ── Step 2: Drop rows with NULL primary keys ─────────
    logger.info("Dropping rows with NULL ORDER_ID or LINEITEM_ID")
    df = bronze_df.filter(
        F.col("ORDER_ID").isNotNull()
        & F.col("LINEITEM_ID").isNotNull()
        & F.col("OPTION_GROUP_NAME").isNotNull()
        & F.col("OPTION_NAME").isNotNull()
    )
    after_pk_filter = df.count()
    logger.info(f"  After PK null filter: {after_pk_filter:,} rows "
                f"({bronze_count - after_pk_filter:,} removed)")

    # ── Step 3: Type casting ─────────────────────────────
    logger.info("Casting types: OPTION_PRICE → DECIMAL(10,2), OPTION_QUANTITY → INT")
    df = (
        df
        .withColumn("OPTION_PRICE",    F.col("OPTION_PRICE").cast(DecimalType(10, 2)))
        .withColumn("OPTION_QUANTITY", F.col("OPTION_QUANTITY").cast(IntegerType()))
    )

    # ── Step 4: Standardize OPTION_GROUP_NAME ────────────
    logger.info("Standardizing OPTION_GROUP_NAME: trim, strip 'CT ' prefix, title-case")
    df = standardize_option_group(df)

    # ── Step 5: Trim OPTION_NAME ─────────────────────────
    logger.info("Trimming whitespace from OPTION_NAME")
    df = df.withColumn("OPTION_NAME", F.trim(F.col("OPTION_NAME")))

    # ── Step 6: Filter junk (defensive — should never hit) ──
    logger.info("Filtering: OPTION_PRICE >= 0 AND OPTION_QUANTITY >= 1")
    df = df.filter(
        (F.col("OPTION_PRICE") >= F.lit(0).cast(DecimalType(10, 2)))
        & (F.col("OPTION_QUANTITY") >= 1)
    )
    after_value_filter = df.count()
    logger.info(f"  After value filter: {after_value_filter:,} rows "
                f"({after_pk_filter - after_value_filter:,} removed)")

    # ── Step 7: Derived columns ──────────────────────────
    logger.info("Adding derived columns: is_paid_option, option_revenue")
    df = (
        df
        .withColumn("is_paid_option", F.col("OPTION_PRICE") > 0)
        .withColumn("option_revenue",
                    (F.col("OPTION_PRICE") * F.col("OPTION_QUANTITY"))
                    .cast(DecimalType(12, 2)))
        .withColumn("silver_ingestion_ts", F.lit(RUN_TS))
    )

    # ── Step 8: Dedupe safety net ────────────────────────
    before_dedup = df.count()
    df = df.dropDuplicates(MERGE_KEYS)
    after_dedup = df.count()
    if before_dedup != after_dedup:
        logger.info(f"  Deduped {before_dedup - after_dedup} duplicate rows on merge keys "
                    f"({before_dedup} → {after_dedup}).")

    silver_count = after_dedup
    logger.info(
        f"  Row count summary — Bronze: {bronze_count:,} → Silver: {silver_count:,} "
        f"({bronze_count - silver_count:,} removed, "
        f"{(silver_count / bronze_count * 100):.2f}% retained)"
    )

    # ── Step 9: Write Silver via Delta MERGE ────────────
    if not delta_table_exists(spark, SILVER_PATH):
        logger.info(f"No existing Silver table — doing initial write.")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .partitionBy("ingestion_date")
              .save(SILVER_PATH)
        )
        logger.info(f"  {TABLE_LABEL}: initial Silver write complete.")
    else:
        on_condition = " AND ".join([f"target.{k} = source.{k}" for k in MERGE_KEYS])
        logger.info(f"Executing DeltaTable merge on {on_condition}")
        (
            DeltaTable.forPath(spark, SILVER_PATH)
            .alias("target")
            .merge(df.alias("source"), on_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"  {TABLE_LABEL}: Silver merge complete.")

    table_results[TABLE_LABEL] = "SUCCESS"
    logger.info(f"✓ {TABLE_LABEL} — SUCCESS")

except Exception as e:
    error_msg = str(e)
    table_results[TABLE_LABEL] = f"FAILED: {error_msg}"
    logger.error(f"✗ {TABLE_LABEL} — FAILED: {error_msg}")


# ─────────────────────────────────────────────
# MANIFEST + ALERT + EXIT
# ─────────────────────────────────────────────

s3_client    = boto3.client("s3", region_name=REGION)
manifest_key = write_manifest(s3_client, S3_BUCKET, JOB_NAME, table_results, RUN_DATE)
logger.info(f"Status manifest written → s3://{S3_BUCKET}/{manifest_key}")

any_failed = any(v != "SUCCESS" for v in table_results.values())

if any_failed:
    logger.error(f"{TABLE_LABEL} failed. Sending SNS alert.")
    send_failure_alert(SNS_TOPIC, REGION, JOB_NAME, RUN_DATE, table_results)
    job.commit()
    raise Exception(f"{TABLE_LABEL} Silver ETL failed — check manifest and CloudWatch logs.")
else:
    logger.info(f"{TABLE_LABEL} Silver ETL complete. Pipeline continues.")
    job.commit()
