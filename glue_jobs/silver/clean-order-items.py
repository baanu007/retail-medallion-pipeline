"""
clean-order-items.py
Silver layer ETL — cleans and conforms order_items from Bronze → Silver.

Architecture:
    S3 Bronze (raw Delta) ──read──▶ PySpark cleaning ──Delta MERGE──▶ S3 Silver

Source:  s3://{bucket}/bronze/order_items/
Target:  s3://{bucket}/silver/order_items/
Merge key: ORDER_ID + LINEITEM_ID  (true composite PK — verified in Bronze debug)

Operations performed (the "what Silver does" for this table):
    1. Read Bronze Delta table
    2. Business filters:
         - Exclude APP_NAME = "Alltown Fresh - DEVELOPMENT"  (826 test rows)
         - Exclude RESTAURANT_ID = "6050e76361e498ca740bba6f"  (dev-only restaurant)
         - Exclude ITEM_CATEGORY = "Test Items"  (1 test row)
    3. Drop rows with NULL ORDER_ID or NULL LINEITEM_ID (unjoinable)
    4. Type casting:
         - CREATION_TIME_UTC → TIMESTAMP (from ISO 8601 string)
         - ITEM_PRICE        → DECIMAL(10,2)
         - ITEM_QUANTITY     → INT
         - IS_LOYALTY        → BOOLEAN (from "TRUE"/"FALSE" string)
    5. Outlier filtering (verified in data exploration — these distort 79% of raw revenue):
         - ITEM_PRICE    <= 50       (removes ~590 rows with prices up to $5,000)
         - ITEM_QUANTITY BETWEEN 1 AND 20  (removes ~84 rows + 1 zero-qty row)
    6. Category cleaning (URL-embedded values, typos, trailing digits):
         - "BBQ Plateshttps://..."     → "BBQ Plates"
         - "Bowls0"                     → "Bowls"
         - "Sandwiches`1"               → "Sandwiches"
         - "Drip Chttps://...offee"     → "Drip Coffee"
         - "Sqalads"                    → "Salads"
         - "Kid'shttps://..."           → "Kid's"
         - ""                           → "Unknown"
    7. Derived columns for downstream Gold jobs:
         - order_date          = DATE(CREATION_TIME_UTC)
         - order_hour          = HOUR(CREATION_TIME_UTC)  (for hourly sales trend)
         - line_item_revenue   = ITEM_PRICE * ITEM_QUANTITY
         - is_anonymous        = USER_ID IS NULL            (for CLV eligibility)
         - is_zero_price_flag  = ITEM_PRICE = 0             (for revenue completeness)
    8. Log row count delta (Bronze → Silver) to detect silent data loss
    9. Dedupe on merge keys, then MERGE into Silver Delta
   10. Write manifest + SNS alert on failure

Job parameters required:
    --S3_BUCKET       globalpartners-aws
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
# HELPER FUNCTIONS (self-contained — NO common/utils.py imports)
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

JOB_NAME   = args["JOB_NAME"]
S3_BUCKET  = args["S3_BUCKET"]
REGION     = args["REGION"]
SNS_TOPIC  = args["SNS_TOPIC_ARN"]
RUN_DATE   = date.today().isoformat()
RUN_TS     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/order_items"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/order_items"
MERGE_KEYS  = ["ORDER_ID", "LINEITEM_ID"]
TABLE_LABEL = "order_items"


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
# CATEGORY CLEANING MAP
# ─────────────────────────────────────────────
# Explicit mapping for dirty category values verified in data exploration.
# Everything else passes through untouched.

CATEGORY_CLEAN_MAP = {
    # (dirty substring pattern, clean replacement)
    # We use contains-based matching via when/otherwise below.
}


def clean_category_column(df):
    """
    Applies a deterministic series of WHEN/OTHERWISE rules to clean
    ITEM_CATEGORY based on the dirty values found in data exploration.
    """
    c = F.col("ITEM_CATEGORY")
    cleaned = (
        F.when(c.isNull() | (F.trim(c) == ""), F.lit("Unknown"))
         .when(c.contains("BBQ Plates"), F.lit("BBQ Plates"))
         .when(c == "Bowls0", F.lit("Bowls"))
         .when(c.rlike(r"^\s*Sandwiches.*$"), F.lit("Sandwiches"))
         .when(c.contains("Drip C") & c.contains("offee"), F.lit("Drip Coffee"))
         .when(c == "Sqalads", F.lit("Salads"))
         .when(c.startswith("Kid's") | (c == "Kids"), F.lit("Kid's"))
         .otherwise(F.trim(c))
    )
    return df.withColumn("ITEM_CATEGORY", cleaned)


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

    # ── Step 2: Business filters (exclude dev + test data) ──
    logger.info("Applying business filters: exclude DEVELOPMENT + dev restaurant + test items")
    df = bronze_df.filter(
        (F.col("APP_NAME") != "Alltown Fresh - DEVELOPMENT")
        & (F.col("RESTAURANT_ID") != "6050e76361e498ca740bba6f")
        & ((F.col("ITEM_CATEGORY") != "Test Items") | F.col("ITEM_CATEGORY").isNull())
    )
    after_biz_filter = df.count()
    logger.info(f"  After business filters: {after_biz_filter:,} rows "
                f"({bronze_count - after_biz_filter:,} removed)")

    # ── Step 3: Drop rows with NULL primary keys ─────────
    logger.info("Dropping rows with NULL ORDER_ID or NULL LINEITEM_ID")
    df = df.filter(F.col("ORDER_ID").isNotNull() & F.col("LINEITEM_ID").isNotNull())
    after_pk_filter = df.count()
    logger.info(f"  After PK null filter: {after_pk_filter:,} rows "
                f"({after_biz_filter - after_pk_filter:,} removed)")

    # ── Step 4: Type casting ─────────────────────────────
    logger.info("Casting types: timestamp, decimal, int, boolean")
    df = (
        df
        .withColumn("CREATION_TIME_UTC",
                    F.to_timestamp(F.col("CREATION_TIME_UTC")))
        .withColumn("ITEM_PRICE",
                    F.col("ITEM_PRICE").cast(DecimalType(10, 2)))
        .withColumn("ITEM_QUANTITY",
                    F.col("ITEM_QUANTITY").cast(IntegerType()))
        .withColumn("IS_LOYALTY",
                    F.when(F.upper(F.col("IS_LOYALTY").cast("string")) == "TRUE", F.lit(True))
                     .when(F.upper(F.col("IS_LOYALTY").cast("string")) == "FALSE", F.lit(False))
                     .otherwise(F.lit(None).cast("boolean")))
    )

    # ── Step 5: Outlier filtering ────────────────────────
    logger.info("Filtering outliers: ITEM_PRICE <= 50 AND ITEM_QUANTITY BETWEEN 1 AND 20")
    df = df.filter(
        (F.col("ITEM_PRICE") <= F.lit(50).cast(DecimalType(10, 2)))
        & (F.col("ITEM_PRICE") >= F.lit(0).cast(DecimalType(10, 2)))
        & (F.col("ITEM_QUANTITY") >= 1)
        & (F.col("ITEM_QUANTITY") <= 20)
    )
    after_outlier_filter = df.count()
    logger.info(f"  After outlier filter: {after_outlier_filter:,} rows "
                f"({after_pk_filter - after_outlier_filter:,} removed)")

    # ── Step 6: Clean ITEM_CATEGORY ──────────────────────
    logger.info("Cleaning ITEM_CATEGORY values (URLs, typos, trailing digits)")
    df = clean_category_column(df)

    # ── Step 7: Derived columns ──────────────────────────
    logger.info("Adding derived columns: order_date, order_hour, line_item_revenue, flags")
    df = (
        df
        .withColumn("order_date",         F.to_date(F.col("CREATION_TIME_UTC")))
        .withColumn("order_hour",         F.hour(F.col("CREATION_TIME_UTC")))
        .withColumn("line_item_revenue",
                    (F.col("ITEM_PRICE") * F.col("ITEM_QUANTITY"))
                    .cast(DecimalType(12, 2)))
        .withColumn("is_anonymous",       F.col("USER_ID").isNull())
        .withColumn("is_zero_price_flag", F.col("ITEM_PRICE") == 0)
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

    # ── Step 9: Write Silver via Delta MERGE (or initial overwrite) ──
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
