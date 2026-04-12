"""
build-dim-customer.py
Gold dimension: customer profile with identity + lifetime aggregates.

Serves:
  - gold_clv_snapshot (PRIMARY) — customer identity + total_spend baseline
  - gold_rfm_segments              — customer-level recency/frequency/monetary
  - gold_churn_indicators          — customer activity history
  - gold_loyalty_comparison        — loyalty flag per customer
  - gold_location_perf             — primary restaurant per customer

Grain: 1 row per USER_ID (anonymous orders excluded — they cannot be customer-scored)
Columns: user_id, first_order_date, last_order_date, tenure_days,
         total_orders, total_spend, avg_order_value, is_loyalty,
         primary_restaurant_id, clv_tier

clv_tier: percentile-based global tier across all customers
    top 20%  → "High"
    next 60% → "Medium"
    bottom 20% → "Low"

Job parameters: --S3_BUCKET, --REGION, --SNS_TOPIC_ARN, --datalake-formats delta
"""

import json
import logging
import sys
from datetime import date, datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F


# ─────────────────────────────────────────────
# HELPERS (self-contained)
# ─────────────────────────────────────────────

def get_logger(job_name):
    logger = logging.getLogger(job_name)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(h)
    logger.setLevel(logging.INFO)
    return logger


def write_manifest(s3_client, bucket, job_name, table_results, run_date):
    overall = "SUCCESS" if all(v == "SUCCESS" for v in table_results.values()) else "FAILED"
    manifest = {
        "job": job_name, "run_date": run_date,
        "run_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "overall_status": overall, "tables": table_results
    }
    key = f"manifests/{job_name}/{run_date}/status.json"
    s3_client.put_object(Bucket=bucket, Key=key,
                         Body=json.dumps(manifest, indent=2),
                         ContentType="application/json")
    return key


def send_failure_alert(sns_topic_arn, region, job_name, run_date, table_results):
    failed = {k: v for k, v in table_results.items() if v != "SUCCESS"}
    msg = (f"PIPELINE FAILURE — {job_name}\nRun date: {run_date}\n\n"
           f"Failed tables:\n" + "\n".join(f"  • {t}: {e}" for t, e in failed.items())
           + "\n\nCheck CloudWatch logs and S3 manifest.")
    sns = boto3.client("sns", region_name=region)
    sns.publish(TopicArn=sns_topic_arn,
                Subject=f"[ALERT] Pipeline failure: {job_name} — {run_date}", Message=msg)


# ─────────────────────────────────────────────
# JOB INIT
# ─────────────────────────────────────────────

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "REGION", "SNS_TOPIC_ARN"])
JOB_NAME  = args["JOB_NAME"]
S3_BUCKET = args["S3_BUCKET"]
REGION    = args["REGION"]
SNS_TOPIC = args["SNS_TOPIC_ARN"]
RUN_DATE  = date.today().isoformat()
RUN_TS    = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

SILVER_ORDERS = f"s3://{S3_BUCKET}/silver/order_items"
GOLD_PATH     = f"s3://{S3_BUCKET}/gold/dim_customer"
TABLE_LABEL   = "dim_customer"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

logger = get_logger(JOB_NAME)
logger.info(f"Job started — run_date={RUN_DATE}")


# ─────────────────────────────────────────────
# MAIN LOGIC
# ─────────────────────────────────────────────

table_results = {}

try:
    logger.info(f"━━━ Processing table: {TABLE_LABEL} ━━━")

    # Step 1: Read Silver, filter to known customers only
    logger.info(f"Reading Silver order_items, filtering anonymous (USER_ID IS NULL)")
    orders = (
        spark.read.format("delta").load(SILVER_ORDERS)
        .filter(F.col("USER_ID").isNotNull())
    )
    total_rows = orders.count()
    logger.info(f"  Rows with known USER_ID: {total_rows:,}")

    # Step 2: Primary restaurant per customer (mode via count-rank)
    logger.info("Computing primary restaurant per customer (most-ordered-from)")
    restaurant_counts = (
        orders
        .groupBy("USER_ID", "RESTAURANT_ID")
        .agg(F.count("*").alias("visit_count"))
    )
    primary_w = Window.partitionBy("USER_ID").orderBy(F.col("visit_count").desc())
    primary_restaurant = (
        restaurant_counts
        .withColumn("rn", F.row_number().over(primary_w))
        .filter(F.col("rn") == 1)
        .select("USER_ID",
                F.col("RESTAURANT_ID").alias("primary_restaurant_id"))
    )

    # Step 3: Customer-level aggregates
    logger.info("Aggregating customer lifetime metrics")
    customer_agg = (
        orders
        .groupBy("USER_ID")
        .agg(
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("ORDER_ID").alias("total_orders"),
            F.sum("line_item_revenue").alias("total_spend"),
            F.max(F.col("IS_LOYALTY").cast("int")).alias("loyalty_flag_int"),
        )
        .withColumn("tenure_days",
                    F.datediff(F.col("last_order_date"), F.col("first_order_date")))
        .withColumn("avg_order_value",
                    F.round(F.col("total_spend") / F.col("total_orders"), 2))
        .withColumn("is_loyalty", F.col("loyalty_flag_int") == 1)
        .drop("loyalty_flag_int")
    )

    # Step 4: Join primary restaurant
    customer_agg = customer_agg.join(primary_restaurant, on="USER_ID", how="left")

    # Step 5: CLV tier — ntile(5) on total_spend, global across all customers
    #   Tile 5 (top 20%) → High
    #   Tiles 2-4 (mid 60%) → Medium
    #   Tile 1 (bottom 20%) → Low
    logger.info("Computing CLV tier (High/Medium/Low) via ntile(5) on total_spend")
    ntile_w = Window.orderBy(F.col("total_spend").asc_nulls_first())
    customer_agg = (
        customer_agg
        .withColumn("spend_quintile", F.ntile(5).over(ntile_w))
        .withColumn("clv_tier",
                    F.when(F.col("spend_quintile") == 5, F.lit("High"))
                     .when(F.col("spend_quintile") == 1, F.lit("Low"))
                     .otherwise(F.lit("Medium")))
        .drop("spend_quintile")
    )

    # Step 6: Finalize schema
    customer_dim = (
        customer_agg
        .withColumnRenamed("USER_ID", "user_id")
        .withColumn("gold_ingestion_ts", F.lit(RUN_TS))
        .select(
            "user_id", "first_order_date", "last_order_date", "tenure_days",
            "total_orders", "total_spend", "avg_order_value", "is_loyalty",
            "primary_restaurant_id", "clv_tier", "gold_ingestion_ts"
        )
    )

    unique_customers = customer_dim.count()
    high_tier = customer_dim.filter(F.col("clv_tier") == "High").count()
    loyalty_count = customer_dim.filter(F.col("is_loyalty")).count()
    logger.info(
        f"  Unique customers: {unique_customers:,} | "
        f"High tier: {high_tier:,} | Loyalty: {loyalty_count:,}"
    )

    # Step 7: Write Gold (full overwrite)
    logger.info(f"Writing to {GOLD_PATH} (OVERWRITE)")
    (
        customer_dim.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_PATH)
    )

    table_results[TABLE_LABEL] = "SUCCESS"
    logger.info(f"✓ {TABLE_LABEL} — SUCCESS")

except Exception as e:
    error_msg = str(e)
    table_results[TABLE_LABEL] = f"FAILED: {error_msg}"
    logger.error(f"✗ {TABLE_LABEL} — FAILED: {error_msg}")


# ─────────────────────────────────────────────
# MANIFEST + EXIT
# ─────────────────────────────────────────────

s3_client = boto3.client("s3", region_name=REGION)
manifest_key = write_manifest(s3_client, S3_BUCKET, JOB_NAME, table_results, RUN_DATE)
logger.info(f"Manifest → s3://{S3_BUCKET}/{manifest_key}")

if any(v != "SUCCESS" for v in table_results.values()):
    send_failure_alert(SNS_TOPIC, REGION, JOB_NAME, RUN_DATE, table_results)
    job.commit()
    raise Exception(f"{TABLE_LABEL} Gold ETL failed — check manifest and CloudWatch logs.")
else:
    job.commit()
