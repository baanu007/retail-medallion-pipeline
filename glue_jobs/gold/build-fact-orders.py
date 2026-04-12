"""
build-fact-orders.py
Gold fact table: order-level transactional facts.

Serves (FEEDS EVERY METRIC):
  - gold_clv_snapshot        — daily cumulative spend per customer
  - gold_rfm_segments        — recency/frequency/monetary per customer
  - gold_churn_indicators    — activity patterns per customer
  - gold_sales_*             — daily/weekly/monthly revenue rollups
  - gold_loyalty_comparison  — loyalty vs non-loyalty split
  - gold_location_perf       — per-restaurant revenue

Grain: 1 row per ORDER_ID
Joins silver/order_items ⨝ silver/order_item_options (both keyed on
ORDER_ID + LINEITEM_ID).

Columns:
  order_id, user_id, restaurant_id, order_date, order_datetime, order_hour,
  item_count, total_item_revenue, total_option_revenue, total_revenue,
  has_paid_options, paid_option_count, is_loyalty, is_anonymous,
  order_year, order_month, order_year_month, order_week

Partition key: order_year_month (YYYY-MM) — per architecture v3 section 3.8

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
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


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
JOB_NAME, S3_BUCKET, REGION, SNS_TOPIC = (
    args["JOB_NAME"], args["S3_BUCKET"], args["REGION"], args["SNS_TOPIC_ARN"]
)
RUN_DATE = date.today().isoformat()
RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

SILVER_ORDERS  = f"s3://{S3_BUCKET}/silver/order_items"
SILVER_OPTIONS = f"s3://{S3_BUCKET}/silver/order_item_options"
GOLD_PATH      = f"s3://{S3_BUCKET}/gold/fact_orders"
TABLE_LABEL    = "fact_orders"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

logger = get_logger(JOB_NAME)
logger.info(f"Job started — run_date={RUN_DATE}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

table_results = {}

try:
    logger.info(f"━━━ Processing table: {TABLE_LABEL} ━━━")

    # Step 1: Read sources
    logger.info("Reading silver/order_items")
    items = spark.read.format("delta").load(SILVER_ORDERS)
    items_count = items.count()
    logger.info(f"  Line items: {items_count:,}")

    logger.info("Reading silver/order_item_options")
    options = spark.read.format("delta").load(SILVER_OPTIONS)
    options_count = options.count()
    logger.info(f"  Option rows: {options_count:,}")

    # Step 2: Aggregate options to line-item level
    #   Each line item can have many options — sum them into totals.
    logger.info("Aggregating options → line-item level")
    options_agg = (
        options
        .groupBy("ORDER_ID", "LINEITEM_ID")
        .agg(
            F.round(F.sum("option_revenue"), 2).alias("line_option_revenue"),
            F.sum(F.col("is_paid_option").cast("int")).alias("line_paid_option_count"),
        )
    )

    # Step 3: Left join — line items WITH their option totals (or 0 if none)
    logger.info("Joining line items + option totals (left join)")
    items_with_opts = (
        items.alias("i")
        .join(
            options_agg.alias("o"),
            on=["ORDER_ID", "LINEITEM_ID"],
            how="left"
        )
        .withColumn("line_option_revenue",
                    F.coalesce(F.col("line_option_revenue"),
                               F.lit(0).cast(DecimalType(12, 2))))
        .withColumn("line_paid_option_count",
                    F.coalesce(F.col("line_paid_option_count"), F.lit(0)))
    )

    # Step 4: Aggregate to ORDER_ID grain
    logger.info("Aggregating to ORDER_ID (1 row per order)")
    fact = (
        items_with_opts
        .groupBy("ORDER_ID")
        .agg(
            F.first("USER_ID", ignorenulls=False).alias("user_id"),
            F.first("RESTAURANT_ID").alias("restaurant_id"),
            F.min("order_date").alias("order_date"),
            F.min("CREATION_TIME_UTC").alias("order_datetime"),
            F.min("order_hour").alias("order_hour"),
            F.count("LINEITEM_ID").alias("item_count"),
            F.round(F.sum("line_item_revenue"), 2).alias("total_item_revenue"),
            F.round(F.sum("line_option_revenue"), 2).alias("total_option_revenue"),
            F.sum("line_paid_option_count").alias("paid_option_count"),
            F.max(F.col("IS_LOYALTY").cast("int")).alias("loyalty_flag_int"),
        )
        .withColumn("total_revenue",
                    F.round(F.col("total_item_revenue") + F.col("total_option_revenue"), 2))
        .withColumn("has_paid_options", F.col("paid_option_count") > 0)
        .withColumn("is_loyalty", F.col("loyalty_flag_int") == 1)
        .withColumn("is_anonymous", F.col("user_id").isNull())
        .drop("loyalty_flag_int")
    )

    # Step 5: Date-derived columns (for partitioning + metrics)
    logger.info("Adding date-derived columns (year, month, week, year_month)")
    fact = (
        fact
        .withColumn("order_year",       F.year("order_date"))
        .withColumn("order_month",      F.month("order_date"))
        .withColumn("order_week",       F.weekofyear("order_date"))
        .withColumn("order_year_month", F.date_format("order_date", "yyyy-MM"))
        .withColumnRenamed("ORDER_ID", "order_id")
        .withColumn("gold_ingestion_ts", F.lit(RUN_TS))
    )

    # Final column order
    fact = fact.select(
        "order_id", "user_id", "restaurant_id",
        "order_date", "order_datetime", "order_hour",
        "order_year", "order_month", "order_week", "order_year_month",
        "item_count", "total_item_revenue", "total_option_revenue", "total_revenue",
        "has_paid_options", "paid_option_count",
        "is_loyalty", "is_anonymous", "gold_ingestion_ts"
    )

    order_count = fact.count()
    distinct_customers = fact.filter(~F.col("is_anonymous")).select("user_id").distinct().count()
    total_revenue = fact.agg(F.sum("total_revenue")).collect()[0][0]
    anon_pct = (fact.filter(F.col("is_anonymous")).count() / order_count) * 100
    logger.info(
        f"  Orders: {order_count:,} | Unique customers: {distinct_customers:,} | "
        f"Total revenue: ${total_revenue:,.2f} | Anonymous: {anon_pct:.1f}%"
    )

    # Step 6: Write Gold — partitioned by order_year_month
    logger.info(f"Writing to {GOLD_PATH} partitioned by order_year_month")
    (
        fact.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("order_year_month")
        .save(GOLD_PATH)
    )

    table_results[TABLE_LABEL] = "SUCCESS"
    logger.info(f"✓ {TABLE_LABEL} — SUCCESS")

except Exception as e:
    table_results[TABLE_LABEL] = f"FAILED: {str(e)}"
    logger.error(f"✗ {TABLE_LABEL} — FAILED: {e}")


# Manifest + exit
s3_client = boto3.client("s3", region_name=REGION)
manifest_key = write_manifest(s3_client, S3_BUCKET, JOB_NAME, table_results, RUN_DATE)
logger.info(f"Manifest → s3://{S3_BUCKET}/{manifest_key}")

if any(v != "SUCCESS" for v in table_results.values()):
    send_failure_alert(SNS_TOPIC, REGION, JOB_NAME, RUN_DATE, table_results)
    job.commit()
    raise Exception(f"{TABLE_LABEL} Gold ETL failed — check manifest.")
else:
    job.commit()
