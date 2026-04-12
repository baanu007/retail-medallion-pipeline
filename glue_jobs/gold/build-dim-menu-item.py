"""
build-dim-menu-item.py
Gold dimension: menu item catalog with aggregate stats.

Serves:
  - gold_upsell_analysis  — links option groups to menu items
  - gold_sales_daily      — category breakdown
  - dashboard filters     — user selects item / category

Grain: 1 row per (ITEM_NAME, ITEM_CATEGORY) pair (~431 distinct items → ~40 categories)
Columns: item_name, item_category, total_orders_sold, total_quantity_sold,
         total_revenue, avg_price, min_price, max_price,
         first_sold_date, last_sold_date

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


args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "REGION", "SNS_TOPIC_ARN"])
JOB_NAME, S3_BUCKET, REGION, SNS_TOPIC = (
    args["JOB_NAME"], args["S3_BUCKET"], args["REGION"], args["SNS_TOPIC_ARN"]
)
RUN_DATE = date.today().isoformat()
RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

SILVER_ORDERS = f"s3://{S3_BUCKET}/silver/order_items"
GOLD_PATH     = f"s3://{S3_BUCKET}/gold/dim_menu_item"
TABLE_LABEL   = "dim_menu_item"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

logger = get_logger(JOB_NAME)
logger.info(f"Job started — run_date={RUN_DATE}")


# ─── Main ───
table_results = {}

try:
    logger.info(f"━━━ Processing table: {TABLE_LABEL} ━━━")

    orders = (
        spark.read.format("delta").load(SILVER_ORDERS)
        .filter(F.col("ITEM_NAME").isNotNull())
    )

    logger.info("Aggregating menu-item-level metrics")
    dim = (
        orders
        .groupBy("ITEM_NAME", "ITEM_CATEGORY")
        .agg(
            F.countDistinct("ORDER_ID").alias("total_orders_sold"),
            F.sum("ITEM_QUANTITY").alias("total_quantity_sold"),
            F.round(F.sum("line_item_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("ITEM_PRICE"), 2).alias("avg_price"),
            F.min("ITEM_PRICE").alias("min_price"),
            F.max("ITEM_PRICE").alias("max_price"),
            F.min("order_date").alias("first_sold_date"),
            F.max("order_date").alias("last_sold_date"),
        )
        .withColumnRenamed("ITEM_NAME", "item_name")
        .withColumnRenamed("ITEM_CATEGORY", "item_category")
        .withColumn("gold_ingestion_ts", F.lit(RUN_TS))
    )

    n = dim.count()
    n_cat = dim.select("item_category").distinct().count()
    logger.info(f"  Menu items: {n:,} across {n_cat} categories")

    logger.info(f"Writing to {GOLD_PATH} (OVERWRITE)")
    (
        dim.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").save(GOLD_PATH)
    )

    table_results[TABLE_LABEL] = "SUCCESS"
    logger.info(f"✓ {TABLE_LABEL} — SUCCESS")

except Exception as e:
    table_results[TABLE_LABEL] = f"FAILED: {str(e)}"
    logger.error(f"✗ {TABLE_LABEL} — FAILED: {e}")


s3_client = boto3.client("s3", region_name=REGION)
manifest_key = write_manifest(s3_client, S3_BUCKET, JOB_NAME, table_results, RUN_DATE)
logger.info(f"Manifest → s3://{S3_BUCKET}/{manifest_key}")

if any(v != "SUCCESS" for v in table_results.values()):
    send_failure_alert(SNS_TOPIC, REGION, JOB_NAME, RUN_DATE, table_results)
    job.commit()
    raise Exception(f"{TABLE_LABEL} Gold ETL failed — check manifest.")
else:
    job.commit()
