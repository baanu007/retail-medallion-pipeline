"""
build-dim-restaurant.py
Gold dimension: restaurant profile with performance aggregates.

Serves:
  - gold_location_perf   (PRIMARY consumer) — per-restaurant revenue/AOV/rank
  - gold_sales_daily     — restaurant dimension
  - dashboard filters    — user selects restaurant on sales/CLV pages

Grain: 1 row per RESTAURANT_ID (~27 locations after dev exclusion)
Columns: restaurant_id, total_orders, total_revenue, unique_customers,
         avg_order_value, first_order_date, last_order_date,
         active_days, revenue_rank

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


# Job init
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "REGION", "SNS_TOPIC_ARN"])
JOB_NAME, S3_BUCKET, REGION, SNS_TOPIC = (
    args["JOB_NAME"], args["S3_BUCKET"], args["REGION"], args["SNS_TOPIC_ARN"]
)
RUN_DATE = date.today().isoformat()
RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

SILVER_ORDERS = f"s3://{S3_BUCKET}/silver/order_items"
GOLD_PATH     = f"s3://{S3_BUCKET}/gold/dim_restaurant"
TABLE_LABEL   = "dim_restaurant"

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

    orders = spark.read.format("delta").load(SILVER_ORDERS)

    logger.info("Aggregating restaurant-level metrics")
    dim = (
        orders
        .groupBy("RESTAURANT_ID")
        .agg(
            F.countDistinct("ORDER_ID").alias("total_orders"),
            F.round(F.sum("line_item_revenue"), 2).alias("total_revenue"),
            F.countDistinct("USER_ID").alias("unique_customers"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("order_date").alias("active_days"),
        )
        .withColumn(
            "avg_order_value",
            F.round(F.col("total_revenue") / F.col("total_orders"), 2)
        )
    )

    logger.info("Ranking by total_revenue (1 = top)")
    rank_w = Window.orderBy(F.col("total_revenue").desc())
    dim = dim.withColumn("revenue_rank", F.row_number().over(rank_w))

    dim = (
        dim
        .withColumnRenamed("RESTAURANT_ID", "restaurant_id")
        .withColumn("gold_ingestion_ts", F.lit(RUN_TS))
        .select(
            "restaurant_id", "total_orders", "total_revenue", "unique_customers",
            "avg_order_value", "first_order_date", "last_order_date",
            "active_days", "revenue_rank", "gold_ingestion_ts"
        )
    )

    total_locations = dim.count()
    logger.info(f"  Locations: {total_locations:,}")

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
