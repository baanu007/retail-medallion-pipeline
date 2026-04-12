"""
build-gold-sales-daily.py
Gold metric: Sales trends — daily rollups.

Grain: 1 row per (order_date, restaurant_id, item_category)

Serves: dashboard sales page — heat maps, restaurant comparisons, category analysis.

This is the most granular sales aggregate. Weekly and monthly roll up from here
(via separate jobs for clarity / simpler debugging).

Columns:
    order_date, restaurant_id, item_category,
    order_count, unique_customers, total_revenue, total_item_revenue,
    total_option_revenue, avg_order_value, loyalty_order_count
"""

import json, logging, sys
from datetime import date, datetime, timezone
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


def get_logger(n):
    lg = logging.getLogger(n)
    if not lg.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter(fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        lg.addHandler(h)
    lg.setLevel(logging.INFO); return lg


def write_manifest(c, b, jn, tr, rd):
    ov = "SUCCESS" if all(v == "SUCCESS" for v in tr.values()) else "FAILED"
    m = {"job": jn, "run_date": rd, "run_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "overall_status": ov, "tables": tr}
    k = f"manifests/{jn}/{rd}/status.json"
    c.put_object(Bucket=b, Key=k, Body=json.dumps(m, indent=2), ContentType="application/json")
    return k


def send_failure_alert(arn, rg, jn, rd, tr):
    f_ = {k: v for k, v in tr.items() if v != "SUCCESS"}
    msg = f"PIPELINE FAILURE — {jn}\nRun date: {rd}\n\nFailed tables:\n" + "\n".join(f"  • {t}: {e}" for t, e in f_.items())
    boto3.client("sns", region_name=rg).publish(TopicArn=arn, Subject=f"[ALERT] Pipeline failure: {jn} — {rd}", Message=msg)


args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "REGION", "SNS_TOPIC_ARN"])
JOB_NAME, S3_BUCKET, REGION, SNS_TOPIC = args["JOB_NAME"], args["S3_BUCKET"], args["REGION"], args["SNS_TOPIC_ARN"]
RUN_DATE = date.today().isoformat()
RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Uses BOTH silver (for item_category per line item) and fact (for order-grain metrics)
SILVER_ORDERS = f"s3://{S3_BUCKET}/silver/order_items"
FACT          = f"s3://{S3_BUCKET}/gold/fact_orders"
GOLD_PATH     = f"s3://{S3_BUCKET}/gold/gold_sales_daily"
TABLE_LABEL   = "gold_sales_daily"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)
logger = get_logger(JOB_NAME)
logger.info(f"Job started — run_date={RUN_DATE}")

table_results = {}

try:
    logger.info(f"━━━ Processing: {TABLE_LABEL} ━━━")

    # Read silver at line-item grain (has ITEM_CATEGORY)
    items = (
        spark.read.format("delta").load(SILVER_ORDERS)
        .select(
            "ORDER_ID", "order_date", "RESTAURANT_ID",
            F.col("ITEM_CATEGORY").alias("item_category"),
            F.col("line_item_revenue").alias("item_revenue"),
            F.col("USER_ID").alias("user_id"),
            "IS_LOYALTY"
        )
    )

    # Aggregate to (date, restaurant, category)
    daily = (
        items
        .groupBy("order_date", "RESTAURANT_ID", "item_category")
        .agg(
            F.countDistinct("ORDER_ID").alias("order_count"),
            F.countDistinct("user_id").alias("unique_customers"),
            F.round(F.sum("item_revenue"), 2).alias("total_item_revenue"),
            F.sum(F.when(F.col("IS_LOYALTY") == True, 1).otherwise(0)).alias("loyalty_line_items"),
        )
        .withColumnRenamed("RESTAURANT_ID", "restaurant_id")
        .withColumn("avg_order_revenue",
                    F.round(F.col("total_item_revenue") / F.col("order_count"), 2))
        .withColumn("snapshot_year", F.year("order_date"))
        .withColumn("gold_ingestion_ts", F.lit(RUN_TS))
    )

    n = daily.count()
    logger.info(f"  Daily rows: {n:,}")

    logger.info(f"Writing to {GOLD_PATH} partitioned by snapshot_year")
    (
        daily.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("snapshot_year")
        .save(GOLD_PATH)
    )

    table_results[TABLE_LABEL] = "SUCCESS"
    logger.info(f"✓ {TABLE_LABEL} — SUCCESS")

except Exception as e:
    table_results[TABLE_LABEL] = f"FAILED: {str(e)}"
    logger.error(f"✗ {TABLE_LABEL} — FAILED: {e}")


s3_client = boto3.client("s3", region_name=REGION)
mk = write_manifest(s3_client, S3_BUCKET, JOB_NAME, table_results, RUN_DATE)
logger.info(f"Manifest → s3://{S3_BUCKET}/{mk}")
if any(v != "SUCCESS" for v in table_results.values()):
    send_failure_alert(SNS_TOPIC, REGION, JOB_NAME, RUN_DATE, table_results)
    job.commit()
    raise Exception(f"{TABLE_LABEL} failed")
else:
    job.commit()
