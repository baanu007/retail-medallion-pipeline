"""
build-gold-location-perf.py
Gold metric: Location (restaurant) performance.

Grain: 1 row per restaurant_id
Columns:
    restaurant_id, total_orders, unique_customers, total_revenue,
    avg_order_value, active_weeks, orders_per_week, revenue_per_week,
    loyalty_order_pct,
    repeat_customer_count, retention_rate_pct,
    revenue_rank, orders_rank, customers_rank

retention_rate_pct: % of non-anonymous customers at this restaurant who
  ordered 2 or more times here. Matches the Step 6 Location Performance
  dashboard requirement that explicitly asks for "customer retention" as
  a distinguishing metric for top performers.
"""

import json, logging, sys
from datetime import date, datetime, timezone
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
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
FACT = f"s3://{S3_BUCKET}/gold/fact_orders"
GOLD_PATH = f"s3://{S3_BUCKET}/gold/gold_location_perf"
TABLE_LABEL = "gold_location_perf"

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

    fact = spark.read.format("delta").load(FACT)

    # Base aggregate: per-restaurant volume and revenue metrics
    loc = (
        fact
        .groupBy("restaurant_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("user_id").alias("unique_customers"),
            F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("total_revenue"), 2).alias("avg_order_value"),
            F.countDistinct(F.concat(F.col("order_year"), F.lit("-"), F.col("order_week"))).alias("active_weeks"),
            F.sum(F.when(F.col("is_loyalty"), 1).otherwise(0)).alias("loyalty_orders"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .withColumn("orders_per_week",
                    F.round(F.col("total_orders") / F.col("active_weeks"), 1))
        .withColumn("revenue_per_week",
                    F.round(F.col("total_revenue") / F.col("active_weeks"), 2))
        .withColumn("loyalty_order_pct",
                    F.round((F.col("loyalty_orders") / F.col("total_orders")) * 100, 1))
        .drop("loyalty_orders")
    )

    # Customer retention per location:
    #   retention_rate_pct = % of (non-anonymous) customers at this
    #                       restaurant who ordered >= 2 times here.
    # This matches the Step 6 requirement asking for "customer retention"
    # as a distinguishing metric for top-performing locations.
    logger.info("Computing customer retention per location")
    customer_visit_counts = (
        fact
        .filter(~F.col("is_anonymous"))
        .groupBy("restaurant_id", "user_id")
        .agg(F.countDistinct("order_id").alias("visit_count"))
    )
    retention = (
        customer_visit_counts
        .groupBy("restaurant_id")
        .agg(
            F.count("user_id").alias("known_customers"),
            F.sum(F.when(F.col("visit_count") >= 2, 1).otherwise(0)).alias("repeat_customer_count"),
        )
        .withColumn(
            "retention_rate_pct",
            F.round((F.col("repeat_customer_count") / F.col("known_customers")) * 100, 1)
        )
        .select("restaurant_id", "repeat_customer_count", "retention_rate_pct")
    )

    loc = loc.join(retention, on="restaurant_id", how="left")

    # Rankings
    rev_w   = Window.orderBy(F.col("total_revenue").desc())
    ord_w   = Window.orderBy(F.col("total_orders").desc())
    cust_w  = Window.orderBy(F.col("unique_customers").desc())
    loc = (
        loc
        .withColumn("revenue_rank", F.row_number().over(rev_w))
        .withColumn("orders_rank",  F.row_number().over(ord_w))
        .withColumn("customers_rank", F.row_number().over(cust_w))
        .withColumn("gold_ingestion_ts", F.lit(RUN_TS))
    )

    n = loc.count()
    logger.info(f"  Locations: {n}")
    loc.show(5, truncate=False)

    logger.info(f"Writing to {GOLD_PATH}")
    loc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_PATH)

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
