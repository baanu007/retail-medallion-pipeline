"""
build-gold-loyalty.py
Gold metric: Loyalty vs non-loyalty comparison.

Grain: 1 row per loyalty segment ("Loyalty" / "Non-Loyalty" / "Anonymous")
Columns:
    segment, customer_count, order_count, total_revenue,
    avg_spend_per_order, avg_orders_per_customer, avg_customer_spend,
    repeat_customer_rate_pct, option_attach_rate_pct
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
FACT = f"s3://{S3_BUCKET}/gold/fact_orders"
GOLD_PATH = f"s3://{S3_BUCKET}/gold/gold_loyalty_comparison"
TABLE_LABEL = "gold_loyalty_comparison"

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

    # Segment column: Anonymous / Loyalty / Non-Loyalty
    fact = fact.withColumn(
        "segment",
        F.when(F.col("is_anonymous"), "Anonymous")
         .when(F.col("is_loyalty"),   "Loyalty")
         .otherwise("Non-Loyalty")
    )

    # Segment-level aggregates
    seg = (
        fact
        .groupBy("segment")
        .agg(
            F.countDistinct("user_id").alias("customer_count"),
            F.countDistinct("order_id").alias("order_count"),
            F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("total_revenue"), 2).alias("avg_spend_per_order"),
            F.sum(F.when(F.col("has_paid_options"), 1).otherwise(0)).alias("orders_with_paid_options"),
        )
    )

    # Per-customer averages (anonymous excluded from per-customer stats)
    per_customer = (
        fact
        .filter(~F.col("is_anonymous"))
        .groupBy("segment", "user_id")
        .agg(F.count("order_id").alias("orders_per_customer"),
             F.sum("total_revenue").alias("spend_per_customer"))
    )
    per_cust_stats = (
        per_customer
        .groupBy("segment")
        .agg(
            F.round(F.avg("orders_per_customer"), 2).alias("avg_orders_per_customer"),
            F.round(F.avg("spend_per_customer"), 2).alias("avg_customer_spend"),
            F.round(
                (F.sum(F.when(F.col("orders_per_customer") >= 2, 1).otherwise(0))
                 / F.count("*")) * 100, 1
            ).alias("repeat_customer_rate_pct"),
        )
    )

    loyalty = (
        seg
        .join(per_cust_stats, on="segment", how="left")
        .withColumn(
            "option_attach_rate_pct",
            F.round((F.col("orders_with_paid_options") / F.col("order_count")) * 100, 1)
        )
        .drop("orders_with_paid_options")
        .withColumn("gold_ingestion_ts", F.lit(RUN_TS))
    )

    loyalty.show(truncate=False)

    logger.info(f"Writing to {GOLD_PATH}")
    loyalty.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_PATH)

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
