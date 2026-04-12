"""
build-gold-upsell.py
Gold metric: Upsell analysis — the PIVOT from discount analysis.

Background:
    Requirements originally said "use option_price < 0 to detect discounts."
    Data exploration confirmed ZERO negative option prices exist in the data.
    Architecture v3 section 2.8 documents the pivot to upsell analysis:
    compare FREE ($0, 66.3% of options) vs PAID add-ons (33.7%), identify
    top-grossing paid options, and compute per-order upsell impact.

Outputs TWO tables (single job, different paths):
    1. gold_upsell_summary     — 1 row with overall statistics
    2. gold_upsell_top_options — 1 row per top paid option by revenue

Sources: silver/order_item_options + gold/fact_orders
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
OPTIONS = f"s3://{S3_BUCKET}/silver/order_item_options"
FACT = f"s3://{S3_BUCKET}/gold/fact_orders"
SUMMARY_PATH = f"s3://{S3_BUCKET}/gold/gold_upsell_summary"
TOP_PATH     = f"s3://{S3_BUCKET}/gold/gold_upsell_top_options"
TABLE_LABEL_1 = "gold_upsell_summary"
TABLE_LABEL_2 = "gold_upsell_top_options"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)
logger = get_logger(JOB_NAME)
logger.info(f"Job started — run_date={RUN_DATE}")

table_results = {}

try:
    options = spark.read.format("delta").load(OPTIONS)
    fact    = spark.read.format("delta").load(FACT)

    # ─── Summary ────────────────────────────────────────
    logger.info(f"━━━ Processing: {TABLE_LABEL_1} ━━━")
    total_opts  = options.count()
    total_paid  = options.filter(F.col("is_paid_option")).count()
    total_free  = total_opts - total_paid
    total_option_rev = options.agg(F.sum("option_revenue")).collect()[0][0] or 0

    total_orders  = fact.count()
    orders_w_paid = fact.filter(F.col("has_paid_options")).count()
    total_rev     = fact.agg(F.sum("total_revenue")).collect()[0][0] or 0

    summary_row = [{
        "total_option_rows":        total_opts,
        "free_option_rows":         total_free,
        "paid_option_rows":         total_paid,
        "free_option_pct":          round((total_free / total_opts) * 100, 2),
        "paid_option_pct":          round((total_paid / total_opts) * 100, 2),
        "total_option_revenue":     round(float(total_option_rev), 2),
        "total_orders":             total_orders,
        "orders_with_paid_options": orders_w_paid,
        "order_upsell_rate_pct":    round((orders_w_paid / total_orders) * 100, 2),
        "total_order_revenue":      round(float(total_rev), 2),
        "upsell_share_of_revenue_pct":
            round((float(total_option_rev) / float(total_rev)) * 100, 2) if total_rev else 0,
        "avg_option_revenue_per_upsell_order":
            round(float(total_option_rev) / orders_w_paid, 2) if orders_w_paid else 0,
        "gold_ingestion_ts": RUN_TS,
    }]
    summary_df = spark.createDataFrame(summary_row)
    summary_df.show(truncate=False)

    logger.info(f"Writing summary to {SUMMARY_PATH}")
    summary_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SUMMARY_PATH)
    table_results[TABLE_LABEL_1] = "SUCCESS"
    logger.info(f"✓ {TABLE_LABEL_1} — SUCCESS")

    # ─── Top paid options by revenue ─────────────────────
    logger.info(f"━━━ Processing: {TABLE_LABEL_2} ━━━")
    top_options = (
        options
        .filter(F.col("is_paid_option"))
        .groupBy("OPTION_GROUP_NAME", "OPTION_NAME")
        .agg(
            F.count("*").alias("attach_count"),
            F.round(F.sum("option_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("OPTION_PRICE"), 2).alias("avg_price"),
        )
        .orderBy(F.col("total_revenue").desc())
        .withColumnRenamed("OPTION_GROUP_NAME", "option_group_name")
        .withColumnRenamed("OPTION_NAME", "option_name")
        .withColumn("gold_ingestion_ts", F.lit(RUN_TS))
    )
    top_options.show(20, truncate=False)

    logger.info(f"Writing top options to {TOP_PATH}")
    top_options.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(TOP_PATH)
    table_results[TABLE_LABEL_2] = "SUCCESS"
    logger.info(f"✓ {TABLE_LABEL_2} — SUCCESS")

except Exception as e:
    error_msg = str(e)
    if TABLE_LABEL_1 not in table_results:
        table_results[TABLE_LABEL_1] = f"FAILED: {error_msg}"
    if TABLE_LABEL_2 not in table_results:
        table_results[TABLE_LABEL_2] = f"FAILED: {error_msg}"
    logger.error(f"✗ FAILED: {e}")


s3_client = boto3.client("s3", region_name=REGION)
mk = write_manifest(s3_client, S3_BUCKET, JOB_NAME, table_results, RUN_DATE)
logger.info(f"Manifest → s3://{S3_BUCKET}/{mk}")
if any(v != "SUCCESS" for v in table_results.values()):
    send_failure_alert(SNS_TOPIC, REGION, JOB_NAME, RUN_DATE, table_results)
    job.commit()
    raise Exception("Upsell Gold ETL failed — check manifest.")
else:
    job.commit()
