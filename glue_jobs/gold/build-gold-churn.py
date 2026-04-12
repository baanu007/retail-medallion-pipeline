"""
build-gold-churn.py
Gold metric: Churn indicators.

Grain: 1 row per user_id (repeat customers only, >= 2 orders)
Why repeat-only: Data exploration showed 50% of customers are one-time buyers.
    "Churn" has no meaning for one-timers — their first order IS their last.
    The dashboard handles one-time buyers as a separate bucket in the loyalty view.

Columns:
    user_id, total_orders, first_order_date, last_order_date,
    days_since_last_order, avg_gap_days, recent_spend, prior_spend, spend_change_pct,
    churn_tag, churn_tag_personal

TWO churn tags are produced — matches both the strict spec reading and the
data-science-best-practice reading:

1. `churn_tag` — ABSOLUTE threshold (matches the spec example "> 45 days = at risk"):
       Active      : days_since_last_order <= 30
       Cooling Off : 30 < days <= 45
       At Risk     : 45 < days <= 60
       Inactive    : days > 60

2. `churn_tag_personal` — PERSONAL-GAP multiplier (more sophisticated, handles
   the fact that a weekly customer is "at risk" much sooner than a monthly one):
       Active      : days_since_last_order <= avg_gap_days × 1.5
       Cooling Off : 1.5 × avg < days <= 2 × avg
       At Risk     : 2 × avg  < days <= 3 × avg
       Inactive    : days     > 3 × avg

The dashboard shows both so analysts can compare.

Spend trend: compares last-order total vs average of previous orders
    → detects declining engagement even before full churn
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
GOLD_PATH = f"s3://{S3_BUCKET}/gold/gold_churn_indicators"
TABLE_LABEL = "gold_churn_indicators"

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

    fact = (
        spark.read.format("delta").load(FACT)
        .filter(~F.col("is_anonymous"))
    )

    ref_date = fact.agg(F.max("order_date")).collect()[0][0]
    logger.info(f"  Reference date: {ref_date}")

    # Per-order row number per customer (ordered by date)
    order_w = Window.partitionBy("user_id").orderBy("order_date")
    fact_with_rn = (
        fact
        .select("user_id", "order_date", "total_revenue")
        .withColumn("rn", F.row_number().over(order_w))
        .withColumn("total_orders_customer",
                    F.count("*").over(Window.partitionBy("user_id")))
    )

    # last order total, prior orders average (for spend trend)
    agg = (
        fact_with_rn
        .groupBy("user_id")
        .agg(
            F.count("*").alias("total_orders"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.round(F.sum(F.when(F.col("rn") == F.col("total_orders_customer"),
                                 F.col("total_revenue"))), 2).alias("recent_spend"),
            F.round(F.avg(F.when(F.col("rn") < F.col("total_orders_customer"),
                                 F.col("total_revenue"))), 2).alias("prior_avg_spend"),
        )
        .filter(F.col("total_orders") >= 2)   # drop one-time buyers
    )

    # Average gap between orders (in days)
    agg = agg.withColumn(
        "avg_gap_days",
        F.round(
            F.datediff(F.col("last_order_date"), F.col("first_order_date"))
            / (F.col("total_orders") - 1), 1
        )
    )

    # Days since last order (relative to ref_date)
    agg = agg.withColumn(
        "days_since_last_order",
        F.datediff(F.lit(ref_date), F.col("last_order_date"))
    )

    # churn_tag — ABSOLUTE threshold (matches spec example ">45 days = at risk")
    #   Active      : days <= 30
    #   Cooling Off : 30 <  days <= 45
    #   At Risk     : 45 <  days <= 60
    #   Inactive    : days >  60
    agg = agg.withColumn(
        "churn_tag",
        F.when(F.col("days_since_last_order") <= 30, "Active")
         .when(F.col("days_since_last_order") <= 45, "Cooling Off")
         .when(F.col("days_since_last_order") <= 60, "At Risk")
         .otherwise("Inactive")
    )

    # churn_tag_personal — PERSONAL-GAP multiplier (sophisticated alt version)
    # Handle avg_gap_days=0 (two orders same day) by treating as 1
    agg = agg.withColumn(
        "gap_safe",
        F.when(F.col("avg_gap_days") <= 0, F.lit(1.0)).otherwise(F.col("avg_gap_days"))
    ).withColumn(
        "churn_tag_personal",
        F.when(F.col("days_since_last_order") <= F.col("gap_safe") * 1.5, "Active")
         .when(F.col("days_since_last_order") <= F.col("gap_safe") * 2.0, "Cooling Off")
         .when(F.col("days_since_last_order") <= F.col("gap_safe") * 3.0, "At Risk")
         .otherwise("Inactive")
    ).drop("gap_safe")

    # Spend trend pct
    agg = agg.withColumn(
        "spend_change_pct",
        F.when(
            F.col("prior_avg_spend") > 0,
            F.round(
                ((F.col("recent_spend") - F.col("prior_avg_spend")) / F.col("prior_avg_spend")) * 100, 1
            )
        ).otherwise(F.lit(None))
    )

    agg = agg.withColumn("gold_ingestion_ts", F.lit(RUN_TS))

    n = agg.count()
    logger.info(f"  Churn rows (repeat customers): {n:,}")

    logger.info(f"Writing to {GOLD_PATH} (OVERWRITE)")
    agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_PATH)

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
