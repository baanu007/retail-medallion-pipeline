"""
build-gold-rfm.py
Gold metric: RFM (Recency, Frequency, Monetary) segmentation.

Grain: 1 row per user_id (non-anonymous customers only)

Columns:
    user_id, is_loyalty,
    recency_days, frequency, monetary,
    r_score, f_score, m_score, rfm_score,
    segment

Window:
    Recency = lifetime (days since customer's LAST order)
    Frequency = orders in the LAST 12 MONTHS (per the business requirement)
    Monetary  = spend  in the LAST 12 MONTHS (per the business requirement)

Customers whose last order is >12 months before the reference date will have
Frequency=0 and Monetary=0 — they still receive R/F/M scores and most will
fall into "Churn Risk" or "Regular". This exactly matches the spec which
says "Frequency: Number of purchases in last N months" / "Monetary: Total
spend in last N months".

Scoring: ntile(5) on each dimension
    Recency: LOWER days → HIGHER score (more recent = better)
    Frequency: HIGHER count → HIGHER score
    Monetary: HIGHER spend → HIGHER score

Segment logic (matches Step 5 spec):
    VIP          : R>=4 AND F>=4 AND M>=4    (High R, F, M)
    Loyal        : F>=4 AND M>=3 (not VIP)
    Big Spender  : M>=4 (not VIP/Loyal)
    New Customer : R>=4 AND F<=2             (Low F, High R)
    At Risk      : R<=2 AND F>=3
    Churn Risk   : R<=2 AND F<=2 AND M<=2    (Low R, Low F)
    Regular      : everyone else

is_loyalty is joined from dim_customer so the dashboard can cross-tab RFM
segments against loyalty membership (per Step 6 Customer Segmentation
dashboard which explicitly asks to segment by "purchase behavior AND
loyalty status").

Reference date for recency: max order_date in fact (project frozen-at-max-date).
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
DIM_CUST = f"s3://{S3_BUCKET}/gold/dim_customer"
GOLD_PATH = f"s3://{S3_BUCKET}/gold/gold_rfm_segments"
TABLE_LABEL = "gold_rfm_segments"
WINDOW_DAYS = 365  # "last N months" from the spec — using 12 months

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

    # Reference date = max order_date in fact (end of project data window)
    ref_date_row = fact.agg(F.max("order_date").alias("ref")).collect()[0]
    ref_date = ref_date_row["ref"]
    logger.info(f"  Reference date (for recency): {ref_date}")
    logger.info(f"  F/M window: last {WINDOW_DAYS} days before {ref_date}")

    # Recency = LIFETIME days since last order (per customer)
    recency_df = (
        fact.groupBy("user_id")
        .agg(F.datediff(F.lit(ref_date), F.max("order_date")).alias("recency_days"))
    )

    # Frequency + Monetary = ONLY orders within the last N days ("last N months")
    window_fact = fact.filter(
        F.col("order_date") >= F.date_sub(F.lit(ref_date), WINDOW_DAYS)
    )
    fm_df = (
        window_fact.groupBy("user_id")
        .agg(
            F.countDistinct("order_id").alias("frequency"),
            F.round(F.sum("total_revenue"), 2).alias("monetary"),
        )
    )

    # Load dim_customer for loyalty flag (also serves as the customer universe)
    customers_df = (
        spark.read.format("delta").load(DIM_CUST)
        .select("user_id", "is_loyalty")
    )

    # Build final RFM: every known customer, with their recency, F-in-window,
    # M-in-window, and loyalty flag. Customers with no orders in the window
    # get F=0 and M=0 (they'll naturally fall into low-score tiers).
    rfm = (
        customers_df
        .join(recency_df, on="user_id", how="left")
        .join(fm_df,      on="user_id", how="left")
        .na.fill({"frequency": 0, "monetary": 0.0})
    )

    # ntile(5) scoring
    r_w = Window.orderBy(F.col("recency_days").desc())   # fewer days → higher tile (last)
    f_w = Window.orderBy(F.col("frequency").asc())
    m_w = Window.orderBy(F.col("monetary").asc())
    rfm = (
        rfm
        .withColumn("r_score", F.ntile(5).over(r_w))
        .withColumn("f_score", F.ntile(5).over(f_w))
        .withColumn("m_score", F.ntile(5).over(m_w))
        .withColumn("rfm_score",
                    F.concat(F.col("r_score"), F.col("f_score"), F.col("m_score")))
    )

    # Segment labels
    rfm = rfm.withColumn(
        "segment",
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "VIP")
         .when((F.col("f_score") >= 4) & (F.col("m_score") >= 3), "Loyal")
         .when(F.col("m_score") >= 4, "Big Spender")
         .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), "New Customer")
         .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), "At Risk")
         .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2) & (F.col("m_score") <= 2), "Churn Risk")
         .otherwise("Regular")
    )

    rfm = (
        rfm
        .withColumn("gold_ingestion_ts", F.lit(RUN_TS))
        .select(
            "user_id", "is_loyalty",
            "recency_days", "frequency", "monetary",
            "r_score", "f_score", "m_score", "rfm_score",
            "segment", "gold_ingestion_ts",
        )
    )

    n = rfm.count()
    in_window = rfm.filter(F.col("frequency") > 0).count()
    logger.info(f"  RFM rows: {n:,} total | {in_window:,} with orders in last {WINDOW_DAYS}d")

    logger.info(f"Writing to {GOLD_PATH} (OVERWRITE)")
    rfm.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_PATH)

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
