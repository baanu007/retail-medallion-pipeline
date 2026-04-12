"""
build-dim-date.py
Gold dimension: extended calendar table.

Serves: ALL metrics that need a continuous timeline — most importantly
        gold_clv_snapshot (PRIMARY metric, "CLV daily evolution") which
        cross-joins customers with every calendar day.

Why this exists:
    silver/date_dim only covers 2023 (365 days). Real order data spans
    2020-04-21 → 2024-02-21 (~1,402 days). We dynamically determine the
    date range from silver/order_items and generate a complete calendar
    with weekday/weekend flags and US federal holidays for all years.

Output: s3://{bucket}/gold/dim_date/ (full overwrite, ~1,400 rows, tiny table)

Grain: 1 row per calendar day
Columns: date_key (DATE), year, month, day_of_month, week, day_of_week,
         day_name, is_weekend, is_holiday, holiday_name

Job parameters:
    --S3_BUCKET, --REGION, --SNS_TOPIC_ARN, --datalake-formats delta
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
from pyspark.sql import Row
from pyspark.sql import functions as F


# ─────────────────────────────────────────────
# HELPERS (self-contained)
# ─────────────────────────────────────────────

def get_logger(job_name):
    logger = logging.getLogger(job_name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(handler)
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
    message = (f"PIPELINE FAILURE — {job_name}\nRun date: {run_date}\n\n"
               f"Failed tables:\n"
               + "\n".join(f"  • {t}: {e}" for t, e in failed.items())
               + "\n\nCheck CloudWatch logs and S3 manifest.")
    sns = boto3.client("sns", region_name=region)
    sns.publish(TopicArn=sns_topic_arn,
                Subject=f"[ALERT] Pipeline failure: {job_name} — {run_date}",
                Message=message)


# ─────────────────────────────────────────────
# US FEDERAL HOLIDAYS (2020-2025)
# ─────────────────────────────────────────────
# Hardcoded for the full range. Floating holidays (MLK, Memorial, etc.) were
# computed offline and are listed here explicitly.

US_HOLIDAYS = [
    # 2020
    ("2020-01-01", "New Year's Day"),
    ("2020-01-20", "Martin Luther King Jr. Day"),
    ("2020-02-17", "Presidents Day"),
    ("2020-05-25", "Memorial Day"),
    ("2020-07-03", "Independence Day (Observed)"),
    ("2020-09-07", "Labor Day"),
    ("2020-10-12", "Columbus Day"),
    ("2020-11-11", "Veterans Day"),
    ("2020-11-26", "Thanksgiving Day"),
    ("2020-12-25", "Christmas Day"),
    # 2021
    ("2021-01-01", "New Year's Day"),
    ("2021-01-18", "Martin Luther King Jr. Day"),
    ("2021-02-15", "Presidents Day"),
    ("2021-05-31", "Memorial Day"),
    ("2021-06-18", "Juneteenth (Observed)"),
    ("2021-07-05", "Independence Day (Observed)"),
    ("2021-09-06", "Labor Day"),
    ("2021-10-11", "Columbus Day"),
    ("2021-11-11", "Veterans Day"),
    ("2021-11-25", "Thanksgiving Day"),
    ("2021-12-24", "Christmas Day (Observed)"),
    # 2022
    ("2022-01-01", "New Year's Day"),
    ("2022-01-17", "Martin Luther King Jr. Day"),
    ("2022-02-21", "Presidents Day"),
    ("2022-05-30", "Memorial Day"),
    ("2022-06-20", "Juneteenth (Observed)"),
    ("2022-07-04", "Independence Day"),
    ("2022-09-05", "Labor Day"),
    ("2022-10-10", "Columbus Day"),
    ("2022-11-11", "Veterans Day"),
    ("2022-11-24", "Thanksgiving Day"),
    ("2022-12-26", "Christmas Day (Observed)"),
    # 2023
    ("2023-01-02", "New Year's Day (Observed)"),
    ("2023-01-16", "Martin Luther King Jr. Day"),
    ("2023-02-20", "Presidents Day"),
    ("2023-05-29", "Memorial Day"),
    ("2023-06-19", "Juneteenth"),
    ("2023-07-04", "Independence Day"),
    ("2023-09-04", "Labor Day"),
    ("2023-10-09", "Columbus Day"),
    ("2023-11-10", "Veterans Day (Observed)"),
    ("2023-11-23", "Thanksgiving Day"),
    ("2023-12-25", "Christmas Day"),
    # 2024
    ("2024-01-01", "New Year's Day"),
    ("2024-01-15", "Martin Luther King Jr. Day"),
    ("2024-02-19", "Presidents Day"),
    ("2024-05-27", "Memorial Day"),
    ("2024-06-19", "Juneteenth"),
    ("2024-07-04", "Independence Day"),
    ("2024-09-02", "Labor Day"),
    ("2024-10-14", "Columbus Day"),
    ("2024-11-11", "Veterans Day"),
    ("2024-11-28", "Thanksgiving Day"),
    ("2024-12-25", "Christmas Day"),
]


# ─────────────────────────────────────────────
# JOB INIT
# ─────────────────────────────────────────────

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "REGION", "SNS_TOPIC_ARN"])
JOB_NAME   = args["JOB_NAME"]
S3_BUCKET  = args["S3_BUCKET"]
REGION     = args["REGION"]
SNS_TOPIC  = args["SNS_TOPIC_ARN"]
RUN_DATE   = date.today().isoformat()
RUN_TS     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

SILVER_ORDERS = f"s3://{S3_BUCKET}/silver/order_items"
GOLD_PATH     = f"s3://{S3_BUCKET}/gold/dim_date"
TABLE_LABEL   = "dim_date"

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

    # Step 1: Determine date range from silver/order_items min/max order_date
    logger.info("Determining date range from silver/order_items")
    orders = spark.read.format("delta").load(SILVER_ORDERS)
    date_range = orders.agg(
        F.min("order_date").alias("min_d"),
        F.max("order_date").alias("max_d")
    ).collect()[0]
    min_date, max_date = date_range["min_d"], date_range["max_d"]
    logger.info(f"  Date range from orders: {min_date} → {max_date}")

    # Step 2: Generate full date sequence using sequence() + explode()
    logger.info("Generating continuous date sequence")
    date_df = (
        spark.sql(
            f"SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'), "
            f"interval 1 day)) AS date_key"
        )
    )
    total_days = date_df.count()
    logger.info(f"  Generated {total_days:,} days")

    # Step 3: Compute calendar attributes
    logger.info("Computing calendar attributes (year, month, week, day_of_week, is_weekend)")
    date_df = (
        date_df
        .withColumn("year",          F.year("date_key"))
        .withColumn("month",         F.month("date_key"))
        .withColumn("day_of_month",  F.dayofmonth("date_key"))
        .withColumn("week",          F.weekofyear("date_key"))
        .withColumn("day_of_week",   F.dayofweek("date_key"))   # 1=Sun, 7=Sat
        .withColumn("day_name",      F.date_format("date_key", "EEEE"))
        .withColumn("is_weekend",    F.col("day_of_week").isin(1, 7))
    )

    # Step 4: Join US federal holidays
    logger.info(f"Joining {len(US_HOLIDAYS)} US federal holidays")
    holiday_rows = [Row(date_key=h[0], holiday_name=h[1]) for h in US_HOLIDAYS]
    holiday_df = (
        spark.createDataFrame(holiday_rows)
        .withColumn("date_key", F.to_date("date_key"))
        .withColumn("is_holiday", F.lit(True))
    )

    date_df = (
        date_df
        .join(holiday_df, on="date_key", how="left")
        .withColumn("is_holiday",
                    F.when(F.col("is_holiday").isNull(), F.lit(False))
                     .otherwise(F.col("is_holiday")))
        .withColumn("gold_ingestion_ts", F.lit(RUN_TS))
        .orderBy("date_key")
    )

    final_count = date_df.count()
    holiday_count = date_df.filter(F.col("is_holiday")).count()
    weekend_count = date_df.filter(F.col("is_weekend")).count()
    logger.info(
        f"  Final: {final_count:,} days ({weekend_count:,} weekends, "
        f"{holiday_count:,} holidays)"
    )

    # Step 5: Write Gold (full overwrite — tiny table)
    logger.info(f"Writing to {GOLD_PATH} (OVERWRITE)")
    (
        date_df.write
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
