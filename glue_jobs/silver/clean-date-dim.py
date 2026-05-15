"""
clean-date-dim.py
Silver layer ETL — cleans and conforms date_dim from Bronze → Silver.

Architecture:
    S3 Bronze (raw Delta) ──read──▶ PySpark cleaning ──Delta OVERWRITE──▶ S3 Silver

Source:  s3://{bucket}/bronze/date_dim/
Target:  s3://{bucket}/silver/date_dim/
Strategy: FULL OVERWRITE (365 rows, tiny table, no merge needed)

Business context:
    Bronze date_dim covers Jan 1 – Dec 31 2023 (365 rows).
    Order data spans Apr 2020 – Feb 2024 (~1,402 days).
    GOLD (not Silver) will extend dim_date to the full order range and
    add US federal holidays for the uncovered years. Silver just cleans
    what Bronze gives us.

Operations performed (the "what Silver does" for this table):
    1. Read Bronze Delta table
    2. Drop rows with NULL date_key (unjoinable)
    3. Parse date_key from "DD-MM-YYYY" string → proper DATE type
         - Source format is DD-MM-YYYY (European), NOT US MM-DD-YYYY
         - Verified in data exploration
    4. Type casting:
         - year, month, week    → INT
         - is_weekend           → BOOLEAN (from "TRUE"/"FALSE" string)
         - is_holiday           → BOOLEAN
    5. Trim holiday_name and convert empty strings to NULL
       (3.3% populated — only on 12 holiday dates)
    6. Sanity-check date parsing succeeded (reject rows where parse returned NULL)
    7. Full overwrite to Silver Delta (365 rows → full table rewrite is cheap)
    8. Write manifest + SNS alert on failure

Why overwrite instead of merge:
    date_dim is 365 rows, 5 KB total. Full overwrite is faster and simpler
    than merging. Matches the Bronze strategy for this table.

Job parameters required:
    --S3_BUCKET       <BUCKET_NAME>
    --REGION          us-east-1
    --SNS_TOPIC_ARN   arn:aws:sns:us-east-1:...:pipeline-failure-alerts
    --datalake-formats delta
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
from pyspark.sql.types import IntegerType


# ─────────────────────────────────────────────
# HELPER FUNCTIONS (self-contained)
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
    overall = (
        "SUCCESS" if all(v == "SUCCESS" for v in table_results.values())
        else "PARTIAL_FAILURE" if any(v == "SUCCESS" for v in table_results.values())
        else "FAILED"
    )
    manifest = {
        "job": job_name,
        "run_date": run_date,
        "run_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "overall_status": overall,
        "tables": table_results
    }
    key = f"manifests/{job_name}/{run_date}/status.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json"
    )
    return key


def send_failure_alert(sns_topic_arn, region, job_name, run_date, table_results):
    failed = {k: v for k, v in table_results.items() if v != "SUCCESS"}
    message = (
        f"PIPELINE FAILURE — {job_name}\n"
        f"Run date: {run_date}\n\n"
        f"Failed tables:\n"
        + "\n".join(f"  • {tbl}: {err}" for tbl, err in failed.items())
        + "\n\nCheck CloudWatch logs and S3 manifest for details."
    )
    sns = boto3.client("sns", region_name=region)
    sns.publish(
        TopicArn=sns_topic_arn,
        Subject=f"[ALERT] Pipeline failure: {job_name} — {run_date}",
        Message=message
    )


def cast_bool(col):
    """Converts a string column to proper BOOLEAN (handles 'TRUE'/'FALSE'/'true'/'1'/etc.)"""
    c = F.upper(F.trim(col.cast("string")))
    return (
        F.when(c.isin("TRUE", "T", "1", "YES", "Y"), F.lit(True))
         .when(c.isin("FALSE", "F", "0", "NO", "N"),  F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    )


# ─────────────────────────────────────────────
# JOB ARGUMENTS
# ─────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "S3_BUCKET",
    "REGION",
    "SNS_TOPIC_ARN",
])

JOB_NAME    = args["JOB_NAME"]
S3_BUCKET   = args["S3_BUCKET"]
REGION      = args["REGION"]
SNS_TOPIC   = args["SNS_TOPIC_ARN"]
RUN_DATE    = date.today().isoformat()
RUN_TS      = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/date_dim"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/date_dim"
TABLE_LABEL = "date_dim"


# ─────────────────────────────────────────────
# SPARK + GLUE INITIALISATION
# ─────────────────────────────────────────────

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(JOB_NAME, args)

logger = get_logger(JOB_NAME)
logger.info(f"Job started — run_date={RUN_DATE}, bucket={S3_BUCKET}")
logger.info(f"Source: {BRONZE_PATH}")
logger.info(f"Target: {SILVER_PATH}")


# ─────────────────────────────────────────────
# MAIN CLEANING LOGIC
# ─────────────────────────────────────────────

table_results = {}

try:
    logger.info(f"━━━ Processing table: {TABLE_LABEL} ━━━")

    # ── Step 1: Read Bronze ──────────────────────────────
    logger.info(f"Reading Bronze Delta from {BRONZE_PATH}")
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    bronze_count = bronze_df.count()
    logger.info(f"  Bronze row count: {bronze_count:,}")

    # ── Step 2: Drop rows with NULL date_key ─────────────
    logger.info("Dropping rows with NULL date_key")
    df = bronze_df.filter(F.col("date_key").isNotNull())
    after_pk_filter = df.count()
    logger.info(f"  After PK null filter: {after_pk_filter:,} rows "
                f"({bronze_count - after_pk_filter:,} removed)")

    # ── Step 3: Parse date_key DD-MM-YYYY → DATE ────────
    # Keep the original string as 'date_key_raw' for traceability,
    # and produce a proper DATE column for joins.
    logger.info("Parsing date_key from 'DD-MM-YYYY' → DATE type")
    df = (
        df
        .withColumnRenamed("date_key", "date_key_raw")
        .withColumn("date_key", F.to_date(F.col("date_key_raw"), "dd-MM-yyyy"))
    )

    # ── Step 4: Type casting for numerics + booleans ────
    logger.info("Casting types: year/month/week → INT, is_weekend/is_holiday → BOOLEAN")
    df = (
        df
        .withColumn("year",       F.col("year").cast(IntegerType()))
        .withColumn("month",      F.col("month").cast(IntegerType()))
        .withColumn("week",       F.col("week").cast(IntegerType()))
        .withColumn("is_weekend", cast_bool(F.col("is_weekend")))
        .withColumn("is_holiday", cast_bool(F.col("is_holiday")))
    )

    # ── Step 5: Clean holiday_name ───────────────────────
    logger.info("Cleaning holiday_name: trim whitespace, empty-string → NULL")
    df = df.withColumn(
        "holiday_name",
        F.when(
            F.trim(F.col("holiday_name")).isNull() | (F.trim(F.col("holiday_name")) == ""),
            F.lit(None).cast("string")
        ).otherwise(F.trim(F.col("holiday_name")))
    )

    # ── Step 6: Reject rows where date parse failed ──────
    # If to_date returned NULL on a non-null source, the source was malformed.
    logger.info("Rejecting rows where date parse failed (date_key IS NULL)")
    df = df.filter(F.col("date_key").isNotNull())
    after_date_parse = df.count()
    logger.info(f"  After date-parse filter: {after_date_parse:,} rows "
                f"({after_pk_filter - after_date_parse:,} removed)")

    # ── Step 7: Add silver ingestion timestamp ───────────
    df = df.withColumn("silver_ingestion_ts", F.lit(RUN_TS))

    silver_count = after_date_parse
    logger.info(
        f"  Row count summary — Bronze: {bronze_count:,} → Silver: {silver_count:,} "
        f"({bronze_count - silver_count:,} removed, "
        f"{(silver_count / bronze_count * 100):.2f}% retained)"
    )

    # ── Step 8: Full overwrite to Silver ─────────────────
    logger.info(f"Writing Silver → OVERWRITE at {SILVER_PATH}")
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .partitionBy("ingestion_date")
          .save(SILVER_PATH)
    )
    logger.info(f"  {TABLE_LABEL}: overwrite complete.")

    table_results[TABLE_LABEL] = "SUCCESS"
    logger.info(f"✓ {TABLE_LABEL} — SUCCESS")

except Exception as e:
    error_msg = str(e)
    table_results[TABLE_LABEL] = f"FAILED: {error_msg}"
    logger.error(f"✗ {TABLE_LABEL} — FAILED: {error_msg}")


# ─────────────────────────────────────────────
# MANIFEST + ALERT + EXIT
# ─────────────────────────────────────────────

s3_client    = boto3.client("s3", region_name=REGION)
manifest_key = write_manifest(s3_client, S3_BUCKET, JOB_NAME, table_results, RUN_DATE)
logger.info(f"Status manifest written → s3://{S3_BUCKET}/{manifest_key}")

any_failed = any(v != "SUCCESS" for v in table_results.values())

if any_failed:
    logger.error(f"{TABLE_LABEL} failed. Sending SNS alert.")
    send_failure_alert(SNS_TOPIC, REGION, JOB_NAME, RUN_DATE, table_results)
    job.commit()
    raise Exception(f"{TABLE_LABEL} Silver ETL failed — check manifest and CloudWatch logs.")
else:
    logger.info(f"{TABLE_LABEL} Silver ETL complete. Pipeline continues.")
    job.commit()
