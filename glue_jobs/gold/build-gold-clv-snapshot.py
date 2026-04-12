"""
build-gold-clv-snapshot.py
Gold PRIMARY METRIC: CLV daily evolution.

Requirement (from architecture v3 section 3.9):
    "Show how LTV evolves for each customer DAILY."

This is THE primary metric. One row per customer per calendar day, carrying
forward the customer's cumulative spend. Enables the dashboard to plot CLV
trajectories over time for any customer cohort.

Grain: 1 row per (user_id, date_key)
Rows: ~20,174 customers × ~1,400 days ≈ 28 million

Algorithm:
    1. Read fact_orders (non-anonymous only) + dim_customer + dim_date
    2. Aggregate fact to daily customer revenue
    3. Build (customer × date) grid from each customer's first_order_date
       to global max date (via inner join on date_key >= first_order_date)
    4. Left join daily revenue → cumulative spend via window function
    5. Compute daily CLV tier via ntile(5) partitioned by date:
         Tile 5 → "High"   (top 20%)
         Tiles 2-4 → "Medium"
         Tile 1 → "Low"    (bottom 20%)
    6. Partition output by year for query performance

Columns: user_id, date_key, daily_revenue, cumulative_spend, cumulative_orders,
         clv_tier, days_since_first_order, is_active_day

Partition: snapshot_year

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

FACT_PATH   = f"s3://{S3_BUCKET}/gold/fact_orders"
DIM_CUST    = f"s3://{S3_BUCKET}/gold/dim_customer"
DIM_DATE    = f"s3://{S3_BUCKET}/gold/dim_date"
GOLD_PATH   = f"s3://{S3_BUCKET}/gold/gold_clv_snapshot"
TABLE_LABEL = "gold_clv_snapshot"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.shuffle.partitions", "200")
job = Job(glueContext)
job.init(JOB_NAME, args)

logger = get_logger(JOB_NAME)
logger.info(f"Job started — run_date={RUN_DATE}")


# ─── Main ───
table_results = {}

try:
    logger.info(f"━━━ Processing table: {TABLE_LABEL} ━━━")

    # Step 1: Read sources
    logger.info("Reading fact_orders, dim_customer, dim_date")
    fact = (
        spark.read.format("delta").load(FACT_PATH)
        .filter(~F.col("is_anonymous"))
        .select("user_id", "order_date", "total_revenue")
    )
    customers = (
        spark.read.format("delta").load(DIM_CUST)
        .select("user_id", "first_order_date")
    )
    dates = (
        spark.read.format("delta").load(DIM_DATE)
        .select("date_key")
    )
    customer_count = customers.count()
    date_count = dates.count()
    logger.info(f"  Customers: {customer_count:,} | Dates: {date_count:,}")

    # Step 2: Daily revenue per customer
    logger.info("Aggregating fact to daily customer revenue")
    daily_revenue = (
        fact
        .groupBy("user_id", "order_date")
        .agg(
            F.round(F.sum("total_revenue"), 2).alias("daily_revenue"),
            F.count("*").alias("daily_order_count")
        )
    )

    # Step 3: Build (customer × active-date) grid using a range-join style
    # Each customer is cross-joined with dates from their first_order_date forward.
    logger.info("Building customer × date grid (each customer from first_order_date)")
    grid = (
        customers.alias("c")
        .join(
            dates.alias("d"),
            F.col("d.date_key") >= F.col("c.first_order_date"),
            "inner"
        )
        .select("c.user_id", "d.date_key", "c.first_order_date")
    )

    # Step 4: Left join daily revenue onto grid
    logger.info("Left-joining daily revenue onto the grid")
    grid_with_rev = (
        grid.alias("g")
        .join(
            daily_revenue.alias("r"),
            (F.col("g.user_id") == F.col("r.user_id"))
            & (F.col("g.date_key") == F.col("r.order_date")),
            "left"
        )
        .select(
            F.col("g.user_id").alias("user_id"),
            F.col("g.date_key").alias("date_key"),
            F.col("g.first_order_date").alias("first_order_date"),
            F.coalesce(F.col("r.daily_revenue"), F.lit(0.0)).alias("daily_revenue"),
            F.coalesce(F.col("r.daily_order_count"), F.lit(0)).alias("daily_order_count"),
        )
    )

    # Step 5: Cumulative spend + cumulative orders via window
    logger.info("Computing cumulative_spend and cumulative_orders per customer")
    cum_w = (
        Window
        .partitionBy("user_id")
        .orderBy("date_key")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    snapshot = (
        grid_with_rev
        .withColumn("cumulative_spend",
                    F.round(F.sum("daily_revenue").over(cum_w), 2))
        .withColumn("cumulative_orders",
                    F.sum("daily_order_count").over(cum_w))
        .withColumn("days_since_first_order",
                    F.datediff(F.col("date_key"), F.col("first_order_date")))
        .withColumn("is_active_day", F.col("daily_revenue") > 0)
    )

    # Step 6: Daily CLV tier — ntile(5) PARTITIONED BY date, ordered by cumulative_spend
    # Only customers who have actually ordered at least once (cumulative_spend > 0) get a tier.
    logger.info("Computing daily CLV tier (ntile(5) per date)")
    tier_w = Window.partitionBy("date_key").orderBy(F.col("cumulative_spend").asc())
    snapshot = (
        snapshot
        .withColumn("spend_quintile",
                    F.when(F.col("cumulative_spend") > 0,
                           F.ntile(5).over(tier_w)).otherwise(F.lit(None)))
        .withColumn("clv_tier",
                    F.when(F.col("spend_quintile") == 5, F.lit("High"))
                     .when(F.col("spend_quintile") == 1, F.lit("Low"))
                     .when(F.col("spend_quintile").isNotNull(), F.lit("Medium"))
                     .otherwise(F.lit("Pre-Activity")))
        .drop("spend_quintile")
    )

    # Step 7: Add partition column
    snapshot = (
        snapshot
        .withColumn("snapshot_year", F.year("date_key"))
        .withColumn("gold_ingestion_ts", F.lit(RUN_TS))
        .select(
            "user_id", "date_key", "daily_revenue", "daily_order_count",
            "cumulative_spend", "cumulative_orders", "clv_tier",
            "days_since_first_order", "is_active_day",
            "snapshot_year", "gold_ingestion_ts"
        )
    )

    logger.info(f"Writing to {GOLD_PATH} partitioned by snapshot_year")
    (
        snapshot.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("snapshot_year")
        .save(GOLD_PATH)
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
