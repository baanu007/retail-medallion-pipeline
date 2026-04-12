"""
bronze-ingest-all-tables.py
Bronze ingestion job — reads all 3 source tables from RDS SQL Server via JDBC
and writes them to S3 Bronze as Delta Lake tables using MERGE/UPSERT.

Architecture:
    RDS SQL Server ──JDBC SSL──▶ PySpark DataFrame ──Delta MERGE──▶ S3 Bronze

Tables ingested:
    dbo.order_items          → s3://bucket/bronze/order_items/
    dbo.order_item_options   → s3://bucket/bronze/order_item_options/
    dbo.date_dim             → s3://bucket/bronze/date_dim/

Job parameters required:
    --S3_BUCKET       globalpartners-aws
    --SECRET_NAME     globalpartners/aurora/credentials
    --REGION          us-east-1
    --SNS_TOPIC_ARN   arn:aws:sns:...
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
from botocore.exceptions import ClientError
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ─────────────────────────────────────────────
# HELPER FUNCTIONS (inlined from utils.py)
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


def get_secret(secret_name, region):
    client = boto3.client("secretsmanager", region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except ClientError as e:
        raise RuntimeError(
            f"Failed to retrieve secret '{secret_name}': {e.response['Error']['Message']}"
        ) from e


def build_jdbc_url(host, port, dbname):
    return (
        f"jdbc:sqlserver://{host}:{port};"
        f"databaseName={dbname};"
        f"encrypt=true;"
        f"trustServerCertificate=false;"
        f"loginTimeout=30;"
    )


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


# ─────────────────────────────────────────────
# JOB ARGUMENTS
# ─────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "S3_BUCKET",
    "SECRET_NAME",
    "REGION",
    "SNS_TOPIC_ARN",
])

JOB_NAME    = args["JOB_NAME"]
S3_BUCKET   = args["S3_BUCKET"]
SECRET_NAME = args["SECRET_NAME"]
REGION      = args["REGION"]
SNS_TOPIC   = args["SNS_TOPIC_ARN"]
RUN_DATE    = date.today().isoformat()
RUN_TS      = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


# ─────────────────────────────────────────────
# CREDENTIALS
# ─────────────────────────────────────────────

logger.info(f"Fetching credentials from Secrets Manager: {SECRET_NAME}")
creds    = get_secret(SECRET_NAME, REGION)
jdbc_url = build_jdbc_url(creds["host"], creds["port"], creds["dbname"])
logger.info("Credentials retrieved successfully.")


# ─────────────────────────────────────────────
# TABLE DEFINITIONS
# ─────────────────────────────────────────────

TABLES = [
    {
        "source_table": "dbo.order_items",
        "s3_path":      f"s3://{S3_BUCKET}/bronze/order_items",
        "merge_keys":   ["ORDER_ID", "LINEITEM_ID"],
        "strategy":     "merge",
    },
    {
        "source_table": "dbo.order_item_options",
        "s3_path":      f"s3://{S3_BUCKET}/bronze/order_item_options",
        "merge_keys":   ["ORDER_ID", "LINEITEM_ID", "OPTION_GROUP_NAME", "OPTION_NAME"],
        "strategy":     "merge",
    },
    {
        "source_table": "dbo.date_dim",
        "s3_path":      f"s3://{S3_BUCKET}/bronze/date_dim",
        "merge_keys":   ["date_key"],
        "strategy":     "overwrite",
    },
]


# ─────────────────────────────────────────────
# HELPERS: READ + WRITE
# ─────────────────────────────────────────────

def delta_table_exists(s3_path):
    try:
        spark.read.format("delta").load(s3_path).limit(1).count()
        return True
    except Exception:
        return False


def read_source_table(table_name):
    logger.info(f"  Reading {table_name} via JDBC...")
    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", creds["username"])
        .option("password", creds["password"])
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("fetchsize", "10000")
        .option("numPartitions", "4")
        .load()
    )
    df = (
        df
        .withColumn("ingestion_date",      F.lit(RUN_DATE))
        .withColumn("ingestion_timestamp", F.lit(RUN_TS))
    )
    logger.info(f"  {table_name}: {df.count():,} rows loaded.")
    return df


def write_bronze(df, s3_path, merge_keys, strategy, table_name):
    if strategy == "overwrite":
        logger.info(f"  Writing {table_name} → OVERWRITE at {s3_path}")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("ingestion_date")
            .save(s3_path)
        )
        logger.info(f"  {table_name}: overwrite complete.")
        return

    logger.info(f"  Writing {table_name} → DELTA MERGE at {s3_path}")

    before = df.count()
    df = df.dropDuplicates(merge_keys)
    after = df.count()
    if before != after:
        logger.info(f"  {table_name}: deduped {before - after} duplicate rows on merge keys ({before} → {after}).")

    if not delta_table_exists(s3_path):
        logger.info(f"  No existing Delta table — doing initial write.")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("ingestion_date")
            .save(s3_path)
        )
        logger.info(f"  {table_name}: initial write complete.")
        return

    on_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
    logger.info(f"  Executing DeltaTable merge for {table_name}...")
    (
        DeltaTable.forPath(spark, s3_path)
        .alias("target")
        .merge(df.alias("source"), on_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info(f"  {table_name}: merge/upsert complete.")


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────

table_results = {}

for table_cfg in TABLES:
    source  = table_cfg["source_table"]
    s3_path = table_cfg["s3_path"]
    keys    = table_cfg["merge_keys"]
    strat   = table_cfg["strategy"]
    label   = source.split(".")[-1]

    logger.info(f"━━━ Processing table: {source} ━━━")
    try:
        df = read_source_table(source)
        write_bronze(df, s3_path, keys, strat, source)
        table_results[label] = "SUCCESS"
        logger.info(f"✓ {source} — SUCCESS")
    except Exception as e:
        error_msg = str(e)
        table_results[label] = f"FAILED: {error_msg}"
        logger.error(f"✗ {source} — FAILED: {error_msg}")


# ─────────────────────────────────────────────
# MANIFEST + ALERT + EXIT
# ─────────────────────────────────────────────

s3_client    = boto3.client("s3", region_name=REGION)
manifest_key = write_manifest(s3_client, S3_BUCKET, JOB_NAME, table_results, RUN_DATE)
logger.info(f"Status manifest written → s3://{S3_BUCKET}/{manifest_key}")

any_failed = any(v != "SUCCESS" for v in table_results.values())

if any_failed:
    logger.error("One or more tables failed. Sending SNS alert.")
    send_failure_alert(SNS_TOPIC, REGION, JOB_NAME, RUN_DATE, table_results)
    job.commit()
    raise Exception("One or more tables failed — check manifest and CloudWatch logs.")
else:
    logger.info("All tables ingested successfully. Pipeline continues to Silver.")
    job.commit()
