"""
common/utils.py
Shared helper functions for all Glue jobs (Bronze, Silver, Gold).
Used by: ingest_all_tables.py, all silver jobs, all gold jobs
"""

import json
import logging
import sys
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

def get_logger(job_name: str) -> logging.Logger:
    """
    Returns a configured logger that writes to stdout (captured by CloudWatch).
    Usage: logger = get_logger("bronze_ingest")
    """
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


# ─────────────────────────────────────────────
# SECRETS MANAGER
# ─────────────────────────────────────────────

def get_secret(secret_name: str, region: str) -> dict:
    """
    Fetches a JSON secret from AWS Secrets Manager and returns it as a dict.

    Args:
        secret_name: e.g. "globalpartners/aurora/credentials"
        region:      e.g. "us-east-1"

    Returns:
        dict with keys: username, password, host, port, dbname

    Raises:
        RuntimeError if the secret cannot be retrieved.
    """
    client = boto3.client("secretsmanager", region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except ClientError as e:
        raise RuntimeError(
            f"Failed to retrieve secret '{secret_name}': {e.response['Error']['Message']}"
        ) from e


# ─────────────────────────────────────────────
# JDBC URL BUILDER
# ─────────────────────────────────────────────

def build_jdbc_url(host: str, port: str, dbname: str) -> str:
    """
    Builds a SQL Server JDBC URL with SSL encryption enabled.

    Args:
        host:   Aurora endpoint, e.g. "mydb.cluster-xxxx.us-east-1.rds.amazonaws.com"
        port:   "1433"
        dbname: "globalpartners"

    Returns:
        JDBC URL string with SSL enabled.
    """
    return (
        f"jdbc:sqlserver://{host}:{port};"
        f"databaseName={dbname};"
        f"encrypt=true;"
        f"trustServerCertificate=false;"
        f"loginTimeout=30;"
    )


# ─────────────────────────────────────────────
# S3 STATUS MANIFEST
# ─────────────────────────────────────────────

def write_manifest(
    s3_client,
    bucket: str,
    job_name: str,
    table_results: dict,
    run_date: str
) -> str:
    """
    Writes a JSON status manifest to S3 after a job run.
    Downstream jobs and monitoring can read this to check per-table status.

    Args:
        s3_client:     boto3 S3 client
        bucket:        S3 bucket name
        job_name:      e.g. "bronze_ingest"
        table_results: dict of {table_name: "SUCCESS" | "FAILED: <error>"}
        run_date:      "YYYY-MM-DD"

    Returns:
        S3 key where manifest was written.

    Example manifest:
        {
          "job": "bronze_ingest",
          "run_date": "2026-04-09",
          "run_timestamp": "2026-04-09T02:05:23Z",
          "overall_status": "PARTIAL_FAILURE",
          "tables": {
            "order_items": "SUCCESS",
            "order_item_options": "FAILED: Connection timeout",
            "date_dim": "SUCCESS"
          }
        }
    """
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


# ─────────────────────────────────────────────
# SNS ALERT
# ─────────────────────────────────────────────

def send_failure_alert(
    sns_topic_arn: str,
    region: str,
    job_name: str,
    run_date: str,
    table_results: dict
) -> None:
    """
    Publishes a failure alert to an SNS topic.
    Only called when at least one table failed.

    Args:
        sns_topic_arn: ARN of your SNS topic
        region:        AWS region
        job_name:      e.g. "bronze_ingest"
        run_date:      "YYYY-MM-DD"
        table_results: dict of {table_name: status_string}
    """
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
