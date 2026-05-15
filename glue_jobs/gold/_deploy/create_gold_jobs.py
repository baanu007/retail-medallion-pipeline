"""
Helper script that generates Glue job configs and creates them via AWS CLI.
Reusable for all Gold jobs. Run via: python create_gold_jobs.py <job1> <job2> ...
If no args, creates ALL jobs defined in JOBS dict.
"""
import json
import subprocess
import sys
from pathlib import Path

# ALL Gold jobs — add new ones here as they're built
JOBS = {
    # Dims + Fact
    "build-dim-date":       {"desc": "Gold dim_date — extended calendar 2020-2024 with US federal holidays", "timeout": 15, "workers": 2},
    "build-dim-customer":   {"desc": "Gold dim_customer — customer profile with CLV tier", "timeout": 20, "workers": 2},
    "build-dim-restaurant": {"desc": "Gold dim_restaurant — restaurant profile with revenue rank", "timeout": 15, "workers": 2},
    "build-dim-menu-item":  {"desc": "Gold dim_menu_item — menu item catalog", "timeout": 15, "workers": 2},
    "build-fact-orders":    {"desc": "Gold fact_orders — order-grain fact table", "timeout": 30, "workers": 2},
    # Metrics
    "build-gold-clv-snapshot":      {"desc": "Gold CLV snapshot (PRIMARY) — daily CLV evolution", "timeout": 45, "workers": 4},
    "build-gold-rfm":                {"desc": "Gold RFM segments", "timeout": 20, "workers": 2},
    "build-gold-churn":              {"desc": "Gold churn indicators", "timeout": 20, "workers": 2},
    "build-gold-sales-daily":        {"desc": "Gold sales_daily — revenue by date x restaurant x category", "timeout": 20, "workers": 2},
    "build-gold-sales-weekly":       {"desc": "Gold sales_weekly", "timeout": 15, "workers": 2},
    "build-gold-sales-monthly":      {"desc": "Gold sales_monthly", "timeout": 15, "workers": 2},
    "build-gold-loyalty":            {"desc": "Gold loyalty_comparison", "timeout": 15, "workers": 2},
    "build-gold-location-perf":      {"desc": "Gold location_perf", "timeout": 15, "workers": 2},
    "build-gold-upsell":             {"desc": "Gold upsell_analysis", "timeout": 20, "workers": 2},
}

BUCKET = "<BUCKET_NAME>"
REGION = "us-east-1"
SNS_ARN = "arn:aws:sns:us-east-1:<AWS_ACCOUNT_ID>:pipeline-failure-alerts"
ROLE = "AWSGlueServiceRole-retail-chain"
PROFILE = "retail-chain"


def build_config(job_name, desc, timeout, workers):
    return {
        "Name": job_name,
        "Description": desc,
        "Role": ROLE,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": f"s3://{BUCKET}/scripts/{job_name}.py",
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--job-language": "python",
            "--job-bookmark-option": "job-bookmark-disable",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-spark-ui": "false",
            "--datalake-formats": "delta",
            "--S3_BUCKET": BUCKET,
            "--REGION": REGION,
            "--SNS_TOPIC_ARN": SNS_ARN,
            "--TempDir": f"s3://{BUCKET}/temp/",
        },
        "MaxRetries": 0,
        "Timeout": timeout,
        "GlueVersion": "4.0",
        "NumberOfWorkers": workers,
        "WorkerType": "G.1X",
    }


def run(job_name):
    cfg = JOBS[job_name]
    config = build_config(job_name, cfg["desc"], cfg["timeout"], cfg["workers"])
    deploy_dir = Path(__file__).parent
    cfg_path = deploy_dir / f"{job_name}.json"
    cfg_path.write_text(json.dumps(config, indent=2))

    result = subprocess.run(
        ["aws", "glue", "create-job",
         "--cli-input-json", f"file://{cfg_path.as_posix()}",
         "--profile", PROFILE],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"  CREATED: {job_name}")
    elif "already exists" in result.stderr:
        print(f"  EXISTS:  {job_name} (updating instead)")
        subprocess.run(
            ["aws", "glue", "update-job",
             "--job-name", job_name,
             "--job-update", json.dumps({k: v for k, v in config.items() if k != "Name"}),
             "--profile", PROFILE],
            capture_output=True, text=True
        )
    else:
        print(f"  ERROR:   {job_name} — {result.stderr}")


if __name__ == "__main__":
    targets = sys.argv[1:] if len(sys.argv) > 1 else list(JOBS.keys())
    for j in targets:
        if j not in JOBS:
            print(f"  UNKNOWN: {j}")
            continue
        run(j)
