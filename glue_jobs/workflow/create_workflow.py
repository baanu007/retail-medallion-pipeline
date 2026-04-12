"""
create_workflow.py
Creates the `globalpartners-daily-pipeline` Glue Workflow and all 6 triggers.

Idempotent: deletes + recreates triggers on each run so re-running the script
always yields the same final state. The workflow itself is not deleted.

Run:
    python glue_jobs/workflow/create_workflow.py

Triggers created:
    1. gp-t0-ondemand         — ON_DEMAND     -> bronze-ingest-all-tables
    2. gp-t0-schedule         — SCHEDULED     -> bronze-ingest-all-tables
       (cron(0 2 * * ? *) = daily 02:00 UTC)
    3. gp-t1-bronze-to-silver — CONDITIONAL   -> 3 silver jobs
       (predicate: bronze SUCCEEDED)
    4. gp-t2-silver-to-dims   — CONDITIONAL   -> 4 dim jobs
       (predicate: ALL 3 silver jobs SUCCEEDED)
    5. gp-t3-dims-to-fact     — CONDITIONAL   -> fact_orders
       (predicate: ALL 4 dim jobs SUCCEEDED)
    6. gp-t4-fact-to-metrics  — CONDITIONAL   -> 9 metric jobs
       (predicate: fact_orders SUCCEEDED)
"""

import sys
import boto3
from botocore.exceptions import ClientError

WORKFLOW = "globalpartners-daily-pipeline"
PROFILE = "globalpartners"
REGION = "us-east-1"

session = boto3.Session(profile_name=PROFILE, region_name=REGION)
glue = session.client("glue")


def ensure_workflow():
    try:
        glue.get_workflow(Name=WORKFLOW)
        print(f"  EXISTS: workflow {WORKFLOW}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        glue.create_workflow(
            Name=WORKFLOW,
            Description="Daily BI pipeline: Bronze -> Silver -> Dims -> Fact -> Metrics",
            MaxConcurrentRuns=1,
        )
        print(f"  CREATED: workflow {WORKFLOW}")


def ensure_trigger(name, trigger_type, actions, predicate=None, schedule=None, description=""):
    # Delete if exists (simpler than updating)
    try:
        glue.delete_trigger(Name=name)
        waiter_tries = 0
        while waiter_tries < 10:
            try:
                glue.get_trigger(Name=name)
                import time
                time.sleep(1)
                waiter_tries += 1
            except ClientError as e:
                if e.response["Error"]["Code"] == "EntityNotFoundException":
                    break
                raise
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise

    kwargs = {
        "Name": name,
        "WorkflowName": WORKFLOW,
        "Type": trigger_type,
        "Description": description,
        "Actions": [{"JobName": a} for a in actions],
        # ON_DEMAND triggers reject StartOnCreation (they have no "state"
        # to start — they fire when the API is called).
        "StartOnCreation": trigger_type != "ON_DEMAND",
    }
    if predicate is not None:
        kwargs["Predicate"] = predicate
    if schedule is not None:
        kwargs["Schedule"] = schedule

    glue.create_trigger(**kwargs)
    print(f"  CREATED: trigger {name} ({trigger_type}) -> {actions}")


def main():
    ensure_workflow()

    # Clean up any prior ON_DEMAND starting trigger (Glue allows only ONE
    # starting trigger per workflow; we use SCHEDULED as the entry point).
    try:
        glue.delete_trigger(Name="gp-t0-ondemand")
        print("  DELETED: legacy gp-t0-ondemand trigger")
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise

    # T0: SCHEDULED — daily 02:00 UTC.
    # This is the only "starting" trigger. For ad-hoc runs, use the
    # `aws glue start-workflow-run` API which starts the workflow run
    # regardless of the schedule.
    ensure_trigger(
        "gp-t0-schedule",
        "SCHEDULED",
        actions=["bronze-ingest-all-tables"],
        schedule="cron(0 2 * * ? *)",
        description="Production schedule: fires daily at 02:00 UTC",
    )

    # T1: bronze SUCCEEDED -> silver layer (3 jobs in parallel)
    ensure_trigger(
        "gp-t1-bronze-to-silver",
        "CONDITIONAL",
        actions=[
            "clean-order-items",
            "clean-order-item-options",
            "clean-date-dim",
        ],
        predicate={
            "Logical": "AND",
            "Conditions": [{
                "LogicalOperator": "EQUALS",
                "JobName": "bronze-ingest-all-tables",
                "State": "SUCCEEDED",
            }],
        },
        description="On bronze SUCCEEDED, start all 3 silver ETLs in parallel",
    )

    # T2: all silver SUCCEEDED -> dim layer (4 jobs in parallel)
    ensure_trigger(
        "gp-t2-silver-to-dims",
        "CONDITIONAL",
        actions=[
            "build-dim-date",
            "build-dim-customer",
            "build-dim-restaurant",
            "build-dim-menu-item",
        ],
        predicate={
            "Logical": "AND",
            "Conditions": [
                {"LogicalOperator": "EQUALS", "JobName": "clean-order-items",         "State": "SUCCEEDED"},
                {"LogicalOperator": "EQUALS", "JobName": "clean-order-item-options",  "State": "SUCCEEDED"},
                {"LogicalOperator": "EQUALS", "JobName": "clean-date-dim",            "State": "SUCCEEDED"},
            ],
        },
        description="On all 3 silver jobs SUCCEEDED, start all 4 Gold dim jobs in parallel",
    )

    # T3: all dims SUCCEEDED -> fact_orders
    ensure_trigger(
        "gp-t3-dims-to-fact",
        "CONDITIONAL",
        actions=["build-fact-orders"],
        predicate={
            "Logical": "AND",
            "Conditions": [
                {"LogicalOperator": "EQUALS", "JobName": "build-dim-date",       "State": "SUCCEEDED"},
                {"LogicalOperator": "EQUALS", "JobName": "build-dim-customer",   "State": "SUCCEEDED"},
                {"LogicalOperator": "EQUALS", "JobName": "build-dim-restaurant", "State": "SUCCEEDED"},
                {"LogicalOperator": "EQUALS", "JobName": "build-dim-menu-item",  "State": "SUCCEEDED"},
            ],
        },
        description="On all 4 dim jobs SUCCEEDED, start fact_orders",
    )

    # T4: fact SUCCEEDED -> metrics (9 jobs in parallel)
    ensure_trigger(
        "gp-t4-fact-to-metrics",
        "CONDITIONAL",
        actions=[
            "build-gold-clv-snapshot",
            "build-gold-rfm",
            "build-gold-churn",
            "build-gold-sales-daily",
            "build-gold-sales-weekly",
            "build-gold-sales-monthly",
            "build-gold-loyalty",
            "build-gold-location-perf",
            "build-gold-upsell",
        ],
        predicate={
            "Logical": "AND",
            "Conditions": [{
                "LogicalOperator": "EQUALS",
                "JobName": "build-fact-orders",
                "State": "SUCCEEDED",
            }],
        },
        description="On fact_orders SUCCEEDED, start all 9 metric jobs in parallel",
    )

    print("\nWorkflow ready. Manually run with:")
    print(f"  aws glue start-workflow-run --name {WORKFLOW} --profile {PROFILE}")
    print("\nThe SCHEDULED trigger is ACTIVATED and will fire daily at 02:00 UTC.")


if __name__ == "__main__":
    try:
        main()
    except ClientError as e:
        print(f"AWS error: {e}", file=sys.stderr)
        sys.exit(1)
