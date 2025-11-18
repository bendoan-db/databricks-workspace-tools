#!/usr/bin/env python3
"""
Local testing script for the trigger_databricks_flow function.

This allows you to test the function locally before registering it in Unity Catalog.

Usage:
    python local_test.py --job-id YOUR_JOB_ID
    python local_test.py --job-id YOUR_JOB_ID --params '{"env": "test"}'
    python local_test.py --job-id YOUR_JOB_ID --python-params '["--verbose"]'
"""

import argparse
import json
from trigger_flow_function import trigger_databricks_flow


def main():
    parser = argparse.ArgumentParser(
        description="Test the trigger_databricks_flow function locally"
    )
    parser.add_argument(
        "--job-id",
        required=True,
        help="The Databricks Job ID to trigger"
    )
    parser.add_argument(
        "--params",
        "--notebook-params",
        dest="notebook_params",
        help='Notebook parameters as JSON string, e.g. \'{"param1": "value1"}\''
    )
    parser.add_argument(
        "--python-params",
        help='Python parameters as JSON array, e.g. \'["--arg1", "value1"]\''
    )
    parser.add_argument(
        "--jar-params",
        help='JAR parameters as JSON array, e.g. \'["arg1", "arg2"]\''
    )
    parser.add_argument(
        "--pipeline-params",
        help='Pipeline parameters as JSON object, e.g. \'{"full_refresh": "true"}\''
    )
    parser.add_argument(
        "--sql-params",
        help='SQL parameters as JSON object, e.g. \'{"date": "2024-01-01"}\''
    )
    parser.add_argument(
        "--dbt-commands",
        help='dbt commands as JSON array, e.g. \'["dbt run", "dbt test"]\''
    )
    parser.add_argument(
        "--idempotency-token",
        help="Idempotency token to prevent duplicate runs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be executed without actually triggering the job"
    )

    args = parser.parse_args()

    print("=" * 80)
    print("Databricks Job Trigger - Local Test")
    print("=" * 80)
    print(f"Job ID: {args.job_id}")

    if args.notebook_params:
        print(f"Notebook Parameters: {args.notebook_params}")
    if args.python_params:
        print(f"Python Parameters: {args.python_params}")
    if args.jar_params:
        print(f"JAR Parameters: {args.jar_params}")
    if args.pipeline_params:
        print(f"Pipeline Parameters: {args.pipeline_params}")
    if args.sql_params:
        print(f"SQL Parameters: {args.sql_params}")
    if args.dbt_commands:
        print(f"dbt Commands: {args.dbt_commands}")
    if args.idempotency_token:
        print(f"Idempotency Token: {args.idempotency_token}")

    print("=" * 80)

    if args.dry_run:
        print("\n[DRY RUN] Would trigger job with the parameters above")
        print("Remove --dry-run flag to actually execute")
        return

    print("\nTriggering job...")
    print()

    try:
        result = trigger_databricks_flow(
            job_id=args.job_id,
            notebook_params=args.notebook_params,
            python_params=args.python_params,
            jar_params=args.jar_params,
            pipeline_params=args.pipeline_params,
            sql_params=args.sql_params,
            dbt_commands=args.dbt_commands,
            idempotency_token=args.idempotency_token
        )

        # Pretty print the result
        result_dict = json.loads(result)
        print("Result:")
        print("=" * 80)
        print(json.dumps(result_dict, indent=2))
        print("=" * 80)

        if result_dict.get('success'):
            print("\n✓ Job triggered successfully!")
            print(f"  Run ID: {result_dict.get('run_id')}")
            print(f"  Run Number: {result_dict.get('number_in_job')}")
            print(f"  Status: {result_dict.get('state', {}).get('life_cycle_state')}")
            print(f"  View Run: {result_dict.get('run_page_url')}")
        else:
            print("\n✗ Failed to trigger job")
            print(f"  Error: {result_dict.get('error_type')}")
            print(f"  Message: {result_dict.get('message')}")

    except Exception as e:
        print(f"\n✗ Exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
