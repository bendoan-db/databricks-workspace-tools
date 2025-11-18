"""
Register Unity Catalog Functions for Job Triggering

This script registers a UC function that allows LLMs to trigger Databricks jobs
through the UC Function Toolkit.

Usage:
    python register_uc_functions.py --catalog <catalog_name> --schema <schema_name>

Or run in a Databricks notebook:
    %run ./register_uc_functions
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    FunctionParameterInfo,
    FunctionParameterInfos,
    CreateFunctionRequest,
    FunctionInfo
)
import argparse


def create_trigger_flow_function(catalog: str, schema: str) -> FunctionInfo:
    """
    Register the trigger_databricks_flow function in Unity Catalog.

    Args:
        catalog: The catalog name (e.g., 'main')
        schema: The schema name (e.g., 'default')

    Returns:
        FunctionInfo object with details of the created function
    """
    w = WorkspaceClient()

    function_name = f"{catalog}.{schema}.trigger_databricks_flow"

    # SQL function definition with inline Python
    sql_body = """CREATE OR REPLACE FUNCTION {function_name}(
    job_id STRING COMMENT 'The unique identifier of the Databricks Job to trigger. Find this in the Job URL or through the Jobs API.',
    notebook_params STRING DEFAULT NULL COMMENT 'JSON string of key-value pairs for notebook parameters. Example: {{"param1": "value1", "param2": "value2"}}',
    python_params STRING DEFAULT NULL COMMENT 'JSON array of command-line parameters for Python tasks. Example: ["--input", "data.csv"]',
    jar_params STRING DEFAULT NULL COMMENT 'JSON array of command-line parameters for JAR tasks. Example: ["arg1", "arg2"]',
    pipeline_params STRING DEFAULT NULL COMMENT 'JSON object for Delta Live Tables pipeline parameters. Example: {{"full_refresh": "true"}}',
    sql_params STRING DEFAULT NULL COMMENT 'JSON object for SQL query parameters. Example: {{"date": "2024-01-01"}}',
    dbt_commands STRING DEFAULT NULL COMMENT 'JSON array of dbt commands to run. Example: ["dbt run", "dbt test"]',
    idempotency_token STRING DEFAULT NULL COMMENT 'Token to guarantee idempotency. Multiple requests with the same token will not create duplicate runs.'
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Trigger a Databricks Job/Flow and return the run details. This function can be called by LLMs through the UC Function Toolkit to initiate workflows. Returns JSON with run_id, status, and run_page_url.'
AS $$
  from databricks.sdk import WorkspaceClient
  from databricks.sdk.service.jobs import RunNowResponse
  import json

  # Validate required parameters
  if not job_id or not job_id.strip():
      raise ValueError("job_id is required and cannot be empty")

  # Initialize Databricks Workspace Client
  w = WorkspaceClient()

  # Parse JSON parameters if provided
  parsed_notebook_params = None
  parsed_python_params = None
  parsed_jar_params = None
  parsed_pipeline_params = None
  parsed_sql_params = None
  parsed_dbt_commands = None

  try:
      if notebook_params:
          parsed_notebook_params = json.loads(notebook_params)
          if not isinstance(parsed_notebook_params, dict):
              raise ValueError("notebook_params must be a JSON object (dictionary)")

      if python_params:
          parsed_python_params = json.loads(python_params)
          if not isinstance(parsed_python_params, list):
              raise ValueError("python_params must be a JSON array")

      if jar_params:
          parsed_jar_params = json.loads(jar_params)
          if not isinstance(parsed_jar_params, list):
              raise ValueError("jar_params must be a JSON array")

      if pipeline_params:
          parsed_pipeline_params = json.loads(pipeline_params)
          if not isinstance(parsed_pipeline_params, dict):
              raise ValueError("pipeline_params must be a JSON object (dictionary)")

      if sql_params:
          parsed_sql_params = json.loads(sql_params)
          if not isinstance(parsed_sql_params, dict):
              raise ValueError("sql_params must be a JSON object (dictionary)")

      if dbt_commands:
          parsed_dbt_commands = json.loads(dbt_commands)
          if not isinstance(parsed_dbt_commands, list):
              raise ValueError("dbt_commands must be a JSON array")

  except json.JSONDecodeError as e:
      raise ValueError(f"Invalid JSON in parameters: {{str(e)}}")

  # Trigger the job
  try:
      run_response = w.jobs.run_now(
          job_id=int(job_id),
          notebook_params=parsed_notebook_params,
          python_params=parsed_python_params,
          jar_params=parsed_jar_params,
          pipeline_params=parsed_pipeline_params,
          sql_params=parsed_sql_params,
          dbt_commands=parsed_dbt_commands,
          idempotency_token=idempotency_token
      )

      # Get additional run details
      run_id = run_response.run_id
      run_details = w.jobs.get_run(run_id=run_id)

      # Construct response
      result = {{
          "success": True,
          "run_id": run_id,
          "number_in_job": run_response.number_in_job,
          "job_id": job_id,
          "state": {{
              "life_cycle_state": run_details.state.life_cycle_state.value if run_details.state else None,
              "state_message": run_details.state.state_message if run_details.state else None
          }},
          "run_page_url": run_details.run_page_url,
          "start_time": run_details.start_time,
          "message": f"Successfully triggered job {{job_id}}. Run ID: {{run_id}}"
      }}

      return json.dumps(result, indent=2)

  except Exception as e:
      error_result = {{
          "success": False,
          "job_id": job_id,
          "error": str(e),
          "error_type": type(e).__name__,
          "message": f"Failed to trigger job {{job_id}}: {{str(e)}}"
      }}
      return json.dumps(error_result, indent=2)
$$;""".format(function_name=function_name)

    print(f"Creating function: {function_name}")
    print("=" * 80)

    # Execute the SQL to create the function
    try:
        # Use SQL execution to create the function
        from databricks.sdk.service.sql import StatementState

        statement = w.statement_execution.execute_statement(
            warehouse_id=None,  # Will use default warehouse
            statement=sql_body,
            catalog=catalog,
            schema=schema
        )

        # Wait for completion
        statement = w.statement_execution.get_statement(statement.statement_id)
        while statement.status.state in [StatementState.PENDING, StatementState.RUNNING]:
            import time
            time.sleep(1)
            statement = w.statement_execution.get_statement(statement.statement_id)

        if statement.status.state == StatementState.SUCCEEDED:
            print(f"✓ Successfully created function: {function_name}")
        else:
            print(f"✗ Failed to create function: {statement.status.error}")
            raise Exception(f"Function creation failed: {statement.status.error}")

    except Exception as e:
        print(f"Note: Could not use SQL execution API. Trying direct SQL approach...")
        print(f"Error: {e}")
        print("\nPlease run the following SQL in your Databricks workspace:\n")
        print(sql_body)
        print("\n" + "=" * 80)
        return None

    # Verify the function was created
    try:
        function_info = w.functions.get(name=function_name)
        print(f"\n✓ Function verified: {function_info.full_name}")
        print(f"  Comment: {function_info.comment}")
        print(f"  Parameters: {len(function_info.input_params.parameters)}")
        return function_info
    except Exception as e:
        print(f"\n⚠ Warning: Could not verify function creation: {e}")
        return None


def register_all_functions(catalog: str = "main", schema: str = "default"):
    """
    Register all UC functions for job triggering.

    Args:
        catalog: The catalog name (default: 'main')
        schema: The schema name (default: 'default')
    """
    print("=" * 80)
    print("Unity Catalog Function Registration")
    print("=" * 80)
    print(f"Catalog: {catalog}")
    print(f"Schema: {schema}")
    print("=" * 80)
    print()

    # Create the trigger flow function
    function_info = create_trigger_flow_function(catalog, schema)

    print("\n" + "=" * 80)
    print("Registration Complete!")
    print("=" * 80)

    if function_info:
        print(f"\nFunction created: {catalog}.{schema}.trigger_databricks_flow")
        print("\nYou can now use this function:")
        print(f"  - In SQL: SELECT {catalog}.{schema}.trigger_databricks_flow('job_id')")
        print(f"  - With GenAI agents via UC Function Toolkit")
        print(f"\nTo test:")
        print(f"  SELECT {catalog}.{schema}.trigger_databricks_flow('YOUR_JOB_ID') as result;")
    else:
        print("\nPlease execute the SQL statement shown above in your Databricks workspace.")

    print()


def main():
    """Main entry point for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Register Unity Catalog functions for Databricks job triggering"
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="main",
        help="Unity Catalog catalog name (default: main)"
    )
    parser.add_argument(
        "--schema",
        type=str,
        default="default",
        help="Unity Catalog schema name (default: default)"
    )

    args = parser.parse_args()

    register_all_functions(catalog=args.catalog, schema=args.schema)


if __name__ == "__main__":
    # Check if running in Databricks notebook
    try:
        # This will exist in Databricks notebooks
        dbutils  # type: ignore
        print("Running in Databricks notebook environment")
        # Use default values when running in notebook
        register_all_functions(catalog="main", schema="default")
    except NameError:
        # Running as a script
        main()
