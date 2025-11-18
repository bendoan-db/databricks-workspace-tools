"""
Unity Catalog Function to trigger Databricks Jobs/Flows.

This function can be called by LLMs through the UC Function Toolkit to trigger
Databricks workflows programmatically.

This is a standalone Python version for testing. The actual UC function
is registered via register_uc_functions.py
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunNowResponse
from typing import Optional
import json


def trigger_databricks_flow(
    job_id: str,
    notebook_params: Optional[str] = None,
    python_params: Optional[str] = None,
    jar_params: Optional[str] = None,
    pipeline_params: Optional[str] = None,
    sql_params: Optional[str] = None,
    dbt_commands: Optional[str] = None,
    idempotency_token: Optional[str] = None
) -> str:
    """
    Trigger a Databricks Job/Flow and return the run details.

    This function initiates a Databricks Job run using the Workspace SDK. It supports
    various parameter types for different task types (notebooks, Python scripts, JARs,
    Delta Live Tables pipelines, SQL queries, and dbt projects).

    Args:
        job_id (str): The unique identifier of the Databricks Job to trigger.
                     You can find this in the Job URL or through the Jobs API.
                     Example: "123456789"

        notebook_params (str, optional): JSON string of key-value pairs for notebook parameters.
                                        Example: '{"param1": "value1", "param2": "value2"}'

        python_params (str, optional): JSON array of command-line parameters for Python tasks.
                                      Example: '["--input", "data.csv", "--output", "results.csv"]'

        jar_params (str, optional): JSON array of command-line parameters for JAR tasks.
                                   Example: '["arg1", "arg2"]'

        pipeline_params (str, optional): JSON object for Delta Live Tables pipeline parameters.
                                        Example: '{"full_refresh": "true"}'

        sql_params (str, optional): JSON object for SQL query parameters.
                                   Example: '{"date": "2024-01-01"}'

        dbt_commands (str, optional): JSON array of dbt commands to run.
                                     Example: '["dbt run", "dbt test"]'

        idempotency_token (str, optional): Token to guarantee idempotency. If specified,
                                          multiple requests with the same token will not
                                          create duplicate runs.

    Returns:
        str: JSON string containing run details including:
            - run_id: The unique identifier for this job run
            - number_in_job: The sequential run number for this job
            - status: Current status of the run
            - run_page_url: Direct URL to view the run in the Databricks UI
            - job_id: The job that was triggered

    Raises:
        ValueError: If job_id is not provided or invalid JSON in parameter strings
        Exception: If the Databricks API call fails

    Examples:
        # Trigger a simple job
        trigger_databricks_flow(job_id="123456789")

        # Trigger a notebook job with parameters
        trigger_databricks_flow(
            job_id="123456789",
            notebook_params='{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
        )

        # Trigger a Python job with command-line arguments
        trigger_databricks_flow(
            job_id="987654321",
            python_params='["--mode", "production", "--verbose"]'
        )

        # Trigger a DLT pipeline with full refresh
        trigger_databricks_flow(
            job_id="555555555",
            pipeline_params='{"full_refresh": "true"}'
        )

    Note:
        This function requires proper Databricks authentication. It will use the default
        authentication methods from databricks-sdk:
        - Databricks CLI configuration (~/.databrickscfg)
        - Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)
        - Azure CLI authentication (for Azure Databricks)
        - Google Cloud credentials (for GCP Databricks)
    """

    # Validate required parameters
    if not job_id or not job_id.strip():
        raise ValueError("job_id is required and cannot be empty")

    # Initialize Databricks Workspace Client
    # The SDK will automatically handle authentication using default methods
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
        raise ValueError(f"Invalid JSON in parameters: {str(e)}")

    # Trigger the job
    try:
        run_response: RunNowResponse = w.jobs.run_now(
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
        result = {
            "success": True,
            "run_id": run_id,
            "number_in_job": run_response.number_in_job,
            "job_id": job_id,
            "state": {
                "life_cycle_state": run_details.state.life_cycle_state.value if run_details.state else None,
                "state_message": run_details.state.state_message if run_details.state else None
            },
            "run_page_url": run_details.run_page_url,
            "start_time": run_details.start_time,
            "message": f"Successfully triggered job {job_id}. Run ID: {run_id}"
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        error_result = {
            "success": False,
            "job_id": job_id,
            "error": str(e),
            "error_type": type(e).__name__,
            "message": f"Failed to trigger job {job_id}: {str(e)}"
        }
        return json.dumps(error_result, indent=2)


# Example usage for testing
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python trigger_flow_function.py <job_id> [notebook_params_json]")
        print("\nExample:")
        print('  python trigger_flow_function.py 123456789')
        print('  python trigger_flow_function.py 123456789 \'{"param1": "value1"}\'')
        sys.exit(1)

    job_id = sys.argv[1]
    notebook_params = sys.argv[2] if len(sys.argv) > 2 else None

    print(f"Triggering job {job_id}...")
    result = trigger_databricks_flow(job_id, notebook_params)
    print(result)
