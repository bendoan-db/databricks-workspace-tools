# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Functions for Job Triggering - Example Usage
# MAGIC
# MAGIC This notebook demonstrates how to use the Unity Catalog functions to trigger and monitor Databricks Jobs.
# MAGIC These functions are designed to be called by LLMs through the UC Function Toolkit for GenAI applications.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC 1. Register the UC functions using `register_uc_functions.sql`
# MAGIC 2. Have at least one Databricks Job created
# MAGIC 3. Ensure proper permissions to trigger jobs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Install Required Packages

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.40.0 pydantic>=2.0.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: List Available Jobs
# MAGIC
# MAGIC First, let's discover what jobs are available in the workspace.

# COMMAND ----------

# List all jobs (up to 25)
jobs_result = spark.sql("SELECT list_jobs() as result").collect()[0]['result']
print(jobs_result)

# COMMAND ----------

# List jobs with a specific name filter
etl_jobs = spark.sql("SELECT list_jobs(50, 'etl', false) as result").collect()[0]['result']
print(etl_jobs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Trigger a Simple Job
# MAGIC
# MAGIC Trigger a job without any parameters.

# COMMAND ----------

# Replace with your actual job_id
job_id = "123456789"  # Update this!

# Trigger the job
result = spark.sql(f"""
    SELECT trigger_databricks_flow('{job_id}') as result
""").collect()[0]['result']

print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Trigger a Notebook Job with Parameters
# MAGIC
# MAGIC Pass parameters to a notebook-based job.

# COMMAND ----------

import json

job_id = "123456789"  # Update this!

# Define notebook parameters
notebook_params = {
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "environment": "production"
}

# Convert to JSON string for the function
params_json = json.dumps(notebook_params)

# Trigger with parameters
result = spark.sql(f"""
    SELECT trigger_databricks_flow(
        '{job_id}',
        '{params_json}'
    ) as result
""").collect()[0]['result']

print(result)

# Parse the result to get the run_id
import json
result_dict = json.loads(result)
run_id = result_dict.get('run_id')
print(f"\nJob triggered! Run ID: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Trigger a Python Job with Command-Line Arguments

# COMMAND ----------

job_id = "987654321"  # Update this!

# Define Python parameters (command-line arguments)
python_params = ["--mode", "production", "--verbose", "--config", "/path/to/config.json"]

# Convert to JSON string
params_json = json.dumps(python_params)

# Trigger with Python parameters
result = spark.sql(f"""
    SELECT trigger_databricks_flow(
        job_id => '{job_id}',
        python_params => '{params_json}'
    ) as result
""").collect()[0]['result']

print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5: Check Job Run Status
# MAGIC
# MAGIC Monitor the status of a running or completed job.

# COMMAND ----------

# Use the run_id from a previous trigger
run_id = "12345678901"  # Update with actual run_id

# Get status
status_result = spark.sql(f"""
    SELECT get_job_status('{run_id}') as result
""").collect()[0]['result']

print(status_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 6: Trigger Delta Live Tables Pipeline with Full Refresh

# COMMAND ----------

dlt_job_id = "555555555"  # Update with your DLT job ID

# DLT pipeline parameters
pipeline_params = {
    "full_refresh": "true"
}

params_json = json.dumps(pipeline_params)

result = spark.sql(f"""
    SELECT trigger_databricks_flow(
        job_id => '{dlt_job_id}',
        pipeline_params => '{params_json}'
    ) as result
""").collect()[0]['result']

print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 7: Using with UC Function Toolkit for GenAI
# MAGIC
# MAGIC These functions are designed to work with the UC Function Toolkit, which allows LLMs to call them as tools.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import FunctionInfo

w = WorkspaceClient()

# Get function info (this shows what the LLM would see)
function_info = w.functions.get(name="main.default.trigger_databricks_flow")

print(f"Function Name: {function_info.full_name}")
print(f"Comment: {function_info.comment}")
print(f"\nParameters:")
for param in function_info.input_params.parameters:
    print(f"  - {param.name}: {param.type_name} - {param.comment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 8: Create a GenAI Agent with Job Triggering Capability
# MAGIC
# MAGIC Here's how you would set up an agent that can trigger jobs.

# COMMAND ----------

# Example: Creating tools for an agent
from typing import List, Dict

# Define the tools that the agent can use
tools = [
    {
        "name": "trigger_databricks_flow",
        "description": "Trigger a Databricks Job/Flow and return the run details",
        "catalog": "main",
        "schema": "default"
    },
    {
        "name": "get_job_status",
        "description": "Get the current status of a Databricks Job run",
        "catalog": "main",
        "schema": "default"
    },
    {
        "name": "list_jobs",
        "description": "List Databricks Jobs with optional filtering",
        "catalog": "main",
        "schema": "default"
    }
]

# These tools can be registered with the UC Function Toolkit
# The LLM can then call them to manage Databricks workflows

print("Tools configured for GenAI agent:")
for tool in tools:
    print(f"  - {tool['catalog']}.{tool['schema']}.{tool['name']}: {tool['description']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 9: Idempotent Job Triggers
# MAGIC
# MAGIC Use idempotency tokens to prevent duplicate job runs.

# COMMAND ----------

import uuid

job_id = "123456789"
idempotency_token = str(uuid.uuid4())  # Generate a unique token

# First call - creates a new run
result1 = spark.sql(f"""
    SELECT trigger_databricks_flow(
        job_id => '{job_id}',
        idempotency_token => '{idempotency_token}'
    ) as result
""").collect()[0]['result']

print("First call result:")
print(result1)

# Second call with same token - returns the same run (won't create duplicate)
result2 = spark.sql(f"""
    SELECT trigger_databricks_flow(
        job_id => '{job_id}',
        idempotency_token => '{idempotency_token}'
    ) as result
""").collect()[0]['result']

print("\nSecond call result (same run):")
print(result2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 10: Error Handling
# MAGIC
# MAGIC The functions return structured error information when something goes wrong.

# COMMAND ----------

# Try to trigger a non-existent job
try:
    result = spark.sql("""
        SELECT trigger_databricks_flow('999999999') as result
    """).collect()[0]['result']

    result_dict = json.loads(result)

    if not result_dict.get('success'):
        print("Error occurred:")
        print(f"  Error Type: {result_dict.get('error_type')}")
        print(f"  Message: {result_dict.get('message')}")
        print(f"  Details: {result_dict.get('error')}")
    else:
        print("Success!")

except Exception as e:
    print(f"Exception: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 11: Workflow - Trigger and Monitor
# MAGIC
# MAGIC Complete workflow: trigger a job and monitor it until completion.

# COMMAND ----------

import time
import json

def trigger_and_monitor_job(job_id: str, notebook_params: dict = None, poll_interval: int = 30, max_wait: int = 3600):
    """
    Trigger a job and monitor it until completion.

    Args:
        job_id: The job to trigger
        notebook_params: Optional parameters for the job
        poll_interval: How often to check status (seconds)
        max_wait: Maximum time to wait (seconds)
    """
    # Trigger the job
    if notebook_params:
        params_json = json.dumps(notebook_params)
        result = spark.sql(f"""
            SELECT trigger_databricks_flow('{job_id}', '{params_json}') as result
        """).collect()[0]['result']
    else:
        result = spark.sql(f"""
            SELECT trigger_databricks_flow('{job_id}') as result
        """).collect()[0]['result']

    result_dict = json.loads(result)

    if not result_dict.get('success'):
        print(f"Failed to trigger job: {result_dict.get('message')}")
        return None

    run_id = result_dict.get('run_id')
    print(f"Job triggered successfully! Run ID: {run_id}")
    print(f"Run URL: {result_dict.get('run_page_url')}")

    # Monitor the job
    elapsed = 0
    while elapsed < max_wait:
        # Check status
        status_result = spark.sql(f"""
            SELECT get_job_status('{run_id}') as result
        """).collect()[0]['result']

        status_dict = json.loads(status_result)

        if status_dict.get('success'):
            state = status_dict.get('state', {})
            life_cycle_state = state.get('life_cycle_state')
            result_state = state.get('result_state')

            print(f"[{elapsed}s] State: {life_cycle_state}, Result: {result_state}")

            # Check if completed
            if life_cycle_state in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
                print(f"\nJob completed with state: {result_state}")
                return status_dict

        time.sleep(poll_interval)
        elapsed += poll_interval

    print(f"\nTimeout after {max_wait} seconds")
    return None


# Example usage
job_id = "123456789"  # Update this!
params = {"env": "dev", "date": "2024-01-01"}

final_status = trigger_and_monitor_job(job_id, params, poll_interval=30, max_wait=600)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Integrate with GenAI Agents**: Use these functions with the UC Function Toolkit to enable LLMs to manage workflows
# MAGIC 2. **Add Custom Logic**: Extend the functions with additional features like retry logic, notifications, etc.
# MAGIC 3. **Security**: Ensure proper access controls are set on the UC functions
# MAGIC 4. **Monitoring**: Set up logging and monitoring for job triggers
# MAGIC
# MAGIC ## Resources
# MAGIC - [Databricks SDK Documentation](https://databricks-sdk-py.readthedocs.io)
# MAGIC - [UC Function Toolkit](https://docs.databricks.com/aws/en/generative-ai/agent-framework/create-custom-tool)
# MAGIC - [Unity Catalog Functions](https://docs.databricks.com/en/udf/unity-catalog.html)
