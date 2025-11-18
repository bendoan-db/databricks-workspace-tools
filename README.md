# Databricks Workspace Tools - Unity Catalog Job Trigger Function

A Unity Catalog function that enables LLMs to trigger Databricks Jobs/Flows through the UC Function Toolkit. This function is designed for GenAI applications using the Databricks Agent Framework.

## Features

- **🚀 Trigger Databricks Jobs**: Start workflows programmatically with support for various parameter types
- **🤖 LLM-Callable**: Designed to work seamlessly with the UC Function Toolkit for GenAI agents
- **🛡️ Error Handling**: Structured error responses for robust integrations
- **🔄 Idempotency**: Support for idempotency tokens to prevent duplicate runs
- **📝 Simple Testing**: Easy-to-use test notebook for validation

## Function

### `trigger_databricks_flow`

Trigger a Databricks Job/Flow with optional parameters.

**Parameters:**
- `job_id` (required): The unique identifier of the Databricks Job
- `notebook_params` (optional): JSON string of key-value pairs for notebook parameters
- `python_params` (optional): JSON array of command-line parameters for Python tasks
- `jar_params` (optional): JSON array of command-line parameters for JAR tasks
- `pipeline_params` (optional): JSON object for Delta Live Tables pipeline parameters
- `sql_params` (optional): JSON object for SQL query parameters
- `dbt_commands` (optional): JSON array of dbt commands
- `idempotency_token` (optional): Token to guarantee idempotency

**Returns:** JSON string with run details including `run_id`, `state`, and `run_page_url`

**Examples:**
```sql
-- Simple trigger
SELECT trigger_databricks_flow('123456789') as result;

-- With notebook parameters
SELECT trigger_databricks_flow(
    '123456789',
    '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
) as result;

-- With Python parameters
SELECT trigger_databricks_flow(
    job_id => '123456789',
    python_params => '["--mode", "production"]'
) as result;
```

## Installation

### 1. Install Python Dependencies

In your Databricks notebook:
```python
%pip install databricks-sdk>=0.40.0
dbutils.library.restartPython()
```

Or locally:
```bash
pip install -r requirements.txt
```

### 2. Register the Unity Catalog Function

#### Option A: In a Databricks Notebook
```python
%run ./register_uc_functions
```

#### Option B: From Command Line
```bash
python register_uc_functions.py --catalog main --schema default
```

The function will be registered as `main.default.trigger_databricks_flow` (or your specified catalog/schema).

### 3. Verify Registration

```sql
DESCRIBE FUNCTION main.default.trigger_databricks_flow;
```

## Quick Start - Testing

Use the provided test notebook to validate the function:

```python
# In Databricks notebook
%run ./test_function
```

The test notebook includes examples for:
- Simple job triggering
- Passing notebook parameters
- Passing Python command-line arguments
- Error handling
- Idempotency testing

## Usage Examples

### Example 1: Basic Job Trigger

```python
# Trigger a simple job
result = spark.sql("SELECT trigger_databricks_flow('123456789') as result").collect()[0]['result']
print(result)
```

### Example 2: Trigger with Notebook Parameters

```python
import json

notebook_params = {
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "environment": "production"
}

params_json = json.dumps(notebook_params)

result = spark.sql(f"""
    SELECT trigger_databricks_flow(
        '{job_id}',
        '{params_json}'
    ) as result
""").collect()[0]['result']

# Parse response
result_dict = json.loads(result)
if result_dict.get('success'):
    print(f"Run ID: {result_dict['run_id']}")
    print(f"URL: {result_dict['run_page_url']}")
```

### Example 3: Standalone Python Usage

```python
from trigger_flow_function import trigger_databricks_flow

# Test locally or in notebook without UC
result = trigger_databricks_flow(
    job_id="123456789",
    notebook_params='{"env": "test"}'
)
print(result)
```

### Example 4: Local Testing with CLI

```bash
# Simple trigger
python local_test.py --job-id 123456789

# With notebook parameters
python local_test.py --job-id 123456789 --params '{"env": "test"}'

# With Python parameters
python local_test.py --job-id 123456789 --python-params '["--verbose"]'

# Dry run (preview without executing)
python local_test.py --job-id 123456789 --params '{"env": "test"}' --dry-run
```

## GenAI Integration with UC Function Toolkit

These functions are designed to be used with the Databricks UC Function Toolkit, enabling LLMs to trigger and monitor workflows.

### Example: Creating an Agent with Job Triggering Capability

```python
# Define the UC function as a tool for your GenAI agent
tool = {
    "name": "trigger_databricks_flow",
    "description": "Trigger a Databricks Job/Flow and return run details",
    "function_name": "main.default.trigger_databricks_flow"
}

# Register with UC Function Toolkit
# The LLM can now call this function to trigger workflows
```

### Example Agent Prompt

```
You are a Databricks workflow assistant. You can help users trigger
Databricks jobs with appropriate parameters.

You have access to the trigger_databricks_flow tool which can:
- Start any Databricks job by job_id
- Pass notebook parameters as JSON
- Pass Python command-line arguments
- Trigger Delta Live Tables pipelines

Always provide clear feedback about the job trigger status and include
the run_page_url for users to monitor their jobs.
```

## Authentication

The functions use the Databricks SDK's default authentication methods:

1. **Databricks CLI configuration** (`~/.databrickscfg`)
2. **Environment variables**:
   - `DATABRICKS_HOST`
   - `DATABRICKS_TOKEN`
3. **Azure CLI authentication** (for Azure Databricks)
4. **Google Cloud credentials** (for GCP Databricks)

When running inside Databricks (notebooks, jobs), authentication is handled automatically.

## Error Handling

All functions return structured JSON responses with error information:

```json
{
  "success": false,
  "job_id": "123456789",
  "error": "Job not found",
  "error_type": "ResourceDoesNotExist",
  "message": "Failed to trigger job 123456789: Job not found"
}
```

### Common Errors

- **Invalid job_id**: Job doesn't exist or you don't have access
- **Invalid JSON parameters**: Malformed JSON in parameter strings
- **Authentication errors**: Missing or invalid credentials
- **Permission errors**: Insufficient privileges to trigger jobs

## Advanced Usage

### Idempotent Job Triggers

Prevent duplicate job runs using idempotency tokens:

```python
import uuid

token = str(uuid.uuid4())

# Multiple calls with the same token return the same run
result = spark.sql(f"""
    SELECT trigger_databricks_flow(
        job_id => '123456789',
        idempotency_token => '{token}'
    ) as result
""").collect()[0]['result']
```

### Delta Live Tables Integration

Trigger DLT pipelines with specific parameters:

```python
pipeline_params = {"full_refresh": "true"}
params_json = json.dumps(pipeline_params)

result = spark.sql(f"""
    SELECT trigger_databricks_flow(
        job_id => '{dlt_job_id}',
        pipeline_params => '{params_json}'
    ) as result
""").collect()[0]['result']
```

## Files in This Repository

- **`trigger_flow_function.py`**: Core Python function (can be used standalone for testing)
- **`register_uc_functions.py`**: Python script to register the function in Unity Catalog
- **`test_function.py`**: Test notebook with comprehensive examples
- **`local_test.py`**: Command-line tool for local testing before UC registration
- **`example_usage.py`**: Additional usage examples (Databricks notebook format)
- **`requirements.txt`**: Python dependencies
- **`QUICKSTART.md`**: 5-minute quick start guide
- **`README.md`**: This documentation

## Permissions Required

To use these functions, you need:

1. **CREATE FUNCTION** privilege in the target catalog and schema
2. **CAN MANAGE** or **CAN MANAGE RUN** permission on the jobs you want to trigger
3. **CAN VIEW** permission to list and check status of jobs

### Grant Permissions Example

```sql
-- Grant function execution permissions
GRANT EXECUTE ON FUNCTION main.default.trigger_databricks_flow TO `user@example.com`;
GRANT EXECUTE ON FUNCTION main.default.get_job_status TO `user@example.com`;
GRANT EXECUTE ON FUNCTION main.default.list_jobs TO `user@example.com`;
```

## Best Practices

1. **Use idempotency tokens** for critical workflows to prevent duplicate runs
2. **Implement retry logic** for transient failures
3. **Monitor job status** after triggering to ensure completion
4. **Set appropriate timeouts** when monitoring long-running jobs
5. **Use name filters** when listing jobs to reduce response size
6. **Validate parameters** before passing to job triggers
7. **Log job triggers** for audit trails
8. **Set up alerts** for failed job runs

## Troubleshooting

### Function Not Found

```sql
-- Verify function registration
DESCRIBE FUNCTION main.default.trigger_databricks_flow;
```

### Authentication Issues

```python
# Test authentication
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print(w.current_user.me())
```

### Permission Errors

```sql
-- Check function permissions
SHOW GRANTS ON FUNCTION main.default.trigger_databricks_flow;
```

### Job Not Found

```python
# Test with the standalone Python function first
from trigger_flow_function import trigger_databricks_flow
result = trigger_databricks_flow("YOUR_JOB_ID")
print(result)
```

### Registration Fails

If `register_uc_functions.py` fails, it will print the SQL statement. You can:
1. Copy the SQL output
2. Run it directly in a Databricks SQL editor or notebook
3. Verify the function is created with `DESCRIBE FUNCTION`

## Extending

This is a simple example for testing. You can extend it with:

- Additional UC functions for job status checking
- Job cancellation functionality
- Job listing and discovery
- Notification integrations (email, Slack, etc.)
- Custom retry logic and error handling

## Resources

- [Databricks SDK for Python](https://databricks-sdk-py.readthedocs.io)
- [UC Function Toolkit Documentation](https://docs.databricks.com/aws/en/generative-ai/agent-framework/create-custom-tool)
- [Unity Catalog Functions](https://docs.databricks.com/en/udf/unity-catalog.html)
- [Databricks Jobs API](https://docs.databricks.com/api/workspace/jobs)
- [Databricks Agent Framework](https://docs.databricks.com/en/generative-ai/agent-framework/)

## License

This project is provided as-is for educational and development purposes.

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the example usage notebook
3. Consult Databricks documentation
4. Open an issue in this repository
