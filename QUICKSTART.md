# Quick Start Guide

Get up and running with the UC Job Trigger function in 5 minutes.

## Step 1: Upload Files to Databricks

Upload these files to your Databricks workspace:
- `register_uc_functions.py`
- `test_function.py`
- `trigger_flow_function.py`

## Step 2: Register the Function

In a Databricks notebook, run:

```python
%pip install databricks-sdk>=0.40.0
dbutils.library.restartPython()
```

Then:

```python
%run ./register_uc_functions
```

This creates the function: `main.default.trigger_databricks_flow`

## Step 3: Test It

```python
# Replace with your actual job ID
job_id = "YOUR_JOB_ID"

# Trigger the job
result = spark.sql(f"SELECT trigger_databricks_flow('{job_id}') as result").collect()[0]['result']
print(result)
```

Expected output:
```json
{
  "success": true,
  "run_id": 123456789,
  "number_in_job": 1,
  "job_id": "YOUR_JOB_ID",
  "state": {
    "life_cycle_state": "PENDING",
    "state_message": ""
  },
  "run_page_url": "https://your-workspace.databricks.com/...",
  "start_time": 1234567890000,
  "message": "Successfully triggered job YOUR_JOB_ID. Run ID: 123456789"
}
```

## Step 4: Use with GenAI

Register with UC Function Toolkit:

```python
tool = {
    "name": "trigger_databricks_flow",
    "description": "Trigger a Databricks workflow",
    "function_name": "main.default.trigger_databricks_flow"
}
```

## Common Use Cases

### Trigger with Parameters

```python
import json

params = {"env": "prod", "date": "2024-01-01"}
params_json = json.dumps(params)

result = spark.sql(f"""
    SELECT trigger_databricks_flow('{job_id}', '{params_json}') as result
""").collect()[0]['result']
```

### Idempotent Trigger

```python
import uuid

token = str(uuid.uuid4())

result = spark.sql(f"""
    SELECT trigger_databricks_flow(
        job_id => '{job_id}',
        idempotency_token => '{token}'
    ) as result
""").collect()[0]['result']
```

## Troubleshooting

**Function not found?**
```sql
DESCRIBE FUNCTION main.default.trigger_databricks_flow;
```

**Authentication error?**
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print(w.current_user.me())
```

**Need help?** Check the full [README.md](README.md) for detailed documentation.

## Next Steps

1. Grant permissions to users who need to trigger jobs
2. Integrate with your GenAI agent
3. Add monitoring and alerting
4. Extend with additional functions (job status, listing, etc.)

For comprehensive examples, see [test_function.py](test_function.py).
