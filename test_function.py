# Databricks notebook source
# MAGIC %md
# MAGIC # Test Unity Catalog Job Trigger Function
# MAGIC
# MAGIC This notebook demonstrates how to test the `trigger_databricks_flow` UC function.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC 1. Run `register_uc_functions.py` to register the UC function
# MAGIC 2. Have at least one Databricks Job created
# MAGIC 3. Know the job_id of a job you want to test with

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Dependencies

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.40.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Register the UC Function
# MAGIC
# MAGIC Run the registration script to create the function in Unity Catalog.

# COMMAND ----------

# MAGIC %run ./register_uc_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test the Function
# MAGIC
# MAGIC ### Test 1: Trigger a simple job (no parameters)

# COMMAND ----------

# Replace with your actual job_id
job_id = "YOUR_JOB_ID_HERE"  # Update this!

# Trigger the job using the UC function
result = spark.sql(f"""
    SELECT trigger_databricks_flow('{job_id}') as result
""").collect()[0]['result']

print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 2: Trigger a job with notebook parameters

# COMMAND ----------

import json

job_id = "YOUR_JOB_ID_HERE"  # Update this!

# Define notebook parameters
notebook_params = {
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "environment": "test"
}

# Convert to JSON string
params_json = json.dumps(notebook_params)

# Trigger with parameters
result = spark.sql(f"""
    SELECT trigger_databricks_flow(
        '{job_id}',
        '{params_json}'
    ) as result
""").collect()[0]['result']

print(result)

# Parse the result
result_dict = json.loads(result)
if result_dict.get('success'):
    print(f"\n✓ Job triggered successfully!")
    print(f"  Run ID: {result_dict['run_id']}")
    print(f"  View run: {result_dict['run_page_url']}")
else:
    print(f"\n✗ Failed to trigger job")
    print(f"  Error: {result_dict.get('message')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 3: Trigger a Python job with command-line arguments

# COMMAND ----------

job_id = "YOUR_JOB_ID_HERE"  # Update this!

# Python command-line parameters
python_params = ["--mode", "test", "--verbose"]
params_json = json.dumps(python_params)

result = spark.sql(f"""
    SELECT trigger_databricks_flow(
        job_id => '{job_id}',
        python_params => '{params_json}'
    ) as result
""").collect()[0]['result']

print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 4: Test error handling (invalid job_id)

# COMMAND ----------

# Try with a non-existent job
result = spark.sql("""
    SELECT trigger_databricks_flow('999999999') as result
""").collect()[0]['result']

result_dict = json.loads(result)
print("Error handling test:")
print(f"  Success: {result_dict.get('success')}")
print(f"  Message: {result_dict.get('message')}")
print(f"  Error Type: {result_dict.get('error_type')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 5: Test with idempotency token

# COMMAND ----------

import uuid

job_id = "YOUR_JOB_ID_HERE"  # Update this!
idempotency_token = str(uuid.uuid4())

print(f"Using idempotency token: {idempotency_token}")

# First call
result1 = spark.sql(f"""
    SELECT trigger_databricks_flow(
        job_id => '{job_id}',
        idempotency_token => '{idempotency_token}'
    ) as result
""").collect()[0]['result']

result1_dict = json.loads(result1)
print(f"\nFirst call - Run ID: {result1_dict.get('run_id')}")

# Second call with same token (should return same run)
result2 = spark.sql(f"""
    SELECT trigger_databricks_flow(
        job_id => '{job_id}',
        idempotency_token => '{idempotency_token}'
    ) as result
""").collect()[0]['result']

result2_dict = json.loads(result2)
print(f"Second call - Run ID: {result2_dict.get('run_id')}")
print(f"\nSame run returned: {result1_dict.get('run_id') == result2_dict.get('run_id')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Function Registration

# COMMAND ----------

# Check that the function is registered
functions = spark.sql("""
    SELECT function_name, data_type, comment
    FROM system.information_schema.routines
    WHERE routine_catalog = current_catalog()
      AND routine_schema = current_schema()
      AND function_name = 'trigger_databricks_flow'
""").collect()

if functions:
    print("✓ Function registered successfully:")
    for func in functions:
        print(f"  Name: {func.function_name}")
        print(f"  Type: {func.data_type}")
        print(f"  Comment: {func.comment}")
else:
    print("✗ Function not found. Please run register_uc_functions.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Test Standalone Python Function (Optional)
# MAGIC
# MAGIC You can also test the function directly from Python without UC.

# COMMAND ----------

from trigger_flow_function import trigger_databricks_flow

job_id = "YOUR_JOB_ID_HERE"  # Update this!

# Call the Python function directly
result = trigger_databricks_flow(job_id)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Once testing is complete, you can:
# MAGIC 1. Use this function in GenAI applications via the UC Function Toolkit
# MAGIC 2. Grant permissions to users/agents who need to trigger jobs
# MAGIC 3. Monitor job runs through the Databricks Jobs UI
# MAGIC
# MAGIC ### Grant Permissions Example
# MAGIC ```sql
# MAGIC GRANT EXECUTE ON FUNCTION main.default.trigger_databricks_flow TO `user@example.com`;
# MAGIC ```
