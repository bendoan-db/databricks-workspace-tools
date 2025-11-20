# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %pip install -q -U -r ./requirements.txt
# MAGIC %pip install -qqq uv
# MAGIC %pip install -q pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
from dbruntime.databricks_repl_context import get_context

HOSTNAME = get_context().browserHostName
USERNAME = get_context().user
TOKEN = get_context().apiToken

# os.environ['DATABRICKS_TOKEN'] = dbutils.secrets.get(scope="doan", key="db-pat-token")
os.environ['DATABRICKS_TOKEN'] = TOKEN
os.environ['DATABRICKS_HOST'] = "https://"+HOSTNAME

# COMMAND ----------

import yaml

with open('./config.yaml', 'r') as file:
    config = yaml.safe_load(file)

#load global configs
databricks_config = config['databricks_config']
agent_configs = config["llm_config"]

llm_name = agent_configs["llm_endpoint_name"]

#load uc configs
catalog=databricks_config['catalog']
schema=databricks_config['schema']
model_name=databricks_config['model']
mlflow_experiment=databricks_config['experiment']

# COMMAND ----------

print(mlflow_experiment)

# COMMAND ----------

import mlflow
from dbruntime.databricks_repl_context import get_context

experiment_fqdn = f"/Users/{get_context().user}/{mlflow_experiment}"

# Check if the experiment exists
experiment = mlflow.get_experiment_by_name(experiment_fqdn)

if experiment:
    experiment_id = experiment.experiment_id
    # Create the experiment if it does not exist
else:
    experiment_id = mlflow.create_experiment(experiment_fqdn)

mlflow.set_experiment(experiment_fqdn)

# COMMAND ----------

# MAGIC %run ./01b_agent_with_local_tool

# COMMAND ----------

from IPython.display import display, Image
 
display(Image(AGENT.agent.get_graph().draw_mermaid_png()))

# COMMAND ----------

example_input = {
        "messages": [
            {
                "role": "user",
                "content": "Run the test job with ID: 898077102753814 using your workspace tool",
            }
        ]
    }

# COMMAND ----------

response = AGENT.predict(example_input)

# COMMAND ----------

output_data = response.model_dump(exclude_none=True)

if output_data:
    print(output_data["messages"][-1]["content"])
else:
    print('No output available')


# COMMAND ----------

# MAGIC %md
# MAGIC # Register Model

# COMMAND ----------

import mlflow
from mlflow.models.resources import (
  DatabricksVectorSearchIndex,
  DatabricksServingEndpoint,
  DatabricksSQLWarehouse,
  DatabricksFunction,
  DatabricksGenieSpace,
  DatabricksTable,
  DatabricksUCConnection
)

from mlflow.models.auth_policy import AuthPolicy, SystemAuthPolicy, UserAuthPolicy

resources = [DatabricksServingEndpoint(endpoint_name=llm_name)]
system_auth_policy = SystemAuthPolicy(resources=resources)
user_auth_policy = UserAuthPolicy(api_scopes=["sql.statement-execution","sql.warehouses"])

with mlflow.start_run():
    logged_chain_info = mlflow.pyfunc.log_model(
        python_model=os.path.join(os.getcwd(), "01b_agent_with_local_tool"),
        model_config=os.path.join(os.getcwd(), "config.yaml"), 
        name=model_name,  # Required by MLflow
        input_example=example_input,
        # resources=[
        # DatabricksServingEndpoint(endpoint_name=llm_name),
        # ],
        pip_requirements=["-r ./requirements.txt"],
        auth_policy=AuthPolicy(
            system_auth_policy=system_auth_policy,
            #user_auth_policy=user_auth_policy
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Deploy

# COMMAND ----------

mlflow.models.predict(
    model_uri=f"runs:/{logged_chain_info.run_id}/{model_name}",
    input_data=example_input,
    env_manager="uv",
)

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_chain_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

from databricks import agents

agents.deploy(
    model_name=UC_MODEL_NAME,
    model_version=uc_registered_model_info.version,
)

# COMMAND ----------


