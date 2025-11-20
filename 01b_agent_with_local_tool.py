# Databricks notebook source
import json
from typing import Annotated, Any, Generator, Optional, Sequence, TypedDict, Union, Dict
from uuid import uuid4

import mlflow
from databricks_langchain import (
    ChatDatabricks,
    UCFunctionToolkit,
    VectorSearchRetrieverTool,
)

from databricks.sdk import WorkspaceClient
from databricks_ai_bridge import ModelServingUserCredentials  # if you want OBO in serving

from langchain_core.language_models import LanguageModelLike
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    BaseMessage,
    convert_to_openai_messages,
)
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph
from langgraph.graph.graph import CompiledGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.entities import SpanType

from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)

# TODO make sure you update the config file
configs = mlflow.models.ModelConfig(development_config="./config.yaml")

databricks_config = configs.get('databricks_config')
agent_configs = configs.get("llm_config")

CATALOG = databricks_config.get("catalog")
SCHEMA = databricks_config.get("schema")

LLM = ChatDatabricks(endpoint=agent_configs.get("llm_endpoint_name"))
LLM_TEMPERATURE = agent_configs.get("llm_parameters").get("temperature")
SYSTEM_PROMPT = agent_configs.get("llm_prompt_template")

########################################################
# Define custom tool to run job using workspace client #
########################################################
class RunJobTool(BaseTool):
    """
    LangChain tool that runs a Databricks job and returns the run result as a JSON string.

    Attributes:
        job_id: The Databricks job ID to run when the tool is invoked.
    """

    # ---- LangChain / BaseTool metadata ----
    name: str = "run_job"
    description: str = (
        "Run the Databricks job with a given job_id and return the run details as JSON. "
        "Use this when you need to trigger a predefined workflow."
    )
    job_id: str = ""

    def _run(self, job_id: Optional[str] = None) -> str:
        """
        Run the configured Databricks job.

        Args:
            job_id: Optional override for the job ID. If not provided, uses the default
                job_id set on the tool instance.

        Returns:
            str: A JSON string containing the run result.
        """

        # Instantiate the WorkspaceClient with OBO credentials strategy.
        # This is appropriate for model serving when the endpoint is configured
        # with an AuthPolicy that supports on-behalf-of (OBO) auth.
        
        #client = WorkspaceClient(hostname=os.environ.get("DATABRICKS_HOST"), token=os.environ.get("DATABRICKS_TOKEN"))
        client = WorkspaceClient(credentials_strategy=ModelServingUserCredentials())
                
        # Trigger job run and wait for completion
        run = client.jobs.run_now(job_id=job_id)
        run_result = run.result()  # blocks until job finishes

        # Return the result as a JSON string
        return json.dumps(run_result.as_dict())

#you can also leave no job id to have model dynamically pass in job ids
job_tool = RunJobTool(job_id="898077102753814")
TOOLS = [job_tool]


########################
## Define agent logic ##
########################

mlflow.set_tracking_uri("databricks")

def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[ToolNode, Sequence[BaseTool]],
    system_prompt: Optional[str] = None,
) -> CompiledGraph:
    model = model.bind_tools(tools)

    # Define the function that determines which node to go to
    def should_continue(state: ChatAgentState):
        messages = state["messages"]
        last_message = messages[-1]
        # If there are function calls, continue. else, end
        if last_message.get("tool_calls"):
            return "continue"
        else:
            return "end"

    if system_prompt:
        preprocessor = RunnableLambda(
            lambda state: [{"role": "system", "content": system_prompt}]
            + state["messages"]
        )
    else:
        preprocessor = RunnableLambda(lambda state: state["messages"])
    model_runnable = preprocessor | model

    def call_model(
        state: ChatAgentState,
        config: RunnableConfig,
    ):
        response = model_runnable.invoke(state, config)

        return {"messages": [response]}

    workflow = StateGraph(ChatAgentState)

    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ChatAgentToolNode(tools))

    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        "agent",
        should_continue,
        {
            "continue": "tools",
            "end": END,
        },
    )
    workflow.add_edge("tools", "agent")

    return workflow.compile()


class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent: CompiledStateGraph):
        self.agent = agent

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        request = {"messages": self._convert_messages_to_dict(messages)}

        messages = []
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                messages.extend(
                    ChatAgentMessage(**msg) for msg in node_data.get("messages", [])
                )
        return ChatAgentResponse(messages=messages)

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        request = {"messages": self._convert_messages_to_dict(messages)}
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                yield from (
                    ChatAgentChunk(**{"delta": msg}) for msg in node_data["messages"]
                )


# Create the agent object, and specify it as the agent object to use when
# loading the agent back for inference via mlflow.models.set_model()
mlflow.langchain.autolog()
agent = create_tool_calling_agent(LLM, TOOLS, SYSTEM_PROMPT)
AGENT = LangGraphChatAgent(agent)
mlflow.models.set_model(AGENT)
