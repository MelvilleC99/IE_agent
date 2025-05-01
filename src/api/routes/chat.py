# src/api/routes/chat.py
import os
import sys
import logging
import time
import json
import re
from functools import lru_cache
from typing import Dict, Any, List, Tuple, Optional
from fastapi import APIRouter, Body
from langchain.agents import initialize_agent, AgentType, Tool
from langchain_community.chat_models.openai import ChatOpenAI

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("chat_api")

router = APIRouter()

# Dynamically add the project root to the sys path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    logger.info(f"Added {project_root} to Python path")

# Import all tools from maintenance_agent.py
from src.agents.maintenance.maintenance_agent import (
    get_raw_maintenance_data,
    get_analysis_summary,
    get_mechanic_performance,
    compare_mechanics,
    get_machine_performance,
    get_machine_reason_data,
    run_scheduled_maintenance
)

# Import supabase tools
from src.agents.maintenance.tools.supabase_tool import (
    query_database,
    get_schema_info,
    insert_or_update_data
)

# Import MCP components
from src.MCP import (
    MCPProtocol, 
    MCPContextManager, 
    MCPToolRegistry, 
    MCPResponseFormatter, 
    FastPathDetector,
    MCPOrchestrator
)

# Load system prompt from file
prompt_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'agents',
    'maintenance',
    'prompts',
    'system_prompt.txt'
)

# Load the tool selection prompt
tool_selection_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'agents',
    'maintenance',
    'prompts',
    'tool_selection_prompt.txt'
)

# Read the system prompt file
with open(prompt_path, 'r') as f:
    SYSTEM_PROMPT = f.read()
logger.info(f"Loaded system prompt from: {prompt_path}")

# Read the tool selection prompt if it exists
TOOL_SELECTION_PROMPT = ""
if os.path.exists(tool_selection_path):
    with open(tool_selection_path, 'r') as f:
        TOOL_SELECTION_PROMPT = f.read()
    logger.info(f"Loaded tool selection prompt from: {tool_selection_path}")
else:
    logger.warning(f"Tool selection prompt not found at: {tool_selection_path}")

# Initialize MCP orchestrator - the central component of our architecture
mcp_orchestrator = MCPOrchestrator()

# Load prompts into orchestrator
if hasattr(mcp_orchestrator, 'load_prompts'):
    mcp_orchestrator.load_prompts(prompt_path, tool_selection_path)

# Add caching for database queries
@lru_cache(maxsize=32)
def cached_query_database(query_params: str, timestamp: int) -> str:
    """Cached version of the database query function."""
    return query_database(query_params)

@router.post("/agent/chat")
def chat(payload: Dict[str, Any] = Body(...)):
    """
    Process a chat request through the MCP architecture.
    
    Args:
        payload: Request payload with query
        
    Returns:
        Formatted response
    """
    start_time = time.time()
    query = payload.get("query", "")
    logger.info(f"Received query: {query}")
    
    # Process through MCP orchestrator
    response = mcp_orchestrator.process_query(query)
    
    # Check if orchestrator has provided a direct response (fast path or direct tool)
    if "_requires_llm" not in response:
        execution_time = time.time() - start_time
        logger.info(f"MCP orchestrator provided direct response in {execution_time:.2f} seconds")
        return response
    
    # If we get here, the orchestrator needs LLM processing
    logger.info("Query requires LLM processing")
    
    # Initialize LLM with DeepSeek API key
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        logger.error("DEEPSEEK_API_KEY not found")
        return {"error": "DEEPSEEK_API_KEY not found"}
    
    llm = ChatOpenAI(
        model="deepseek-chat",
        temperature=0.7,
        api_key=api_key,
        base_url="https://api.deepseek.com/v1"
    )
    
    # Get context and MCP message from orchestrator response
    mcp_message = response.get("_mcp_message", {})
    
    # Generate system prompt from MCP message
    mcp_protocol = MCPProtocol()
    mcp_system_prompt = mcp_protocol.generate_system_prompt(mcp_message)
    
    # Combine all prompt elements for the best context
    enhanced_prompt = SYSTEM_PROMPT + "\n\n" + TOOL_SELECTION_PROMPT + "\n\n" + mcp_system_prompt
    
    # Add formatting instructions to reduce commentary
    enhanced_prompt += "\n\nIMPORTANT: Provide direct answers without unnecessary commentary. Do not suggest follow-up actions unless explicitly asked. When reporting time measurements, convert milliseconds to minutes (divide by 60,000) and include the unit."
    
    # Create tools with proper functions (not method references that can't be serialized)
    tools = [
        Tool(
            name="QueryDatabase",
            func=lambda query_params: cached_query_database(query_params, int(time.time() / 300) * 300),
            description="Get database information. Use this for lists of mechanics, tasks, or current information. Format: 'table_name:column1,column2;filter1=value1,filter2=value2;limit=100'"
        ),
        Tool(
            name="RunScheduledMaintenance",
            func=lambda action="run": run_scheduled_maintenance(action),
            description="Create new maintenance tasks. Use only when asked to generate or create new schedules."
        ),
        Tool(
            name="RawMaintenanceData",
            func=lambda query=None: get_raw_maintenance_data(query),
            description="Get historical maintenance records for analysis."
        ),
        Tool(
            name="GetSchemaInfo",
            func=lambda query="": get_schema_info(query),
            description="Get database schema information to know what tables and fields exist."
        )
    ]
    
    logger.info(f"Initializing agent with {len(tools)} tools")
    agent = initialize_agent(
        tools,
        llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True,
        handle_parsing_errors=True,
        agent_kwargs={"prefix": enhanced_prompt}
    )
    
    try:
        logger.info("Running agent...")
        llm_start = time.time()
        llm_response = agent.run(query)
        llm_time = time.time() - llm_start
        logger.info(f"LLM execution completed in {llm_time:.2f} seconds")
        
        # Check if the response was a database query that we can format ourselves
        if "QueryDatabase" in llm_response:
            try:
                # Extract the query params using regex
                format_start = time.time()
                query_pattern = r'Action Input: [\'"]([^\'"]+)[\'"]'
                query_match = re.search(query_pattern, llm_response)
                
                if query_match:
                    query_params = query_match.group(1)
                    
                    # Get the data and format it ourselves
                    db_result = cached_query_database(query_params, int(time.time() / 300) * 300)
                    if db_result:
                        try:
                            result_data = json.loads(db_result)
                            formatted_result = mcp_orchestrator.response_formatter.format_data_adaptively(
                                result_data, query
                            )
                            
                            # Use this instead of the LLM's verbose response
                            llm_response = formatted_result
                            logger.info(f"Formatted database results directly in {time.time() - format_start:.2f} seconds")
                        except Exception as e:
                            logger.error(f"Error parsing database result: {e}")
            
            except Exception as formatting_error:
                logger.error(f"Error formatting database results: {formatting_error}")
        
        # Process the response using MCP orchestrator
        processed_response = mcp_orchestrator.process_llm_response(llm_response)
        
        execution_time = time.time() - start_time
        logger.info(f"Agent execution completed in {execution_time:.2f} seconds")
        
        return processed_response
    except Exception as e:
        logger.error(f"Error in agent execution: {e}", exc_info=True)
        return {"error": str(e)}