# src/api/routes/chat.py
import os
import sys
import logging
import time
from functools import lru_cache
from fastapi import APIRouter, Body
from typing import Dict, Any, Optional
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

# Load system prompt from file
prompt_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'agents',
    'maintenance',
    'prompts',
    'system_prompt.txt'
)

# Read the system prompt file
with open(prompt_path, 'r') as f:
    SYSTEM_PROMPT = f.read()
logger.info(f"Loaded system prompt from: {prompt_path}")

# Add caching for expensive tool calls
@lru_cache(maxsize=32)
def cached_get_analysis_summary(query: str, timestamp: int) -> str:
    """Cached version of the analysis summary function."""
    return get_analysis_summary(query)

# Fast response functions
def is_simple_query(query: str) -> bool:
    """Check if query is simple and doesn't need tools."""
    query = query.lower().strip()
    
    # Simple greetings
    greetings = ["hi", "hello", "hey", "good morning", "good afternoon", "good evening"]
    if any(query == greeting for greeting in greetings):
        return True
    
    # Simple thank you messages
    thanks_phrases = ["thank you", "thanks", "appreciate it"]
    if any(phrase in query for phrase in thanks_phrases) and len(query.split()) < 8:
        return True
    
    # Tool queries - specific patterns about what tools are available
    tool_queries = ["what tool", "which tool", "available tool", "what kind of tool", 
                   "what type of tool", "tools do you have", "tools available"]
    if any(phrase in query.lower() for phrase in tool_queries):
        return True
    
    # Simple capability questions
    capability_questions = ["what can you do", "help me", "what are your capabilities"]
    if any(phrase in query for phrase in capability_questions) and len(query.split()) < 10:
        return True
    
    return False

def get_direct_response(query: str) -> Optional[str]:
    """Generate direct responses for simple queries."""
    query = query.lower().strip()
    
    # Simple greetings
    greetings = ["hi", "hello", "hey", "good morning", "good afternoon", "good evening"]
    if any(query == greeting for greeting in greetings):
        return "Hello! I'm your Maintenance Performance Analyst. How can I help you today?"
    
    # Thank you messages
    thanks_phrases = ["thank you", "thanks", "appreciate it"]
    if any(phrase in query for phrase in thanks_phrases):
        return "You're welcome! Let me know if you need any other assistance."
    
    # Tool queries
    tool_queries = ["what tool", "which tool", "available tool", "what kind of tool", 
                   "what type of tool", "tools do you have", "tools available"]
    if any(phrase in query.lower() for phrase in tool_queries):
        return """I have access to the following specialized tools:

1. **Raw Maintenance Data Access** - Retrieves detailed maintenance records
2. **Analysis Summary Tool** - Provides statistical summaries of performance metrics
3. **Mechanic Performance Profiler** - Gets metrics for individual mechanics
4. **Mechanic Comparison Tool** - Compares mechanics on specific metrics
5. **Machine Performance Analyzer** - Shows data for specific machine types
6. **Machine-Reason Combination Tool** - Analyzes specific failure patterns
7. **Scheduled Maintenance Workflow** - Runs preventative maintenance scheduling

Which of these would you like to use for your analysis?"""
    
    # Capability questions
    capability_questions = ["what can you do", "help me", "what are your capabilities"]
    if any(phrase in query for phrase in capability_questions):
        return """I can help with:
1. Analyzing mechanic performance data
2. Identifying maintenance issues
3. Running scheduled maintenance workflows
4. Comparing repair times
5. Analyzing machine performance

What would you like help with?"""
    
    return None

def needs_maintenance_tool(query: str) -> bool:
    """
    Determine if a query is specifically about scheduling maintenance.
    """
    maintenance_keywords = [
        "schedule maintenance", "preventative maintenance", "preventive maintenance",
        "maintenance schedule", "maintenance workflow", "maintenance tasks",
        "run maintenance", "create maintenance", "schedule service"
    ]
    
    query_lower = query.lower()
    return any(keyword in query_lower for keyword in maintenance_keywords)

@router.post("/agent/chat")
def chat(payload: Dict[str, Any] = Body(...)):
    start_time = time.time()
    query = payload.get("query", "")
    logger.info(f"Received query: {query}")
    
    # Fast path for simple queries
    if is_simple_query(query):
        direct_response = get_direct_response(query)
        if direct_response:
            logger.info(f"Using fast path. Time: {time.time() - start_time:.2f}s")
            return {"answer": direct_response}
    
    # Fast path for maintenance queries
    if needs_maintenance_tool(query):
        logger.info("Query involves maintenance scheduling. Using maintenance tool directly.")
        try:
            # Run the maintenance tool
            maintenance_result = run_scheduled_maintenance("run")
            
            # Format a user-friendly response
            formatted_response = f"""I've run the scheduled maintenance workflow for you. Here are the results:

{maintenance_result}

Would you like me to explain these results or make any adjustments to the maintenance schedule?"""
            
            logger.info(f"Completed maintenance scheduling. Time: {time.time() - start_time:.2f}s")
            return {"answer": formatted_response}
        except Exception as e:
            logger.error(f"Error running maintenance tool: {e}", exc_info=True)
            return {"answer": f"I encountered an issue while scheduling maintenance: {str(e)}. Would you like me to try again?"}
    
    # Initialize LLM
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
    
    # Add instruction to provide progress updates
    enhanced_prompt = SYSTEM_PROMPT + "\n\nIMPORTANT: When performing analysis, let the user know what you're doing."
    
    # Initialize tools with caching
    tools = [
        Tool(
            name="RawMaintenanceData",
            func=get_raw_maintenance_data,
            description="Get raw maintenance data. Use 'sample' for first 5 records."
        ),
        Tool(
            name="AnalysisSummary",
            func=lambda query: cached_get_analysis_summary(query, int(time.time() / 300) * 300),
            description="Get analysis summary data. Use 'all' for entire summary or a specific section name."
        ),
        Tool(
            name="MechanicPerformance",
            func=get_mechanic_performance,
            description="Get performance data for a specific mechanic. Input should be the mechanic's name."
        ),
        Tool(
            name="CompareMechanics",
            func=compare_mechanics,
            description="Compare all mechanics based on a metric. Input should be 'repair_time', 'response_time', etc."
        ),
        Tool(
            name="MachinePerformance",
            func=get_machine_performance,
            description="Get performance data for a specific machine type. Input should be the machine type name."
        ),
        Tool(
            name="MachineReasonData",
            func=get_machine_reason_data,
            description="Get data for machine and reason combinations. Input should be in format 'MachineType_ReasonType'."
        ),
        Tool(
            name="RunScheduledMaintenance",
            func=run_scheduled_maintenance,
            description="Run the factory's scheduled maintenance workflow. Use this tool when asked to run scheduled maintenance, generate a maintenance schedule, or schedule maintenance tasks."
        )
    ]
    
    logger.info("Initializing agent...")
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
        result = agent.run(query)
        execution_time = time.time() - start_time
        logger.info(f"Agent execution completed in {execution_time:.2f} seconds")
        return {"answer": result}
    except Exception as e:
        logger.error(f"Error in agent execution: {e}", exc_info=True)
        return {"error": str(e)}