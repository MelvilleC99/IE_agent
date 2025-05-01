import os
import json
import sys
import logging
from typing import Optional
from dotenv import load_dotenv

# Dynamically add the project root to the sys path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("maintenance_agent")

# Load environment variables
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
env_path = os.path.join(project_root, '.env.local')
logger.info(f"Loading .env.local from: {env_path}")
logger.info(f"File exists: {os.path.exists(env_path)}")
load_dotenv(dotenv_path=env_path)

# Load system prompt from external file
prompt_path = os.path.join(
    os.path.dirname(__file__),
    'prompts',
    'system_prompt.txt'
)
if os.path.exists(prompt_path):
    with open(prompt_path, 'r') as f:
        SYSTEM_PROMPT = f.read()
else:
    SYSTEM_PROMPT = ''
    logger.warning(f"Warning: system prompt not found at {prompt_path}")

# Print the API key to verify it's loaded
api_key = os.getenv('DEEPSEEK_API_KEY')
if api_key:
    logger.info(f"API key loaded: {api_key[:5]}...")
else:
    logger.warning("API key not found in environment variables")

# Import LangChain components
from langchain.agents import initialize_agent, AgentType, Tool
from langchain_community.chat_models.openai import ChatOpenAI

# Add call_llm function
def call_llm(prompt: str) -> str:
    """
    Call the language model with a prompt.
    
    Args:
        prompt: The prompt to send to the language model
        
    Returns:
        The model's response as a string
    """
    api_key = os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        raise ValueError("DEEPSEEK_API_KEY not found in environment variables")
        
    llm = ChatOpenAI(
        model="deepseek-chat",
        temperature=0.7,
        api_key=api_key,
        base_url="https://api.deepseek.com/v1"
    )
    
    try:
        response = llm.invoke(prompt)
        if isinstance(response.content, str):
            return response.content
        elif isinstance(response.content, list):
            return "\n".join(str(item) for item in response.content)
        else:
            return str(response.content)
    except Exception as e:
        logger.error(f"Error calling LLM: {e}")
        return f"Error calling language model: {str(e)}"

# Data file paths - Use environment variable for RAW_DATA_PATH
RAW_DATA_PATH = os.getenv('RAW_DATA_PATH')
if not RAW_DATA_PATH:
    default_root = os.path.dirname(os.path.abspath(__file__))
    RAW_DATA_PATH = os.path.join(default_root, 'maintenance_data.json')

# Ensure RAW_DATA_PATH is a string and exists
if not isinstance(RAW_DATA_PATH, str):
    raise ValueError("RAW_DATA_PATH must be a string")
if not os.path.exists(RAW_DATA_PATH):
    raise FileNotFoundError(f"Maintenance data file not found at: {RAW_DATA_PATH}")

logger.info(f"Using RAW_DATA_PATH: {RAW_DATA_PATH}")
logger.info(f"RAW_DATA_PATH exists: {os.path.exists(RAW_DATA_PATH)}")

ANALYSIS_SUMMARY_PATH = os.getenv('ANALYSIS_SUMMARY_PATH', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_summary_output.json'))
logger.info(f"Using ANALYSIS_SUMMARY_PATH: {ANALYSIS_SUMMARY_PATH}")

# Workflow wrapper will be imported as a tool
from src.agents.maintenance.tools.scheduled_maintenance_tool import scheduled_maintenance_tool

# Import Supabase tools
from src.agents.maintenance.tools.supabase_tool import query_database, get_schema_info, insert_or_update_data

# Existing tool functions
def get_raw_maintenance_data(query: Optional[str] = None) -> str:
    """Get raw maintenance data from the JSON file."""
    logger.info(f"get_raw_maintenance_data called with query: {query}")
    try:
        if not isinstance(RAW_DATA_PATH, str):
            raise ValueError("RAW_DATA_PATH must be a string")
        if not os.path.exists(RAW_DATA_PATH):
            raise FileNotFoundError(f"Maintenance data file not found at: {RAW_DATA_PATH}")
            
        logger.info(f"Attempting to read from RAW_DATA_PATH: {RAW_DATA_PATH}")
        with open(str(RAW_DATA_PATH), 'r', encoding='utf-8') as f:
            data = json.load(f)
        return json.dumps(data[:5] if query == "sample" else data, indent=2)
    except Exception as e:
        logger.error(f"Error accessing raw data: {e}")
        return f"Error accessing raw data: {str(e)}"

def get_analysis_summary(query: Optional[str] = None) -> str:
    """Get analysis summary from the JSON file."""
    logger.info(f"get_analysis_summary called with query: {query}")
    try:
        # Create empty file if it doesn't exist to prevent errors
        if not os.path.exists(ANALYSIS_SUMMARY_PATH):
            logger.warning(f"Analysis summary file not found, creating empty file: {ANALYSIS_SUMMARY_PATH}")
            with open(ANALYSIS_SUMMARY_PATH, 'w') as f:
                json.dump({}, f)
                
        with open(ANALYSIS_SUMMARY_PATH, 'r') as f:
            data = json.load(f)
        if query and query != "all" and query in data:
            return json.dumps(data[query], indent=2)
        return json.dumps(data, indent=2)
    except Exception as e:
        logger.error(f"Error accessing analysis summary: {e}")
        return f"Error accessing analysis summary: {str(e)}"

def get_mechanic_performance(mechanic_name: str) -> str:
    """Get performance metrics for a specific mechanic."""
    logger.info(f"get_mechanic_performance called for mechanic: {mechanic_name}")
    try:
        with open(ANALYSIS_SUMMARY_PATH, 'r') as f:
            data = json.load(f)
        stats = data.get('overall_response', {}).get('mechanic_stats', [])
        for stat in stats:
            if stat.get('mechanicName') == mechanic_name:
                return json.dumps(stat, indent=2)
        return f"No performance data found for mechanic: {mechanic_name}"
    except Exception as e:
        logger.error(f"Error accessing mechanic performance: {e}")
        return f"Error accessing mechanic performance: {str(e)}"

def compare_mechanics(metric: str = "repair_time") -> str:
    """Compare mechanics by a given metric."""
    logger.info(f"compare_mechanics called with metric: {metric}")
    try:
        with open(ANALYSIS_SUMMARY_PATH, 'r') as f:
            data = json.load(f)
        stats = data.get('overall_response', {}).get('mechanic_stats', [])
        metric_map = {
            "repair_time": "avgRepairTime_min",
            "response_time": "avgResponseTime_min",
            "repair_z_score": "repair_z_score",
            "response_z_score": "response_z_score"
        }
        field = metric_map.get(metric, "avgRepairTime_min")
        sorted_list = sorted(stats, key=lambda x: x.get(field, 0))
        result = {"metric": metric, "rankings": []}
        for idx, mech in enumerate(sorted_list, start=1):
            result["rankings"].append({
                "rank": idx,
                "mechanicName": mech.get('mechanicName'),
                "value": mech.get(field)
            })
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error comparing mechanics: {e}")
        return f"Error comparing mechanics: {str(e)}"

def get_machine_performance(machine_type: str) -> str:
    """Get performance data for a specific machine type."""
    logger.info(f"get_machine_performance called for type: {machine_type}")
    try:
        with open(ANALYSIS_SUMMARY_PATH, 'r') as f:
            data = json.load(f)
        machine_data = data.get('machine_repair', {})
        return json.dumps(machine_data.get(machine_type, {}), indent=2)
    except Exception as e:
        logger.error(f"Error getting machine performance: {e}")
        return f"Error getting machine performance: {str(e)}"

def get_machine_reason_data(combo: str) -> str:
    """Get data for machine & reason combination."""
    logger.info(f"get_machine_reason_data called for combo: {combo}")
    try:
        with open(ANALYSIS_SUMMARY_PATH, 'r') as f:
            data = json.load(f)
        mr = data.get('machine_reason_repair', {})
        return json.dumps(mr.get(combo, {}), indent=2)
    except Exception as e:
        logger.error(f"Error getting machine-reason data: {e}")
        return f"Error getting machine-reason data: {str(e)}"

def run_scheduled_maintenance(action: str = "run") -> str:
    """Run scheduled maintenance workflow."""
    logger.info(f"run_scheduled_maintenance called with action: {action}")
    try:
        # Pass the RAW_DATA_PATH explicitly to the scheduled_maintenance_tool
        result = scheduled_maintenance_tool(action=action, records_path=RAW_DATA_PATH)
        logger.info("Scheduled maintenance tool executed successfully")
        return result
    except Exception as e:
        logger.error(f"Error running scheduled maintenance: {e}")
        return f"Error running scheduled maintenance: {str(e)}"

# Interactive agent
def run_interactive_agent():
    api_key = os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        raise ValueError("DEEPSEEK_API_KEY not found in environment variables")

    llm = ChatOpenAI(
        model="deepseek-chat",
        temperature=0.7,
        api_key=api_key,
        base_url="https://api.deepseek.com/v1"
    )

    tools = [
        # Existing tools
        Tool(
            name="RawMaintenanceData",
            func=get_raw_maintenance_data,
            description="Get raw maintenance data; use 'sample' for first 5 records."
        ),
        Tool(
            name="AnalysisSummary",
            func=get_analysis_summary,
            description="Get analysis summary data; pass 'all' or a specific key."
        ),
        Tool(
            name="MechanicPerformance",
            func=get_mechanic_performance,
            description="Get performance for a mechanic by name."
        ),
        Tool(
            name="CompareMechanics",
            func=compare_mechanics,
            description="Compare mechanics on metrics like 'repair_time'."
        ),
        Tool(
            name="MachinePerformance",
            func=get_machine_performance,
            description="Get performance for a machine type."
        ),
        Tool(
            name="MachineReasonData",
            func=get_machine_reason_data,
            description="Get data for machine_reason combos 'Type_Reason'."
        ),
        Tool(
            name="RunScheduledMaintenance",
            func=run_scheduled_maintenance,
            description="Run the factory's scheduled maintenance workflow. Use this tool when asked to run scheduled maintenance."
        ),
        
        # New Supabase tools
        Tool(
            name="QueryDatabase",
            func=query_database,
            description="Query database tables. Format: 'table_name:column1,column2;filter1=value1,filter2=value2;limit=100'"
        ),
        Tool(
            name="GetSchemaInfo",
            func=get_schema_info,
            description="Get database schema information. Input a table name or search query to find relevant tables."
        ),
        Tool(
            name="InsertOrUpdateData",
            func=insert_or_update_data,
            description="Insert or update data. Format: 'operation|table_name|json_data|match_column'"
        )
    ]

    # Use the approach from the working code
    agent = initialize_agent(
        tools,
        llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,  # Use ZERO_SHOT_REACT_DESCRIPTION as in the working code
        verbose=True,
        handle_parsing_errors=True,
        agent_kwargs={"prefix": SYSTEM_PROMPT}  # Pass as prefix instead of system_message
    )

    print("\n=== Maintenance Performance Analyst Chat ===")
    print("Type queries or 'exit' to quit.\n")

    while True:
        query = input("You: ")
        if query.strip().lower() in ["exit", "quit", "bye"]:
            print("Goodbye!")
            break
        try:
            result = agent.run(query)
            print("\nAgent:", result)
        except Exception as e:
            print(f"Error: {e}")

def create_agent():
    """Create and return the agent for use in the API."""
    logger.info("Creating agent for API use")
    
    api_key = os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        raise ValueError("DEEPSEEK_API_KEY not found in environment variables")

    llm = ChatOpenAI(
        model="deepseek-chat",
        temperature=0.7,
        api_key=api_key,
        base_url="https://api.deepseek.com/v1"
    )

    tools_list = [
        # Existing tools
        Tool(
            name="RawMaintenanceData",
            func=get_raw_maintenance_data,
            description="Get raw maintenance data; use 'sample' for first 5 records."
        ),
        Tool(
            name="AnalysisSummary",
            func=get_analysis_summary,
            description="Get analysis summary data; pass 'all' or a specific key."
        ),
        Tool(
            name="MechanicPerformance",
            func=get_mechanic_performance,
            description="Get performance for a mechanic by name."
        ),
        Tool(
            name="CompareMechanics",
            func=compare_mechanics,
            description="Compare mechanics on metrics like 'repair_time'."
        ),
        Tool(
            name="MachinePerformance",
            func=get_machine_performance,
            description="Get performance for a machine type."
        ),
        Tool(
            name="MachineReasonData",
            func=get_machine_reason_data,
            description="Get data for machine_reason combos 'Type_Reason'."
        ),
        Tool(
            name="RunScheduledMaintenance",
            func=run_scheduled_maintenance,
            description="Run the factory's scheduled maintenance workflow. Use this tool when asked to run scheduled maintenance."
        ),
        
        # New Supabase tools
        Tool(
            name="QueryDatabase",
            func=query_database,
            description="Query database tables. Format: 'table_name:column1,column2;filter1=value1,filter2=value2;limit=100'"
        ),
        Tool(
            name="GetSchemaInfo",
            func=get_schema_info,
            description="Get database schema information. Input a table name or search query to find relevant tables."
        ),
        Tool(
            name="InsertOrUpdateData",
            func=insert_or_update_data,
            description="Insert or update data. Format: 'operation|table_name|json_data|match_column'"
        )
    ]

    # Use the approach from the working code
    agent = initialize_agent(
        tools_list,
        llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,  # Use ZERO_SHOT_REACT_DESCRIPTION as in the working code
        verbose=True,
        handle_parsing_errors=True,
        agent_kwargs={"prefix": SYSTEM_PROMPT}  # Pass as prefix instead of system_message
    )
    
    logger.info("Agent created successfully")
    return agent

if __name__ == '__main__':
    run_interactive_agent()