import os
import json
import sys
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# Load environment variables
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
env_path = os.path.join(project_root, '.env.local')
print(f"Loading .env.local from: {env_path}")
print(f"File exists: {os.path.exists(env_path)}")
load_dotenv(dotenv_path=env_path)

# Print the API key (first few characters) to verify it's loaded
api_key = os.getenv("DEEPSEEK_API_KEY")
if api_key:
    print(f"API key loaded: {api_key[:5]}...")
else:
    print("API key not found in environment variables")

# Import LangChain components
from langchain.agents import initialize_agent, AgentType, Tool
from langchain_community.chat_models.openai import ChatOpenAI

# Data file paths
RAW_DATA_PATH = "/Users/melville/Documents/Industrial_Engineering_Agent/maintenance_data.json"
ANALYSIS_SUMMARY_PATH = "/Users/melville/Documents/Industrial_Engineering_Agent/test_summary_output.json"

def get_raw_maintenance_data(query: Optional[str] = None) -> str:
    """Get raw maintenance data from the JSON file."""
    try:
        with open(RAW_DATA_PATH, 'r') as f:
            data = json.load(f)
        return json.dumps(data[:5] if query == "sample" else data, indent=2)
    except Exception as e:
        return f"Error accessing raw data: {str(e)}"

def get_analysis_summary(query: Optional[str] = None) -> str:
    """Get analysis summary from the JSON file."""
    try:
        with open(ANALYSIS_SUMMARY_PATH, 'r') as f:
            data = json.load(f)
        
        if query and query != "all" and query in data:
            return json.dumps(data[query], indent=2)
        
        return json.dumps(data, indent=2)
    except Exception as e:
        return f"Error accessing analysis summary: {str(e)}"

def get_mechanic_performance(mechanic_name: str) -> str:
    """Get performance metrics for a specific mechanic."""
    try:
        with open(ANALYSIS_SUMMARY_PATH, 'r') as f:
            data = json.load(f)
        
        # Look in overall_response for this mechanic
        if 'overall_response' in data and 'mechanic_stats' in data['overall_response']:
            for stat in data['overall_response']['mechanic_stats']:
                if stat.get('mechanicName') == mechanic_name:
                    return json.dumps(stat, indent=2)
        
        return f"No performance data found for mechanic: {mechanic_name}"
    except Exception as e:
        return f"Error accessing mechanic performance: {str(e)}"

def compare_mechanics(metric: str = "repair_time") -> str:
    """Compare all mechanics based on the specified metric."""
    try:
        with open(ANALYSIS_SUMMARY_PATH, 'r') as f:
            data = json.load(f)
        
        if 'overall_response' not in data or 'mechanic_stats' not in data['overall_response']:
            return "Analysis data not available"
        
        # Map the metric name to the actual field in the data
        metric_map = {
            "repair_time": "avgRepairTime_min",
            "response_time": "avgResponseTime_min",
            "repair_z_score": "repair_z_score",
            "response_z_score": "response_z_score"
        }
        
        field = metric_map.get(metric, "avgRepairTime_min")
        
        # Extract and sort the mechanics by the specified metric
        mechanics = data['overall_response']['mechanic_stats']
        sorted_mechanics = sorted(mechanics, key=lambda x: x.get(field, 0))
        
        # Format the result
        result = {
            "metric": metric,
            "field_name": field,
            "rankings": []
        }
        
        for i, mech in enumerate(sorted_mechanics):
            result["rankings"].append({
                "rank": i + 1,
                "mechanicName": mech.get("mechanicName", "Unknown"),
                "value": mech.get(field, 0)
            })
        
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error comparing mechanics: {str(e)}"

def get_machine_performance(machine_type: str) -> str:
    """Get performance data for a specific machine type."""
    try:
        with open(ANALYSIS_SUMMARY_PATH, 'r') as f:
            data = json.load(f)
        
        if 'machine_repair' not in data or machine_type not in data['machine_repair']:
            return f"No data found for machine type: {machine_type}"
        
        return json.dumps(data['machine_repair'][machine_type], indent=2)
    except Exception as e:
        return f"Error getting machine performance: {str(e)}"

def get_machine_reason_data(combo: str) -> str:
    """Get data for specific machine and reason combinations."""
    try:
        with open(ANALYSIS_SUMMARY_PATH, 'r') as f:
            data = json.load(f)
        
        if 'machine_reason_repair' not in data:
            return "Machine and reason analysis data not available"
        
        # Parse combo - expects format like "Overlocker_Slip Stitch"
        if '_' not in combo:
            return "Query should be in format 'MachineType_ReasonType'"
        
        if combo in data['machine_reason_repair']:
            return json.dumps(data['machine_reason_repair'][combo], indent=2)
        
        return f"No data found for combination: {combo}"
    except Exception as e:
        return f"Error getting machine-reason data: {str(e)}"

# System prompt for the agent
SYSTEM_PROMPT = """You are a Maintenance Performance Analyst AI that helps identify issues with mechanic performance and recommends actions.

Your primary responsibilities:
1. Analyze statistical findings about mechanic performance
2. Identify root causes for performance issues 
3. Recommend appropriate interventions

The data you work with includes:
- Overall response time for mechanics
- Repair time by machine type
- Repair time by machine and reason combinations
- Statistical measures like z-scores and absolute differences

Always base your analysis on the data provided through your tools."""

def run_interactive_agent():
    """Run an interactive session with the agent"""
    # Initialize LLM with DeepSeek API key
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise ValueError("DEEPSEEK_API_KEY not found in environment variables")
    
    llm = ChatOpenAI(
        model="deepseek-chat",
        temperature=0.7,
        api_key=api_key,
        base_url="https://api.deepseek.com/v1"
    )
    
    # Initialize agent with tools
    tools = [
        Tool(
            name="RawMaintenanceData",
            func=get_raw_maintenance_data,
            description="Get raw maintenance data. Use 'sample' for first 5 records."
        ),
        Tool(
            name="AnalysisSummary",
            func=get_analysis_summary,
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
        )
    ]
    
    agent = initialize_agent(
        tools,
        llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True,
        handle_parsing_errors=True,
        agent_kwargs={"prefix": SYSTEM_PROMPT}
    )
    
    print("\n=== Maintenance Performance Analyst Chat ===")
    print("Ask questions about mechanic performance, or type 'exit' to quit")
    
    while True:
        query = input("\nYou: ")
        if query.lower() in ["exit", "quit", "bye"]:
            print("Goodbye!")
            break
        
        try:
            result = agent.run(query)
            print("\nAgent: " + result)
        except Exception as e:
            print(f"\nError: {str(e)}")
            print("Let's try another question.")

if __name__ == "__main__":
    run_interactive_agent()