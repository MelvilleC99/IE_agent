import sys
import os

# Add project root to path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import token tracker from MCP
from src.MCP.token_tracker import token_tracker

# Import analysis workflows
from src.agents.maintenance.tools.analysis_workflows import (
    run_daily_analysis,
    run_hourly_analysis,
    run_mechanic_performance,
    run_pareto_analysis,
    run_repeat_failure_analysis
)

import os
import json
import sys
import logging
import time
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from langchain_community.callbacks.manager import get_openai_callback

# Dynamically add the project root to the sys path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("deepseek_agent")

# Load environment variables
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
env_path = os.path.join(project_root, '.env.local')
logger.info(f"Loading .env.local from: {env_path}")
logger.info(f"File exists: {os.path.exists(env_path)}")
load_dotenv(dotenv_path=env_path)

class DeepSeekAgent:
    """
    Agent for handling complex analysis tasks using DeepSeek.
    """
    
    def __init__(self):
        """Initialize the DeepSeek agent."""
        self.analysis_workflows = {
            'daily': run_daily_analysis,
            'hourly': run_hourly_analysis,
            'mechanic': run_mechanic_performance,
            'pareto': run_pareto_analysis,
            'repeat': run_repeat_failure_analysis
        }
        logger.info("DeepSeek agent initialized")
    
    def run(self, query: str) -> str:
        """
        Run the agent with a query.
        
        Args:
            query: The query to process
            
        Returns:
            The agent's response
        """
        try:
            # Track token usage
            with get_openai_callback() as cb:
                result = self._process_query(query)
                token_tracker.track_openai_usage(cb)
            
            return result
        except Exception as e:
            logger.error(f"Error running agent: {e}")
            return f"I apologize, but I encountered an error: {str(e)}"
    
    def _process_query(self, query: str) -> str:
        """
        Process a query using the appropriate analysis workflow.
        
        Args:
            query: The query to process
            
        Returns:
            The analysis results
        """
        # Determine which analysis to run based on the query
        if "daily" in query.lower():
            return self.analysis_workflows['daily']()
        elif "hourly" in query.lower():
            return self.analysis_workflows['hourly']()
        elif "mechanic" in query.lower():
            return self.analysis_workflows['mechanic']()
        elif "pareto" in query.lower():
            return self.analysis_workflows['pareto']()
        elif "repeat" in query.lower():
            return self.analysis_workflows['repeat']()
        else:
            return "I'm not sure which analysis to run. Please specify daily, hourly, mechanic, pareto, or repeat failure analysis."

def _track_token_usage(response: Any) -> None:
    """
    Track token usage for DeepSeek API calls.
    
    Args:
        response: API response object
    """
    try:
        if hasattr(response, 'usage'):
            token_tracker.track_openai_usage(response)
        else:
            # For responses without usage info, estimate tokens
            if isinstance(response, str):
                tokens = len(response) // 4  # Rough estimate
                logger.info(f"Estimated token usage: {tokens}")
    except Exception as e:
        logger.error(f"Error tracking token usage: {e}") 