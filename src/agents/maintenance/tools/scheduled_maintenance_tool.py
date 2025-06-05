# src/agents/maintenance/tools/scheduled_maintenance_tool.py
import json
import os
import re
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging
from dotenv import load_dotenv

# Load environment variables
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
env_path = os.path.join(project_root, '.env.local')
load_dotenv(dotenv_path=env_path)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("maintenance_workflow")

# Import the workflow and date selector
from ..workflows.scheduled_maintenance_workflow import ScheduledMaintenanceWorkflow
from ..tools.date_selector import DateSelector
from shared_services.supabase_client import get_shared_supabase_client

def scheduled_maintenance_tool(
    action: str = "run", 
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    mode: str = "interactive",
    use_database: bool = True,
    force: bool = False
) -> str:
    """
    Tool for an agent to interact with the scheduled maintenance workflow.
    
    Args:
        action: The action to perform ('run' to run the workflow)
        start_date: Optional start date for analysis (YYYY-MM-DD)
        end_date: Optional end date for analysis (YYYY-MM-DD)
        mode: Date selection mode ('interactive' or 'args')
        use_database: Whether to use the database
        force: Override 30-day clustering frequency limit (default: False)
        
    Returns:
        JSON string with the results
    """
    try:
        # Clean the action input to handle various formats from the agent
        logger.info(f"Original action input: {action}")
        
        if isinstance(action, str):
            # Clean up the action string
            action = action.strip('"\'')
            action = re.sub(r'\s*["\']\s*$', '', action)
            
            # Handle action='value' pattern
            if '=' in action:
                match = re.match(r'(?:action|param)=[\'\"]?([a-zA-Z0-9_]+)[\'\"]?', action)
                if match:
                    action = match.group(1)
            
            # Handle JSON-like string
            if action.startswith('{') and action.endswith('}'):
                try:
                    match = re.search(r'[\'"]action[\'"]\s*:\s*[\'"]([a-zA-Z0-9_]+)[\'"]', action)
                    if match:
                        action = match.group(1)
                    else:
                        action_dict = json.loads(action.replace("'", '"'))
                        if 'action' in action_dict:
                            action = action_dict['action']
                except Exception as e:
                    logger.warning(f"Failed to parse action as JSON: {e}")
        
        # Normalize action
        action = action.strip().lower()
        logger.info(f"Processed action: {action}")
        
        # Initialize the workflow
        workflow = ScheduledMaintenanceWorkflow()
        logger.info("Maintenance workflow initialized")
        
        # Handle date selection
        period_start = None
        period_end = None
        
        if start_date and end_date:
            try:
                period_start = datetime.strptime(start_date, "%Y-%m-%d")
                period_end = datetime.strptime(end_date, "%Y-%m-%d")
                logger.info(f"Using specified date range: {period_start.date()} to {period_end.date()}")
            except ValueError:
                logger.warning(f"Invalid date format. Using DateSelector instead.")
                start_date_str, end_date_str = DateSelector.get_date_range(mode=mode)
                period_start = datetime.strptime(start_date_str, "%Y-%m-%d")
                period_end = datetime.strptime(end_date_str, "%Y-%m-%d")
        else:
            # Use interactive date selection
            start_date_str, end_date_str = DateSelector.get_date_range(mode=mode)
            period_start = datetime.strptime(start_date_str, "%Y-%m-%d")
            period_end = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        # Set end date to end of day
        period_end = period_end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # Perform the requested action
        if action in ["run", "execute", "start", "analyze", "schedule"]:
            logger.info("Running maintenance workflow")
            result = workflow.run(
                period_start=period_start,
                period_end=period_end,
                force=force
            )
        else:
            logger.error(f"Unknown action: {action}")
            result = {
                "status": "error",
                "message": f"Unknown action: {action}. Valid actions are: run, execute, start, analyze, schedule."
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        import traceback
        error_traceback = traceback.format_exc()
        logger.error(f"Error in scheduled_maintenance_tool: {str(e)}\n{error_traceback}")
        return json.dumps({
            "status": "error",
            "message": str(e),
            "traceback": error_traceback
        }, indent=2)