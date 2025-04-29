# src/agents/maintenance/tools/scheduled_maintenance_tool.py
import json
import os
import re
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

# Correct import statement
from ..workflows.scheduled_maintenance_workflow import ScheduledMaintenanceWorkflow

def scheduled_maintenance_tool(
    action: str = "run", 
    records_path: Optional[str] = None,
    max_tasks: int = 10,
    cluster_output_path: str = "cluster.json",
    recipient: str = "maintenance_manager",
    notification_type: str = "dashboard"
) -> str:
    """
    Tool for an agent to interact with the scheduled maintenance workflow.
    
    Args:
        action: The action to perform ('run', 'analyze', 'schedule', 'get_schedule')
        records_path: Path to maintenance records JSON file (optional)
        max_tasks: Maximum number of tasks to create
        cluster_output_path: Path to save/load cluster results
        recipient: Who should receive the notification (default: maintenance_manager)
        notification_type: Type of notification (default: dashboard)
        
    Returns:
        JSON string with the results
    """
    try:
        # Clean the action input to handle various formats from the agent
        logger.info(f"Original action input: {action}")
        
        if isinstance(action, str):
            # More aggressive cleaning of the action string
            # 1. Remove any quotes at beginning and end
            action = action.strip('"\'')
            
            # 2. Remove any quotes and whitespace that might be in the middle or end
            action = re.sub(r'\s*"\s*$', '', action)
            action = re.sub(r'\s*\'\s*$', '', action)
            
            # 3. Clean up any action='value' pattern
            if '=' in action:
                try:
                    # Check for patterns like action='run'
                    match = re.match(r'(?:action|param)=[\'\"]?([a-zA-Z0-9_]+)[\'\"]?', action)
                    if match:
                        action = match.group(1)
                except Exception as e:
                    logger.warning(f"Failed to parse action with equals sign: {e}")
            
            # 4. Handle JSON-like string but use regex for more flexibility
            if action.startswith('{') and action.endswith('}'):
                try:
                    # Try to extract the action from a JSON-like string
                    match = re.search(r'[\'"]action[\'"]\s*:\s*[\'"]([a-zA-Z0-9_]+)[\'"]', action)
                    if match:
                        action = match.group(1)
                    else:
                        # Try to parse as proper JSON
                        action_dict = json.loads(action.replace("'", '"'))
                        if 'action' in action_dict:
                            action = action_dict['action']
                except Exception as e:
                    logger.warning(f"Failed to parse action as JSON: {e}")
        
        # Final normalization: trim whitespace and force lowercase for consistency
        action = action.strip().lower()
        
        logger.info(f"Processed action: {action}")
        
        # Initialize the workflow
        workflow = ScheduledMaintenanceWorkflow(
            cluster_output_path=cluster_output_path,
            max_tasks=max_tasks
        )
        
        logger.info("Maintenance workflow initialized")
        
        # Load maintenance records if path provided, otherwise use environment variable
        maintenance_records: List[Dict[str, Any]] = []  # Initialize with empty list
        if records_path and os.path.exists(records_path):
            with open(records_path, 'r') as f:
                loaded_records = json.load(f)
                if isinstance(loaded_records, list):
                    maintenance_records = loaded_records
                    logger.info(f"Loaded {len(maintenance_records)} records from provided path: {records_path}")
        else:
            # Use environment variable or default path
            raw_data_path = os.getenv('RAW_DATA_PATH')
            
            if raw_data_path:
                logger.info(f"Looking for data file from RAW_DATA_PATH: {raw_data_path}")
                
                if os.path.exists(raw_data_path):
                    with open(raw_data_path, 'r') as f:
                        loaded_records = json.load(f)
                        if isinstance(loaded_records, list):
                            maintenance_records = loaded_records
                            logger.info(f"Loaded {len(maintenance_records)} records from RAW_DATA_PATH")
                        else:
                            logger.warning(f"File at {raw_data_path} is not a valid list of records")
                else:
                    logger.warning(f"No data file found at RAW_DATA_PATH: {raw_data_path}")
            else:
                # Fallback to default path in project root directory
                project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
                default_path = os.path.join(project_root, "maintenance_data.json")
                
                logger.info(f"Looking for data file at default path: {default_path}")
                
                if os.path.exists(default_path):
                    with open(default_path, 'r') as f:
                        loaded_records = json.load(f)
                        if isinstance(loaded_records, list):
                            maintenance_records = loaded_records
                            logger.info(f"Loaded {len(maintenance_records)} records from default path")
                        else:
                            logger.warning(f"File at {default_path} is not a valid list of records")
                else:
                    logger.warning(f"No data file found at default path: {default_path}")
            
        # If no records and trying to run analysis, return error
        if not maintenance_records and action in ["run", "analyze"]:
            logger.warning("No maintenance records found for analysis")
            return json.dumps({
                "status": "error",
                "message": "No maintenance records found for analysis"
            }, indent=2)
        
        # Perform the requested action - normalize to exactly match expected actions
        if action in ["run", "execute", "start"]:
            logger.info("Running complete workflow")
            result = workflow.run_complete_workflow(maintenance_records)
        elif action in ["analyze", "analysis"]:
            logger.info("Running cluster analysis")
            result = workflow.run_cluster_analysis(maintenance_records)
        elif action in ["schedule", "create"]:
            logger.info("Scheduling maintenance tasks")
            # No records needed for scheduling if we're using an existing cluster file
            result = workflow.schedule_maintenance_tasks()
            
            # If tasks were created and there's no notification in the result, explicitly send one
            if (result.get('tasks_created', 0) > 0 and 
                'notification' not in result and 
                'error' not in result):
                logger.info("Explicitly sending notification from tool")
                from agents.maintenance.tracker.scheduled_maintenance.scheduled_maintenance_notification import send_maintenance_schedule_notification
                notification_result = send_maintenance_schedule_notification(
                    result,
                    recipient=recipient,
                    notification_type=notification_type
                )
                result['notification'] = notification_result
                
        elif action in ["get_schedule", "getschedule", "view", "list"]:
            logger.info("Getting current schedule")
            result = {"current_schedule": workflow.get_current_schedule()}
        else:
            logger.error(f"Unknown action: {action}")
            result = {
                "status": "error",
                "message": f"Unknown action: {action}\n"
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