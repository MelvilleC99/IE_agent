# src/workflows/scheduled_maintenance_workflow.py
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("maintenance_workflow.log")
    ]
)
logger = logging.getLogger("maintenance_workflow")

# Load environment variables
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '../../../'))
env_path = os.path.join(project_root, '.env.local')
logger.info(f"Loading environment from: {env_path}")
load_dotenv(dotenv_path=env_path)

# Add the src directory to Python's path
src_dir = os.path.abspath(os.path.join(current_dir, '../'))
sys.path.insert(0, src_dir)

# Import the required modules
from agents.maintenance.analytics.MachineCluster import run_analysis
from agents.maintenance.tracker.scheduled_maintenance.schedule_maintenance import MaintenanceScheduler
from agents.maintenance.tracker.scheduled_maintenance.scheduled_maintenance_notification import send_maintenance_schedule_notification


class ScheduledMaintenanceWorkflow:
    """
    Orchestrates the machine maintenance workflow from data analysis to task scheduling.
    """
    
    def __init__(
        self, 
        cluster_output_path: str = "cluster.json",
        max_tasks: int = 10
    ):
        """
        Initialize the workflow.
        
        Args:
            cluster_output_path: Path to save the cluster analysis results
            max_tasks: Maximum number of maintenance tasks to create
        """
        self.cluster_output_path = os.path.join(src_dir, cluster_output_path)
        self.max_tasks = max_tasks
        self.scheduler = MaintenanceScheduler()
        logger.info("Maintenance workflow initialized")
        
    def run_cluster_analysis(self, maintenance_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Run the machine clustering analysis on maintenance records.
        
        Args:
            maintenance_records: List of maintenance record dictionaries
            
        Returns:
            Dict containing cluster analysis results
        """
        logger.info(f"Starting cluster analysis with {len(maintenance_records)} records")
        
        try:
            # Transform the maintenance records to the format expected by the cluster analysis
            transformed_records = []
            
            for record in maintenance_records:
                # Create a copy of the record to avoid modifying the original
                transformed_record = record.copy()
                
                # Create the machineData field expected by the cluster analysis
                transformed_record['machineData'] = {
                    'type': record.get('machineType', 'Unknown'),
                    'make': record.get('machineMake', 'Unknown'),
                    'model': record.get('machineModel', 'Unknown'),
                    'purchaseDate': record.get('machinePurchaseDate')
                }
                
                # Make sure machineNumber is present
                if 'machineNumber' not in transformed_record and 'machineNumber' in record:
                    transformed_record['machineNumber'] = record['machineNumber']
                
                # Ensure other required fields are present
                if 'machineNumber' in transformed_record and 'machineData' in transformed_record:
                    transformed_records.append(transformed_record)
            
            logger.info(f"Transformed {len(transformed_records)} records for cluster analysis")
            
            if not transformed_records:
                logger.error("No valid records for analysis after transformation")
                return {"error": "No valid records after transformation"}
            
            # Run the cluster analysis with transformed records
            analysis_results = run_analysis(transformed_records)
            
            if "error" in analysis_results:
                logger.error(f"Cluster analysis failed: {analysis_results['error']}")
                return analysis_results
            
            # Save results to file for later use
            with open(self.cluster_output_path, 'w') as f:
                json.dump(analysis_results, f, indent=2)
            
            logger.info(f"Cluster analysis completed and saved to {self.cluster_output_path}")
            return analysis_results
            
        except Exception as e:
            import traceback
            error_traceback = traceback.format_exc()
            logger.exception(f"Error in cluster analysis: {str(e)}\n{error_traceback}")
            return {"error": str(e), "traceback": error_traceback}
    
    def schedule_maintenance_tasks(self, cluster_results: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Create maintenance tasks based on cluster analysis results.
        
        Args:
            cluster_results: Optional cluster analysis results. If None, load from file.
            
        Returns:
            Dict containing scheduling results
        """
        try:
            # If results not provided, load from file
            if cluster_results is None:
                logger.info(f"Loading cluster results from {self.cluster_output_path}")
                if not os.path.exists(self.cluster_output_path):
                    error_msg = f"Cluster results file not found: {self.cluster_output_path}"
                    logger.error(error_msg)
                    return {"error": error_msg}
                
                with open(self.cluster_output_path, 'r') as f:
                    cluster_results = json.load(f)
            
            # Check if cluster tables exist in database
            tables_exist = self.scheduler.ensure_tables_exist()
            if not tables_exist:
                error_msg = "Required tables don't exist in the database"
                logger.error(error_msg)
                return {"error": error_msg}
            
            # Generate maintenance schedule
            logger.info("Generating service schedule from cluster results")
            scheduling_results = self.scheduler.generate_service_schedule_from_cluster(
                cluster_file=self.cluster_output_path,
                max_tasks=self.max_tasks
            )
            
            logger.info(f"Created {scheduling_results['tasks_created']} tasks, " 
                        f"skipped {len(scheduling_results['skipped'])} machines")
            
            # Send notification about scheduled maintenance tasks
            if scheduling_results.get('tasks_created', 0) > 0:
                logger.info("Sending maintenance schedule notification")
                notification_result = send_maintenance_schedule_notification(scheduling_results)
                scheduling_results['notification'] = notification_result
            else:
                logger.info("No tasks created, skipping notification")
                
            return scheduling_results
            
        except Exception as e:
            import traceback
            error_traceback = traceback.format_exc()
            logger.exception(f"Error in scheduling maintenance tasks: {str(e)}\n{error_traceback}")
            return {"error": str(e), "traceback": error_traceback}
    
    def get_current_schedule(self, status: str = "open") -> List[Dict[str, Any]]:
        """
        Get the current maintenance schedule.
        
        Args:
            status: Filter tasks by status (open, completed, etc.)
            
        Returns:
            List of maintenance tasks
        """
        try:
            tasks = self.scheduler.get_service_schedule(status=status)
            logger.info(f"Retrieved {len(tasks)} {status} tasks from schedule")
            return tasks
        except Exception as e:
            logger.exception("Error retrieving maintenance schedule")
            return []
    
    def run_complete_workflow(self, maintenance_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Run the complete workflow from analysis to scheduling.
        
        Args:
            maintenance_records: List of maintenance record dictionaries
            
        Returns:
            Dict containing the workflow results
        """
        logger.info("Starting complete maintenance workflow")
        
        # Step 1: Run cluster analysis
        analysis_results = self.run_cluster_analysis(maintenance_records)
        if "error" in analysis_results:
            return {"status": "failed", "step": "analysis", "error": analysis_results["error"]}
        
        # Step 2: Schedule maintenance tasks
        scheduling_results = self.schedule_maintenance_tasks(analysis_results)
        if "error" in scheduling_results:
            return {"status": "failed", "step": "scheduling", "error": scheduling_results["error"]}
        
        # Step 3: Get current schedule for reporting
        current_schedule = self.get_current_schedule()
        
        # Return complete workflow results
        return {
            "status": "success",
            "analysis_results": analysis_results,
            "scheduling_results": scheduling_results,
            "current_schedule": current_schedule,
            "notification": scheduling_results.get('notification', {})  # Include notification info
        }


# Example usage
if __name__ == "__main__":
    try:
        # Create workflow instance
        workflow = ScheduledMaintenanceWorkflow(
            cluster_output_path="cluster.json", 
            max_tasks=10
        )
        
        # Use RAW_DATA_PATH from environment
        raw_data_path = os.getenv('RAW_DATA_PATH')
        if raw_data_path and os.path.exists(raw_data_path):
            logger.info(f"Loading maintenance records from RAW_DATA_PATH: {raw_data_path}")
            with open(raw_data_path, 'r') as f:
                maintenance_records = json.load(f)
                
            # Run the complete workflow
            results = workflow.run_complete_workflow(maintenance_records)
            
            if results["status"] == "success":
                logger.info("Workflow completed successfully")
                print("\nWorkflow Summary:")
                print(f"- Analyzed {len(maintenance_records)} maintenance records")
                print(f"- Identified {results['scheduling_results']['total_problematic_machines']} problematic machines")
                print(f"- Created {results['scheduling_results']['tasks_created']} maintenance tasks")
                print(f"- Current schedule has {len(results['current_schedule'])} open tasks")
                
                # Print notification status if available
                if 'notification' in results:
                    notification = results['notification']
                    notification_status = notification.get('status', 'unknown')
                    print(f"- Notification status: {notification_status}")
                
                # Print detailed task information
                if results['scheduling_results']['created']:
                    print("\nNew Tasks Created:")
                    for task in results['scheduling_results']['created']:
                        machine_id = task.get('machine_id', '')
                        machine_type = task.get('machine_type', 'Unknown')
                        priority = task.get('priority', 'medium')
                        assignee = task.get('mechanic_name', task.get('assignee', 'Unassigned'))
                        print(f"- {priority.upper()} Priority: {machine_type} (#{machine_id}) assigned to {assignee}")
            else:
                logger.error(f"Workflow failed at {results['step']} step: {results['error']}")
                print(f"\nWorkflow failed: {results['error']}")
        else:
            logger.warning(f"No maintenance records file found at {raw_data_path or 'RAW_DATA_PATH not set'}")
            print(f"Please ensure RAW_DATA_PATH is set correctly in .env.local and the file exists")
            
    except Exception as e:
        import traceback
        error_traceback = traceback.format_exc()
        logger.exception(f"Unhandled error in workflow execution: {str(e)}\n{error_traceback}")
        print(f"Error: {str(e)}")