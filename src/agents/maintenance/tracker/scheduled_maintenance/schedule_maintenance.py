# src/agents/maintenance/tracker/schedule_maintenance.py
import uuid
from datetime import datetime, timedelta
import os
import sys
import json
import logging
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
from supabase.client import create_client, Client
from supabase.__version__ import __version__ as supabase_version

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("maintenance_scheduler")

# Add the src directory to Python's path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, '../../../'))
project_root = os.path.abspath(os.path.join(src_dir, '../'))
sys.path.insert(0, src_dir)

# Load environment variables from project root
env_path = os.path.join(project_root, '.env.local')
logger.info(f"Loading environment from: {env_path}")
logger.info(f"File exists: {os.path.exists(env_path)}")
load_dotenv(dotenv_path=env_path)

# Get RAW_DATA_PATH from environment
raw_data_path = os.getenv('RAW_DATA_PATH')
logger.info(f"RAW_DATA_PATH from environment: {raw_data_path}")
if raw_data_path:
    logger.info(f"File exists: {os.path.exists(raw_data_path)}")
else:
    logger.warning("RAW_DATA_PATH not set in environment variables")

# Initialize Supabase client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")

if not supabase_url or not supabase_key:
    logger.error("Supabase URL and key must be set in .env.local file")
    raise ValueError("Supabase URL and key must be set in .env.local file")

supabase: Client = create_client(supabase_url, supabase_key)

class MaintenanceScheduler:
    def __init__(self):
        logger.info("Initializing MaintenanceScheduler")
        logger.info(f"SUPABASE_URL: {'Found' if supabase_url else 'Not found'}")
        logger.info(f"SUPABASE_KEY: {'Found' if supabase_key else 'Not found'}")
        logger.info(f"Supabase client version: {supabase_version}")
    
    def ensure_tables_exist(self) -> bool:
        """Check if the required tables exist."""
        try:
            result1 = supabase.table('scheduled_maintenance').select('count').limit(1).execute()
            logger.info(f"scheduled_maintenance table exists, got result: {result1}")
            result2 = supabase.table('mechanics').select('count').limit(1).execute()
            logger.info(f"mechanics table exists, got result: {result2}")
            return True
        except Exception as e:
            logger.error(f"Error checking tables: {e}")
            logger.error("Tables may not exist or there's an issue with permissions.")
            return False
    
    def get_mechanics(self) -> List[Dict[str, Any]]:
        """Get all mechanics."""
        try:
            logger.info("Attempting to fetch mechanics from database...")
            result = supabase.table('mechanics').select('*').execute()
            mechanics = result.data if result and hasattr(result, 'data') else []
            logger.info(f"Retrieved {len(mechanics)} mechanics")
            if mechanics:
                for mech in mechanics:
                    logger.info(f"Mechanic: {mech.get('name')} {mech.get('surname')} (#{mech.get('employee_number')})")
            else:
                logger.warning("No mechanics found. Please check the mechanics table in Supabase.")
            return mechanics
        except Exception as e:
            logger.error(f"Error fetching mechanics: {e}")
            return []
    
    def assign_mechanic(self) -> Tuple[str, str]:
        """
        Assign a mechanic using simple workload balancing.
        Returns a tuple of (employee_number, full_name).
        """
        try:
            mechanics = self.get_mechanics()
            if not mechanics:
                logger.warning("No mechanics found. Using 'unassigned' as fallback.")
                return ("unassigned", "Unassigned")
                
            # Get current workload for each mechanic
            for mechanic in mechanics:
                try:
                    tasks_result = supabase.table('scheduled_maintenance') \
                        .select('*') \
                        .eq('status', 'open') \
                        .eq('assignee', mechanic.get("employee_number")).execute()
                    open_tasks = tasks_result.data if tasks_result and hasattr(tasks_result, 'data') else []
                except Exception as e:
                    logger.error(f"Error getting tasks for mechanic {mechanic.get('employee_number')}: {e}")
                    open_tasks = []
                mechanic["current_workload"] = len(open_tasks)
                logger.info(f"Mechanic {mechanic.get('name')} {mechanic.get('surname')} has {len(open_tasks)} open tasks")
            
            # Find mechanics with minimal workload
            min_workload = min(mechanics, key=lambda m: m.get("current_workload", 0)).get("current_workload", 0)
            mechanics_with_min_workload = [m for m in mechanics if m.get("current_workload", 0) == min_workload]
            
            # Randomly select from mechanics with minimal workload
            import random
            selected_mechanic = random.choice(mechanics_with_min_workload)
            employee_number = selected_mechanic.get("employee_number", "unassigned")
            name = selected_mechanic.get("name", "")
            surname = selected_mechanic.get("surname", "")
            full_name = f"{name} {surname}".strip()
            logger.info(f"Selected mechanic: {full_name} (#{employee_number})")
            return (employee_number, full_name)
        except Exception as e:
            logger.error(f"Error assigning mechanic: {e}", exc_info=True)
            logger.warning("Using 'unassigned' as fallback.")
            return ("unassigned", "Unassigned")
    
    def create_task(
        self,
        machine_id: str,
        machine_type: str,
        issue_type: str,
        description: str,
        assignee: str,
        assignee_name: str,
        priority: str = "medium",
        due_days: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Create a new maintenance task.
        Extra fields added:
         - machine_type: Type of the machine.
         - mechanic_name: Mechanic's full name.
        """
        # Check if machine already has an open task
        existing_tasks = self.get_tasks(status="open", machine_id=machine_id)
        if existing_tasks:
            logger.info(f"Machine {machine_id} already has an open maintenance task. Skipping.")
            return existing_tasks[0]
        
        # Set due date based on priority
        if due_days is None:
            due_days = 7 if priority == "high" else 14
        
        now = datetime.now()
        due_date = now + timedelta(days=due_days)
        
        # Build task record with extra fields
        task = {
            "machine_id": machine_id,
            "machine_type": machine_type,     # Type of machine
            "issue_type": issue_type,
            "description": description,
            "assignee": assignee,             # Employee ID
            "mechanic_name": assignee_name,   # Mechanic's full name
            "priority": priority,
            "status": "open",
            "due_by": due_date.isoformat(),
            "created_at": now.isoformat(),
        }
        
        logger.info(f"Creating task for machine {machine_id} (type: {machine_type}) assigned to {assignee_name} ({assignee})...")
        try:
            result = supabase.table('scheduled_maintenance').insert(task).execute()
            logger.info("Task inserted successfully")
            return result.data[0] if result and hasattr(result, 'data') and result.data else task
        except Exception as e:
            logger.error(f"Error inserting task: {e}", exc_info=True)
            logger.error(f"Could not insert task for machine {machine_id}")
            return None
    
    def update_task(self, task_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing task."""
        updates["updated_at"] = datetime.now().isoformat()
        if "status" in updates and updates["status"] == "completed":
            updates["completed_at"] = datetime.now().isoformat()
        try:
            result = supabase.table('scheduled_maintenance').update(updates).eq('id', task_id).execute()
            if result and hasattr(result, 'data') and result.data:
                logger.info(f"Task {task_id} updated successfully")
                return result.data[0]
            else:
                logger.warning(f"Task {task_id} update returned no data")
                return None
        except Exception as e:
            logger.error(f"Error updating task {task_id}: {e}", exc_info=True)
            return None
    
    def get_tasks(
        self,
        status: Optional[str] = None,
        assignee: Optional[str] = None,
        machine_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get tasks with optional filtering."""
        try:
            query = supabase.table('scheduled_maintenance').select('*')
            filters = []
            
            if status:
                query = query.eq('status', status)
                filters.append(f"status={status}")
                
            if assignee:
                query = query.eq('assignee', assignee)
                filters.append(f"assignee={assignee}")
                
            if machine_id:
                query = query.eq('machine_id', machine_id)
                filters.append(f"machine_id={machine_id}")
                
            filter_str = " AND ".join(filters) if filters else "no filters"
            logger.info(f"Getting tasks with {filter_str}")
            
            result = query.execute()
            tasks = result.data if result and hasattr(result, 'data') else []
            logger.info(f"Retrieved {len(tasks)} tasks")
            return tasks
        except Exception as e:
            logger.error(f"Error in get_tasks: {e}", exc_info=True)
            return []
    
    def list_all_tasks(self) -> List[Dict[str, Any]]:
        """List all tasks in the database."""
        try:
            result = supabase.table('scheduled_maintenance').select('*').execute()
            tasks = result.data if result and hasattr(result, 'data') else []
            logger.info(f"Found {len(tasks)} tasks in database")
            
            for task in tasks:
                logger.info(f"Task: Machine {task.get('machine_id')} - {task.get('description')} - Assigned to: {task.get('assignee')}")
                
            return tasks
        except Exception as e:
            logger.error(f"Error listing tasks: {e}", exc_info=True)
            return []
    
    def generate_service_schedule_from_cluster(self, cluster_file, max_tasks=None):
        """
        Generate a service schedule based on cluster analysis results.
        Uses the 80/20 rule to determine priority.
        """
        # Validate cluster file
        logger.info(f"Loading cluster file from: {cluster_file}")
        if not os.path.exists(cluster_file):
            logger.error(f"Cluster file not found: {cluster_file}")
            return {"error": f"Cluster file not found: {cluster_file}"}
            
        try:
            with open(cluster_file, 'r') as f:
                cluster_data = json.load(f)
                
            # Validate expected structure
            if "aggregated_data" not in cluster_data:
                logger.error("Invalid cluster data format: 'aggregated_data' key not found")
                return {"error": "Invalid cluster data format"}
                
            # Get machines in bad cluster (cluster 1)
            bad_machines = [m for m in cluster_data["aggregated_data"] if m["cluster"] == 1]
            logger.info(f"Found {len(bad_machines)} machines in the problematic cluster")
            
            if not bad_machines:
                logger.info("No problematic machines found in cluster analysis")
                return {
                    "created": [],
                    "skipped": [],
                    "total_problematic_machines": 0,
                    "high_priority_count": 0,
                    "medium_priority_count": 0,
                    "tasks_created": 0
                }
            
            # Sort by failure count (most failures first)
            bad_machines.sort(key=lambda x: x["failure_count"], reverse=True)
            
            # Calculate priority using 80/20 rule (Pareto principle)
            total_failures = sum(m["failure_count"] for m in bad_machines)
            running_sum = 0
            high_priority_machines = []
            medium_priority_machines = []
            
            for machine in bad_machines:
                running_sum += machine["failure_count"]
                percentage_contribution = running_sum / total_failures
                if percentage_contribution <= 0.8:
                    high_priority_machines.append(machine)
                else:
                    medium_priority_machines.append(machine)
            
            logger.info(f"Identified {len(high_priority_machines)} high priority and {len(medium_priority_machines)} medium priority machines")
            
            # Combine with priority information
            machines_to_service = []
            for machine in high_priority_machines:
                machines_to_service.append({**machine, "priority": "high"})
            for machine in medium_priority_machines:
                machines_to_service.append({**machine, "priority": "medium"})
            
            # Limit by max_tasks if specified
            if max_tasks and max_tasks > 0:
                original_count = len(machines_to_service)
                machines_to_service = machines_to_service[:max_tasks]
                logger.info(f"Limiting to {max_tasks} tasks from {original_count} identified machines")
            
            # Create tasks for each machine
            tasks_created = []
            skipped_machines = []
            
            for machine in machines_to_service:
                machine_id = machine["machineNumber"]
                machine_type = machine.get("machine_type", "Unknown")
                
                # Check if machine already has open tasks
                existing_tasks = self.get_tasks(status="open", machine_id=machine_id)
                if existing_tasks:
                    logger.info(f"Machine {machine_id} already has an open task. Skipping.")
                    skipped_machines.append(machine_id)
                    continue
                
                # Assign mechanic using workload balancing algorithm
                assignee, assignee_name = self.assign_mechanic()
                
                # Create the maintenance task
                task = self.create_task(
                    machine_id=machine_id,
                    machine_type=machine_type,
                    issue_type="preventative_maintenance",
                    description=(
                        f"Schedule maintenance for {machine_type} (#{machine_id}) - "
                        f"Identified in high failure cluster with {machine['failure_count']} failures"
                    ),
                    assignee=assignee,
                    assignee_name=assignee_name,
                    priority=machine["priority"],
                    due_days=7 if machine["priority"] == "high" else 14
                )
                
                if task:
                    tasks_created.append(task)
                    logger.info(f"Created {machine['priority']} priority task for machine {machine_id}")
            
            # Return summary of the scheduling operation
            result = {
                "created": tasks_created,
                "skipped": skipped_machines,
                "total_problematic_machines": len(bad_machines),
                "high_priority_count": len(high_priority_machines),
                "medium_priority_count": len(medium_priority_machines),
                "tasks_created": len(tasks_created)
            }
            
            logger.info(f"Service schedule generation complete. Created {len(tasks_created)} tasks, skipped {len(skipped_machines)} machines.")
            return result
            
        except Exception as e:
            logger.error(f"Error generating service schedule: {e}", exc_info=True)
            return {"error": str(e)}
    
    def get_service_schedule(self, status="open"):
        """Get the current service schedule."""
        try:
            tasks = self.get_tasks(status=status)
            logger.info(f"Retrieved {len(tasks)} {status} tasks")
            
            # Sort by priority and due date
            priority_order = {"high": 0, "medium": 1, "low": 2}
            sorted_tasks = tasks.copy() if tasks else []
            
            if sorted_tasks:
                sorted_tasks.sort(key=lambda t: (
                    priority_order.get(t.get("priority", "medium"), 999),
                    t.get("due_by", "")
                ))
                logger.info("Tasks sorted by priority and due date")
                
            return sorted_tasks
        except Exception as e:
            logger.error(f"Error getting service schedule: {e}", exc_info=True)
            return []


# Example usage if run directly
if __name__ == "__main__":
    try:
        logger.info("Running MaintenanceScheduler directly")
        scheduler = MaintenanceScheduler()
        
        # Check database tables
        tables_exist = scheduler.ensure_tables_exist()
        if not tables_exist:
            logger.error("Required tables don't exist in Supabase")
            sys.exit(1)
        
        # Get mechanics
        mechanics = scheduler.get_mechanics()
        logger.info(f"Found {len(mechanics)} mechanics")
        
        # Get current tasks
        tasks = scheduler.get_tasks()
        logger.info(f"Found {len(tasks)} existing tasks")
        
        # Check for RAW_DATA_PATH
        raw_data_path = os.getenv('RAW_DATA_PATH')
        if not raw_data_path:
            logger.error("RAW_DATA_PATH environment variable not set")
            logger.info("Please set RAW_DATA_PATH in your .env.local file")
            sys.exit(1)
            
        if not os.path.exists(raw_data_path):
            logger.error(f"File not found at RAW_DATA_PATH: {raw_data_path}")
            logger.info("Please ensure the file exists at the specified path")
            sys.exit(1)
        
        logger.info(f"Using maintenance data from: {raw_data_path}")
        
        # Generate or use existing cluster file
        cluster_file = os.path.join(src_dir, "cluster.json")
        
        # If cluster file doesn't exist, create it
        if not os.path.exists(cluster_file):
            logger.info("Cluster file not found, generating from maintenance data...")
            
            try:
                # Import run_analysis
                from agents.maintenance.analytics.MachineCluster import run_analysis
                
                # Load maintenance data
                with open(raw_data_path, 'r') as f:
                    maintenance_records = json.load(f)
                logger.info(f"Loaded {len(maintenance_records)} maintenance records")
                
                # Run analysis
                analysis_results = run_analysis(maintenance_records)
                logger.info("Analysis completed successfully")
                
                # Save results
                with open(cluster_file, 'w') as f:
                    json.dump(analysis_results, f, indent=2)
                logger.info(f"Saved cluster analysis to {cluster_file}")
                
            except Exception as e:
                logger.error(f"Error generating cluster file: {e}", exc_info=True)
                logger.info("Will attempt to use existing cluster file if available")
        else:
            logger.info(f"Using existing cluster file: {cluster_file}")
        
        # Generate service schedule if cluster file exists
        if os.path.exists(cluster_file):
            logger.info(f"Generating service schedule from {cluster_file}")
            result = scheduler.generate_service_schedule_from_cluster(cluster_file=cluster_file, max_tasks=10)
            
            if "error" in result:
                logger.error(f"Error in schedule generation: {result['error']}")
            else:
                logger.info("Service schedule generated successfully")
                logger.info(f"Created {result['tasks_created']} new tasks")
                logger.info(f"Skipped {len(result['skipped'])} machines that already have open tasks")
                logger.info(f"High priority machines: {result['high_priority_count']}")
                logger.info(f"Medium priority machines: {result['medium_priority_count']}")
                
                if result['created']:
                    logger.info("Newly created tasks:")
                    for idx, task in enumerate(result['created'], 1):
                        logger.info(f"{idx}. Machine {task.get('machine_id')} ({task.get('priority', 'medium')} priority)")
                        logger.info(f"   Description: {task.get('description', '')}")
                        logger.info(f"   Assigned to: {task.get('mechanic_name', '')}")
                        logger.info(f"   Due by: {task.get('due_by', '')}")
        else:
            logger.error(f"No cluster file found at {cluster_file}")
            logger.info("Cannot generate maintenance schedule without cluster analysis")
        
        # Verify final task count
        all_tasks = scheduler.list_all_tasks()
        logger.info(f"Total tasks in database: {len(all_tasks)}")
        
    except Exception as e:
        logger.error(f"Unhandled error in MaintenanceScheduler: {e}", exc_info=True)
        import traceback
        traceback.print_exc()