# src/agents/maintenance/tracker/schedule_maintenance.py
import uuid
from datetime import datetime, timedelta
import os
import sys
import json
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
from supabase.client import create_client, Client
from supabase.__version__ import __version__ as supabase_version

# Add the src directory to Python's path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, '../../../'))
project_root = os.path.abspath(os.path.join(src_dir, '../'))
sys.path.insert(0, src_dir)

# Load environment variables from project root
env_path = os.path.join(project_root, '.env.local')
print(f"Loading environment from: {env_path}")
print(f"File exists: {os.path.exists(env_path)}")
load_dotenv(dotenv_path=env_path)

# Initialize Supabase client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")

if not supabase_url or not supabase_key:
    raise ValueError("Supabase URL and key must be set in .env.local file")

supabase: Client = create_client(supabase_url, supabase_key)

class MaintenanceScheduler:
    def __init__(self):
        print("Checking for Supabase credentials...")
        print(f"SUPABASE_URL: {'Found' if supabase_url else 'Not found'}")
        print(f"SUPABASE_KEY: {'Found' if supabase_key else 'Not found'}")
        print(f"Supabase client version: {supabase_version}")
    
    def ensure_tables_exist(self) -> bool:
        """Check if the required tables exist."""
        try:
            result1 = supabase.table('scheduled_maintenance').select('count').limit(1).execute()
            print(f"scheduled_maintenance table exists, got result: {result1}")
            result2 = supabase.table('mechanics').select('count').limit(1).execute()
            print(f"mechanics table exists, got result: {result2}")
            return True
        except Exception as e:
            print(f"Error checking tables: {e}")
            print("Tables may not exist or there's an issue with permissions.")
            return False
    
    def get_mechanics(self) -> List[Dict[str, Any]]:
        """Get all mechanics."""
        try:
            print("Attempting to fetch mechanics from database...")
            result = supabase.table('mechanics').select('*').execute()
            mechanics = result.data if result and hasattr(result, 'data') else []
            print(f"Retrieved {len(mechanics)} mechanics")
            if mechanics:
                for mech in mechanics:
                    print(f"Mechanic: {mech.get('name')} {mech.get('surname')} (#{mech.get('employee_number')})")
            else:
                print("No mechanics found. Please check the mechanics table in Supabase.")
            return mechanics
        except Exception as e:
            print(f"Error fetching mechanics: {e}")
            return []
    
    def assign_mechanic(self) -> Tuple[str, str]:
        """
        Assign a mechanic using simple workload balancing.
        Returns a tuple of (employee_number, full_name).
        """
        try:
            mechanics = self.get_mechanics()
            if not mechanics:
                print("No mechanics found. Using 'unassigned' as fallback.")
                return ("unassigned", "Unassigned")
            for mechanic in mechanics:
                try:
                    tasks_result = supabase.table('scheduled_maintenance') \
                        .select('*') \
                        .eq('status', 'open') \
                        .eq('assignee', mechanic.get("employee_number")).execute()
                    open_tasks = tasks_result.data if tasks_result and hasattr(tasks_result, 'data') else []
                except Exception as e:
                    print(f"Error getting tasks for mechanic {mechanic.get('employee_number')}: {e}")
                    open_tasks = []
                mechanic["current_workload"] = len(open_tasks)
                print(f"Mechanic {mechanic.get('name')} {mechanic.get('surname')} has {len(open_tasks)} open tasks")
            min_workload = min(mechanics, key=lambda m: m.get("current_workload", 0)).get("current_workload", 0)
            mechanics_with_min_workload = [m for m in mechanics if m.get("current_workload", 0) == min_workload]
            import random
            selected_mechanic = random.choice(mechanics_with_min_workload)
            employee_number = selected_mechanic.get("employee_number", "unassigned")
            name = selected_mechanic.get("name", "")
            surname = selected_mechanic.get("surname", "")
            full_name = f"{name} {surname}".strip()
            print(f"Selected mechanic: {full_name} (#{employee_number})")
            return (employee_number, full_name)
        except Exception as e:
            print(f"Error assigning mechanic: {e}")
            print("Using 'unassigned' as fallback.")
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
         - mechanic_name: Mechanic’s full name.
        """
        existing_tasks = self.get_tasks(status="open", machine_id=machine_id)
        if existing_tasks:
            print(f"Machine {machine_id} already has an open maintenance task. Skipping.")
            return existing_tasks[0]
        
        if due_days is None:
            due_days = 7 if priority == "high" else 14
        
        now = datetime.now()
        due_date = now + timedelta(days=due_days)
        
        # Build task record with extra fields.
        task = {
            "machine_id": machine_id,
            "machine_type": machine_type,     # New field: machine type
            "issue_type": issue_type,
            "description": description,
            "assignee": assignee,             # Stores employee ID
            "mechanic_name": assignee_name,   # New field: mechanic's full name (name and surname)
            "priority": priority,
            "status": "open",
            "due_by": due_date.isoformat(),
        }
        
        print(f"Attempting to insert task for machine {machine_id} (type: {machine_type}) assigned to {assignee_name} ({assignee})...")
        try:
            result = supabase.table('scheduled_maintenance').insert(task).execute()
            print("Task inserted successfully")
            return result.data[0] if result and hasattr(result, 'data') and result.data else task
        except Exception as e:
            print(f"Error inserting task: {e}")
            print(f"Could not insert task for machine {machine_id}")
            return None
    
    def update_task(self, task_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing task."""
        updates["updated_at"] = datetime.now().isoformat()
        if "status" in updates and updates["status"] == "completed":
            updates["completed_at"] = datetime.now().isoformat()
        try:
            result = supabase.table('scheduled_maintenance').update(updates).eq('id', task_id).execute()
            return result.data[0] if result and hasattr(result, 'data') and result.data else None
        except Exception as e:
            print(f"Error updating task: {e}")
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
            if status:
                query = query.eq('status', status)
            if assignee:
                query = query.eq('assignee', assignee)
            if machine_id:
                query = query.eq('machine_id', machine_id)
            result = query.execute()
            return result.data if result and hasattr(result, 'data') else []
        except Exception as e:
            print(f"Error in get_tasks: {e}")
            return []
    
    def list_all_tasks(self) -> List[Dict[str, Any]]:
        """List all tasks in the database."""
        try:
            result = supabase.table('scheduled_maintenance').select('*').execute()
            tasks = result.data if result and hasattr(result, 'data') else []
            print(f"\nFound {len(tasks)} tasks in database:")
            for task in tasks:
                print(f"- Machine {task.get('machine_id')}: {task.get('description')} (Assigned to: {task.get('assignee')})")
            return tasks
        except Exception as e:
            print(f"Error listing tasks: {e}")
            return []
    
    def generate_service_schedule_from_cluster(self, cluster_file, max_tasks=None):
        """
        Generate a service schedule based on cluster analysis results.
        Uses the 80/20 rule to determine priority.
        """
        with open(cluster_file, 'r') as f:
            cluster_data = json.load(f)
        
        bad_machines = [m for m in cluster_data["aggregated_data"] if m["cluster"] == 1]
        bad_machines.sort(key=lambda x: x["failure_count"], reverse=True)
        
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
        
        machines_to_service = []
        for machine in high_priority_machines:
            machines_to_service.append({**machine, "priority": "high"})
        for machine in medium_priority_machines:
            machines_to_service.append({**machine, "priority": "medium"})
        
        if max_tasks:
            machines_to_service = machines_to_service[:max_tasks]
        
        tasks_created = []
        skipped_machines = []
        
        for machine in machines_to_service:
            machine_id = machine["machineNumber"]
            machine_type = machine.get("machine_type", "Unknown")
            existing_tasks = self.get_tasks(status="open", machine_id=machine_id)
            if existing_tasks:
                skipped_machines.append(machine_id)
                continue
            
            assignee, assignee_name = self.assign_mechanic()
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
        
        result = {
            "created": tasks_created,
            "skipped": skipped_machines,
            "total_problematic_machines": len(bad_machines),
            "high_priority_count": len(high_priority_machines),
            "medium_priority_count": len(medium_priority_machines),
            "tasks_created": len(tasks_created)
        }
        return result
    
    def get_service_schedule(self, status="open"):
        """Get the current service schedule."""
        tasks = self.get_tasks(status=status)
        priority_order = {"high": 0, "medium": 1, "low": 2}
        sorted_tasks = tasks.copy() if tasks else []
        if sorted_tasks:
            sorted_tasks.sort(key=lambda t: (
                priority_order.get(t.get("priority", "medium"), 999),
                t.get("due_by", "")
            ))
        return sorted_tasks


# Example usage if run directly
if __name__ == "__main__":
    try:
        scheduler = MaintenanceScheduler()
        tables_exist = scheduler.ensure_tables_exist()
        if not tables_exist:
            print("Please make sure the necessary tables exist in Supabase")
        
        mechanics = scheduler.get_mechanics()
        print(f"Found {len(mechanics)} mechanics:")
        for mechanic in mechanics:
            print(f"- {mechanic.get('name', '')} {mechanic.get('surname', '')} (Employee #: {mechanic.get('employee_number', '')})")
        
        tasks = scheduler.get_tasks()
        print(f"Successfully connected to Supabase. Found {len(tasks)} existing tasks.")
        
        cluster_file = os.path.join(src_dir, "cluster.json")
        if os.path.exists(cluster_file):
            print(f"Generating service schedule from {cluster_file}...")
            result = scheduler.generate_service_schedule_from_cluster(cluster_file=cluster_file, max_tasks=10)
            print("Service schedule generated:")
            print(f"- Created {len(result['created'])} new tasks")
            print(f"- Skipped {len(result['skipped'])} machines that already have open tasks")
            print(f"- High priority machines: {result['high_priority_count']}")
            print(f"- Medium priority machines: {result['medium_priority_count']}")
            if result['created']:
                print("\nCreated tasks:")
                for task in result['created']:
                    print(f"- Machine {task.get('machine_id', '')} ({task.get('priority', 'medium')} priority): {task.get('description', '')}")
                    print(f"  Assigned to: {task.get('assignee')} (Mechanic: {task.get('mechanic_name', '')})")
                    print(f"  Due by: {task.get('due_by', '')}")
                    print()
        else:
            print(f"Cluster file not found at {cluster_file}")
            print(f"Looking for: {cluster_file}")
            print(f"Current directory: {os.getcwd()}")
            print(f"Files in src dir: {os.listdir(src_dir)}")
        
        print("\nVerifying tasks in database...")
        scheduler.list_all_tasks()
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
