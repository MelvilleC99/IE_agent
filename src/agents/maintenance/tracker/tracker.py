# src/agents/maintenance/tracker/tracker.py
import uuid
from datetime import datetime, timedelta
import os
import sys
import json
from dotenv import load_dotenv

# Add the src directory to Python's path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, '../../../'))
project_root = os.path.abspath(os.path.join(src_dir, '../'))
sys.path.insert(0, src_dir)

# Load environment variables from project root
env_path = os.path.join(project_root, '.env.local')
print(f"Loading environment from: {env_path}")
load_dotenv(env_path)

# Now import from config
from config.settings import SUPABASE_URL, SUPABASE_KEY
from supabase import create_client, Client

class MaintenanceTracker:
    def __init__(self):
        print(f"Checking for Supabase credentials...")
        print(f"SUPABASE_URL: {'Found' if SUPABASE_URL else 'Not found'}")
        print(f"SUPABASE_KEY: {'Found' if SUPABASE_KEY else 'Not found'}")
        
        # Check Supabase version
        import supabase
        print(f"Supabase client version: {supabase.__version__}")
        
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Supabase URL and key must be set in .env.local file")
        
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    def ensure_table_exists(self):
        """Check if the maintenance_tasks table exists"""
        try:
            # Try to query the table
            result = self.supabase.table('maintenance_tasks').select('count').limit(1)
            print(f"Table exists, got result: {result}")
            return True
        except Exception as e:
            print(f"Error checking table: {e}")
            print("Table may not exist or there's an issue with permissions.")
            return False
    
    def create_task(self, machine_id, issue_type, description, assignee, priority="medium", due_days=7):
        """Create a new maintenance task"""
        # First check if there's already an open task for this machine
        existing_tasks = self.get_tasks(status="open", machine_id=machine_id)
        if existing_tasks:
            print(f"Machine {machine_id} already has an open maintenance task. Skipping.")
            return existing_tasks[0]  # Return the existing task
        
        # No existing task, create a new one
        now = datetime.now()
        due_date = now + timedelta(days=due_days)
        
        # Note: We're not including id since the table has DEFAULT uuid_generate_v4()
        task = {
            "machine_id": machine_id,
            "issue_type": issue_type,
            "description": description,
            "assignee": assignee,
            "priority": priority,
            "status": "open",
            "due_by": due_date.isoformat(),
        }
        
        print(f"Attempting to insert task for machine {machine_id}...")
        try:
            # Try with execute()
            result = self.supabase.table('maintenance_tasks').insert(task).execute()
            print(f"Task inserted successfully with .execute()")
            print(f"Result: {result}")
            return result.data[0] if hasattr(result, 'data') and result.data else task
        except Exception as e1:
            print(f"Error with .execute(): {str(e1)}")
            try:
                # Try without execute()
                result = self.supabase.table('maintenance_tasks').insert(task)
                print(f"Task inserted without .execute()")
                print(f"Result: {result}")
                return result.data[0] if hasattr(result, 'data') and result.data else task
            except Exception as e2:
                print(f"Error without .execute(): {str(e2)}")
                print(f"Could not insert task for machine {machine_id}")
                return None
    
    def update_task(self, task_id, updates):
        """Update an existing task"""
        updates["updated_at"] = datetime.now().isoformat()
        
        # If status is being changed to completed, set completed_at
        if "status" in updates and updates["status"] == "completed":
            updates["completed_at"] = datetime.now().isoformat()
        
        try:
            # Try with execute()
            result = self.supabase.table('maintenance_tasks').update(updates).eq('id', task_id).execute()
            return result.data[0] if hasattr(result, 'data') and result.data else None
        except Exception as e1:
            print(f"Error updating task with .execute(): {str(e1)}")
            try:
                # Try without execute()
                result = self.supabase.table('maintenance_tasks').update(updates).eq('id', task_id)
                return result.data[0] if hasattr(result, 'data') and result.data else None
            except Exception as e2:
                print(f"Error updating task without .execute(): {str(e2)}")
                return None
    
    def get_tasks(self, status=None, assignee=None, machine_id=None):
        """Get tasks with optional filtering"""
        try:
            query = self.supabase.table('maintenance_tasks').select('*')
            
            if status:
                query = query.eq('status', status)
            if assignee:
                query = query.eq('assignee', assignee)
            if machine_id:
                query = query.eq('machine_id', machine_id)
                
            # Try with execute()
            try:
                result = query.execute()
            except Exception as e:
                print(f"Error with .execute() in get_tasks: {str(e)}")
                # Try without execute()
                result = query
            
            return result.data if hasattr(result, 'data') else []
        except Exception as e:
            print(f"Error in get_tasks: {str(e)}")
            return []
    
    def generate_service_schedule_from_cluster(self, cluster_file, assignee="maintenance_manager", max_tasks=5):
        """
        Generate a service schedule based on cluster analysis results.
        Checks for existing open tasks before creating new ones.
        """
        # Load cluster data from file
        with open(cluster_file, 'r') as f:
            cluster_data = json.load(f)
        
        # Find machines in the problematic cluster (cluster 1)
        bad_machines = [m for m in cluster_data["aggregated_data"] if m["cluster"] == 1]
        
        # Sort by failure count (highest first)
        bad_machines.sort(key=lambda x: x["failure_count"], reverse=True)
        
        # Limit to max_tasks if needed
        machines_to_service = bad_machines[:max_tasks]
        
        tasks_created = []
        skipped_machines = []
        
        for machine in machines_to_service:
            machine_id = machine["machineNumber"]
            
            # Check if this machine already has an open task
            existing_tasks = self.get_tasks(status="open", machine_id=machine_id)
            
            if existing_tasks:
                # Machine already has an open task, skip
                skipped_machines.append(machine_id)
                continue
            
            # Create a task for the machine
            task = self.create_task(
                machine_id=machine_id,
                issue_type="preventative_maintenance",
                description=f"Schedule maintenance for {machine['machine_type']} (#{machine_id}) - Identified in high failure cluster with {machine['failure_count']} failures",
                assignee=assignee,
                priority="high" if machine["failure_count"] > 7 else "medium",
                due_days=3 if machine["failure_count"] > 7 else 7
            )
            
            if task:
                tasks_created.append(task)
        
        result = {
            "created": tasks_created,
            "skipped": skipped_machines,
            "total_problematic_machines": len(bad_machines),
            "tasks_created": len(tasks_created)
        }
        
        return result
    
    def get_service_schedule(self, status="open"):
        """Get the current service schedule"""
        tasks = self.get_tasks(status=status)
        
        # Sort by priority (high first) and then due date (soonest first)
        priority_order = {"high": 0, "medium": 1, "low": 2}
        
        # Make a copy of tasks to avoid modifying the original
        sorted_tasks = tasks.copy() if tasks else []
        
        # Sort by priority and due date
        if sorted_tasks:
            sorted_tasks.sort(key=lambda t: (
                priority_order.get(t.get("priority", "medium"), 999),
                t.get("due_by", "")
            ))
        
        return sorted_tasks


# Example usage if run directly
if __name__ == "__main__":
    try:
        # Initialize tracker
        tracker = MaintenanceTracker()
        
        # Check if table exists
        table_exists = tracker.ensure_table_exists()
        if not table_exists:
            print("Please make sure the maintenance_tasks table exists in Supabase")
        
        # Test connection by getting tasks
        tasks = tracker.get_tasks()
        print(f"Successfully connected to Supabase. Found {len(tasks)} existing tasks.")
        
        # Optional: Generate service schedule from cluster analysis
        cluster_file = os.path.join(src_dir, "cluster.json")
        if os.path.exists(cluster_file):
            print(f"Generating service schedule from {cluster_file}...")
            result = tracker.generate_service_schedule_from_cluster(
                cluster_file=cluster_file,
                assignee="Patrick",
                max_tasks=5
            )
            print(f"Service schedule generated:")
            print(f"- Created {len(result['created'])} new tasks")
            print(f"- Skipped {len(result['skipped'])} machines that already have open tasks")
            
            # Print created tasks for verification
            if result['created']:
                print("\nCreated tasks:")
                for task in result['created']:
                    print(f"- Machine {task['machine_id']}: {task['description']}")
        else:
            print(f"Cluster file not found at {cluster_file}")
            print(f"Looking for: {cluster_file}")
            print(f"Current directory: {os.getcwd()}")
            print(f"Files in src dir: {os.listdir(src_dir)}")
    
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()