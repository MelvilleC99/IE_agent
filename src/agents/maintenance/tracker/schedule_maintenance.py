# src/agents/maintenance/tracker/schedule_maintenance.py
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

class MaintenanceScheduler:
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
    
    def ensure_tables_exist(self):
        """Check if the required tables exist"""
        try:
            # Check maintenance_tasks table
            result1 = self.supabase.table('maintenance_tasks').select('count').limit(1)
            print(f"maintenance_tasks table exists, got result: {result1}")
            
            # Check mechanics table
            result2 = self.supabase.table('mechanics').select('count').limit(1)
            print(f"mechanics table exists, got result: {result2}")
            
            return True
        except Exception as e:
            print(f"Error checking tables: {e}")
            print("Tables may not exist or there's an issue with permissions.")
            return False
    
    def get_mechanics(self):
        """Get all mechanics"""
        try:
            print("Attempting to fetch mechanics from database...")
            # Try with execute()
            try:
                result = self.supabase.table('mechanics').select('*').execute()
                mechanics = result.data if hasattr(result, 'data') else []
            except Exception as e:
                print(f"Error with .execute() when fetching mechanics: {str(e)}")
                # Try without execute()
                result = self.supabase.table('mechanics').select('*')
                mechanics = result.data if hasattr(result, 'data') else []
            
            print(f"Retrieved {len(mechanics)} mechanics")
            
            # Debug information
            if mechanics:
                for mech in mechanics:
                    print(f"Mechanic: {mech.get('name')} {mech.get('surname')} (#{mech.get('employee_number')})")
            else:
                print("No mechanics found. Please check the mechanics table in Supabase.")
                print("Make sure you've run the correct SQL to create and populate the table.")
            
            return mechanics
        except Exception as e:
            print(f"Error fetching mechanics: {str(e)}")
            return []
    
    def assign_mechanic(self):
        """
        Assign a mechanic using simple workload balancing.
        Returns a tuple of (employee_number, full_name).
        """
        try:
            # Get all mechanics
            mechanics = self.get_mechanics()
            
            if not mechanics:
                print("No mechanics found. Using 'unassigned' as fallback.")
                return ("unassigned", "Unassigned")
            
            # Get current task count for each mechanic
            for mechanic in mechanics:
                try:
                    tasks_result = self.supabase.table('maintenance_tasks').select('*').eq('status', 'open').eq('assignee', mechanic.get("employee_number")).execute()
                    open_tasks = tasks_result.data if hasattr(tasks_result, 'data') else []
                except Exception as e:
                    print(f"Error getting tasks for mechanic {mechanic.get('employee_number')}: {str(e)}")
                    # Try without execute
                    try:
                        tasks_result = self.supabase.table('maintenance_tasks').select('*').eq('status', 'open').eq('assignee', mechanic.get("employee_number"))
                        open_tasks = tasks_result.data if hasattr(tasks_result, 'data') else []
                    except Exception as e2:
                        print(f"Error without execute(): {str(e2)}")
                        open_tasks = []
                
                mechanic["current_workload"] = len(open_tasks)
                print(f"Mechanic {mechanic.get('name')} {mechanic.get('surname')} has {len(open_tasks)} open tasks")
            
            # Find mechanics with the minimum workload
            min_workload = min(mechanics, key=lambda m: m.get("current_workload", 0)).get("current_workload", 0)
            mechanics_with_min_workload = [m for m in mechanics if m.get("current_workload", 0) == min_workload]
            
            # If multiple mechanics have the same minimum workload, pick one randomly
            import random
            selected_mechanic = random.choice(mechanics_with_min_workload)
            
            # Get employee number and name
            employee_number = selected_mechanic.get("employee_number", "unassigned")
            name = selected_mechanic.get("name", "")
            surname = selected_mechanic.get("surname", "")
            full_name = f"{name} {surname}".strip()
            
            print(f"Selected mechanic: {full_name} (#{employee_number})")
            return (employee_number, full_name)
        
        except Exception as e:
            print(f"Error assigning mechanic: {str(e)}")
            print("Using 'unassigned' as fallback.")
            return ("unassigned", "Unassigned")
    
    def create_task(self, machine_id, issue_type, description, assignee, assignee_name, priority="medium", due_days=None):
        """Create a new maintenance task"""
        # First check if there's already an open task for this machine
        existing_tasks = self.get_tasks(status="open", machine_id=machine_id)
        if existing_tasks:
            print(f"Machine {machine_id} already has an open maintenance task. Skipping.")
            return existing_tasks[0]  # Return the existing task
        
        # Set due date based on priority if not specified
        if due_days is None:
            due_days = 7 if priority == "high" else 14
        
        # No existing task, create a new one
        now = datetime.now()
        due_date = now + timedelta(days=due_days)
        
        # Only include fields that exist in the database
        task = {
            "machine_id": machine_id,
            "issue_type": issue_type,
            "description": description,
            "assignee": assignee,  # Only store employee ID, not the name
            "priority": priority,
            "status": "open",
            "due_by": due_date.isoformat(),
        }
        
        print(f"Attempting to insert task for machine {machine_id} assigned to {assignee_name} ({assignee})...")
        try:
            # Try with execute()
            result = self.supabase.table('maintenance_tasks').insert(task).execute()
            print(f"Task inserted successfully with .execute()")
            return result.data[0] if hasattr(result, 'data') and result.data else task
        except Exception as e1:
            print(f"Error with .execute(): {str(e1)}")
            try:
                # Try without execute()
                result = self.supabase.table('maintenance_tasks').insert(task)
                print(f"Task inserted without .execute()")
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
    
    def list_all_tasks(self):
        """List all tasks in the database"""
        try:
            result = self.supabase.table('maintenance_tasks').select('*')
            tasks = result.data if hasattr(result, 'data') else []
            print(f"\nFound {len(tasks)} tasks in database:")
            for task in tasks:
                print(f"- Machine {task.get('machine_id')}: {task.get('description')} (Assigned to: {task.get('assignee')})")
            return tasks
        except Exception as e:
            print(f"Error listing tasks: {str(e)}")
            return []
    
    def generate_service_schedule_from_cluster(self, cluster_file, max_tasks=None):
        """
        Generate a service schedule based on cluster analysis results.
        Uses 80/20 rule to determine priority.
        """
        # Load cluster data from file
        with open(cluster_file, 'r') as f:
            cluster_data = json.load(f)
        
        # Find machines in the problematic cluster (cluster 1)
        bad_machines = [m for m in cluster_data["aggregated_data"] if m["cluster"] == 1]
        
        # Sort by failure count (highest first)
        bad_machines.sort(key=lambda x: x["failure_count"], reverse=True)
        
        # Apply 80/20 rule to determine priorities
        total_failures = sum(m["failure_count"] for m in bad_machines)
        running_sum = 0
        high_priority_machines = []
        medium_priority_machines = []
        
        # Classify machines based on cumulative contribution to failures
        for machine in bad_machines:
            running_sum += machine["failure_count"]
            percentage_contribution = running_sum / total_failures
            
            if percentage_contribution <= 0.8:
                # Machines contributing to first 80% of failures
                high_priority_machines.append(machine)
            else:
                # Remaining machines (the 20%)
                medium_priority_machines.append(machine)
        
        # Combine both lists, preserving the priority classification
        machines_to_service = []
        for machine in high_priority_machines:
            machines_to_service.append({**machine, "priority": "high"})
        for machine in medium_priority_machines:
            machines_to_service.append({**machine, "priority": "medium"})
        
        # Limit to max_tasks if specified
        if max_tasks:
            machines_to_service = machines_to_service[:max_tasks]
        
        tasks_created = []
        skipped_machines = []
        
        for machine in machines_to_service:
            machine_id = machine["machineNumber"]
            machine_type = machine.get("machine_type", "Unknown")
            
            # Check if this machine already has an open task
            existing_tasks = self.get_tasks(status="open", machine_id=machine_id)
            
            if existing_tasks:
                # Machine already has an open task, skip
                skipped_machines.append(machine_id)
                continue
            
            # Assign a mechanic
            assignee, assignee_name = self.assign_mechanic()
            
            # Create a task for the machine
            task = self.create_task(
                machine_id=machine_id,
                issue_type="preventative_maintenance",
                description=f"Schedule maintenance for {machine_type} (#{machine_id}) - Identified in high failure cluster with {machine['failure_count']} failures",
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
        # Initialize scheduler
        scheduler = MaintenanceScheduler()
        
        # Check if tables exist
        tables_exist = scheduler.ensure_tables_exist()
        if not tables_exist:
            print("Please make sure the necessary tables exist in Supabase")
        
        # Get mechanics to verify
        mechanics = scheduler.get_mechanics()
        print(f"Found {len(mechanics)} mechanics:")
        for mechanic in mechanics:
            print(f"- {mechanic.get('name', '')} {mechanic.get('surname', '')} (Employee #: {mechanic.get('employee_number', '')})")
        
        # Test connection by getting tasks
        tasks = scheduler.get_tasks()
        print(f"Successfully connected to Supabase. Found {len(tasks)} existing tasks.")
        
        # Optional: Generate service schedule from cluster analysis
        cluster_file = os.path.join(src_dir, "cluster.json")
        if os.path.exists(cluster_file):
            print(f"Generating service schedule from {cluster_file}...")
            result = scheduler.generate_service_schedule_from_cluster(
                cluster_file=cluster_file,
                max_tasks=10  # Adjust as needed
            )
            print(f"Service schedule generated:")
            print(f"- Created {len(result['created'])} new tasks")
            print(f"- Skipped {len(result['skipped'])} machines that already have open tasks")
            print(f"- High priority machines: {result['high_priority_count']}")
            print(f"- Medium priority machines: {result['medium_priority_count']}")
            
            # Print created tasks for verification
            if result['created']:
                print("\nCreated tasks:")
                for task in result['created']:
                    print(f"- Machine {task.get('machine_id', '')} ({task.get('priority', 'medium')} priority): {task.get('description', '')}")
                    print(f"  Assigned to: {task.get('assignee')} (employee number)")
                    print(f"  Due by: {task.get('due_by', '')}")
                    print()
        else:
            print(f"Cluster file not found at {cluster_file}")
            print(f"Looking for: {cluster_file}")
            print(f"Current directory: {os.getcwd()}")
            print(f"Files in src dir: {os.listdir(src_dir)}")
        
        # List all tasks in the database to verify they were created
        print("\nVerifying tasks in database...")
        scheduler.list_all_tasks()
    
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()