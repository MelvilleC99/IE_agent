import sys
import os
from datetime import datetime, timedelta
import calendar
import json

# Properly set up the path to find shared_services
current_dir = os.path.dirname(os.path.abspath(__file__))

# Go up to the src directory (assuming we're in src/agents/maintenance/tracker)
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Import the database client
from shared_services.db_client import get_connection

class TaskMonitorChecker:
    """
    Task Monitor Checker identifies which tasks need measurements today
    and which tasks have reached their end date and need evaluation.
    """
    
    def __init__(self):
        """Initialize the task monitor checker"""
        try:
            self.supabase = get_connection()
            if self.supabase:
                print("CHECKER: Connected to database successfully")
            else:
                print("CHECKER: Failed to connect to database")
            
            self.today = datetime.now().date()
            self.day_of_week = calendar.day_name[self.today.weekday()]
            print(f"CHECKER: Running for {self.today} ({self.day_of_week})")
        except Exception as e:
            print(f"CHECKER: Error connecting to database: {e}")
            self.supabase = None
    
    def should_measure_today(self, task):
        """
        Determine if a task should be measured today based on its frequency
        
        Args:
            task: The task record
            
        Returns:
            bool: True if the task should be measured today, False otherwise
        """
        if task['monitor_status'] != 'active':
            return False
        
        # Check if we've passed the end date
        monitor_end_date = datetime.strptime(task['monitor_end_date'], '%Y-%m-%d').date()
        if self.today > monitor_end_date:
            return False
        
        # Check if the task should be measured based on frequency
        frequency = task['monitor_frequency']
        
        if frequency == 'daily':
            return True
        elif frequency == 'weekly':
            # For weekly, measure on Mondays
            return self.day_of_week == 'Monday'
        elif frequency == 'monthly':
            # For monthly, measure on the 1st of each month
            return self.today.day == 1
        
        return False
    
    def should_evaluate_today(self, task):
        """
        Determine if a task has reached its end date and should be evaluated
        
        Args:
            task: The task record
            
        Returns:
            bool: True if the task should be evaluated today, False otherwise
        """
        if task['monitor_status'] != 'active':
            return False
        
        # Check if today is the end date or we've passed it
        monitor_end_date = datetime.strptime(task['monitor_end_date'], '%Y-%m-%d').date()
        return self.today >= monitor_end_date
    
    def check_tasks(self):
        """
        Check all active tasks to determine which ones need measurement or evaluation
        
        Returns:
            dict: Dictionary with lists of tasks for daily, weekly measurements and evaluation
        """
        if not self.supabase:
            print("CHECKER: No database connection, skipping task check")
            return {
                'daily_tasks': [],
                'weekly_tasks': [],
                'evaluation_tasks': []
            }
        
        try:
            # Get all active monitoring tasks
            tasks_result = self.supabase.table('tasks').select('*').eq('monitor_status', 'active').execute()
            
            if not tasks_result.data:
                print("CHECKER: No active monitoring tasks found")
                return {
                    'daily_tasks': [],
                    'weekly_tasks': [],
                    'evaluation_tasks': []
                }
            
            print(f"CHECKER: Found {len(tasks_result.data)} active monitoring tasks")
            
            # Initialize result lists
            daily_tasks = []
            weekly_tasks = []
            evaluation_tasks = []
            
            # Process each task
            for task in tasks_result.data:
                # Check if task needs evaluation (end of monitoring period)
                if self.should_evaluate_today(task):
                    evaluation_tasks.append(task)
                    print(f"CHECKER: Task ID {task['id']} ({task['title']}) needs end-of-period evaluation")
                
                # Check if task needs measurement
                if self.should_measure_today(task):
                    if task['monitor_frequency'] == 'daily':
                        daily_tasks.append(task)
                        print(f"CHECKER: Task ID {task['id']} ({task['title']}) needs daily measurement")
                    elif task['monitor_frequency'] == 'weekly':
                        weekly_tasks.append(task)
                        print(f"CHECKER: Task ID {task['id']} ({task['title']}) needs weekly measurement")
            
            return {
                'daily_tasks': daily_tasks,
                'weekly_tasks': weekly_tasks, 
                'evaluation_tasks': evaluation_tasks
            }
            
        except Exception as e:
            print(f"CHECKER: Error checking tasks: {e}")
            return {
                'daily_tasks': [],
                'weekly_tasks': [],
                'evaluation_tasks': []
            }
    
    def run(self):
        """
        Run the task monitor check and return the results
        
        Returns:
            dict: Dictionary with tasks that need attention
        """
        print("CHECKER: Starting task monitor check...")
        
        # Check which tasks need attention
        tasks = self.check_tasks()
        
        # Print summary
        print("\nCHECKER: Task Monitor Check Summary:")
        print(f"- {len(tasks['daily_tasks'])} tasks need daily measurement")
        print(f"- {len(tasks['weekly_tasks'])} tasks need weekly measurement")
        print(f"- {len(tasks['evaluation_tasks'])} tasks need end-of-period evaluation")
        
        return tasks


# Example usage (for testing this script directly)
if __name__ == '__main__':
    # Print system path for debugging
    print(f"Current directory: {os.getcwd()}")
    print(f"Python path: {sys.path}")
    
    # Create the task monitor checker
    checker = TaskMonitorChecker()
    
    # Run the check
    tasks_to_process = checker.run()
    
    # Output detailed results for verification
    print("\n--- Task Monitor Check Details ---")
    
    # Show daily measurement tasks
    if tasks_to_process['daily_tasks']:
        print("\nDaily Measurement Tasks:")
        for i, task in enumerate(tasks_to_process['daily_tasks']):
            print(f"{i+1}. Task ID {task['id']}: {task['title']}")
            print(f"   Entity: {task['entity_id']}, Issue: {task['issue_type']}")
    else:
        print("\nNo daily measurement tasks for today")
    
    # Show weekly measurement tasks
    if tasks_to_process['weekly_tasks']:
        print("\nWeekly Measurement Tasks:")
        for i, task in enumerate(tasks_to_process['weekly_tasks']):
            print(f"{i+1}. Task ID {task['id']}: {task['title']}")
            print(f"   Entity: {task['entity_id']}, Issue: {task['issue_type']}")
    else:
        print("\nNo weekly measurement tasks for today")
    
    # Show evaluation tasks
    if tasks_to_process['evaluation_tasks']:
        print("\nEnd-of-Period Evaluation Tasks:")
        for i, task in enumerate(tasks_to_process['evaluation_tasks']):
            print(f"{i+1}. Task ID {task['id']}: {task['title']}")
            print(f"   Entity: {task['entity_id']}, Issue: {task['issue_type']}")
            print(f"   Monitoring End Date: {task['monitor_end_date']}")
    else:
        print("\nNo evaluation tasks for today")