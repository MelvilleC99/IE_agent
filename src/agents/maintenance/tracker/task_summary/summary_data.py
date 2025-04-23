#!/usr/bin/env python3
import sys
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env.local")

from shared_services.db_client import get_connection

class SummaryDataCollector:
    """
    Collects measurement data and task details from the database
    for performance summary analysis.
    """
    def __init__(self):
        self.today = datetime.now().date()
        print(f"DATA: Initializing data collector for {self.today}")
        
        try:
            # Connect to the database
            self.supabase = get_connection()
            print("DATA: Connected to Supabase")
        except Exception as e:
            print(f"DATA: Error connecting to database: {e}")
            self.supabase = None
    
    def get_all_measurements(self, task_id):
        """
        Get all measurements for a task in chronological order
        
        Args:
            task_id: ID of the task to get measurements for
            
        Returns:
            list: List of measurement records or empty list if none found
        """
        if not self.supabase:
            print("DATA: No database connection available")
            return []
            
        try:
            result = (self.supabase.table('measurements')
                       .select('*')
                       .eq('task_id', task_id)
                       .order('measurement_date')
                       .execute())
            
            if result.data:
                print(f"DATA: Retrieved {len(result.data)} measurements for task {task_id}")
                return result.data
            
            print(f"DATA: No measurements found for task {task_id}")
            return []
        except Exception as e:
            print(f"DATA: Error retrieving measurements: {e}")
            return []
    
    def get_task_details(self, task_id):
        """
        Get detailed task information
        
        Args:
            task_id: ID of the task to get details for
            
        Returns:
            dict: Task details or None if not found
        """
        if not self.supabase:
            print("DATA: No database connection available")
            return None
            
        try:
            result = (self.supabase.table('tasks')
                       .select('*')
                       .eq('id', task_id)
                       .execute())
            
            if result.data:
                print(f"DATA: Retrieved task details for {task_id}")
                return result.data[0]
                
            print(f"DATA: No task found with ID {task_id}")
            return None
        except Exception as e:
            print(f"DATA: Error retrieving task details: {e}")
            return None
    
    def get_tasks_for_evaluation(self):
        """
        Get all tasks that have reached their end date and need evaluation
        
        Returns:
            list: List of tasks ready for evaluation
        """
        if not self.supabase:
            print("DATA: No database connection available")
            return []
            
        try:
            # Get tasks where today is equal to or past the end date
            # and the status is still 'active'
            result = (self.supabase.table('tasks')
                       .select('*')
                       .eq('monitor_status', 'active')
                       .lte('monitor_end_date', self.today.isoformat())
                       .execute())
            
            if result.data:
                print(f"DATA: Found {len(result.data)} tasks ready for evaluation")
                return result.data
                
            print("DATA: No tasks ready for evaluation")
            return []
        except Exception as e:
            print(f"DATA: Error retrieving tasks for evaluation: {e}")
            return []
    
    def collect_data_for_task(self, task_id):
        """
        Collect all data needed for task evaluation
        
        Args:
            task_id: ID of the task to collect data for
            
        Returns:
            dict: Dictionary with task details and measurements, or None if task not found
        """
        # Get task details
        task = self.get_task_details(task_id)
        if not task:
            print(f"DATA: Could not find task {task_id}")
            return None
        
        # Get measurements
        measurements = self.get_all_measurements(task_id)
        
        # Return combined data
        return {
            'task': task,
            'measurements': measurements,
            'collected_at': datetime.now().isoformat()
        }


# For testing this module directly
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect data for task evaluation')
    parser.add_argument('--task-id', help='ID of task to collect data for')
    parser.add_argument('--find-ready', action='store_true', help='Find tasks ready for evaluation')
    args = parser.parse_args()
    
    collector = SummaryDataCollector()
    
    if args.task_id:
        data = collector.collect_data_for_task(args.task_id)
        if data:
            task = data['task']
            measurements = data['measurements']
            print(f"\nTask: {task['title']} (ID: {task['id']})")
            print(f"Measurements: {len(measurements)}")
            if measurements:
                print("First measurement:", measurements[0])
                print("Last measurement:", measurements[-1])
    
    if args.find_ready:
        tasks = collector.get_tasks_for_evaluation()
        print(f"\nTasks ready for evaluation: {len(tasks)}")
        for i, task in enumerate(tasks):
            print(f"{i+1}. Task ID {task['id']}: {task['title']}")