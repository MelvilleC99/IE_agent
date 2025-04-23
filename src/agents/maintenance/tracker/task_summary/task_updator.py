#!/usr/bin/env python3
import sys
import os
import json
from datetime import datetime, timedelta
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
from summary_writer import SummaryWriter

class TaskUpdater:
    """
    Updates task status in the database based on evaluation decisions.
    Handles task extensions, closures, and status changes.
    """
    def __init__(self):
        self.today = datetime.now().date()
        print(f"UPDATER: Initializing for {self.today}")
        
        try:
            # Connect to the database
            self.supabase = get_connection()
            print("UPDATER: Connected to Supabase")
            
            # Initialize summary writer for updating summaries
            self.summary_writer = SummaryWriter()
        except Exception as e:
            print(f"UPDATER: Error initializing: {e}")
            self.supabase = None
            self.summary_writer = None
    
    def get_task_details(self, task_id):
        """
        Get task details from the database
        
        Args:
            task_id: ID of the task to retrieve
            
        Returns:
            dict: Task details or None if not found
        """
        if not self.supabase:
            print("UPDATER: No database connection available")
            return None
            
        try:
            result = self.supabase.table('tasks').select('*').eq('id', task_id).execute()
            
            if result.data:
                return result.data[0]
            else:
                print(f"UPDATER: No task found with ID {task_id}")
                return None
        except Exception as e:
            print(f"UPDATER: Error retrieving task: {e}")
            return None
    
    def get_evaluation(self, evaluation_id):
        """
        Get evaluation details from the database
        
        Args:
            evaluation_id: ID of the evaluation to retrieve
            
        Returns:
            dict: Evaluation details or None if not found
        """
        if not self.supabase:
            print("UPDATER: No database connection available")
            return None
            
        try:
            result = self.supabase.table('task_evaluations').select('*').eq('id', evaluation_id).execute()
            
            if result.data:
                return result.data[0]
            else:
                print(f"UPDATER: No evaluation found with ID {evaluation_id}")
                return None
        except Exception as e:
            print(f"UPDATER: Error retrieving evaluation: {e}")
            return None
    
    def extend_task(self, task_id, original_end_date, reason, extension_days=14):
        """
        Extend a task for continued monitoring
        
        Args:
            task_id: ID of the task to extend
            original_end_date: Current end date of the task
            reason: Reason for the extension
            extension_days: Number of days to extend (default: 14)
            
        Returns:
            dict: Updated task or None if failed
        """
        if not self.supabase:
            print("UPDATER: No database connection available")
            return None
            
        try:
            # Get current task details
            task = self.get_task_details(task_id)
            if not task:
                return None
                
            # Calculate the new end date
            original_date = datetime.fromisoformat(original_end_date)
            new_end_date = original_date + timedelta(days=extension_days)
            
            # Get the current extension count
            extension_count = task.get('extension_count', 0)
            new_extension_count = extension_count + 1
            
            # Update the task
            update_data = {
                'monitor_end_date': new_end_date.isoformat(),
                'monitor_status': 'extended',
                'extension_count': new_extension_count,
                'updated_at': datetime.now().isoformat()
            }
            
            update_result = self.supabase.table('tasks').update(update_data).eq('id', task_id).execute()
            
            if not update_result.data:
                print(f"UPDATER: Failed to update task {task_id}")
                return None
                
            # Record the extension in the extensions table
            extension_data = {
                'task_id': task_id,
                'original_end_date': original_end_date,
                'new_end_date': new_end_date.isoformat(),
                'reason': reason,
                'extension_number': new_extension_count
            }
            
            extension_result = self.supabase.table('task_extensions').insert(extension_data).execute()
            
            # If the task has a summary, mark it as non-final
            if 'summary_id' in task:
                self.summary_writer.update_summary_status(task['summary_id'], False)
            
            print(f"UPDATER: Extended task {task_id} to {new_end_date.isoformat()}")
            print(f"UPDATER: New extension count: {new_extension_count}")
            
            return update_result.data[0]
            
        except Exception as e:
            print(f"UPDATER: Error extending task: {e}")
            return None
    
    def close_task(self, task_id, reason):
        """
        Close a task (mark as completed)
        
        Args:
            task_id: ID of the task to close
            reason: Reason for closure
            
        Returns:
            dict: Updated task or None if failed
        """
        if not self.supabase:
            print("UPDATER: No database connection available")
            return None
            
        try:
            # Update the task
            update_data = {
                'status': 'completed',
                'monitor_status': 'completed',
                'completed_at': datetime.now().isoformat(),
                'completion_notes': reason,
                'updated_at': datetime.now().isoformat()
            }
            
            result = self.supabase.table('tasks').update(update_data).eq('id', task_id).execute()
            
            if result.data:
                print(f"UPDATER: Closed task {task_id}")
                return result.data[0]
            else:
                print(f"UPDATER: Failed to close task {task_id}")
                return None
                
        except Exception as e:
            print(f"UPDATER: Error closing task: {e}")
            return None
    
    def update_task_status(self, task_id, status, notes=None):
        """
        Update a task's status
        
        Args:
            task_id: ID of the task to update
            status: New status (needs_review, needs_intervention)
            notes: Optional notes about the status change
            
        Returns:
            dict: Updated task or None if failed
        """
        if not self.supabase:
            print("UPDATER: No database connection available")
            return None
            
        try:
            # Prepare update data
            update_data = {
                'monitor_status': status,
                'updated_at': datetime.now().isoformat()
            }
            
            if notes:
                update_data['notes'] = notes
            
            # Update the task
            result = self.supabase.table('tasks').update(update_data).eq('id', task_id).execute()
            
            if result.data:
                print(f"UPDATER: Updated task {task_id} status to {status}")
                return result.data[0]
            else:
                print(f"UPDATER: Failed to update task {task_id}")
                return None
                
        except Exception as e:
            print(f"UPDATER: Error updating task: {e}")
            return None
    
    def process_evaluation(self, evaluation_id):
        """
        Process an evaluation and update the task accordingly
        
        Args:
            evaluation_id: ID of the evaluation to process
            
        Returns:
            dict: Result of the update operation or None if failed
        """
        if not self.supabase:
            print("UPDATER: No database connection available")
            return None
            
        # Get the evaluation
        evaluation = self.get_evaluation(evaluation_id)
        if not evaluation:
            return None
            
        # Get the task
        task_id = evaluation['task_id']
        task = self.get_task_details(task_id)
        if not task:
            return None
            
        # Get the decision
        decision = evaluation['decision']
        explanation = evaluation['explanation']
        
        # Process based on decision
        result = None
        if decision == 'close':
            result = self.close_task(task_id, explanation)
        elif decision == 'extend':
            result = self.extend_task(
                task_id=task_id,
                original_end_date=task['monitor_end_date'],
                reason=explanation
            )
        elif decision == 'review':
            result = self.update_task_status(task_id, 'needs_review', explanation)
        elif decision == 'intervene':
            result = self.update_task_status(task_id, 'needs_intervention', explanation)
        
        if result:
            # Mark this evaluation as processed
            self.supabase.table('task_evaluations').update({
                'processed_at': datetime.now().isoformat()
            }).eq('id', evaluation_id).execute()
            
            return {
                'task_id': task_id,
                'evaluation_id': evaluation_id,
                'action': decision,
                'status': 'processed'
            }
        else:
            return {
                'task_id': task_id,
                'evaluation_id': evaluation_id,
                'action': decision,
                'status': 'failed'
            }
    
    def find_and_process_evaluations(self):
        """
        Find unprocessed evaluations and process them
        
        Returns:
            list: Results of processing each evaluation
        """
        if not self.supabase:
            print("UPDATER: No database connection available")
            return []
            
        try:
            # Find evaluations that haven't been processed
            result = self.supabase.table('task_evaluations').select('*').is_('processed_at', 'null').execute()
            
            if not result.data:
                print("UPDATER: No unprocessed evaluations found")
                return []
                
            print(f"UPDATER: Found {len(result.data)} unprocessed evaluations")
            
            # Process each evaluation
            results = []
            for evaluation in result.data:
                process_result = self.process_evaluation(evaluation['id'])
                if process_result:
                    results.append(process_result)
            
            # Print summary
            processed = len([r for r in results if r['status'] == 'processed'])
            failed = len([r for r in results if r['status'] == 'failed'])
            print(f"UPDATER: Processed {processed} evaluations, {failed} failed")
            
            return results
                
        except Exception as e:
            print(f"UPDATER: Error finding unprocessed evaluations: {e}")
            return []


# For testing this module directly
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Update tasks based on evaluations')
    parser.add_argument('--evaluation-id', help='ID of evaluation to process')
    parser.add_argument('--extend-task', help='ID of task to extend')
    parser.add_argument('--close-task', help='ID of task to close')
    parser.add_argument('--process-all', action='store_true', help='Process all unprocessed evaluations')
    args = parser.parse_args()
    
    updater = TaskUpdater()
    
    if args.evaluation_id:
        result = updater.process_evaluation(args.evaluation_id)
        if result:
            print(f"Processed evaluation {args.evaluation_id}")
            print(f"Action: {result['action']}, Status: {result['status']}")
    
    elif args.extend_task:
        task = updater.get_task_details(args.extend_task)
        if task:
            result = updater.extend_task(
                task_id=args.extend_task,
                original_end_date=task['monitor_end_date'],
                reason="Manual extension"
            )
            if result:
                print(f"Extended task {args.extend_task}")
                print(f"New end date: {result['monitor_end_date']}")
        else:
            print(f"Task {args.extend_task} not found")
    
    elif args.close_task:
        result = updater.close_task(args.close_task, "Manual closure")
        if result:
            print(f"Closed task {args.close_task}")
    
    elif args.process_all:
        results = updater.find_and_process_evaluations()
        print(f"Processed {len(results)} evaluations")
        
        # Print details
        for result in results:
            print(f"Evaluation {result['evaluation_id']}: {result['action']} - {result['status']}")
    
    else:
        print("No action specified. Use --evaluation-id, --extend-task, --close-task, or --process-all.")