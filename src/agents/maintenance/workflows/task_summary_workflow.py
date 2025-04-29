#!/usr/bin/env python3
import sys
import os
import json
import logging
from datetime import datetime
from pathlib import Path
import argparse
from dotenv import load_dotenv

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env.local")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("TaskSummaryWorkflow")

# Import all required components
from src.agents.maintenance.tracker.task_summary.summary_data import SummaryDataCollector
from src.agents.maintenance.tracker.task_summary.summary_analyzer import SummaryAnalyzer
from src.agents.maintenance.tracker.task_summary.summary_writer import SummaryWriter
from src.agents.maintenance.tracker.task_summary.task_evaluator import TaskEvaluator
from src.agents.maintenance.tracker.task_summary.task_updator import TaskUpdater
from src.agents.maintenance.tracker.Performance_tracking.notification_handler import NotificationHandler

class TaskSummaryWorkflow:
    """
    Task Summary Workflow - Complete process for evaluating tasks at the end of their monitoring period.
    
    This workflow orchestrates the end-to-end process for task evaluation:
    1. Find tasks marked for evaluation
    2. Collect and analyze performance data
    3. Save performance summaries
    4. Evaluate summaries and make decisions
    5. Update task status based on evaluations
    6. Send notifications about outcomes
    """
    def __init__(self):
        self.today = datetime.now().date()
        logger.info(f"Initializing Task Summary Workflow for {self.today}")
        
        # Create output directory
        self.output_dir = Path(current_dir) / "output"
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize components
        try:
            self.data_collector = SummaryDataCollector()
            self.analyzer = SummaryAnalyzer()
            self.writer = SummaryWriter()
            self.evaluator = TaskEvaluator()
            self.updater = TaskUpdater()
            self.notification_handler = NotificationHandler()
            
            logger.info("All components initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing components: {e}")
            raise
        
        # Results tracking
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'tasks_found': 0,
            'summaries_created': 0,
            'evaluations_made': 0,
            'tasks_updated': 0,
            'notifications_sent': 0,
            'errors': []
        }
    
    def save_output(self, data, filename):
        """Save data to output file"""
        filepath = self.output_dir / filename
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved output to {filepath}")
        return filepath
    
    def find_tasks_for_evaluation(self):
        """Find tasks marked for evaluation"""
        logger.info("Finding tasks marked for evaluation")
        try:
            # Use the new method we added to SummaryDataCollector
            tasks = self.data_collector.get_tasks_marked_for_evaluation()
            self.results['tasks_found'] = len(tasks)
            
            if tasks:
                logger.info(f"Found {len(tasks)} tasks marked for evaluation")
                # Save task IDs for reference
                self.save_output([task['id'] for task in tasks], f"evaluation_tasks_{self.today.isoformat()}.json")
            else:
                logger.info("No tasks found marked for evaluation")
            
            return tasks
        except Exception as e:
            error_msg = f"Error finding tasks for evaluation: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.results['errors'].append(error_msg)
            return []
    
    def generate_summaries(self, tasks):
        """Generate performance summaries for tasks"""
        logger.info(f"Generating summaries for {len(tasks)} tasks")
        summaries = []
        
        for task in tasks:
            task_id = task['id']
            logger.info(f"Processing task {task_id}")
            
            try:
                # Step 1: Collect data for the task
                task_data = self.data_collector.collect_data_for_task(task_id)
                if not task_data:
                    logger.warning(f"No data available for task {task_id}")
                    continue
                
                # Step 2: Analyze the data
                summary = self.analyzer.analyze_task_data(task_data)
                if not summary:
                    logger.warning(f"Could not analyze data for task {task_id}")
                    continue
                
                # Step 3: Save the summary to the database
                saved_summary = self.writer.save_summary(summary, task_data['task'])
                if saved_summary:
                    # Add the summary ID to the summary object
                    summary['summary_id'] = saved_summary['id']
                    summaries.append(summary)
                    logger.info(f"Created summary for task {task_id} with ID {saved_summary['id']}")
                else:
                    logger.warning(f"Failed to save summary for task {task_id}")
                
            except Exception as e:
                error_msg = f"Error processing task {task_id}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                self.results['errors'].append(error_msg)
        
        self.results['summaries_created'] = len(summaries)
        
        if summaries:
            # Save summaries for reference
            self.save_output(summaries, f"task_summaries_{self.today.isoformat()}.json")
        
        logger.info(f"Created {len(summaries)} summaries")
        return summaries
    
    def evaluate_summaries(self):
        """Evaluate summaries and make decisions"""
        logger.info("Evaluating performance summaries")
        
        try:
            # Use TaskEvaluator to find and evaluate summaries
            evaluations = self.evaluator.find_and_evaluate_summaries()
            self.results['evaluations_made'] = len(evaluations)
            
            if evaluations:
                # Save evaluations for reference
                self.save_output(evaluations, f"task_evaluations_{self.today.isoformat()}.json")
                logger.info(f"Made {len(evaluations)} evaluation decisions")
            else:
                logger.info("No evaluations were made")
            
            return evaluations
        except Exception as e:
            error_msg = f"Error evaluating summaries: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.results['errors'].append(error_msg)
            return []
    
    def update_tasks(self):
        """Update tasks based on evaluations"""
        logger.info("Updating tasks based on evaluations")
        
        try:
            # Use TaskUpdater to find and process evaluations
            update_results = self.updater.find_and_process_evaluations()
            
            # Count only successfully processed updates
            processed_updates = [r for r in update_results if r['status'] == 'processed']
            self.results['tasks_updated'] = len(processed_updates)
            
            if update_results:
                # Save update results for reference
                self.save_output(update_results, f"task_updates_{self.today.isoformat()}.json")
                
                # Count actions by type
                actions = {}
                for result in processed_updates:
                    action = result['action']
                    actions[action] = actions.get(action, 0) + 1
                
                logger.info(f"Updated {len(processed_updates)} tasks")
                for action, count in actions.items():
                    logger.info(f"- {action}: {count} tasks")
            else:
                logger.info("No tasks were updated")
            
            return update_results
        except Exception as e:
            error_msg = f"Error updating tasks: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.results['errors'].append(error_msg)
            return []
    
    def send_notifications(self, update_results):
        """Send notifications about task updates"""
        logger.info("Sending notifications")
        
        if not update_results:
            logger.info("No updates to send notifications for")
            return 0
        
        processed_updates = [r for r in update_results if r['status'] == 'processed']
        if not processed_updates:
            logger.info("No successfully processed updates to notify about")
            return 0
        
        notifications_created = 0
        
        try:
            for update in processed_updates:
                task_id = update['task_id']
                action = update['action']
                evaluation_id = update.get('evaluation_id')
                
                # Get task details
                task = self.updater.get_task_details(task_id)
                if not task:
                    logger.warning(f"Could not find details for task {task_id}")
                    continue
                
                # Get evaluation details
                evaluation = self.evaluator.get_evaluation(evaluation_id) if evaluation_id else None
                if not evaluation and evaluation_id:
                    logger.warning(f"Could not find evaluation {evaluation_id}")
                    continue
                
                # Create notification
                subject = f"Task {action.title()} Notification - {task.get('title', f'Task #{task_id}')}"
                message = self._create_notification_message(task, evaluation, action)
                recipient = self._get_recipient_for_action(task, action)
                
                notification = self.notification_handler.create_notification(
                    subject=subject,
                    message=message,
                    notification_type="task_evaluation",
                    recipient=recipient,
                    status="pending"
                )
                
                if notification:
                    notifications_created += 1
                    logger.info(f"Created notification for task {task_id} ({action})")
            
            # Send all pending notifications
            sent = self.notification_handler.send_pending_notifications()
            self.results['notifications_sent'] = sent
            
            logger.info(f"Sent {sent} notifications")
            return sent
            
        except Exception as e:
            error_msg = f"Error sending notifications: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.results['errors'].append(error_msg)
            return 0
    
    def clear_evaluation_flags(self, task_ids):
        """Clear the needs_evaluation flag for processed tasks"""
        if not task_ids:
            logger.info("No tasks to clear evaluation flags for")
            return 0
        
        logger.info(f"Clearing evaluation flags for {len(task_ids)} tasks")
        cleared = 0
        
        try:
            for task_id in task_ids:
                result = self.data_collector.supabase.table('tasks').update({
                    'needs_evaluation': False,
                    'evaluation_completed_at': datetime.now().isoformat()
                }).eq('id', task_id).execute()
                
                if result.data:
                    cleared += 1
            
            logger.info(f"Cleared evaluation flags for {cleared} tasks")
            return cleared
        except Exception as e:
            error_msg = f"Error clearing evaluation flags: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.results['errors'].append(error_msg)
            return 0
    
    def _create_notification_message(self, task, evaluation, action):
        """Create a notification message based on the action type"""
        task_title = task.get('title', f"Task #{task['id']}")
        
        # Get explanation and recommendation from evaluation if available
        explanation = ""
        recommendation = ""
        if evaluation and isinstance(evaluation, dict):
            if 'explanation' in evaluation:
                explanation = evaluation['explanation']
            elif 'decision' in evaluation and isinstance(evaluation['decision'], dict):
                explanation = evaluation['decision'].get('explanation', '')
                recommendation = evaluation['decision'].get('recommendation', '')
        
        message = f"Task: {task_title}\n\n"
        
        if action == 'close':
            message += f"This task has been closed. {explanation}\n\n"
            if recommendation:
                message += f"Recommendation: {recommendation}\n\n"
            message += f"Completed on: {self.today.isoformat()}"
        
        elif action == 'extend':
            # Get the extension details
            extension_count = task.get('extension_count', 0)
            end_date = task.get('monitor_end_date', 'unknown')
            
            message += f"This task has been extended (Extension #{extension_count}).\n\n"
            if explanation:
                message += f"Reason: {explanation}\n\n"
            message += f"New end date: {end_date}\n\n"
            if recommendation:
                message += f"Recommendation: {recommendation}"
        
        elif action == 'review':
            message += f"This task requires review. {explanation}\n\n"
            if recommendation:
                message += f"Recommendation: {recommendation}\n\n"
            message += "Please review the task details and performance data."
        
        elif action == 'intervene':
            message += f"URGENT: This task requires intervention. {explanation}\n\n"
            if recommendation:
                message += f"Recommendation: {recommendation}\n\n"
            message += "Immediate action is required to address performance issues."
        
        return message
    
    def _get_recipient_for_action(self, task, action):
        """Determine who should receive the notification"""
        # Default recipient based on task
        default_recipient = task.get('assigned_to', 'maintenance_manager')
        
        if action == 'intervene':
            # For intervention, always notify the manager
            return 'maintenance_manager'
        elif action == 'review':
            # For review, notify the manager
            return 'maintenance_manager'
        else:
            # For other actions, notify the assigned person
            return default_recipient
    
    def run(self):
        """
        Run the complete task summary workflow
        
        Returns:
            dict: Results of the workflow execution
        """
        start_time = datetime.now()
        logger.info(f"Starting Task Summary Workflow at {start_time}")
        
        try:
            # Step 1: Find tasks marked for evaluation
            tasks = self.find_tasks_for_evaluation()
            if not tasks:
                logger.info("No tasks to process, workflow complete")
                return self.results
            
            # Step 2: Generate performance summaries
            summaries = self.generate_summaries(tasks)
            
            # Step 3: Evaluate summaries
            evaluations = self.evaluate_summaries()
            
            # Step 4: Update tasks based on evaluations
            update_results = self.update_tasks()
            
            # Get task IDs that were successfully updated
            processed_task_ids = [r['task_id'] for r in update_results if r['status'] == 'processed']
            
            # Step 5: Send notifications
            self.send_notifications(update_results)
            
            # Step 6: Clear evaluation flags for processed tasks
            self.clear_evaluation_flags(processed_task_ids)
            
            # Calculate execution time
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            logger.info(f"Workflow completed in {execution_time:.2f} seconds")
            logger.info(f"Processed {self.results['tasks_found']} tasks, created {self.results['summaries_created']} summaries")
            logger.info(f"Made {self.results['evaluations_made']} evaluations, updated {self.results['tasks_updated']} tasks")
            logger.info(f"Sent {self.results['notifications_sent']} notifications")
            
            if self.results['errors']:
                logger.warning(f"Encountered {len(self.results['errors'])} errors")
            
        except Exception as e:
            error_msg = f"Error in workflow execution: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.results['errors'].append(error_msg)
        
        # Save final results
        self.save_output(self.results, f"workflow_results_{self.today.isoformat()}.json")
        
        return self.results


# For direct execution
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run the Task Summary Workflow')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--log-file', help='Log to the specified file')
    parser.add_argument('--output-dir', help='Output directory for result files')
    args = parser.parse_args()
    
    # Configure logging based on arguments
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger.setLevel(log_level)
    
    # Add file handler if log file specified
    if args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(file_handler)
    
    try:
        # Run the workflow
        workflow = TaskSummaryWorkflow()
        
        # Set custom output directory if specified
        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(exist_ok=True)
            workflow.output_dir = output_dir
        
        results = workflow.run()
        
        # Print summary to console
        print("\nTask Summary Workflow Results:")
        print(f"- Tasks processed: {results['tasks_found']}")
        print(f"- Summaries created: {results['summaries_created']}")
        print(f"- Evaluations made: {results['evaluations_made']}")
        print(f"- Tasks updated: {results['tasks_updated']}")
        print(f"- Notifications sent: {results['notifications_sent']}")
        
        if results['errors']:
            print(f"- Errors encountered: {len(results['errors'])}")
            print(f"  First error: {results['errors'][0]}")
        
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Critical error in workflow: {e}")
        print(f"Critical error: {str(e)}")
        sys.exit(1)