#!/usr/bin/env python3
import sys
import os
import json
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

class NotificationHandler:
    """
    Handles sending notifications based on task evaluations.
    Manages different message templates and notification channels.
    """
    def __init__(self):
        self.today = datetime.now().date()
        print(f"NOTIFY: Initializing for {self.today}")
        
        try:
            # Connect to the database
            self.supabase = get_connection()
            print("NOTIFY: Connected to Supabase")
        except Exception as e:
            print(f"NOTIFY: Error initializing: {e}")
            self.supabase = None
    
    def get_task_details(self, task_id):
        """
        Get task details from the database
        
        Args:
            task_id: ID of the task to retrieve
            
        Returns:
            dict: Task details or None if not found
        """
        if not self.supabase:
            print("NOTIFY: No database connection available")
            return None
            
        try:
            result = self.supabase.table('tasks').select('*').eq('id', task_id).execute()
            
            if result.data:
                return result.data[0]
            else:
                print(f"NOTIFY: No task found with ID {task_id}")
                return None
        except Exception as e:
            print(f"NOTIFY: Error retrieving task: {e}")
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
            print("NOTIFY: No database connection available")
            return None
            
        try:
            result = self.supabase.table('task_evaluations').select('*').eq('id', evaluation_id).execute()
            
            if result.data:
                return result.data[0]
            else:
                print(f"NOTIFY: No evaluation found with ID {evaluation_id}")
                return None
        except Exception as e:
            print(f"NOTIFY: Error retrieving evaluation: {e}")
            return None
    
    def create_message(self, task, evaluation):
        """
        Create a notification message based on task and evaluation
        
        Args:
            task: Task details
            evaluation: Evaluation details
            
        Returns:
            dict: Message with subject and body
        """
        # Extract task details
        task_title = task.get('title', 'Untitled Task')
        mechanic_name = task.get('mechanic_name', 'Mechanic')
        issue_type = task.get('issue_type', 'performance')
        
        # Extract evaluation details
        decision = evaluation.get('decision', 'review')
        explanation = evaluation.get('explanation', '')
        recommendation = evaluation.get('recommendation', '')
        
        # Create message subject
        if decision == 'close':
            subject = f"Performance Task Completed: {task_title}"
        elif decision == 'extend':
            subject = f"Performance Task Extended: {task_title}"
        elif decision == 'review':
            subject = f"Performance Task Needs Review: {task_title}"
        elif decision == 'intervene':
            subject = f"URGENT: Intervention Needed for {task_title}"
        else:
            subject = f"Performance Task Update: {task_title}"
        
        # Create message body based on decision type
        if decision == 'close':
            body = f"""
Hello,

The performance monitoring task "{task_title}" for {mechanic_name} has been completed successfully.

{explanation}

{recommendation}

No further action is required for this task.

Thank you,
Performance Monitoring System
"""
        elif decision == 'extend':
            body = f"""
Hello,

The performance monitoring task "{task_title}" for {mechanic_name} has been extended for further monitoring.

{explanation}

{recommendation}

The task will continue to be monitored for additional improvement.

Thank you,
Performance Monitoring System
"""
        elif decision == 'review':
            body = f"""
Hello,

The performance monitoring task "{task_title}" for {mechanic_name} requires review.

{explanation}

{recommendation}

Please review the task and determine the appropriate next steps.

Thank you,
Performance Monitoring System
"""
        elif decision == 'intervene':
            body = f"""
Hello,

URGENT: The performance monitoring task "{task_title}" for {mechanic_name} requires immediate intervention.

{explanation}

{recommendation}

Please take action as soon as possible to address this performance issue.

Thank you,
Performance Monitoring System
"""
        else:
            body = f"""
Hello,

This is an update regarding the performance monitoring task "{task_title}" for {mechanic_name}.

{explanation}

{recommendation}

Thank you,
Performance Monitoring System
"""
        
        return {
            'subject': subject,
            'body': body
        }
    
    def get_recipients(self, task):
        """
        Determine who should receive notifications for a task
        
        Args:
            task: Task details
            
        Returns:
            list: Email addresses to notify
        """
        recipients = []
        
        # Get the assigned person (if any)
        assigned_to = task.get('assigned_to')
        if assigned_to and '@' in assigned_to:
            recipients.append(assigned_to)
        
        # Get the mechanic's supervisor (if applicable)
        mechanic_id = task.get('mechanic_id')
        if mechanic_id:
            try:
                # This would be your logic to find the supervisor
                # For example, query a mechanics or employees table
                result = self.supabase.table('mechanics').select('supervisor_email').eq('id', mechanic_id).execute()
                if result.data and result.data[0].get('supervisor_email'):
                    recipients.append(result.data[0]['supervisor_email'])
            except Exception as e:
                print(f"NOTIFY: Error finding supervisor: {e}")
        
        # Always include the maintenance manager
        recipients.append('maintenance.manager@company.com')
        
        return list(set(recipients))  # Remove duplicates
    
    def send_email(self, recipients, subject, body):
        """
        Send an email notification
        
        Args:
            recipients: List of email addresses
            subject: Email subject
            body: Email body
            
        Returns:
            bool: True if sent successfully, False otherwise
        """
        # This would be replaced with your actual email sending code
        # For example, using SMTP, SendGrid, or another email service
        print(f"NOTIFY: Sending email to {', '.join(recipients)}")
        print(f"NOTIFY: Subject: {subject}")
        print(f"NOTIFY: Body: {body[:100]}...")
        
        # Mock successful sending for testing
        return True
    
    def log_notification(self, task_id, evaluation_id, recipients, message, status):
        """
        Log a notification in the database
        
        Args:
            task_id: ID of the task
            evaluation_id: ID of the evaluation
            recipients: List of recipients
            message: Message that was sent
            status: Status of the notification (sent, failed)
            
        Returns:
            dict: Saved notification record or None if failed
        """
        if not self.supabase:
            print("NOTIFY: No database connection available")
            return None
            
        try:
            # Prepare notification record
            notification_record = {
                'task_id': task_id,
                'evaluation_id': evaluation_id,
                'recipient': ', '.join(recipients),
                'notification_type': 'email',
                'message': json.dumps(message),
                'status': status,
                'sent_at': datetime.now().isoformat() if status == 'sent' else None
            }
            
            # Insert the record
            result = self.supabase.table('notification_logs').insert(notification_record).execute()
            
            if result.data:
                print(f"NOTIFY: Logged notification with ID {result.data[0]['id']}")
                return result.data[0]
            else:
                print("NOTIFY: Failed to log notification")
                return None
                
        except Exception as e:
            print(f"NOTIFY: Error logging notification: {e}")
            return None
    
    def notify_for_evaluation(self, evaluation_id):
        """
        Send notification for a specific evaluation
        
        Args:
            evaluation_id: ID of the evaluation to notify for
            
        Returns:
            dict: Result of the notification operation
        """
        # Get the evaluation
        evaluation = self.get_evaluation(evaluation_id)
        if not evaluation:
            return {'status': 'error', 'message': f"Evaluation {evaluation_id} not found"}
        
        # Get the task
        task_id = evaluation['task_id']
        task = self.get_task_details(task_id)
        if not task:
            return {'status': 'error', 'message': f"Task {task_id} not found"}
        
        # Create the message
        message = self.create_message(task, evaluation)
        
        # Determine recipients
        recipients = self.get_recipients(task)
        if not recipients:
            return {'status': 'error', 'message': "No recipients found"}
        
        # Send the notification
        success = self.send_email(recipients, message['subject'], message['body'])
        
        # Log the notification
        status = 'sent' if success else 'failed'
        notification = self.log_notification(task_id, evaluation_id, recipients, message, status)
        
        return {
            'task_id': task_id,
            'evaluation_id': evaluation_id,
            'status': status,
            'recipients': recipients,
            'notification_id': notification['id'] if notification else None
        }
    
    def find_and_notify_evaluations(self):
        """
        Find evaluations that need notifications and send them
        
        Returns:
            list: Results of sending notifications
        """
        if not self.supabase:
            print("NOTIFY: No database connection available")
            return []
            
        try:
            # Find evaluations that don't have notifications
            query = """
            SELECT e.id, e.task_id
            FROM task_evaluations e
            LEFT JOIN notification_logs n ON e.id = n.evaluation_id
            WHERE n.id IS NULL
            """
            
            result = self.supabase.rpc('find_unnotified_evaluations').execute()
            
            if not result.data:
                print("NOTIFY: No unnotified evaluations found")
                return []
                
            print(f"NOTIFY: Found {len(result.data)} unnotified evaluations")
            
            # Send notifications for each evaluation
            results = []
            for eval_record in result.data:
                evaluation_id = eval_record['id']
                notify_result = self.notify_for_evaluation(evaluation_id)
                results.append(notify_result)
            
            # Print summary
            sent = len([r for r in results if r['status'] == 'sent'])
            failed = len([r for r in results if r['status'] == 'failed'])
            print(f"NOTIFY: Sent {sent} notifications, {failed} failed")
            
            return results
                
        except Exception as e:
            print(f"NOTIFY: Error finding unnotified evaluations: {e}")
            return []


# For testing this module directly
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Send notifications for task evaluations')
    parser.add_argument('--evaluation-id', help='ID of evaluation to notify for')
    parser.add_argument('--process-all', action='store_true', help='Find and notify all unnotified evaluations')
    args = parser.parse_args()
    
    notifier = NotificationHandler()
    
    if args.evaluation_id:
        result = notifier.notify_for_evaluation(args.evaluation_id)
        print(f"Notification result: {result['status']}")
        if result['status'] == 'sent':
            print(f"Sent to: {', '.join(result['recipients'])}")
    
    elif args.process_all:
        results = notifier.find_and_notify_evaluations()
        print(f"Processed {len(results)} notifications")
        
        # Print details of sent notifications
        for result in results:
            if result['status'] == 'sent':
                print(f"Evaluation {result['evaluation_id']}: Sent to {len(result['recipients'])} recipients")
    
    else:
        print("No action specified. Use --evaluation-id or --process-all.")