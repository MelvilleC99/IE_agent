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

class NotificationHandler:
    """
    Handles creation and sending of notifications for the maintenance system.
    """
    
    def __init__(self):
        """Initialize the notification handler"""
        try:
            self.supabase = get_connection()
            print("NOTIFICATION: Initialized notification handler")
        except Exception as e:
            print(f"NOTIFICATION: Error connecting to database: {e}")
            self.supabase = None
        
    def create_notification(self, task_id=None, evaluation_id=None, recipient="maintenance_manager", 
                           notification_type="dashboard", subject=None, message=None, 
                           status="pending"):
        """
        Create a notification in the database
        
        Args:
            task_id: Optional ID of the related task
            evaluation_id: Optional ID of the related task evaluation
            recipient: Who should receive the notification (email, user ID, role)
            notification_type: Type of notification (email, sms, dashboard)
            subject: Subject line for the notification
            message: Body content of the notification
            status: Initial status of the notification (pending, sent, failed)
            
        Returns:
            dict: The created notification record or None if creation failed
        """
        if not self.supabase:
            print("NOTIFICATION: Cannot create notification - no database connection")
            return None
            
        if not message:
            print("NOTIFICATION: Cannot create notification without message")
            return None
            
        notification_data = {
            "task_id": task_id,
            "evaluation_id": evaluation_id,
            "recipient": recipient,
            "notification_type": notification_type,
            "subject": subject,
            "message": message,
            "status": status,
            "created_at": datetime.now().isoformat()
        }
        
        try:
            result = self.supabase.table('notification_logs').insert(notification_data).execute()
            
            if result.data:
                notification_id = result.data[0]['id']
                print(f"NOTIFICATION: Created notification ID {notification_id}")
                return result.data[0]
            else:
                print("NOTIFICATION: Failed to create notification")
                return None
                
        except Exception as e:
            print(f"NOTIFICATION: Error creating notification: {e}")
            return None
    
    def create_task_creation_notification(self, tasks):
        """
        Create a notification for newly created tasks
        
        Args:
            tasks: List of task records that were created
            
        Returns:
            dict: The created notification or None if creation failed
        """
        if not tasks:
            return None
            
        # Group tasks by mechanic
        mechanics = {}
        for task in tasks:
            mechanic_name = task.get('mechanic_name')
            mechanic_id = task.get('mechanic_id')
            key = f"{mechanic_name} (#{mechanic_id})"
            
            if key not in mechanics:
                mechanics[key] = []
                
            mechanics[key].append(task)
        
        # Create notification message
        subject = f"New Maintenance Tasks Created ({len(tasks)} tasks)"
        
        message = f"The system has identified {len(tasks)} new maintenance tasks for monitoring:\n\n"
        
        for mechanic, mech_tasks in mechanics.items():
            message += f"# {mechanic}: {len(mech_tasks)} tasks\n"
            
            for task in mech_tasks:
                issue_type = task.get('issue_type', '').replace('_', ' ').title()
                end_date = task.get('monitor_end_date', 'unknown')
                frequency = task.get('monitor_frequency', 'unknown')
                
                # Format additional context based on task type
                context = ""
                if task.get('machine_type'):
                    context += f" - Machine: {task.get('machine_type')}"
                if task.get('reason'):
                    context += f" - Issue: {task.get('reason')}"
                    
                message += f"- Task #{task.get('id')}: {issue_type}{context}\n"
                message += f"  Monitoring until {end_date} ({frequency} measurements)\n"
            
            message += "\n"
            
        message += "Please review these tasks within 2 working days and assign them as needed."
        
        # Create the notification
        return self.create_notification(
            subject=subject,
            message=message,
            notification_type="dashboard",
            recipient="maintenance_manager",
            status="pending"
        )
    
    def create_workflow_completion_notification(self, task_count):
        """
        Create a simple notification that the mechanic performance workflow has completed
        
        Args:
            task_count: Number of tasks created
            
        Returns:
            dict: The created notification or None if creation failed
        """
        subject = "Mechanic Performance Analysis Completed"
        
        message = f"The mechanic performance analysis workflow has completed.\n\n"
        message += f"- {task_count} new task(s) have been created\n\n"
        message += "Please review these tasks and assign them as appropriate."
        
        return self.create_notification(
            subject=subject,
            message=message,
            notification_type="dashboard",
            recipient="maintenance_manager",
            status="pending"
        )
    
    def send_pending_notifications(self):
        """
        Process and send all pending notifications
        
        Returns:
            int: Number of notifications successfully sent
        """
        if not self.supabase:
            print("NOTIFICATION: Cannot send notifications - no database connection")
            return 0
            
        # This is a placeholder - in a real system, this would connect to email/SMS/etc. services
        try:
            result = self.supabase.table('notification_logs').select('*').eq('status', 'pending').execute()
            
            if not result.data:
                print("NOTIFICATION: No pending notifications to send")
                return 0
                
            sent_count = 0
            for notification in result.data:
                # In a real implementation, this would send via the appropriate channel
                # For now, we'll just update the status
                
                notification_id = notification['id']
                update_result = self.supabase.table('notification_logs').update({
                    'status': 'sent',
                    'sent_at': datetime.now().isoformat()
                }).eq('id', notification_id).execute()
                
                if update_result.data:
                    sent_count += 1
                    print(f"NOTIFICATION: Marked notification {notification_id} as sent")
                    
            print(f"NOTIFICATION: Processed {sent_count} notifications")
            return sent_count
            
        except Exception as e:
            print(f"NOTIFICATION: Error sending notifications: {e}")
            return 0

# Test function for direct execution
if __name__ == '__main__':
    handler = NotificationHandler()
    
    # Test creating a simple notification
    test_notification = handler.create_notification(
        subject="Test Notification",
        message="This is a test notification from the notification handler.",
        notification_type="dashboard",
        recipient="test_user",
        status="pending"
    )
    
    if test_notification:
        print("Successfully created test notification!")
    
    # Process pending notifications
    sent_count = handler.send_pending_notifications()
    print(f"Sent {sent_count} notifications")