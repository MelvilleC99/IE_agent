#!/usr/bin/env python3
import sys
import os
import logging
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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class NotificationHandler:
    """
    Handles creation and sending of notifications for the maintenance system.
    """
    
    def __init__(self):
        """Initialize the notification handler"""
        try:
            self.supabase = get_connection()
            logger.info("NOTIFICATION: Initialized notification handler")
        except Exception as e:
            logger.error(f"NOTIFICATION: Error connecting to database: {e}")
            self.supabase = None
        
    def create_notification(self, watchlist_id=None, evaluation_id=None, recipient="maintenance_manager", 
                           notification_type="dashboard", subject=None, message=None, 
                           status="pending"):
        """
        Create a notification in the database
        
        Args:
            watchlist_id: Optional ID of the related watchlist item
            evaluation_id: Optional ID of the related watchlist evaluation
            recipient: Who should receive the notification (email, user ID, role)
            notification_type: Type of notification (email, sms, dashboard)
            subject: Subject line for the notification
            message: Body content of the notification
            status: Initial status of the notification (pending, sent, failed)
            
        Returns:
            dict: The created notification record or None if creation failed
        """
        if not self.supabase:
            logger.error("NOTIFICATION: Cannot create notification - no database connection")
            return None
            
        if not message:
            logger.error("NOTIFICATION: Cannot create notification without message")
            return None
            
        notification_data = {
            "watchlist_id": watchlist_id,
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
                logger.info(f"NOTIFICATION: Created notification ID {notification_id}")
                return result.data[0]
            else:
                logger.error("NOTIFICATION: Failed to create notification")
                return None
                
        except Exception as e:
            logger.error(f"NOTIFICATION: Error creating notification: {e}")
            return None
    
    def create_watchlist_creation_notification(self, watchlist_items):
        """
        Create a notification for newly created watchlist items
        
        Args:
            watchlist_items: List of watchlist records that were created
            
        Returns:
            dict: The created notification or None if creation failed
        """
        if not watchlist_items:
            return None
            
        # Group watchlist items by mechanic
        mechanics = {}
        for item in watchlist_items:
            mechanic_name = item.get('mechanic_name')
            mechanic_id = item.get('mechanic_id')
            key = f"{mechanic_name} (#{mechanic_id})"
            
            if key not in mechanics:
                mechanics[key] = []
                
            mechanics[key].append(item)
        
        # Create notification message
        subject = f"New Maintenance Watchlist Items Created ({len(watchlist_items)} items)"
        
        message = f"The system has identified {len(watchlist_items)} new maintenance watchlist items for monitoring:\n\n"
        
        for mechanic, mech_items in mechanics.items():
            message += f"# {mechanic}: {len(mech_items)} watchlist items\n"
            
            for item in mech_items:
                issue_type = item.get('issue_type', '').replace('_', ' ').title()
                end_date = item.get('monitor_end_date', 'unknown')
                frequency = item.get('monitor_frequency', 'unknown')
                
                # Format additional context based on item type
                context = ""
                if item.get('machine_type'):
                    context += f" - Machine: {item.get('machine_type')}"
                if item.get('reason'):
                    context += f" - Issue: {item.get('reason')}"
                    
                message += f"- Watchlist Item #{item.get('id')}: {issue_type}{context}\n"
                message += f"  Monitoring until {end_date} ({frequency} measurements)\n"
            
            message += "\n"
            
        message += "Please review these watchlist items within 2 working days and assign them as needed."
        
        # Create the notification
        return self.create_watchlist_notification(
            subject=subject,
            message=message,
            notification_type="dashboard",
            recipient="maintenance_manager",
            status="pending"
        )
    
    def create_workflow_completion_notification(self, item_count):
        """
        Create a simple notification that the mechanic performance workflow has completed
        
        Args:
            item_count: Number of watchlist items created
            
        Returns:
            dict: The created notification or None if creation failed
        """
        subject = "Mechanic Performance Analysis Completed"
        
        message = f"The mechanic performance analysis workflow has completed.\n\n"
        message += f"- {item_count} new watchlist item(s) have been created\n\n"
        message += "Please review these watchlist items and assign them as appropriate."
        
        return self.create_watchlist_notification(
            subject=subject,
            message=message,
            notification_type="dashboard",
            recipient="maintenance_manager",
            status="pending"
        )

    def create_watchlist_notification(self, watchlist_id=None, evaluation_id=None, action=None, message=None, 
                                    recipient="maintenance_manager", notification_type="dashboard", 
                                    subject=None, status="pending"):
        """
        Create a notification for a watchlist item action
        
        Args:
            watchlist_id: ID of the related watchlist item
            evaluation_id: Optional ID of the related evaluation
            action: Action taken on the watchlist item (e.g., 'close', 'extend')
            message: Body content of the notification
            recipient: Who should receive the notification (email, user ID, role)
            notification_type: Type of notification (email, sms, dashboard)
            subject: Subject line for the notification
            status: Initial status of the notification (pending, sent, failed)
            
        Returns:
            dict: The created notification record or None if creation failed
        """
        if not self.supabase:
            logger.error("NOTIFICATION: Cannot create notification - no database connection")
            return None
            
        if not message:
            logger.error("NOTIFICATION: Cannot create notification without message")
            return None
            
        # Get watchlist item details for subject line if not provided
        if not subject and watchlist_id:
            try:
                item_result = self.supabase.table('watch_list').select('title').eq('id', watchlist_id).execute()
                if item_result.data:
                    title = item_result.data[0]['title']
                    action_desc = action.capitalize() if action else "Update"
                    subject = f"Watchlist Item {action_desc}: {title}"
            except Exception as e:
                logger.warning(f"NOTIFICATION: Could not get watchlist item title: {e}")
        
        if not subject:
            subject = f"Watchlist Notification: {action.capitalize() if action else 'Update'}"
            
        # Create the notification
        notification_data = {
            "watchlist_id": watchlist_id,
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
                logger.info(f"NOTIFICATION: Created notification ID {notification_id}")
                return result.data[0]
            else:
                logger.error("NOTIFICATION: Failed to create notification")
                return None
                
        except Exception as e:
            logger.error(f"NOTIFICATION: Error creating notification: {e}")
            return None
    
    def process_evaluation_notifications(self):
        """
        Process and send all pending evaluation notifications
        
        Returns:
            list: Notifications that were processed
        """
        return self.send_pending_notifications()
    
    def send_pending_notifications(self):
        """
        Process and send all pending notifications
        
        Returns:
            list: Notifications that were processed
        """
        if not self.supabase:
            logger.error("NOTIFICATION: Cannot send notifications - no database connection")
            return []
            
        # This is a placeholder - in a real system, this would connect to email/SMS/etc. services
        try:
            result = self.supabase.table('notification_logs').select('*').eq('status', 'pending').execute()
            
            if not result.data:
                logger.info("NOTIFICATION: No pending notifications to send")
                return []
                
            sent_notifications = []
            for notification in result.data:
                # In a real implementation, this would send via the appropriate channel
                # For now, we'll just update the status
                
                notification_id = notification['id']
                update_result = self.supabase.table('notification_logs').update({
                    'status': 'sent',
                    'sent_at': datetime.now().isoformat()
                }).eq('id', notification_id).execute()
                
                if update_result.data:
                    sent_notifications.append(update_result.data[0])
                    logger.info(f"NOTIFICATION: Marked notification {notification_id} as sent")
                    
            logger.info(f"NOTIFICATION: Processed {len(sent_notifications)} notifications")
            return sent_notifications
            
        except Exception as e:
            logger.error(f"NOTIFICATION: Error sending notifications: {e}")
            return []

# Test function for direct execution
if __name__ == '__main__':
    handler = NotificationHandler()
    
    # Test creating a notification
    test_notification = handler.create_watchlist_notification(
        watchlist_id="123",
        action="extend",
        message="Test notification message",
        subject="Test Notification",
        notification_type="dashboard",
        recipient="maintenance_manager",
        status="pending"
    )
    
    if test_notification:
        print(f"Created notification: {test_notification}")
    else:
        print("Failed to create notification")