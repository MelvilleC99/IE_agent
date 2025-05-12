#!/usr/bin/env python3
import sys
import os
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import logging

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
    Notification Handler - Manages notifications for watchlist updates and evaluations.
    
    This class is responsible for:
    1. Creating notifications for watchlist events
    2. Sending notifications to appropriate recipients
    3. Logging notification history
    """
    
    def __init__(self):
        """Initialize the notification handler."""
        self.supabase = get_connection()
        self.logger = logging.getLogger(__name__)
    
    def get_watchlist_details(self, watchlist_id):
        """
        Get details for a watchlist item.
        
        Args:
            watchlist_id: ID of the watchlist item to retrieve
            
        Returns:
            dict: Watchlist item details or None if not found
        """
        try:
            result = self.supabase.table('watch_list').select('*').eq('id', watchlist_id).execute()
            if result.data:
                return result.data[0]
            self.logger.warning(f"No watchlist item found with ID {watchlist_id}")
            return None
        except Exception as e:
            self.logger.error(f"Error getting watchlist details: {str(e)}")
            return None
    
    def get_evaluation_details(self, evaluation_id):
        """
        Get details for an evaluation.
        
        Args:
            evaluation_id: ID of the evaluation to retrieve
            
        Returns:
            dict: Evaluation details or None if not found
        """
        try:
            result = self.supabase.table('watchlist_evaluations').select('*').eq('id', evaluation_id).execute()
            if result.data:
                return result.data[0]
            self.logger.warning(f"No evaluation found with ID {evaluation_id}")
            return None
        except Exception as e:
            self.logger.error(f"Error getting evaluation details: {str(e)}")
            return None
    
    def create_notification(self, watchlist_id=None, evaluation_id=None, recipient="maintenance_manager",
                          action="update", message=None):
        """
        Create a notification for a watchlist event.
        
        Args:
            watchlist_id: Optional ID of the related watchlist item
            evaluation_id: Optional ID of the related evaluation
            recipient: Recipient of the notification
            action: Type of notification action
            message: Optional custom message
            
        Returns:
            dict: Created notification record
        """
        try:
            # Get watchlist details if available
            watchlist = None
            if watchlist_id:
                watchlist = self.get_watchlist_details(watchlist_id)
            
            # Get evaluation details if available
            evaluation = None
            if evaluation_id:
                evaluation = self.get_evaluation_details(evaluation_id)
            
            # Create notification record
            notification = {
                'watchlist_id': watchlist_id,
                'evaluation_id': evaluation_id,
                'recipient': recipient,
                'action': action,
                'message': message or self._generate_message(watchlist, evaluation, action),
                'created_at': datetime.now().isoformat(),
                'status': 'pending'
            }
            
            # Save notification
            result = self.supabase.table('notifications').insert(notification).execute()
            if result.data:
                self.logger.info(f"Created notification for watchlist {watchlist_id}")
                return result.data[0]
            else:
                self.logger.error(f"Failed to create notification for watchlist {watchlist_id}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating notification: {str(e)}")
            return None
    
    def _generate_message(self, watchlist, evaluation, action):
        """
        Generate a notification message based on the action.
        
        Args:
            watchlist: Watchlist item details
            evaluation: Evaluation details
            action: Type of notification action
            
        Returns:
            str: Generated message
        """
        if not watchlist:
            return "Notification for unknown watchlist item"
            
        watchlist_title = watchlist.get('title', 'Untitled Watchlist')
        mechanic_name = watchlist.get('mechanic_name', 'Unknown Mechanic')
        
        if action == 'completed':
            subject = f"Watchlist Completed: {watchlist_title}"
            message = f"""
            The performance monitoring watchlist "{watchlist_title}" for {mechanic_name} has been completed successfully.
            
            Performance Summary:
            - Trend: {evaluation.get('trend', 'Unknown')}
            - Stability: {evaluation.get('stability', 'Unknown')}
            - Performance: {evaluation.get('performance', 'Unknown')}
            
            Decision: Close watchlist
            Notes: {evaluation.get('notes', 'No additional notes')}
            """
            
        elif action == 'extended':
            subject = f"Watchlist Extended: {watchlist_title}"
            message = f"""
            The performance monitoring watchlist "{watchlist_title}" for {mechanic_name} has been extended for further monitoring.
            
            Performance Summary:
            - Trend: {evaluation.get('trend', 'Unknown')}
            - Stability: {evaluation.get('stability', 'Unknown')}
            - Performance: {evaluation.get('performance', 'Unknown')}
            
            Decision: Extend monitoring
            Notes: {evaluation.get('notes', 'No additional notes')}
            """
            
        elif action == 'review':
            subject = f"Watchlist Needs Review: {watchlist_title}"
            message = f"""
            The performance monitoring watchlist "{watchlist_title}" for {mechanic_name} requires review.
            
            Performance Summary:
            - Trend: {evaluation.get('trend', 'Unknown')}
            - Stability: {evaluation.get('stability', 'Unknown')}
            - Performance: {evaluation.get('performance', 'Unknown')}
            
            Decision: Needs review
            Notes: {evaluation.get('notes', 'No additional notes')}
            """
            
        elif action == 'intervene':
            subject = f"URGENT: Intervention Needed for {watchlist_title}"
            message = f"""
            URGENT: The performance monitoring watchlist "{watchlist_title}" for {mechanic_name} requires immediate intervention.
            
            Performance Summary:
            - Trend: {evaluation.get('trend', 'Unknown')}
            - Stability: {evaluation.get('stability', 'Unknown')}
            - Performance: {evaluation.get('performance', 'Unknown')}
            
            Decision: Immediate intervention required
            Notes: {evaluation.get('notes', 'No additional notes')}
            """
            
        else:  # update
            subject = f"Watchlist Update: {watchlist_title}"
            message = f"""
            This is an update regarding the performance monitoring watchlist "{watchlist_title}" for {mechanic_name}.
            
            Performance Summary:
            - Trend: {evaluation.get('trend', 'Unknown')}
            - Stability: {evaluation.get('stability', 'Unknown')}
            - Performance: {evaluation.get('performance', 'Unknown')}
            
            Decision: {evaluation.get('decision', 'Unknown')}
            Notes: {evaluation.get('notes', 'No additional notes')}
            """
        
        return f"{subject}\n\n{message}"
    
    def log_notification(self, watchlist_id, evaluation_id, recipients, message, status):
        """
        Log a notification in the database.
        
        Args:
            watchlist_id: ID of the watchlist item
            evaluation_id: ID of the evaluation
            recipients: List of notification recipients
            message: Notification message
            status: Notification status
            
        Returns:
            dict: Logged notification record
        """
        try:
            notification = {
                'watchlist_id': watchlist_id,
                'evaluation_id': evaluation_id,
                'recipients': recipients,
                'message': message,
                'status': status,
                'created_at': datetime.now().isoformat()
            }
            
            result = self.supabase.table('notification_log').insert(notification).execute()
            if result.data:
                self.logger.info(f"Logged notification for watchlist {watchlist_id}")
                return result.data[0]
            else:
                self.logger.error(f"Failed to log notification for watchlist {watchlist_id}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error logging notification: {str(e)}")
            return None
    
    def process_evaluation_notifications(self):
        """
        Process notifications for new evaluations.
        
        Returns:
            list: List of processed notifications
        """
        try:
            # Find new evaluations without notifications
            query = """
            SELECT e.id, e.watchlist_id
            FROM watchlist_evaluations e
            LEFT JOIN notifications n ON e.id = n.evaluation_id
            WHERE n.id IS NULL
            """
            
            result = self.supabase.rpc('find_unevaluated_notifications').execute()
            if not result.data:
                self.logger.info("No new evaluations to notify about")
                return []
            
            notifications = []
            for evaluation in result.data:
                watchlist_id = evaluation['watchlist_id']
                evaluation_id = evaluation['id']
                
                # Get watchlist details
                watchlist = self.get_watchlist_details(watchlist_id)
                if not watchlist:
                    self.logger.warning(f"Watchlist {watchlist_id} not found")
                    continue
                
                # Get evaluation details
                evaluation = self.get_evaluation_details(evaluation_id)
                if not evaluation:
                    self.logger.warning(f"Evaluation {evaluation_id} not found")
                    continue
                
                # Determine recipients
                recipients = self._get_recipients(watchlist, evaluation)
                
                # Create notification
                notification = self.create_notification(
                    watchlist_id=watchlist_id,
                    evaluation_id=evaluation_id,
                    recipient=recipients[0],  # Primary recipient
                    action=evaluation.get('decision', 'update'),
                    message=None  # Will be generated
                )
                
                if notification:
                    notifications.append(notification)
            
            return notifications
            
        except Exception as e:
            self.logger.error(f"Error processing evaluation notifications: {str(e)}")
            return []
    
    def _get_recipients(self, watchlist, evaluation):
        """
        Determine notification recipients based on watchlist and evaluation.
        
        Args:
            watchlist: Watchlist item details
            evaluation: Evaluation details
            
        Returns:
            list: List of recipient email addresses
        """
        recipients = []
        
        # Always include maintenance manager
        recipients.append("maintenance_manager")
        
        # Add mechanic if available
        mechanic_email = watchlist.get('mechanic_email')
        if mechanic_email:
            recipients.append(mechanic_email)
        
        # Add supervisor if available
        supervisor_email = watchlist.get('supervisor_email')
        if supervisor_email:
            recipients.append(supervisor_email)
        
        return recipients


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