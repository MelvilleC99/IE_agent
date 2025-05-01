# /Users/melville/Documents/Industrial_Engineering_Agent/src/agents/maintenance/tracker/scheduled_maintenance/scheduled_maintenance_notification.py

import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from supabase.client import create_client, Client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("maintenance_notification")

# Initialize Supabase client
def get_supabase_client() -> Client:
    """Get Supabase client using environment variables."""
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        logger.error("Supabase URL and key must be set in environment variables")
        raise ValueError("Supabase URL and key must be set in environment variables")
    
    return create_client(supabase_url, supabase_key)

def send_maintenance_schedule_notification(
    schedule_results: Dict[str, Any],
    recipient: str = "maintenance_manager",
    notification_type: str = "dashboard"
) -> Dict[str, Any]:
    """
    Send notification about scheduled maintenance tasks.
    
    Args:
        schedule_results: Results from the maintenance scheduling operation
        recipient: Who should receive the notification
        notification_type: Type of notification (email, sms, dashboard, etc.)
        
    Returns:
        Dict containing notification status and details
    """
    try:
        # Create Supabase client
        supabase = get_supabase_client()
        
        # Create notification message
        tasks_created = schedule_results.get('tasks_created', 0)
        high_priority = schedule_results.get('high_priority_count', 0)
        medium_priority = schedule_results.get('medium_priority_count', 0)
        
        subject = "Scheduled Maintenance Tasks Created"
        
        message = f"""The maintenance scheduling workflow has completed.

- {tasks_created} new task(s) have been created
- {high_priority} high priority machines
- {medium_priority} medium priority machines

Please review the scheduled maintenance plan."""

        now = datetime.now().isoformat()
        
        # Prepare notification record
        notification = {
            "recipient": recipient,
            "notification_type": notification_type,
            "subject": subject,
            "message": message,
            "status": "sent",
            "sent_at": now,
            "created_at": now
        }
        
        # Insert notification into database
        result = supabase.table('notification_logs').insert(notification).execute()
        
        if result and hasattr(result, 'data') and result.data:
            logger.info(f"Notification sent successfully to {recipient}")
            return {
                "status": "success",
                "notification": result.data[0]
            }
        else:
            logger.warning("Notification insert returned no data")
            return {
                "status": "unknown",
                "notification": notification
            }
    
    except Exception as e:
        logger.error(f"Error sending notification: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }