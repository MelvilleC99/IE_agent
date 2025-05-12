import logging
from typing import Dict, List, Any
from .scheduled_maintenance_notification import send_maintenance_schedule_notification

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("maintenance_notifier")

class MaintenanceNotifier:
    """Handles notifications for scheduled maintenance tasks."""
    
    def __init__(self):
        """Initialize the maintenance notifier."""
        logger.info("Initializing MaintenanceNotifier")
    
    def send_notifications(self, machines_to_service: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Send notifications about scheduled maintenance tasks.
        
        Args:
            machines_to_service: List of machines that have been scheduled for maintenance
            
        Returns:
            Dict containing notification status and details
        """
        try:
            # Count machines by priority
            priority_counts = {'high': 0, 'medium': 0, 'low': 0}
            for machine in machines_to_service:
                priority = machine.get('priority', 'low')
                priority_counts[priority] += 1
            
            # Prepare schedule results for notification
            schedule_results = {
                'tasks_created': len(machines_to_service),
                'high_priority_count': priority_counts['high'],
                'medium_priority_count': priority_counts['medium'],
                'low_priority_count': priority_counts['low']
            }
            
            # Send notification
            notification_result = send_maintenance_schedule_notification(
                schedule_results=schedule_results,
                recipient="maintenance_manager",
                notification_type="dashboard"
            )
            
            if notification_result.get('status') == 'success':
                logger.info("Maintenance notifications sent successfully")
            else:
                logger.warning(f"Issue sending notifications: {notification_result.get('error', 'Unknown error')}")
            
            return notification_result
            
        except Exception as e:
            logger.error(f"Error sending maintenance notifications: {e}", exc_info=True)
            return {
                'status': 'error',
                'error': str(e)
            } 