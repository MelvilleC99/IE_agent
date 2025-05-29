# src/agents/maintenance/analytics/Scheduled_Maintenance/maintenance_task_scheduler.py

import logging
import random
from typing import Dict, List, Any, Tuple

logger = logging.getLogger("maintenance_scheduler")

class MaintenanceTaskScheduler:
    """Manages the scheduling and assignment of maintenance tasks."""
    
    def __init__(self, db_client):
        """Initialize with database client."""
        self.db = db_client
        
    def get_mechanics(self) -> List[Dict[str, Any]]:
        """Get all mechanics."""
        try:
            logger.info("Attempting to fetch mechanics from database...")
            result = self.db.query_table(
                table_name='mechanics',
                columns='*',
                limit=100
            )
            mechanics = result if result else []
            logger.info(f"Retrieved {len(mechanics)} mechanics")
            return mechanics
        except Exception as e:
            logger.error(f"Error fetching mechanics: {e}")
            return []
    
    def assign_mechanic(self) -> Tuple[str, str]:
        """
        Assign a mechanic using simple workload balancing.
        Returns a tuple of (employee_number, full_name).
        """
        try:
            mechanics = self.get_mechanics()
            if not mechanics:
                logger.warning("No mechanics found. Using 'unassigned' as fallback.")
                return ("unassigned", "Unassigned")
                
            # Get current workload for each mechanic
            for mechanic in mechanics:
                try:
                    tasks_result = self.db.query_table(
                        table_name='scheduled_maintenance',
                        columns='*',
                        filters={
                            'status': 'open',
                            'assignee': mechanic.get("employee_number")
                        },
                        limit=100
                    )
                    open_tasks = tasks_result if tasks_result else []
                except Exception as e:
                    logger.error(f"Error getting tasks for mechanic {mechanic.get('employee_number')}: {e}")
                    open_tasks = []
                mechanic["current_workload"] = len(open_tasks)
                logger.info(f"Mechanic {mechanic.get('name')} {mechanic.get('surname')} has {len(open_tasks)} open tasks")
            
            # Find mechanics with minimal workload
            min_workload = min(mechanics, key=lambda m: m.get("current_workload", 0)).get("current_workload", 0)
            mechanics_with_min_workload = [m for m in mechanics if m.get("current_workload", 0) == min_workload]
            
            # Randomly select from mechanics with minimal workload
            selected_mechanic = random.choice(mechanics_with_min_workload)
            employee_number = selected_mechanic.get("employee_number", "unassigned")
            name = selected_mechanic.get("name", "")
            surname = selected_mechanic.get("surname", "")
            full_name = f"{name} {surname}".strip()
            logger.info(f"Selected mechanic: {full_name} (#{employee_number})")
            return (employee_number, full_name)
        except Exception as e:
            logger.error(f"Error assigning mechanic: {e}")
            logger.warning("Using 'unassigned' as fallback.")
            return ("unassigned", "Unassigned")
            
    def schedule_maintenance_tasks(self, machines_to_service: List[Dict[str, Any]], max_tasks=None) -> Dict[str, Any]:
        """
        Schedule maintenance tasks for the identified machines.
        
        Args:
            machines_to_service: List of machines that need maintenance (from interpreter)
            max_tasks: Maximum number of tasks to create (optional)
            
        Returns:
            Dict with schedule results
        """
        # Limit by max_tasks if specified
        if max_tasks and max_tasks > 0:
            original_count = len(machines_to_service)
            machines_to_service = machines_to_service[:max_tasks]
            logger.info(f"Limiting to {max_tasks} tasks from {original_count} identified machines")
        
        # Schedule results tracking
        tasks_scheduled = []
        skipped_machines = []
        
        # Track machine types for reporting
        high_priority_count = len([m for m in machines_to_service if m.get('priority') == 'high'])
        medium_priority_count = len([m for m in machines_to_service if m.get('priority') == 'medium'])
        
        return {
            'machines_to_service': machines_to_service,
            'high_priority_count': high_priority_count,
            'medium_priority_count': medium_priority_count,
            'total_problematic_machines': len(machines_to_service)
        }