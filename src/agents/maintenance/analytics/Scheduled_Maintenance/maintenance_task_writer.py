# src/agents/maintenance/analytics/Scheduled_Maintenance/maintenance_task_writer.py

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger("maintenance_writer")

class MaintenanceTaskWriter:
    """Writes maintenance tasks to the database."""
    
    def __init__(self, db_client):
        """Initialize with database client."""
        self.db = db_client
        
    def ensure_tables_exist(self) -> bool:
        """Check if the required tables exist."""
        try:
            result1 = self.db.table('scheduled_maintenance').select('count').limit(1).execute()
            logger.info(f"scheduled_maintenance table exists, got result: {result1}")
            result2 = self.db.table('mechanics').select('count').limit(1).execute()
            logger.info(f"mechanics table exists, got result: {result2}")
            return True
        except Exception as e:
            logger.error(f"Error checking tables: {e}")
            logger.error("Tables may not exist or there's an issue with permissions.")
            return False
            
    def check_existing_tasks(self, machine_id: str) -> bool:
        """Check if machine already has open maintenance tasks."""
        try:
            result = self.db.table('scheduled_maintenance') \
                .select('*') \
                .eq('status', 'open') \
                .eq('machine_id', machine_id) \
                .execute()
            tasks = result.data if result and hasattr(result, 'data') else []
            return len(tasks) > 0
        except Exception as e:
            logger.error(f"Error checking existing tasks: {e}")
            return False
            
    def write_maintenance_task(
        self,
        machine_id: str,
        machine_type: str,
        issue_type: str,
        description: str,
        assignee: str,
        assignee_name: str,
        priority: str = "medium",
        due_days: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Write a maintenance task to the database.
        
        Args:
            machine_id: ID of the machine needing maintenance
            machine_type: Type of the machine
            issue_type: Type of issue (e.g., preventative_maintenance)
            description: Description of the maintenance task
            assignee: Employee ID of assigned mechanic
            assignee_name: Full name of assigned mechanic
            priority: Task priority (high, medium, low)
            due_days: Days until task is due (default based on priority)
            
        Returns:
            The created task record or None if error
        """
        # Set due date based on priority
        if due_days is None:
            due_days = 7 if priority == "high" else 14
        
        now = datetime.now()
        due_date = now + timedelta(days=due_days)
        
        # Build task record with extra fields
        task = {
            "machine_id": machine_id,
            "machine_type": machine_type,
            "issue_type": issue_type,
            "description": description,
            "assignee": assignee,
            "mechanic_name": assignee_name,
            "priority": priority,
            "status": "open",
            "due_by": due_date.isoformat(),
            "created_at": now.isoformat(),
        }
        
        logger.info(f"Creating task for machine {machine_id} (type: {machine_type}) assigned to {assignee_name} ({assignee})...")
        try:
            result = self.db.table('scheduled_maintenance').insert(task).execute()
            logger.info("Task inserted successfully")
            return result.data[0] if result and hasattr(result, 'data') and result.data else task
        except Exception as e:
            logger.error(f"Error inserting task: {e}")
            logger.error(f"Could not insert task for machine {machine_id}")
            return None
            
    def write_maintenance_tasks(self, schedule_results, scheduler) -> Dict[str, Any]:
        """
        Write all scheduled maintenance tasks to the database.
        
        Args:
            schedule_results: Output from the scheduler
            scheduler: MaintenanceTaskScheduler instance for mechanic assignment
            
        Returns:
            Dict with write results
        """
        machines = schedule_results.get('machines_to_service', [])
        
        # Check if tables exist
        if not self.ensure_tables_exist():
            return {
                "error": "Required tables don't exist in the database",
                "tasks_created": 0
            }
            
        tasks_created = []
        skipped_machines = []
        
        for machine in machines:
            machine_id = machine["machineNumber"]
            machine_type = machine.get("machine_type", "Unknown")
            
            # Check if machine already has open tasks
            if self.check_existing_tasks(machine_id):
                logger.info(f"Machine {machine_id} already has an open task. Skipping.")
                skipped_machines.append(machine_id)
                continue
                
            # Assign mechanic using workload balancing algorithm
            assignee, assignee_name = scheduler.assign_mechanic()
            
            # Create the maintenance task
            task = self.write_maintenance_task(
                machine_id=machine_id,
                machine_type=machine_type,
                issue_type="preventative_maintenance",
                description=(
                    f"Schedule maintenance for {machine_type} (#{machine_id}) - "
                    f"Identified in high failure cluster with {machine['failure_count']} failures"
                ),
                assignee=assignee,
                assignee_name=assignee_name,
                priority=machine["priority"],
                due_days=7 if machine["priority"] == "high" else 14
            )
            
            if task:
                tasks_created.append(task)
                logger.info(f"Created {machine['priority']} priority task for machine {machine_id}")
        
        # Return summary of the writing operation
        result = {
            "created": tasks_created,
            "skipped": skipped_machines,
            "tasks_created": len(tasks_created),
            "high_priority_count": schedule_results.get('high_priority_count', 0),
            "medium_priority_count": schedule_results.get('medium_priority_count', 0),
            "total_problematic_machines": schedule_results.get('total_problematic_machines', 0)
        }
        
        logger.info(f"Task writing complete. Created {len(tasks_created)} tasks, skipped {len(skipped_machines)} machines.")
        return result
        
    def get_tasks(
        self,
        status: Optional[str] = None,
        assignee: Optional[str] = None,
        machine_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get tasks with optional filtering."""
        try:
            query = self.db.table('scheduled_maintenance').select('*')
            filters = []
            
            if status:
                query = query.eq('status', status)
                filters.append(f"status={status}")
                
            if assignee:
                query = query.eq('assignee', assignee)
                filters.append(f"assignee={assignee}")
                
            if machine_id:
                query = query.eq('machine_id', machine_id)
                filters.append(f"machine_id={machine_id}")
                
            filter_str = " AND ".join(filters) if filters else "no filters"
            logger.info(f"Getting tasks with {filter_str}")
            
            result = query.execute()
            tasks = result.data if result and hasattr(result, 'data') else []
            logger.info(f"Retrieved {len(tasks)} tasks")
            return tasks
        except Exception as e:
            logger.error(f"Error in get_tasks: {e}")
            return []
            
    def get_service_schedule(self, status="open") -> List[Dict[str, Any]]:
        """Get the current service schedule."""
        try:
            tasks = self.get_tasks(status=status)
            logger.info(f"Retrieved {len(tasks)} {status} tasks")
            
            # Sort by priority and due date
            priority_order = {"high": 0, "medium": 1, "low": 2}
            sorted_tasks = tasks.copy() if tasks else []
            
            if sorted_tasks:
                sorted_tasks.sort(key=lambda t: (
                    priority_order.get(t.get("priority", "medium"), 999),
                    t.get("due_by", "")
                ))
                logger.info("Tasks sorted by priority and due date")
                
            return sorted_tasks
        except Exception as e:
            logger.error(f"Error getting service schedule: {e}")
            return []