# src/agents/maintenance/analytics/Scheduled_Maintenance/maintenance_task_writer.py

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import uuid

logger = logging.getLogger("maintenance_writer")

class MaintenanceTaskWriter:
    """Writes maintenance tasks to the database with duplicate prevention."""
    
    def __init__(self, db_client):
        """Initialize with database client."""
        self.db = db_client
        
    def ensure_tables_exist(self) -> bool:
        """Check if the required tables exist."""
        try:
            result1 = self.db.query_table('scheduled_maintenance', columns='count', limit=1)
            logger.info(f"scheduled_maintenance table exists, got result: {result1}")
            result2 = self.db.query_table('mechanics', columns='count', limit=1)
            logger.info(f"mechanics table exists, got result: {result2}")
            return True
        except Exception as e:
            logger.error(f"Error checking tables: {e}")
            logger.error("Tables may not exist or there's an issue with permissions.")
            return False
            
    def check_existing_tasks(self, machine_id: str, include_completed: bool = False) -> Dict[str, Any]:
        """
        Enhanced check for existing maintenance tasks.
        
        Returns:
            Dict with 'has_open_tasks', 'open_count', 'last_completed', etc.
        """
        try:
            # Check for open tasks
            open_result = self.db.query_table(
                'scheduled_maintenance',
                columns='*',
                filters={'machine_id': machine_id, 'status': 'open'},
                limit=100
            )
            open_tasks = open_result if open_result else []
            
            result = {
                'has_open_tasks': len(open_tasks) > 0,
                'open_count': len(open_tasks),
                'open_tasks': open_tasks
            }
            
            # Optionally check completed tasks for scheduling intelligence
            if include_completed:
                completed_result = self.db.query_table(
                    'scheduled_maintenance',
                    columns='*',
                    filters={'machine_id': machine_id, 'status': 'completed'},
                    limit=1
                )
                if completed_result:
                    last_completed = completed_result[0]
                    result['last_completed'] = last_completed
                    result['days_since_last_maintenance'] = self._days_since(last_completed.get('completed_at'))
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking existing tasks for machine {machine_id}: {e}")
            return {'has_open_tasks': False, 'open_count': 0}
    
    def _days_since(self, date_str: str) -> int:
        """Calculate days since a date string."""
        try:
            if date_str:
                date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return (datetime.now() - date_obj).days
        except:
            pass
        return 999  # Large number if can't parse
            
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
            "id": str(uuid.uuid4()),
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
            result = self.db.insert_data('scheduled_maintenance', task)
            logger.info("Task inserted successfully")
            return result if result else task
        except Exception as e:
            logger.error(f"Error inserting task: {e}")
            logger.error(f"Could not insert task for machine {machine_id}")
            return None

    def write_maintenance_tasks(self, schedule_results, scheduler) -> Dict[str, Any]:
        """
        Enhanced task writing with duplicate prevention and detailed logging.
        """
        machines = schedule_results.get('machines_to_service', [])
        logger.info(f"TASK WRITER: Processing {len(machines)} machines for maintenance scheduling")
        
        # Detailed logging of what we received
        for i, machine in enumerate(machines[:3]):
            logger.info(f"TASK WRITER: Machine {i+1} sample: {machine}")
        
        if not self.ensure_tables_exist():
            return {
                "error": "Required tables don't exist in the database",
                "tasks_created": 0
            }
        
        tasks_created = []
        skipped_machines = []
        duplicate_machines = []
        failed_machines = []
        
        for machine in machines:
            # Only use 'machine_id' (do not fallback to 'machineNumber')
            machine_id = machine.get('machine_id')
            machine_type = machine.get('machine_type', 'Unknown')
            priority = machine.get('priority', 'medium')
            
            if not machine_id:
                logger.error(f"Missing machine_id in machine dict: {machine}")
                failed_machines.append({'machine': machine, 'reason': 'No machine_id'})
                continue
            
            logger.info(f"TASK WRITER: Processing machine {machine_id} ({machine_type})")
            
            # Enhanced duplicate checking
            existing_check = self.check_existing_tasks(machine_id, include_completed=True)
            
            if existing_check['has_open_tasks']:
                logger.info(f"DUPLICATE PREVENTION: Machine {machine_id} has {existing_check['open_count']} open tasks. Skipping.")
                duplicate_machines.append({
                    'machine_id': machine_id,
                    'open_tasks': existing_check['open_count'],
                    'reason': 'Already has open maintenance tasks'
                })
                continue
            
            # Check if recently completed maintenance (optional intelligence)
            if existing_check.get('days_since_last_maintenance', 999) < 7:
                logger.info(f"SCHEDULING INTELLIGENCE: Machine {machine_id} had maintenance {existing_check['days_since_last_maintenance']} days ago. Proceeding anyway due to cluster analysis.")
            
            # Assign mechanic
            assignee, assignee_name = scheduler.assign_mechanic()
            
            # Create maintenance task
            task = self.write_maintenance_task(
                machine_id=machine_id,
                machine_type=machine_type,
                issue_type="preventative_maintenance",
                description=self._create_task_description(machine),
                assignee=assignee,
                assignee_name=assignee_name,
                priority=priority,
                due_days=7 if priority == "high" else 14
            )
            
            if task:
                logger.info(f"SUCCESS: Created {priority} priority task for machine {machine_id}")
                tasks_created.append(task)
            else:
                logger.error(f"FAILED: Could not create task for machine {machine_id}")
                failed_machines.append({'machine_id': machine_id, 'reason': 'Task creation failed'})
        
        # Comprehensive result summary
        result = {
            "created": tasks_created,
            "skipped": skipped_machines,
            "duplicates": duplicate_machines,
            "failed": failed_machines,
            "tasks_created": len(tasks_created),
            "high_priority_count": schedule_results.get('high_priority_count', 0),
            "medium_priority_count": schedule_results.get('medium_priority_count', 0),
            "total_processed": len(machines),
            "success_rate": len(tasks_created) / len(machines) if machines else 0
        }
        
        # Summary logging
        logger.info(f"TASK WRITING COMPLETE:")
        logger.info(f"  - Tasks created: {len(tasks_created)}")
        logger.info(f"  - Duplicates prevented: {len(duplicate_machines)}")
        logger.info(f"  - Failed: {len(failed_machines)}")
        logger.info(f"  - Success rate: {result['success_rate']:.1%}")
        
        if duplicate_machines:
            logger.info(f"  - Machines with existing tasks: {[m['machine_id'] for m in duplicate_machines]}")
        
        return result
    
    def _create_task_description(self, machine: Dict) -> str:
        """Create detailed task description from machine data."""
        machine_id = machine.get('machine_id') or machine.get('machineNumber')
        machine_type = machine.get('machine_type', 'Unknown')
        failure_count = machine.get('failure_count', 0)
        cluster = machine.get('cluster', 0)
        reason = machine.get('reason', 'Cluster analysis identified maintenance need')
        
        return (f"Preventative maintenance for {machine_type} #{machine_id} - "
                f"ML clustering analysis identified as high-risk (cluster {cluster}). "
                f"Recent failures: {failure_count}. {reason}")
    
    def get_tasks(
        self,
        status: Optional[str] = None,
        assignee: Optional[str] = None,
        machine_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get tasks with optional filtering."""
        try:
            query = self.db.query_table('scheduled_maintenance', columns='*')
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