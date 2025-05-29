# src/agents/maintenance/tools/additional_scheduling_tool.py

import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# Load environment variables
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
env_path = os.path.join(project_root, '.env.local')
load_dotenv(dotenv_path=env_path)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("additional_scheduling_tool")

def additional_scheduling_tool(
    count: int = 10,
    cluster_filter: str = "all",  # "cluster_0", "cluster_1", "all"
    exclude_existing: bool = True,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> str:
    """
    Tool for scheduling additional machines beyond the initial cluster analysis.
    
    Re-runs clustering analysis to get fresh machine states, then schedules
    additional machines while excluding those already scheduled.
    
    Args:
        count: Number of additional machines to schedule (default: 10)
        cluster_filter: Which cluster to target ("cluster_0", "cluster_1", "all")
        exclude_existing: Whether to exclude machines with existing open tasks (default: True)
        start_date: Optional start date for analysis (YYYY-MM-DD), defaults to 6 months ago
        end_date: Optional end date for analysis (YYYY-MM-DD), defaults to today
        
    Returns:
        JSON string with detailed scheduling results
    """
    try:
        logger.info(f"=== ADDITIONAL SCHEDULING REQUESTED ===")
        logger.info(f"Count: {count}, Cluster filter: {cluster_filter}, Exclude existing: {exclude_existing}")
        
        # Step 1: Set up date range with smart defaults
        if start_date and end_date:
            period_start = datetime.strptime(start_date, "%Y-%m-%d")
            period_end = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            # Smart defaults - last 6 months
            period_end = datetime.now()
            period_start = period_end - timedelta(days=180)
        
        period_end = period_end.replace(hour=23, minute=59, second=59, microsecond=999999)
        logger.info(f"Analysis period: {period_start.date()} to {period_end.date()}")
        
        # Step 2: Get machines that are already scheduled (to exclude them)
        currently_scheduled = get_currently_scheduled_machines()
        logger.info(f"Found {len(currently_scheduled)} machines already scheduled: {list(currently_scheduled)}")
        
        # Step 3: Run fresh clustering analysis
        logger.info("Running fresh clustering analysis...")
        analysis_results = run_fresh_clustering_analysis(period_start, period_end)
        
        if not analysis_results or 'error' in analysis_results:
            error_msg = analysis_results.get('error', 'Unknown clustering error') if analysis_results else 'Clustering failed'
            return json.dumps({
                "status": "error",
                "message": f"Clustering analysis failed: {error_msg}",
                "machines_scheduled": 0
            })
        
        # Step 4: Get all machines from clustering results
        aggregated_data = analysis_results.get('aggregated_data', [])
        cluster_summary = analysis_results.get('cluster_summary', [])
        
        if not aggregated_data:
            return json.dumps({
                "status": "error",
                "message": "No machines found in clustering analysis",
                "machines_scheduled": 0
            })
        
        logger.info(f"Clustering found {len(aggregated_data)} total machines in {len(cluster_summary)} clusters")
        
        # Step 5: Filter and prepare candidates
        all_candidates = []
        cluster_0_machines = []
        cluster_1_machines = []
        
        for machine in aggregated_data:
            machine_id = machine.get('machineNumber')
            cluster = machine.get('cluster', 0)
            failure_count = machine.get('failure_count', 0)
            total_downtime = machine.get('total_downtime_minutes', 0)
            machine_type = machine.get('machine_type', 'Unknown')
            manufacturer = machine.get('manufacturer', 'Unknown')
            machine_age = machine.get('machine_age_years', 0)
            
            if not machine_id:
                continue
            
            # Skip if already scheduled and we're excluding existing
            if exclude_existing and machine_id in currently_scheduled:
                logger.debug(f"Skipping machine {machine_id} - already scheduled")
                continue
            
            # Calculate urgency score for ranking
            urgency_score = (failure_count * 3.0) + (total_downtime / 100.0) + (machine_age * 0.5)
            
            # Determine priority based on cluster
            if cluster == 1:
                priority = 'high'  # High-risk cluster
            else:
                priority = 'medium'  # Better performing cluster
            
            # Create machine record
            machine_record = {
                'machine_id': machine_id,  # Use correct key name for task writer
                'machine_type': machine_type,
                'manufacturer': manufacturer,
                'failure_count': failure_count,
                'total_downtime': round(total_downtime, 1),
                'machine_age_years': round(machine_age, 1),
                'cluster': cluster,
                'priority': priority,
                'urgency_score': round(urgency_score, 2),
                'reason': f"Additional scheduling - cluster {cluster} ({'high-risk' if cluster == 1 else 'better-performing'})"
            }
            
            all_candidates.append(machine_record)
            
            if cluster == 1:
                cluster_1_machines.append(machine_record)
            else:
                cluster_0_machines.append(machine_record)
        
        # Step 6: Apply cluster filtering
        if cluster_filter == "cluster_0":
            filtered_candidates = cluster_0_machines
            logger.info(f"Filtered to {len(filtered_candidates)} machines from cluster 0 (better-performing)")
        elif cluster_filter == "cluster_1":
            filtered_candidates = cluster_1_machines
            logger.info(f"Filtered to {len(filtered_candidates)} machines from cluster 1 (high-risk)")
        else:  # "all"
            filtered_candidates = all_candidates
            logger.info(f"Using all {len(filtered_candidates)} eligible machines")
        
        # Step 7: Sort by urgency score (worst first) and cluster priority
        cluster_priority = {1: 2, 0: 1}  # Cluster 1 gets higher priority
        filtered_candidates.sort(
            key=lambda x: (cluster_priority.get(x['cluster'], 0), x['urgency_score']), 
            reverse=True
        )
        
        # Step 8: Take the requested number of machines
        machines_to_schedule = filtered_candidates[:count]
        
        if not machines_to_schedule:
            return json.dumps({
                "status": "success",
                "message": "No additional machines available for scheduling",
                "machines_scheduled": 0,
                "total_candidates": len(filtered_candidates),
                "already_scheduled": len(currently_scheduled),
                "reason": "All eligible machines already scheduled or no machines meet criteria"
            })
        
        logger.info(f"Selected {len(machines_to_schedule)} machines for additional scheduling")
        
        # Step 9: Create maintenance tasks
        logger.info("Creating maintenance tasks...")
        tasks_created = create_maintenance_tasks(machines_to_schedule)
        
        # Step 10: Send notifications if tasks were created
        if tasks_created > 0:
            logger.info("Sending notifications...")
            send_maintenance_notifications(machines_to_schedule)
        
        # Step 11: Prepare comprehensive response
        cluster_breakdown = {
            "cluster_0": len([m for m in machines_to_schedule if m['cluster'] == 0]),
            "cluster_1": len([m for m in machines_to_schedule if m['cluster'] == 1])
        }
        
        priority_breakdown = {
            "high": len([m for m in machines_to_schedule if m['priority'] == 'high']),
            "medium": len([m for m in machines_to_schedule if m['priority'] == 'medium']),
            "low": len([m for m in machines_to_schedule if m['priority'] == 'low'])
        }
        
        response = {
            "status": "success",
            "message": f"Successfully scheduled {tasks_created} additional machines for maintenance",
            "machines_scheduled": tasks_created,
            "analysis_period": {
                "start_date": period_start.strftime("%Y-%m-%d"),
                "end_date": period_end.strftime("%Y-%m-%d")
            },
            "statistics": {
                "total_machines_analyzed": len(aggregated_data),
                "eligible_candidates": len(filtered_candidates),
                "already_scheduled": len(currently_scheduled),
                "requested_count": count,
                "actually_scheduled": tasks_created
            },
            "cluster_breakdown": cluster_breakdown,
            "priority_breakdown": priority_breakdown,
            "machines_details": [
                {
                    "machine_id": m['machine_id'],
                    "machine_type": m['machine_type'],
                    "cluster": m['cluster'],
                    "priority": m['priority'],
                    "failure_count": m['failure_count'],
                    "total_downtime": m['total_downtime'],
                    "urgency_score": m['urgency_score']
                }
                for m in machines_to_schedule
            ]
        }
        
        logger.info(f"=== ADDITIONAL SCHEDULING COMPLETE ===")
        logger.info(f"Tasks created: {tasks_created} out of {count} requested")
        return json.dumps(response, indent=2)
        
    except Exception as e:
        error_msg = f"Error in additional scheduling: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return json.dumps({
            "status": "error",
            "message": error_msg,
            "machines_scheduled": 0
        })


def run_fresh_clustering_analysis(period_start: datetime, period_end: datetime) -> Dict[str, Any]:
    """
    Run a fresh clustering analysis for the specified time period.
    
    Returns:
        Dictionary containing clustering analysis results
    """
    try:
        logger.info("Fetching maintenance data for clustering...")
        
        # Import required modules
        from shared_services.supabase_client import SupabaseClient
        from agents.maintenance.workflows.scheduled_maintenance_workflow import transform_records_for_clustering
        from agents.maintenance.analytics.Scheduled_Maintenance.MachineCluster import run_analysis
        
        # Initialize database client
        db = SupabaseClient()
        
        # Query database with date filters
        filters = {
            'resolved_at.gte': period_start.isoformat(),
            'resolved_at.lte': period_end.isoformat()
        }
        
        records = db.query_table(
            table_name="downtime_detail",
            columns="*",
            filters=filters,
            limit=1000
        )
        
        if not records:
            logger.warning("No maintenance records found for the specified period")
            return {"error": "No maintenance data found for clustering analysis"}
        
        logger.info(f"Retrieved {len(records)} maintenance records")
        
        # Transform records for clustering
        transformed_records = transform_records_for_clustering(records)
        if not transformed_records:
            return {"error": "No valid records after transformation"}
        
        logger.info(f"Transformed {len(transformed_records)} records for clustering")
        
        # Run clustering analysis
        analysis_results = run_analysis(transformed_records)
        
        if not analysis_results:
            return {"error": "Clustering analysis returned no results"}
        
        if 'error' in analysis_results:
            return analysis_results
        
        logger.info("Clustering analysis completed successfully")
        return analysis_results
        
    except Exception as e:
        logger.error(f"Error in fresh clustering analysis: {e}", exc_info=True)
        return {"error": f"Clustering analysis failed: {str(e)}"}


def get_currently_scheduled_machines() -> set:
    """
    Get set of machine IDs that currently have open maintenance tasks.
    
    Returns:
        Set of machine IDs with open maintenance tasks
    """
    try:
        from shared_services.supabase_client import SupabaseClient
        
        db = SupabaseClient()
        result = db.query_table(
            table_name='scheduled_maintenance',
            columns='machine_id',
            filters={'status': 'open'},
            limit=1000
        )
        
        machine_ids = {str(task['machine_id']) for task in result if task.get('machine_id')}
        logger.debug(f"Currently scheduled machines: {machine_ids}")
        return machine_ids
        
    except Exception as e:
        logger.error(f"Error getting currently scheduled machines: {e}")
        return set()


def create_maintenance_tasks(machines: List[Dict]) -> int:
    """
    Create maintenance tasks for the given machines.
    
    Args:
        machines: List of machine dictionaries to schedule
        
    Returns:
        Number of tasks successfully created
    """
    try:
        logger.info(f"Creating maintenance tasks for {len(machines)} machines...")
        
        # Import task creation infrastructure
        from shared_services.supabase_client import SupabaseClient
        from agents.maintenance.analytics.Scheduled_Maintenance.maintenance_task_scheduler import MaintenanceTaskScheduler
        from agents.maintenance.analytics.Scheduled_Maintenance.maintenance_task_writer import MaintenanceTaskWriter
        
        # Initialize components
        db = SupabaseClient()
        scheduler = MaintenanceTaskScheduler(db)
        writer = MaintenanceTaskWriter(db)
        
        # Prepare schedule results in the format expected by the writer
        schedule_results = {
            'machines_to_service': machines,
            'high_priority_count': len([m for m in machines if m['priority'] == 'high']),
            'medium_priority_count': len([m for m in machines if m['priority'] == 'medium']),
            'low_priority_count': len([m for m in machines if m['priority'] == 'low']),
            'total_problematic_machines': len(machines)
        }
        
        # Write maintenance tasks
        write_results = writer.write_maintenance_tasks(schedule_results, scheduler)
        tasks_created = write_results.get('tasks_created', 0)
        
        logger.info(f"Successfully created {tasks_created} maintenance tasks")
        
        # Log any issues
        duplicates = write_results.get('duplicates', [])
        failed = write_results.get('failed', [])
        
        if duplicates:
            logger.info(f"Prevented {len(duplicates)} duplicate tasks")
        if failed:
            logger.warning(f"Failed to create {len(failed)} tasks")
        
        return tasks_created
        
    except Exception as e:
        logger.error(f"Error creating maintenance tasks: {e}", exc_info=True)
        return 0


def send_maintenance_notifications(machines: List[Dict]) -> bool:
    """
    Send notifications about the newly scheduled maintenance.
    
    Args:
        machines: List of machines that were scheduled
        
    Returns:
        True if notifications sent successfully
    """
    try:
        from agents.maintenance.analytics.Scheduled_Maintenance.maintenance_notifier import MaintenanceNotifier
        
        notifier = MaintenanceNotifier()
        notification_result = notifier.send_notifications(machines)
        
        if notification_result.get('status') == 'success':
            logger.info("Maintenance notifications sent successfully")
            return True
        else:
            logger.warning(f"Issue sending notifications: {notification_result}")
            return False
            
    except Exception as e:
        logger.error(f"Error sending notifications: {e}")
        return False


# Convenience functions for common usage patterns

def schedule_next_worst_machines(count: int = 20) -> str:
    """Schedule the next X worst performing machines regardless of cluster."""
    return additional_scheduling_tool(
        count=count,
        cluster_filter="all",
        exclude_existing=True
    )

def schedule_cluster_0_machines(count: int = 15) -> str:
    """Schedule machines from the better-performing cluster for preventive maintenance."""
    return additional_scheduling_tool(
        count=count,
        cluster_filter="cluster_0",
        exclude_existing=True
    )

def schedule_remaining_cluster_1_machines(count: int = 10) -> str:
    """Schedule any remaining high-risk cluster machines that weren't caught initially."""
    return additional_scheduling_tool(
        count=count,
        cluster_filter="cluster_1",
        exclude_existing=True
    )

def emergency_schedule_all_high_risk(count: int = 50) -> str:
    """Emergency scheduling - schedule high-risk machines even if they have existing tasks."""
    return additional_scheduling_tool(
        count=count,
        cluster_filter="cluster_1",
        exclude_existing=False
    )


# Usage examples for the agent system:
"""
User: "Schedule 15 more machines for maintenance"
Agent calls: additional_scheduling_tool(count=15)

User: "Schedule the next 10 worst performing machines" 
Agent calls: schedule_next_worst_machines(count=10)

User: "Schedule 20 machines from the better cluster for preventive maintenance"
Agent calls: schedule_cluster_0_machines(count=20)

User: "Are there any high-risk machines we missed? Schedule 5 more."
Agent calls: schedule_remaining_cluster_1_machines(count=5)

User: "Emergency! Schedule all high-risk machines regardless of existing tasks"
Agent calls: emergency_schedule_all_high_risk(count=100)
"""

if __name__ == "__main__":
    # Test the tool
    result = additional_scheduling_tool(count=5)
    print(result)