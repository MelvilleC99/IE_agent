import logging
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("machine_cluster_interpreter")

def interpret_results(
    analysis_results: Dict[str, Any], 
    max_machines: Optional[int] = None,
    exclude_scheduled: bool = True
) -> List[Dict[str, Any]]:
    """
    PURE CLUSTER-BASED interpretation - only uses ML clustering results.
    
    Args:
        analysis_results: Dictionary containing clustering analysis results from MachineCluster
        max_machines: Maximum number of machines to schedule (None = all cluster 1)
        exclude_scheduled: Whether to exclude machines that already have open tasks
        
    Returns:
        List of machines identified for maintenance based purely on cluster analysis
    """
    try:
        machines_to_service = []
        
        # Check for error in analysis results
        if 'error' in analysis_results:
            logger.error(f"Analysis error: {analysis_results['error']}")
            return machines_to_service
        
        # FIXED: Get 'aggregated_data' instead of 'clusters'
        aggregated_data = analysis_results.get('aggregated_data', [])
        cluster_summary = analysis_results.get('cluster_summary', [])
        
        if not aggregated_data:
            logger.warning("No aggregated_data found in analysis results")
            logger.debug(f"Available keys in analysis_results: {list(analysis_results.keys())}")
            return machines_to_service
        
        logger.info(f"Processing {len(aggregated_data)} machines from clustering analysis")
        logger.info(f"Found {len(cluster_summary)} clusters")
        
        # Get machines that already have scheduled maintenance (if excluding)
        scheduled_machines = set()
        if exclude_scheduled:
            scheduled_machines = get_machines_with_open_tasks()
            logger.info(f"Excluding {len(scheduled_machines)} machines with existing tasks")
        
        # PURE CLUSTER-BASED LOGIC
        cluster_1_machines = []  # High-risk cluster
        cluster_0_candidates = []  # Better cluster (for progressive scheduling)
        
        for machine in aggregated_data:
            # Always map machine_id from machineNumber or machine_number
            machine_number = machine.get('machineNumber') or machine.get('machine_number')
            cluster = machine.get('cluster', 0)
            failure_count = machine.get('failure_count', 0)
            total_downtime = machine.get('total_downtime_minutes', 0)
            machine_type = machine.get('machine_type', 'Unknown')
            manufacturer = machine.get('manufacturer', 'Unknown')
            machine_age = machine.get('machine_age_years', 0)
            
            if not machine_number:
                continue
            
            # Skip if already scheduled
            if machine_number in scheduled_machines:
                logger.debug(f"Skipping machine {machine_number} - already has open tasks")
                continue
            
            # Calculate maintenance urgency score for ranking
            urgency_score = calculate_maintenance_score(machine)
            
            # PURE CLUSTER CLASSIFICATION
            if cluster == 1:
                # ALL CLUSTER 1 MACHINES = HIGH PRIORITY (worse performing cluster)
                machine_data = {
                    'machine_id': machine_number,  # Always use machine_id for downstream
                    'machine_type': machine_type,
                    'manufacturer': manufacturer,
                    'failure_count': failure_count,
                    'total_downtime': round(total_downtime, 1),
                    'machine_age_years': round(machine_age, 1),
                    'cluster': cluster,
                    'priority': 'high',  # All cluster 1 = high priority
                    'urgency_score': round(urgency_score, 2),
                    'reason': 'High-risk cluster identified by ML clustering analysis'
                }
                cluster_1_machines.append(machine_data)
                logger.debug(f"HIGH-RISK: Machine {machine_number} in cluster 1 - automatic high priority")
                
            else:
                # CLUSTER 0 MACHINES = POTENTIAL CANDIDATES (better performing cluster)
                machine_data = {
                    'machine_id': machine_number,  # Always use machine_id for downstream
                    'machine_type': machine_type,
                    'manufacturer': manufacturer,
                    'failure_count': failure_count,
                    'total_downtime': round(total_downtime, 1),
                    'machine_age_years': round(machine_age, 1),
                    'cluster': cluster,
                    'priority': 'medium',  # Cluster 0 = medium priority
                    'urgency_score': round(urgency_score, 2),
                    'reason': 'Better-performing cluster - candidate for progressive scheduling'
                }
                cluster_0_candidates.append(machine_data)
                logger.debug(f"CANDIDATE: Machine {machine_number} in cluster 0 - candidate for additional scheduling")
        
        # Sort both clusters by urgency score (worst first)
        cluster_1_machines.sort(key=lambda x: x['urgency_score'], reverse=True)
        cluster_0_candidates.sort(key=lambda x: x['urgency_score'], reverse=True)
        
        logger.info(f"Cluster 1 (high-risk): {len(cluster_1_machines)} machines")
        logger.info(f"Cluster 0 (candidates): {len(cluster_0_candidates)} machines")
        
        # SCHEDULING LOGIC BASED ON max_machines PARAMETER
        if max_machines is None:
            # DEFAULT: Schedule ALL cluster 1 machines only
            machines_to_service = cluster_1_machines
            logger.info(f"PURE CLUSTER MODE: Scheduling all {len(machines_to_service)} high-risk cluster machines")
            
        else:
            # PROGRESSIVE SCHEDULING: Fill quota with worst machines across clusters
            machines_to_service = []
            
            # First priority: Take worst machines from cluster 1
            machines_to_service.extend(cluster_1_machines[:max_machines])
            remaining_slots = max_machines - len(machines_to_service)
            
            # If we need more machines, take worst from cluster 0
            if remaining_slots > 0 and cluster_0_candidates:
                machines_to_service.extend(cluster_0_candidates[:remaining_slots])
                logger.info(f"PROGRESSIVE MODE: {len(cluster_1_machines)} from high-risk + {remaining_slots} from better cluster")
            else:
                logger.info(f"PROGRESSIVE MODE: {len(machines_to_service)} machines from high-risk cluster only")
        
        # Final sorting by cluster priority, then urgency score
        cluster_priority = {1: 2, 0: 1}  # Cluster 1 gets higher priority
        machines_to_service.sort(
            key=lambda x: (cluster_priority.get(x['cluster'], 0), x['urgency_score']), 
            reverse=True
        )
        
        # COMPREHENSIVE LOGGING
        high_count = len([m for m in machines_to_service if m['priority'] == 'high'])
        medium_count = len([m for m in machines_to_service if m['priority'] == 'medium'])
        
        logger.info(f"CLUSTER-BASED SCHEDULING COMPLETE!")
        logger.info(f"Total machines identified: {len(machines_to_service)}")
        logger.info(f"Priority breakdown: {high_count} high (cluster 1), {medium_count} medium (cluster 0)")
        
        # Log each machine that will receive maintenance
        if machines_to_service:
            logger.info("Machines scheduled for maintenance:")
            for i, machine in enumerate(machines_to_service):
                logger.info(f"  {i+1}. Machine {machine['machine_id']}: "
                          f"cluster {machine['cluster']}, "
                          f"{machine['failure_count']} failures, "
                          f"score {machine['urgency_score']}, "
                          f"{machine['priority']} priority")
        else:
            logger.warning("No machines identified for maintenance!")
            logger.info("Possible reasons:")
            logger.info("- All cluster 1 machines already have open tasks")
            logger.info("- No machines in cluster 1 (all machines performing well)")
            logger.info("- max_machines set to 0")
        
        return machines_to_service
        
    except Exception as e:
        logger.error(f"Error in cluster-based interpretation: {e}", exc_info=True)
        return []


def calculate_maintenance_score(machine: Dict) -> float:
    """
    Calculate a maintenance urgency score for ranking machines within clusters.
    Higher score = more urgent maintenance needed.
    """
    failure_count = machine.get('failure_count', 0)
    downtime = machine.get('total_downtime_minutes', 0)
    age = machine.get('machine_age_years', 0)
    
    # Weighted scoring (failures most important, then downtime, then age)
    score = (failure_count * 3.0) + (downtime / 100.0) + (age * 0.5)
    return score


def get_machines_with_open_tasks() -> set:
    """
    Get set of machine IDs that already have open maintenance tasks.
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
        machine_ids = {task['machine_id'] for task in result if task.get('machine_id')}
        logger.debug(f"Found {len(machine_ids)} machines with open maintenance tasks")
        return machine_ids
    except Exception as e:
        logger.error(f"Error checking existing tasks: {e}")
        return set()


# Usage functions for different scenarios:

def schedule_cluster_based_maintenance(analysis_results):
    """Default: Schedule all high-risk cluster machines"""
    return interpret_results(analysis_results, max_machines=None)

def schedule_next_worst_machines(analysis_results, count=20):
    """Progressive: Schedule next X worst performing machines"""
    return interpret_results(analysis_results, max_machines=count)

def schedule_without_exclusions(analysis_results, count=10):
    """Override: Schedule regardless of existing tasks"""
    return interpret_results(analysis_results, max_machines=count, exclude_scheduled=False)