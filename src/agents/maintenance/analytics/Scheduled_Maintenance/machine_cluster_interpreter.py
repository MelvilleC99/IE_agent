import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("machine_cluster_interpreter")

def interpret_results(analysis_results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Interpret machine clustering analysis results to identify machines needing maintenance.
    
    Args:
        analysis_results: Dictionary containing clustering analysis results
        
    Returns:
        List of machines identified for maintenance with their priority levels
    """
    try:
        machines_to_service = []
        clusters = analysis_results.get('clusters', [])
        
        if not clusters:
            logger.warning("No clusters found in analysis results")
            return machines_to_service
            
        for cluster in clusters:
            cluster_id = cluster.get('cluster_id')
            machines = cluster.get('machines', [])
            risk_score = cluster.get('risk_score', 0.0)
            
            if not machines:
                continue
                
            # Determine priority based on risk score
            priority = "high" if risk_score >= 0.7 else "medium" if risk_score >= 0.4 else "low"
            
            for machine in machines:
                machine_data = {
                    'machine_id': machine.get('id'),
                    'cluster_id': cluster_id,
                    'risk_score': risk_score,
                    'priority': priority,
                    'last_maintenance': machine.get('last_maintenance'),
                    'downtime_hours': machine.get('downtime_hours', 0),
                    'failure_count': machine.get('failure_count', 0)
                }
                machines_to_service.append(machine_data)
        
        # Sort by priority and risk score
        priority_map = {'high': 3, 'medium': 2, 'low': 1}
        machines_to_service.sort(
            key=lambda x: (priority_map.get(x['priority'], 0), x['risk_score']),
            reverse=True
        )
        
        logger.info(f"Identified {len(machines_to_service)} machines for maintenance")
        return machines_to_service
        
    except Exception as e:
        logger.error(f"Error interpreting analysis results: {e}", exc_info=True)
        return []
