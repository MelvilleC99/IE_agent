# src/agents/maintenance/utils/machine_candidates_saver.py

import logging
from datetime import datetime
from typing import Dict, List, Any
import uuid

logger = logging.getLogger("machine_candidates_saver")

def save_machine_candidates(
    db_client, 
    tool_run_id: str,
    analysis_results: Dict[str, Any]
) -> int:
    """
    Save ranked machine candidates to machine_candidates table.
    
    Args:
        db_client: Database client
        tool_run_id: ID from tool_run_logs table  
        analysis_results: Results from clustering analysis
        
    Returns:
        Number of candidates saved
    """
    try:
        aggregated_data = analysis_results.get('aggregated_data', [])
        if not aggregated_data:
            logger.info("No aggregated data to save as candidates")
            return 0
        
        # Calculate urgency scores and rank machines
        candidates = []
        
        for machine in aggregated_data:
            failure_count = machine.get('failure_count', 0)
            downtime = machine.get('total_downtime_minutes', 0)
            age = machine.get('machine_age_years', 0)
            cluster = machine.get('cluster', 0)
            
            # Calculate urgency score (higher = more urgent)
            urgency_score = (failure_count * 3.0) + (downtime / 100.0) + (age * 0.5)
            
            # Boost score for cluster 1 (worse performing cluster)
            if cluster == 1:
                urgency_score *= 1.5
            
            candidates.append({
                'machine_data': machine,
                'urgency_score': urgency_score,
                'cluster': cluster
            })
        
        # Sort by urgency score (highest first) and assign ranks
        candidates.sort(key=lambda x: x['urgency_score'], reverse=True)
        
        # Insert candidates one by one
        successful_inserts = 0
        for rank, candidate in enumerate(candidates, 1):
            machine = candidate['machine_data']
            
            record = {
                'id': str(uuid.uuid4()),
                'tool_run_id': tool_run_id,  # Link to tool_run_logs instead of clustering_run_id
                'machine_id': str(machine.get('machineNumber')),
                'machine_type': machine.get('machine_type', 'Unknown'),
                'manufacturer': machine.get('manufacturer', 'Unknown'),
                'cluster_assigned': candidate['cluster'],
                'failure_count': machine.get('failure_count', 0),
                'total_downtime_minutes': round(machine.get('total_downtime_minutes', 0), 2),
                'machine_age_years': round(machine.get('machine_age_years', 0), 2),
                'urgency_rank': rank,
                'urgency_score': round(candidate['urgency_score'], 3),
                'is_scheduled': False,
                'created_at': datetime.now().isoformat()
            }
            
            try:
                db_client.insert_data('machine_candidates', record)
                successful_inserts += 1
            except Exception as e:
                logger.error(f"Failed to insert candidate {record['machine_id']}: {e}")
        
        logger.info(f"Successfully saved {successful_inserts} out of {len(candidates)} machine candidates")
        
        # Log top 5 most urgent candidates
        for i in range(min(5, successful_inserts)):
            candidate = candidates[i]
            machine = candidate['machine_data']
            logger.info(f"  Rank {i+1}: Machine {machine.get('machineNumber')} "
                       f"(cluster {candidate['cluster']}, score {candidate['urgency_score']:.3f})")
        
        return successful_inserts
        
    except Exception as e:
        logger.error(f"Error saving machine candidates: {e}", exc_info=True)
        return 0
