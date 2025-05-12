import json
import pandas as pd
import traceback
from datetime import datetime, timedelta
import logging
import numpy as np
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def normalize_field_names(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize field names from camelCase to snake_case."""
    field_mapping = {
        'createdAt': 'created_at',
        'machineNumber': 'machine_number',
        'mechanicId': 'mechanic_id',
        'totalDowntime': 'total_downtime',
        'totalRepairTime': 'total_repair_time',
        'totalResponseTime': 'total_response_time'
    }
    
    normalized_records = []
    for record in records:
        normalized = {}
        for old_key, new_key in field_mapping.items():
            if old_key in record:
                normalized[new_key] = record[old_key]
            elif new_key in record:
                normalized[new_key] = record[new_key]
        # Copy any other fields as is
        for key, value in record.items():
            if key not in field_mapping.values():
                normalized[key] = value
        normalized_records.append(normalized)
    
    return normalized_records

def parse_timestamp(ts_str: str) -> Optional[datetime]:
    """Parse timestamp string in various formats."""
    if not ts_str:
        return None
        
    try:
        # Try parsing as ISO8601
        return pd.to_datetime(ts_str, format='ISO8601')
    except ValueError:
        try:
            # Try parsing with microseconds
            return pd.to_datetime(ts_str, format='%Y-%m-%dT%H:%M:%S.%f%z')
        except ValueError:
            try:
                # Try parsing without microseconds
                return pd.to_datetime(ts_str, format='%Y-%m-%dT%H:%M:%S%z')
            except ValueError:
                logger.warning(f"Could not parse timestamp: {ts_str}")
                return None

def run_analysis(records: List[Dict[str, Any]], threshold_minutes: int = 120) -> Dict[str, Any]:
    """
    Analyze maintenance records to identify repeat failures within a time window.
    
    Args:
        records: List of maintenance records from the database
        threshold_minutes: Time window in minutes to consider for repeat failures
        
    Returns:
        Dictionary containing:
        - repeat_failures: List of repeat failure incidents
        - machine_repeat_failures: Summary of machines with multiple repeat failures
        - mechanic_repeat_failures: Summary of mechanics with multiple repeat failures
        - common_problems: Most frequent problems causing repeat failures
        - explanation: Summary of findings
    """
    if not records:
        logger.warning("No records provided for repeat failure analysis")
        return {
            "error": "No records provided",
            "message": "Repeat failure analysis requires maintenance records"
        }
    
    try:
        # Normalize field names
        records = normalize_field_names(records)
        
        # Convert records to DataFrame
        df = pd.DataFrame(records)
        
        # Ensure required columns exist
        required_cols = ['id', 'machine_number', 'mechanic_id', 'created_at', 'reason']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return {
                "error": "Missing required fields",
                "message": f"Required fields missing: {', '.join(missing_cols)}"
            }
        
        # Convert timestamps
        df['created_at'] = df['created_at'].apply(parse_timestamp)
        
        # Remove rows with invalid timestamps
        invalid_timestamps = df['created_at'].isna().sum()
        if invalid_timestamps > 0:
            logger.warning(f"Removing {invalid_timestamps} rows with invalid timestamps")
            df = df.dropna(subset=['created_at'])
            
        if df.empty:
            return {
                "error": "No valid records after timestamp filtering",
                "message": "All records had invalid timestamps"
            }
        
        # Sort by timestamp
        df = df.sort_values('created_at')
        
        # Initialize results
        repeat_failures = []
        machine_repeats = {}
        mechanic_repeats = {}
        problem_counts = {}
        
        # Group by machine to find repeat failures
        for machine in df['machine_number'].unique():
            machine_df = df[df['machine_number'] == machine].copy()
            
            # For each incident, look for repeats within threshold
            for idx, row in machine_df.iterrows():
                # Look ahead for repeats within threshold
                window_end = row['created_at'] + timedelta(minutes=threshold_minutes)
                repeats = machine_df[
                    (machine_df['created_at'] > row['created_at']) & 
                    (machine_df['created_at'] <= window_end)
                ]
                
                if not repeats.empty:
                    # Found repeat failures
                    repeat_incident = {
                        'machine_number': machine,
                        'initial_incident_id': row['id'],
                        'initial_incident_time': row['created_at'].isoformat(),
                        'initial_reason': row['reason'],
                        'repeat_incidents': []
                    }
                    
                    for _, repeat in repeats.iterrows():
                        repeat_incident['repeat_incidents'].append({
                            'incident_id': repeat['id'],
                            'incident_time': repeat['created_at'].isoformat(),
                            'reason': repeat['reason'],
                            'mechanic_id': repeat['mechanic_id'],
                            'time_since_initial': (repeat['created_at'] - row['created_at']).total_seconds() / 60
                        })
                        
                        # Track mechanic repeats
                        mechanic_id = repeat['mechanic_id']
                        if mechanic_id not in mechanic_repeats:
                            mechanic_repeats[mechanic_id] = 0
                        mechanic_repeats[mechanic_id] += 1
                        
                        # Track problem frequency
                        reason = repeat['reason']
                        if reason not in problem_counts:
                            problem_counts[reason] = 0
                        problem_counts[reason] += 1
                    
                    repeat_failures.append(repeat_incident)
                    
                    # Track machine repeats
                    if machine not in machine_repeats:
                        machine_repeats[machine] = 0
                    machine_repeats[machine] += 1
        
        # Sort and limit common problems
        common_problems = sorted(
            [{'reason': k, 'count': v} for k, v in problem_counts.items()],
            key=lambda x: x['count'],
            reverse=True
        )[:5]  # Top 5 most common problems
        
        # Generate explanation
        total_repeats = len(repeat_failures)
        if total_repeats == 0:
            explanation = f"No repeat failures found within {threshold_minutes} minutes"
        else:
            explanation = (
                f"Found {total_repeats} repeat failures within {threshold_minutes} minutes. "
                f"Most common problem: {common_problems[0]['reason'] if common_problems else 'Unknown'} "
                f"({common_problems[0]['count'] if common_problems else 0} occurrences)."
            )
        
        return {
            'repeat_failures': repeat_failures,
            'machine_repeat_failures': [
                {'machine_number': k, 'repeat_count': v}
                for k, v in sorted(machine_repeats.items(), key=lambda x: x[1], reverse=True)
            ],
            'mechanic_repeat_failures': [
                {'mechanic_id': k, 'repeat_count': v}
                for k, v in sorted(mechanic_repeats.items(), key=lambda x: x[1], reverse=True)
            ],
            'common_problems': common_problems,
            'explanation': explanation
        }
        
    except Exception as e:
        logger.error(f"Error in repeat failure analysis: {str(e)}")
        return {
            "error": str(e),
            "message": "An error occurred during repeat failure analysis"
        }

# For testing the module directly
if __name__ == '__main__':
    logger.info("This module should be imported and used via the Flask API")