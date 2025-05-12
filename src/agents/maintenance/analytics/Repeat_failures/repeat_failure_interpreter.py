import sys
import os
import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Any
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Define your statistical thresholds ---
REPEAT_COUNT_THRESHOLD = 3  # Flag machines with 3+ repeat failures
MECHANIC_REPEAT_THRESHOLD = 2  # Flag mechanics with 2+ repeat failures
TIME_THRESHOLD_CRITICAL = 60  # Flag critical if repeat failure within 60 min

def interpret_repeat_failure_findings(analysis_results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Interpret repeat failure analysis results and generate findings.
    
    Args:
        analysis_results: Dictionary containing repeat failure analysis results
        
    Returns:
        List of findings generated from the analysis
    """
    findings = []
    
    # Check if we have valid data
    if not analysis_results or 'error' in analysis_results:
        logger.warning("No valid analysis results to interpret")
        return findings
        
    logger.info(f"REPEAT_INTERPRETER: Interpreting repeat failure analysis results")
    
    # 1. Handle machine repeat failures
    if 'machine_repeat_failures' in analysis_results:
        for machine in analysis_results['machine_repeat_failures']:
            if machine['repeat_count'] >= 3:  # Flag machines with 3+ repeat failures
                message = (
                    f"Machine {machine['machine_number']} has experienced "
                    f"{machine['repeat_count']} repeat failures, indicating "
                    "potential systemic issues requiring investigation."
                )
                findings.append({
                    'analysis_type': 'repeat_failure_machine',
                    'finding_summary': message,
                    'finding_details': {
                        'machine_number': machine['machine_number'],
                        'repeat_count': machine['repeat_count'],
                        'severity': 'high' if machine['repeat_count'] >= 5 else 'medium',
                        'message': message
                    }
                })
    
    # 2. Handle mechanic repeat failures
    if 'mechanic_repeat_failures' in analysis_results:
        for mechanic in analysis_results['mechanic_repeat_failures']:
            if mechanic['repeat_count'] >= 3:  # Flag mechanics with 3+ repeat failures
                message = (
                    f"Mechanic {mechanic['mechanic_id']} has been involved in "
                    f"{mechanic['repeat_count']} repeat failures, suggesting "
                    "potential training or process improvement opportunities."
                )
                findings.append({
                    'analysis_type': 'repeat_failure_mechanic',
                    'finding_summary': message,
                    'finding_details': {
                        'mechanic_id': mechanic['mechanic_id'],
                        'repeat_count': mechanic['repeat_count'],
                        'severity': 'high' if mechanic['repeat_count'] >= 5 else 'medium',
                        'message': message
                    }
                })
    
    # 3. Handle critical rapid repeat failures
    if 'repeat_failures' in analysis_results:
        for failure in analysis_results['repeat_failures']:
            # Check for very rapid repeats (within 30 minutes)
            rapid_repeats = [
                r for r in failure['repeat_incidents']
                if r['time_since_initial'] <= 30
            ]
            
            if rapid_repeats:
                message = (
                    f"Machine {failure['machine_number']} experienced "
                    f"{len(rapid_repeats)} repeat failures within 30 minutes "
                    "of the initial incident, indicating critical issues."
                )
                findings.append({
                    'analysis_type': 'repeat_failure_rapid',
                    'finding_summary': message,
                    'finding_details': {
                        'machine_number': failure['machine_number'],
                        'initial_incident_id': failure['initial_incident_id'],
                        'rapid_repeat_count': len(rapid_repeats),
                        'time_window_minutes': 30,
                        'severity': 'high',
                        'message': message
                    }
                })
    
    # 4. Handle common problems
    if 'common_problems' in analysis_results and analysis_results['common_problems']:
        top_problem = analysis_results['common_problems'][0]
        if top_problem['count'] >= 3:  # Flag if most common problem occurs 3+ times
            message = (
                f"The most common repeat failure reason is '{top_problem['reason']}' "
                f"with {top_problem['count']} occurrences, suggesting a "
                "systemic issue that needs attention."
            )
            findings.append({
                'analysis_type': 'repeat_failure_common_problem',
                'finding_summary': message,
                'finding_details': {
                    'problem_reason': top_problem['reason'],
                    'occurrence_count': top_problem['count'],
                    'severity': 'high' if top_problem['count'] >= 5 else 'medium',
                    'message': message
                }
            })
    
    logger.info(f"REPEAT_INTERPRETER: Generated {len(findings)} findings from repeat failure analysis")
    return findings

def get_mechanic_info():
    """Get mechanic employee numbers from database"""
    try:
        from shared_services.db_client import get_connection
        supabase = get_connection()
        mechanics_result = supabase.table('mechanics').select('employee_number, full_name').execute()
        
        mechanic_dict = {}
        if mechanics_result.data:
            for mechanic in mechanics_result.data:
                mechanic_dict[mechanic['full_name']] = mechanic['employee_number']
        logger.info(f"REPEAT_INTERPRETER: Loaded {len(mechanic_dict)} mechanic records")
        return mechanic_dict
    except Exception as e:
        logger.error(f"REPEAT_INTERPRETER: Error loading mechanic data: {e}")
        return {}  # Return empty dict on error

# For testing the interpreter directly
if __name__ == "__main__":
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Test the repeat failure interpreter")
    parser.add_argument("--input", required=True, help="Path to repeat failure analysis results JSON file")
    parser.add_argument("--output", help="Path to save findings JSON file (optional)")
    
    args = parser.parse_args()
    
    try:
        with open(args.input, 'r') as f:
            repeat_results = json.load(f)
        
        findings = interpret_repeat_failure_findings(repeat_results)
        
        print(f"\nInterpreter generated {len(findings)} findings from repeat failure analysis")
        
        finding_types = {}
        for f in findings:
            finding_type = f.get('analysis_type', 'unknown')
            finding_types[finding_type] = finding_types.get(finding_type, 0) + 1
        
        print(f"Finding types: {finding_types}")
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(findings, f, indent=2)
            print(f"Saved findings to {args.output}")
    except Exception as e:
        logger.error(f"Error testing interpreter: {e}")
        print(f"Error: {e}")