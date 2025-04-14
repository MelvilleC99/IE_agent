import sys
import os

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import json
from shared_services.db_client import get_connection

# --- Define your thresholds ---
ACTION_THRESHOLD_PCT = 30.0

def interpret_and_save_findings(analysis_summary: dict) -> list[dict]:
    """
    Interprets the analysis summary, identifies actionable findings based on rules,
    saves them to the findings_log table, and returns the list of findings.
    """
    print("INTERPRETER: Interpreting analysis summary...")
    problems_found = []
    analysis_type_prefix = "mechanic_repair_time" # Base type for this interpreter

    # --- Rule 1: Check overall mechanic performance ---
    if 'overall' in analysis_summary and 'mechanic_stats' in analysis_summary['overall']:
        for mechanic_stat in analysis_summary['overall']['mechanic_stats']:
            pct_worse = mechanic_stat.get('pct_worse_than_best')
            if pct_worse is not None and pct_worse > ACTION_THRESHOLD_PCT:
                finding_summary = f"Overall: {mechanic_stat['mechanicName']} repair time ({mechanic_stat['avgRepairTime_min']:.1f} min) is {pct_worse:.1f}% worse than best."
                finding_details = {
                    "mechanic_id": mechanic_stat['mechanicName'],
                    "metric": "pct_worse_than_best",
                    "value": round(pct_worse, 1),
                    "threshold": ACTION_THRESHOLD_PCT,
                    "avg_repair_time": round(mechanic_stat['avgRepairTime_min'], 1),
                    "best_repair_time": round(analysis_summary['overall']['best']['avgRepairTime_min'], 1),
                    "context": "Overall Performance Comparison"
                }
                problem = {
                    "analysis_type": f"{analysis_type_prefix}_overall",
                    "finding_summary": finding_summary,
                    "finding_details": finding_details
                }
                problems_found.append(problem)

    # --- Rule 2: Add more rules here ---
    # Example: Check performance by machine type
    # if 'byMachineType' in analysis_summary:
    #     for machine, machine_data in analysis_summary['byMachineType'].items():
    #         # Check stats within machine_data['mechanic_stats']...
    #         pass # Add your logic

    print(f"INTERPRETER: Identified {len(problems_found)} potential findings.")

    # --- Save findings to Supabase ---
    saved_findings = []
    if problems_found:
        try:
            supabase = get_connection()
            for finding in problems_found:
                # Insert into findings_log table
                result = supabase.table('findings_log').insert({
                    'analysis_type': finding['analysis_type'],
                    'finding_summary': finding['finding_summary'],
                    'finding_details': finding['finding_details'],
                    'status': 'New'
                }).execute()
                
                if result.data:
                    saved_id = result.data[0]['finding_id']
                    print(f"INTERPRETER: Saved finding ID {saved_id} to Supabase: {finding['finding_summary']}")
                    # Add saved ID to the finding dict before returning
                    finding['finding_id'] = saved_id
                    saved_findings.append(finding)
        except Exception as e:
            print(f"INTERPRETER: ERROR saving findings to Supabase: {e}")
            # Return what was successfully processed before error

    return saved_findings

# Example usage (for testing this script directly)
if __name__ == '__main__':
     # Load sample summary data for testing
     try:
         with open("test_summary_output.json", "r") as f:
             test_summary = json.load(f)
         findings = interpret_and_save_findings(test_summary)
         print(f"\n--- Interpreter Test Results ---")
         print(f"Found and attempted to save {len(findings)} findings:")
         for f in findings:
             print(f"- {f.get('finding_id', 'ERR')}: {f['finding_summary']}")
     except FileNotFoundError:
         print("Create a 'test_summary_output.json' file from the analyzer script first.")