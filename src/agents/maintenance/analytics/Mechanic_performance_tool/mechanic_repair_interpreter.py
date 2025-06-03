import sys
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load environment variables from project root - fix the path
env_path = os.path.join(project_root, "../.env.local")
load_dotenv(env_path)

from shared_services.supabase_client import SupabaseClient

# --- Define your statistical thresholds ---
Z_SCORE_THRESHOLD = 1.0  # Flag mechanics with Z-score > 1.0 (beyond 1 standard deviation)
TREND_THRESHOLD_PCT = 5.0  # Flag mechanics with deterioration > 5% per period
TREND_P_VALUE_THRESHOLD = 0.05  # Only consider statistically significant trends

def interpret_analysis_results(analysis_summary: dict, analysis_records: list = None) -> list:
    """
    Interprets the focused analysis summary using statistical methods,
    identifies actionable findings based on the three main dimensions:
    1. Response time overall
    2. Repair time by machine type
    3. Repair time by machine + reason combination
    4. Trend analysis
    
    Args:
        analysis_summary: Results from the analysis
        analysis_records: Written mechanic performance records with IDs
    
    Returns a list of findings (not saved to database)
    """
    print("INTERPRETER: Interpreting analysis summary with statistical methods...")
    findings = []
    analysis_type_prefix = "mechanic_repair_time"
    
    # Create a lookup for mechanic performance IDs
    performance_id_lookup = {}
    if analysis_records:
        for record in analysis_records:
            # Create a key based on context, mechanic, and dimensions
            key = f"{record.get('context', '')}_{record.get('mechanic_name', '')}_{record.get('dimension_1', '')}_{record.get('dimension_2', '')}"
            performance_id_lookup[key] = record.get('id')
            # Debug: print each key created
            if record.get('context') in ['byMachineAndReason', 'trends']:
                print(f"INTERPRETER: Created lookup key: '{key}' -> ID {record.get('id')}")
        print(f"INTERPRETER: Created lookup for {len(performance_id_lookup)} performance records")
    
    def find_performance_id(context, mechanic_name, dimension_1="", dimension_2=""):
        """Helper function to find the corresponding mechanic performance ID"""
        # Convert empty strings to "None" to match the database format
        d1 = "None" if not dimension_1 else dimension_1
        d2 = "None" if not dimension_2 else dimension_2
        
        key = f"{context}_{mechanic_name}_{d1}_{d2}"
        performance_id = performance_id_lookup.get(key)
        if not performance_id:
            # Debug: print available keys if lookup fails
            print(f"INTERPRETER: No performance ID found for key '{key}'")
            print(f"INTERPRETER: Available keys: {list(performance_id_lookup.keys())[:5]}...")  # Show first 5
        else:
            print(f"INTERPRETER: Found performance ID {performance_id} for key '{key}'")
        return performance_id
    
    # Get mechanic info
    mechanic_dict = get_mechanic_info()
    
    # --- Dimension 1: Overall Response Time Performance ---
    if 'overall_response' in analysis_summary and 'mechanic_stats' in analysis_summary['overall_response']:
        statistical_measures = analysis_summary['overall_response'].get('statistical_measures', {})
        mean_response_time = statistical_measures.get('mean_response_time')
        std_dev_response = statistical_measures.get('std_dev_response_time')
        
        for mechanic_stat in analysis_summary['overall_response']['mechanic_stats']:
            mechanic_name = mechanic_stat.get('mechanic_name', '')
            mechanic_employee_num = mechanic_dict.get(mechanic_name, 'Unknown')
            
            # Check response time Z-score
            response_z_score = mechanic_stat.get('response_z_score')
            if response_z_score is not None and response_z_score > Z_SCORE_THRESHOLD:
                response_time = mechanic_stat.get('avg_response_time_min', 0)
                
                finding_summary = (
                    f"RESPONSE TIME ALERT: {mechanic_name} (#{mechanic_employee_num}) average response time is "
                    f"{response_time:.1f} min vs. team average of {mean_response_time:.1f} min "
                    f"(Z-score: {response_z_score:.2f}, {response_z_score:.1f} standard deviations above mean)."
                )
                
                finding_details = {
                    "mechanic_id": mechanic_name,
                    "employee_number": mechanic_employee_num,
                    "metric": "response_time",
                    "value": round(response_time, 1),
                    "mean_value": round(mean_response_time, 1) if mean_response_time else None,
                    "z_score": round(response_z_score, 2),
                    "threshold": Z_SCORE_THRESHOLD,
                    "std_dev": round(std_dev_response, 2) if std_dev_response else None,
                    "sample_count": mechanic_stat.get('count'),
                    "context": "Overall Response Time Performance Analysis",
                    "percentage_above_mean": round(((response_time - mean_response_time) / mean_response_time) * 100, 1) if mean_response_time and mean_response_time > 0 else None
                }
                
                # Find the corresponding mechanic performance ID (use "overall" context, not "overall_response")
                performance_id = find_performance_id("overall", mechanic_name)
                
                finding = {
                    "analysis_type": f"{analysis_type_prefix}_response_time",
                    "finding_summary": finding_summary,
                    "finding_details": finding_details,
                    "mechanic_performance_id": performance_id,
                    "created_at": datetime.now().isoformat()
                }
                findings.append(finding)

    # --- Dimension 2: Repair Time by Machine Type ---
    if 'machine_repair' in analysis_summary:
        for machine_type, machine_data in analysis_summary['machine_repair'].items():
            if not machine_data or 'mechanic_stats' not in machine_data:
                continue
                
            statistical_measures = machine_data.get('statistical_measures', {})
            mean_repair_time = statistical_measures.get('mean_repair_time')
            std_dev = statistical_measures.get('std_dev_repair_time')
            
            # Process each mechanic's performance on this machine type
            for mechanic_stat in machine_data['mechanic_stats']:
                mechanic_name = mechanic_stat.get('mechanic_name', '')
                mechanic_employee_num = mechanic_dict.get(mechanic_name, 'Unknown')
                
                # Check repair time Z-score for this machine type
                z_score = mechanic_stat.get('repair_z_score')
                if z_score is not None and z_score > Z_SCORE_THRESHOLD:
                    repair_time = mechanic_stat.get('avg_repair_time_min', 0)
                    
                    finding_summary = (
                        f"MACHINE-SPECIFIC REPAIR TIME: {mechanic_name} (#{mechanic_employee_num}) on {machine_type} machines: "
                        f"{repair_time:.1f} min vs. team average of {mean_repair_time:.1f} min "
                        f"(Z-score: {z_score:.2f}, {z_score:.1f} standard deviations above mean)."
                    )
                    
                    finding_details = {
                        "mechanic_id": mechanic_name,
                        "employee_number": mechanic_employee_num,
                        "machine_type": machine_type,
                        "metric": "repair_time_by_machine",
                        "value": round(repair_time, 1),
                        "mean_value": round(mean_repair_time, 1) if mean_repair_time else None,
                        "z_score": round(z_score, 2),
                        "threshold": Z_SCORE_THRESHOLD,
                        "std_dev": round(std_dev, 2) if std_dev else None,
                        "sample_count": mechanic_stat.get('count'),
                        "context": f"Machine Type {machine_type} Repair Time Analysis",
                        "percentage_above_mean": round(((repair_time - mean_repair_time) / mean_repair_time) * 100, 1) if mean_repair_time and mean_repair_time > 0 else None,
                        "absolute_difference": round(repair_time - mean_repair_time, 1) if mean_repair_time else None
                    }
                    
                    # Find the corresponding mechanic performance ID (use "byMachineType" context)
                    performance_id = find_performance_id("byMachineType", mechanic_name, machine_type)
                    
                    finding = {
                        "analysis_type": f"{analysis_type_prefix}_machine_repair",
                        "finding_summary": finding_summary,
                        "finding_details": finding_details,
                        "mechanic_performance_id": performance_id,
                        "created_at": datetime.now().isoformat()
                    }
                    findings.append(finding)

    # --- Dimension 3: Repair Time by Machine + Reason Combination ---
    if 'machine_reason_repair' in analysis_summary:
        for combo_key, combo_data in analysis_summary['machine_reason_repair'].items():
            # Only process if there are multiple mechanics (allows meaningful comparison)
            if not combo_data or 'mechanic_stats' not in combo_data or combo_data.get('statistical_measures', {}).get('mechanic_count', 0) <= 1:
                continue
                
            # Get the category information
            machine_type = combo_data.get('machine_type', 'Unknown')
            reason = combo_data.get('reason', 'Unknown')
            
            statistical_measures = combo_data.get('statistical_measures', {})
            mean_repair_time = statistical_measures.get('mean_repair_time')
            std_dev = statistical_measures.get('std_dev_repair_time')
            
            # Process each mechanic's performance on this combination
            for mechanic_stat in combo_data['mechanic_stats']:
                mechanic_name = mechanic_stat.get('mechanic_name', '')
                mechanic_employee_num = mechanic_dict.get(mechanic_name, 'Unknown')
                
                # Check repair time Z-score for this combination
                z_score = mechanic_stat.get('repair_z_score')
                if z_score is not None and z_score > Z_SCORE_THRESHOLD:
                    repair_time = mechanic_stat.get('avg_repair_time_min', 0)
                    
                    finding_summary = (
                        f"SPECIFIC MACHINE-ISSUE COMBINATION: {mechanic_name} (#{mechanic_employee_num}) on {machine_type} with '{reason}' issues: "
                        f"{repair_time:.1f} min vs. team average of {mean_repair_time:.1f} min "
                        f"(Z-score: {z_score:.2f}, {z_score:.1f} standard deviations above mean)."
                    )
                    
                    finding_details = {
                        "mechanic_id": mechanic_name,
                        "employee_number": mechanic_employee_num,
                        "machine_type": machine_type,
                        "reason": reason,
                        "metric": "repair_time_by_machine_reason",
                        "value": round(repair_time, 1),
                        "mean_value": round(mean_repair_time, 1) if mean_repair_time else None,
                        "z_score": round(z_score, 2),
                        "threshold": Z_SCORE_THRESHOLD,
                        "std_dev": round(std_dev, 2) if std_dev else None,
                        "sample_count": mechanic_stat.get('count'),
                        "context": f"Machine '{machine_type}' + Reason '{reason}' Analysis",
                        "percentage_above_mean": round(((repair_time - mean_repair_time) / mean_repair_time) * 100, 1) if mean_repair_time and mean_repair_time > 0 else None,
                        "absolute_difference": round(repair_time - mean_repair_time, 1) if mean_repair_time else None
                    }
                    
                    # Find the corresponding mechanic performance ID (use "byMachineAndReason" context)
                    performance_id = find_performance_id("byMachineAndReason", mechanic_name, machine_type, reason)
                    
                    finding = {
                        "analysis_type": f"{analysis_type_prefix}_machine_reason_repair",
                        "finding_summary": finding_summary,
                        "finding_details": finding_details,
                        "mechanic_performance_id": performance_id,
                        "created_at": datetime.now().isoformat()
                    }
                    findings.append(finding)

    # --- Dimension 4: Trend Analysis for Repair and Response Times ---
    if 'trends' in analysis_summary:
        # Check repair time trends
        if 'repair_time' in analysis_summary['trends']:
            for mechanic_id, trend_data in analysis_summary['trends']['repair_time'].items():
                mechanic_employee_num = mechanic_dict.get(mechanic_id, 'Unknown')
                pct_change = trend_data.get('pct_change_per_period')
                is_significant = trend_data.get('is_significant', False)
                
               # Only flag significant deteriorating trends (positive percentage means increasing repair time)
                if (pct_change is not None and pct_change > TREND_THRESHOLD_PCT and is_significant):
                    periods = trend_data.get('periods_analyzed', 0)
                    p_value = trend_data.get('p_value', 1.0)
                    r_squared = trend_data.get('r_squared', 0)
                    
                    finding_summary = (
                        f"REPAIR TIME TREND ALERT: {mechanic_id} (#{mechanic_employee_num}) shows significant deterioration in performance - "
                        f"repair times increasing by {pct_change:.1f}% per period "
                        f"(p-value: {p_value:.3f}, R²: {r_squared:.2f}, periods analyzed: {periods})."
                    )
                    
                    finding_details = {
                        "mechanic_id": mechanic_id,
                        "employee_number": mechanic_employee_num,
                        "metric": "trend_repair_time",
                        "value": round(pct_change, 1),
                        "threshold": TREND_THRESHOLD_PCT,
                        "p_value": round(p_value, 3),
                        "periods_analyzed": periods,
                        "r_squared": round(r_squared, 3),
                        "context": "Repair Time Trend Analysis",
                        "confidence": "High" if p_value < 0.01 else "Medium" if p_value < 0.05 else "Low"
                    }
                    
                    # Find the corresponding mechanic performance ID for trends (use "trends" context)
                    performance_id = find_performance_id("trends", mechanic_id, "repair_time")
                    
                    finding = {
                        "analysis_type": f"{analysis_type_prefix}_trend_repair",
                        "finding_summary": finding_summary,
                        "finding_details": finding_details,
                        "mechanic_performance_id": performance_id,
                        "created_at": datetime.now().isoformat()
                    }
                    findings.append(finding)
        
        # Check response time trends
        if 'response_time' in analysis_summary['trends']:
            for mechanic_id, trend_data in analysis_summary['trends']['response_time'].items():
                mechanic_employee_num = mechanic_dict.get(mechanic_id, 'Unknown')
                pct_change = trend_data.get('pct_change_per_period')
                is_significant = trend_data.get('is_significant', False)
                
                # Only flag significant deteriorating trends
                if (pct_change is not None and pct_change > TREND_THRESHOLD_PCT and is_significant):
                    periods = trend_data.get('periods_analyzed', 0)
                    p_value = trend_data.get('p_value', 1.0)
                    r_squared = trend_data.get('r_squared', 0)
                    
                    finding_summary = (
                        f"RESPONSE TIME TREND ALERT: {mechanic_id} (#{mechanic_employee_num}) shows significant deterioration - "
                        f"response times increasing by {pct_change:.1f}% per period "
                        f"(p-value: {p_value:.3f}, R²: {r_squared:.2f}, periods analyzed: {periods})."
                    )
                    
                    finding_details = {
                        "mechanic_id": mechanic_id,
                        "employee_number": mechanic_employee_num,
                        "metric": "trend_response_time",
                        "value": round(pct_change, 1),
                        "threshold": TREND_THRESHOLD_PCT,
                        "p_value": round(p_value, 3),
                        "periods_analyzed": periods,
                        "r_squared": round(r_squared, 3),
                        "context": "Response Time Trend Analysis",
                        "confidence": "High" if p_value < 0.01 else "Medium" if p_value < 0.05 else "Low"
                    }
                    
                    # Find the corresponding mechanic performance ID for trends (use "trends" context)
                    performance_id = find_performance_id("trends", mechanic_id, "response_time")
                    
                    finding = {
                        "analysis_type": f"{analysis_type_prefix}_trend_response",
                        "finding_summary": finding_summary,
                        "finding_details": finding_details,
                        "mechanic_performance_id": performance_id,
                        "created_at": datetime.now().isoformat()
                    }
                    findings.append(finding)

    # --- Deduplicate findings ---
    # Create a simple hash for each finding to identify duplicates
    finding_hashes = set()
    deduplicated_findings = []
    
    for finding in findings:
        # Create a simple hash of key elements
        hash_elements = [
            finding['analysis_type'],
            finding['finding_details'].get('mechanic_id', ''),
            finding['finding_details'].get('metric', ''),
            str(finding['finding_details'].get('value', '')),
            finding['finding_details'].get('machine_type', ''),
            finding['finding_details'].get('reason', '')
        ]
        finding_hash = '_'.join(hash_elements)
        
        if finding_hash not in finding_hashes:
            finding_hashes.add(finding_hash)
            deduplicated_findings.append(finding)
    
    print(f"INTERPRETER: Identified {len(findings)} potential findings, deduplicated to {len(deduplicated_findings)}")
    return deduplicated_findings

def get_mechanic_info():
    """Get mechanic employee numbers from database"""
    try:
        supabase = SupabaseClient()
        mechanics_result = supabase.table('mechanics').select('employee_number, full_name').execute()
        
        mechanic_dict = {}
        if mechanics_result.data:
            for mechanic in mechanics_result.data:
                mechanic_dict[mechanic['full_name']] = mechanic['employee_number']
        print(f"INTERPRETER: Loaded {len(mechanic_dict)} mechanic records")
        return mechanic_dict
    except Exception as e:
        print(f"INTERPRETER: Error loading mechanic data: {e}")
        return {}  # Return empty dict on error

# Test function for direct execution
if __name__ == '__main__':
    
    supabase = SupabaseClient()
    if supabase:
        # Get the most recent analysis results from the database
        analysis_result = supabase.table('mechanic_performance').select('*').order('created_at', desc=True).limit(1).execute()
        
        if analysis_result.data:
            analysis_summary = analysis_result.data[0]
            findings = interpret_analysis_results(analysis_summary)
            print(f"\n--- Interpreter Test Results ---")
            print(f"Found {len(findings)} potential findings:")
            for i, f in enumerate(findings):
                if i < 10:  # Only show first 10 findings to avoid overwhelming output
                    print(f"- {f['finding_summary']}")
                elif i == 10:
                    print(f"... and {len(findings) - 10} more findings.")
        else:
            print("No analysis results found in database")
    else:
        print("Could not connect to database")