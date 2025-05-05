# src/agents/maintenance/analytics/time_series_tool/time_series_interpreter.py
import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Define your statistical thresholds ---
Z_SCORE_THRESHOLD = 1.5  # Flag time patterns with Z-score > 1.5 (beyond 1.5 standard deviations)
LINE_VARIANCE_THRESHOLD_PCT = 20.0  # Flag line-specific patterns with variance > 20%
MECHANIC_VARIANCE_THRESHOLD_PCT = 25.0  # Flag mechanic-specific patterns with variance > 25%

def interpret_time_series_results(analysis_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Interprets time series analysis results, identifying patterns in:
    - Overall hourly/daily incident patterns
    - Line-specific hourly/daily patterns
    - Mechanic-specific hourly/daily patterns for response and repair times
    
    Args:
        analysis_summary: Dictionary containing 'hourly' and/or 'daily' analysis results
        
    Returns:
        list: A list of findings dictionaries
    """
    print("TIME_SERIES_INTERPRETER: Interpreting time series analysis results...")
    findings = []
    
    # Get mechanic info if needed
    mechanic_dict = get_mechanic_info()
    
    # --- Process hourly analysis findings ---
    if 'hourly' in analysis_summary:
        hourly_data = analysis_summary['hourly']
        print(f"TIME_SERIES_INTERPRETER: Processing hourly data with keys: {list(hourly_data.keys())}")
        
        # 1. Check statistical outliers (hours with unusually high incident counts)
        if 'statistical_outliers' in hourly_data and hourly_data['statistical_outliers']:
            outliers = hourly_data['statistical_outliers']
            print(f"TIME_SERIES_INTERPRETER: Found {len(outliers)} hourly outliers")
            
            for outlier in outliers:
                hour = outlier.get('hour_of_day')
                count = outlier.get('incident_count')
                z_score = outlier.get('z_score', 0)
                
                if z_score > 0:  # Only alert for hours with higher than average counts
                    finding_summary = (
                        f"HOURLY PATTERN ALERT: Hour {hour}:00 has {count} incidents, "
                        f"significantly higher than average (Z-score: {z_score:.2f})."
                    )
                    
                    finding_details = {
                        "metric": "hourly_pattern",
                        "hour": hour,
                        "incident_count": count,
                        "z_score": round(z_score, 2),
                        "threshold": Z_SCORE_THRESHOLD,
                        "context": "Hourly Pattern Analysis"
                    }
                    
                    finding = {
                        "analysis_type": "time_series_hourly_pattern",
                        "finding_summary": finding_summary,
                        "finding_details": finding_details
                    }
                    findings.append(finding)
        else:
            print("TIME_SERIES_INTERPRETER: No hourly statistical outliers found")
        
        # 2. Check mechanic hourly performance patterns
        if 'mechanic_hourly_stats' in hourly_data and hourly_data['mechanic_hourly_stats']:
            mech_stats_raw = hourly_data['mechanic_hourly_stats']
            print(f"TIME_SERIES_INTERPRETER: Processing {len(mech_stats_raw)} mechanic hourly stats")
            
            try:
                mech_stats = pd.DataFrame(mech_stats_raw)
                
                # Check if required columns exist
                required_cols = {'mechanic_id', 'mechanic_name', 'hour_of_day', 'avg_response', 'avg_repair'}
                if not required_cols.issubset(mech_stats.columns):
                    missing = required_cols - set(mech_stats.columns)
                    print(f"TIME_SERIES_INTERPRETER: Missing columns in mechanic_hourly_stats: {missing}")
                    print(f"TIME_SERIES_INTERPRETER: Available columns: {list(mech_stats.columns)}")
                else:
                    # Calculate global averages per hour
                    hour_avg = mech_stats.groupby('hour_of_day').agg(
                        global_avg_response=('avg_response', 'mean'),
                        global_avg_repair=('avg_repair', 'mean')
                    ).reset_index()
                    
                    # Merge with mechanic stats
                    mech_stats = mech_stats.merge(hour_avg, on='hour_of_day')
                    
                    # Calculate % difference from global average
                    mech_stats['response_pct_diff'] = np.where(
                        mech_stats['global_avg_response'] > 0,
                        (mech_stats['avg_response'] / mech_stats['global_avg_response'] - 1) * 100,
                        0
                    )
                    
                    mech_stats['repair_pct_diff'] = np.where(
                        mech_stats['global_avg_repair'] > 0,
                        (mech_stats['avg_repair'] / mech_stats['global_avg_repair'] - 1) * 100,
                        0
                    )
                    
                    # Filter to significant variances
                    response_outliers = mech_stats[mech_stats['response_pct_diff'] > MECHANIC_VARIANCE_THRESHOLD_PCT]
                    repair_outliers = mech_stats[mech_stats['repair_pct_diff'] > MECHANIC_VARIANCE_THRESHOLD_PCT]
                    
                    print(f"TIME_SERIES_INTERPRETER: Found {len(response_outliers)} mechanic response time hourly outliers")
                    print(f"TIME_SERIES_INTERPRETER: Found {len(repair_outliers)} mechanic repair time hourly outliers")
                    
                    # Create findings for response time outliers
                    for _, row in response_outliers.iterrows():
                        mechanic_name = row['mechanic_name']
                        mechanic_id = row['mechanic_id']
                        employee_num = mechanic_dict.get(mechanic_name, 'Unknown')
                        hour = row['hour_of_day']
                        pct_diff = row['response_pct_diff']
                        avg_time = row['avg_response']
                        global_avg = row['global_avg_response']
                        
                        finding_summary = (
                            f"MECHANIC HOURLY RESPONSE: {mechanic_name} (#{employee_num}) has {pct_diff:.1f}% "
                            f"longer response times at hour {hour}:00 ({avg_time:.1f} min vs. team average of {global_avg:.1f} min)."
                        )
                        
                        finding_details = {
                            "mechanic_id": mechanic_name,
                            "employee_number": employee_num,
                            "metric": "mechanic_hourly_response",
                            "hour": hour,
                            "value": round(avg_time, 1),
                            "mean_value": round(global_avg, 1),
                            "pct_diff": round(pct_diff, 1),
                            "threshold": MECHANIC_VARIANCE_THRESHOLD_PCT,
                            "context": f"Mechanic Response Time at Hour {hour}:00"
                        }
                        
                        finding = {
                            "analysis_type": "time_series_mechanic_hourly_response",
                            "finding_summary": finding_summary,
                            "finding_details": finding_details
                        }
                        findings.append(finding)
                    
                    # Create findings for repair time outliers
                    for _, row in repair_outliers.iterrows():
                        mechanic_name = row['mechanic_name']
                        mechanic_id = row['mechanic_id']
                        employee_num = mechanic_dict.get(mechanic_name, 'Unknown')
                        hour = row['hour_of_day']
                        pct_diff = row['repair_pct_diff']
                        avg_time = row['avg_repair']
                        global_avg = row['global_avg_repair']
                        
                        finding_summary = (
                            f"MECHANIC HOURLY REPAIR: {mechanic_name} (#{employee_num}) has {pct_diff:.1f}% "
                            f"longer repair times at hour {hour}:00 ({avg_time:.1f} min vs. team average of {global_avg:.1f} min)."
                        )
                        
                        finding_details = {
                            "mechanic_id": mechanic_name,
                            "employee_number": employee_num,
                            "metric": "mechanic_hourly_repair",
                            "hour": hour,
                            "value": round(avg_time, 1),
                            "mean_value": round(global_avg, 1),
                            "pct_diff": round(pct_diff, 1),
                            "threshold": MECHANIC_VARIANCE_THRESHOLD_PCT,
                            "context": f"Mechanic Repair Time at Hour {hour}:00"
                        }
                        
                        finding = {
                            "analysis_type": "time_series_mechanic_hourly_repair",
                            "finding_summary": finding_summary,
                            "finding_details": finding_details
                        }
                        findings.append(finding)
            except Exception as e:
                print(f"TIME_SERIES_INTERPRETER: Error processing mechanic hourly stats: {str(e)}")
        else:
            print("TIME_SERIES_INTERPRETER: No mechanic hourly stats available")
        
        # 3. Check line-specific hourly patterns
        if 'line_hourly_outliers' in hourly_data and hourly_data['line_hourly_outliers']:
            line_outliers = hourly_data['line_hourly_outliers']
            print(f"TIME_SERIES_INTERPRETER: Processing {len(line_outliers)} line hourly outliers")
            
            for line_outlier in line_outliers:
                line_id = line_outlier.get('line_id')
                hour = line_outlier.get('hour_of_day')
                pct_diff = line_outlier.get('pct_diff')
                avg_downtime = line_outlier.get('avg_downtime_min')
                global_avg = line_outlier.get('global_avg_downtime')
                
                finding_summary = (
                    f"LINE-SPECIFIC HOURLY PATTERN: Line {line_id} has {pct_diff:.1f}% "
                    f"more downtime at hour {hour}:00 ({avg_downtime:.1f} min vs. average {global_avg:.1f} min)."
                )
                
                finding_details = {
                    "metric": "line_hourly_pattern",
                    "line_id": line_id,
                    "hour": hour,
                    "value": round(avg_downtime, 1),
                    "mean_value": round(global_avg, 1),
                    "pct_diff": round(pct_diff, 1),
                    "threshold": LINE_VARIANCE_THRESHOLD_PCT,
                    "context": f"Line-Specific Pattern at Hour {hour}:00"
                }
                
                finding = {
                    "analysis_type": "time_series_line_hourly",
                    "finding_summary": finding_summary,
                    "finding_details": finding_details
                }
                findings.append(finding)
        else:
            print("TIME_SERIES_INTERPRETER: No line hourly outliers found")
    
    # --- Process daily analysis findings ---
    if 'daily' in analysis_summary:
        daily_data = analysis_summary['daily']
        print(f"TIME_SERIES_INTERPRETER: Processing daily data with keys: {list(daily_data.keys())}")
        
        # 1. Check statistical outliers (days with unusually high incident counts)
        if 'statistical_outliers' in daily_data and daily_data['statistical_outliers']:
            outliers = daily_data['statistical_outliers']
            print(f"TIME_SERIES_INTERPRETER: Found {len(outliers)} daily outliers")
            
            for outlier in outliers:
                day_name = outlier.get('day_name')
                count = outlier.get('incident_count')
                z_score = outlier.get('z_score', 0)
                
                if z_score > 0:  # Only alert for days with higher than average counts
                    finding_summary = (
                        f"DAILY PATTERN ALERT: {day_name} has {count} incidents, "
                        f"significantly higher than average (Z-score: {z_score:.2f})."
                    )
                    
                    finding_details = {
                        "metric": "daily_pattern",
                        "day_name": day_name,
                        "incident_count": count,
                        "z_score": round(z_score, 2),
                        "threshold": Z_SCORE_THRESHOLD,
                        "context": "Daily Pattern Analysis"
                    }
                    
                    finding = {
                        "analysis_type": "time_series_daily_pattern",
                        "finding_summary": finding_summary,
                        "finding_details": finding_details
                    }
                    findings.append(finding)
        else:
            print("TIME_SERIES_INTERPRETER: No daily statistical outliers found")
        
        # 2. Check mechanic daily performance patterns
        if 'mechanic_daily_stats' in daily_data and daily_data['mechanic_daily_stats']:
            mech_stats_raw = daily_data['mechanic_daily_stats']
            print(f"TIME_SERIES_INTERPRETER: Processing {len(mech_stats_raw)} mechanic daily stats")
            
            try:
                mech_stats = pd.DataFrame(mech_stats_raw)
                
                # Check if required columns exist
                required_cols = {'mechanic_id', 'mechanic_name', 'day_of_week', 'avg_response', 'avg_repair'}
                missing_cols = required_cols - set(mech_stats.columns)
                
                if missing_cols:
                    print(f"TIME_SERIES_INTERPRETER: Missing columns in mechanic_daily_stats: {missing_cols}")
                    print(f"TIME_SERIES_INTERPRETER: Available columns: {list(mech_stats.columns)}")
                    
                    # Try to fix missing day_name if needed
                    if 'day_name' in missing_cols and 'day_of_week' in mech_stats.columns:
                        day_names = {
                            0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 
                            3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'
                        }
                        mech_stats['day_name'] = mech_stats['day_of_week'].map(day_names)
                        missing_cols.remove('day_name')
                        print("TIME_SERIES_INTERPRETER: Added day_name column from day_of_week")
                
                # Continue only if we have the essential columns
                essential_cols = {'mechanic_id', 'mechanic_name', 'day_of_week', 'avg_response', 'avg_repair'}
                if not essential_cols - set(mech_stats.columns):
                    # Calculate global averages per day
                    day_avg = mech_stats.groupby('day_of_week').agg(
                        global_avg_response=('avg_response', 'mean'),
                        global_avg_repair=('avg_repair', 'mean')
                    ).reset_index()
                    
                    # Merge with mechanic stats
                    mech_stats = mech_stats.merge(day_avg, on='day_of_week')
                    
                    # Calculate % difference from global average
                    mech_stats['response_pct_diff'] = np.where(
                        mech_stats['global_avg_response'] > 0,
                        (mech_stats['avg_response'] / mech_stats['global_avg_response'] - 1) * 100,
                        0
                    )
                    
                    mech_stats['repair_pct_diff'] = np.where(
                        mech_stats['global_avg_repair'] > 0,
                        (mech_stats['avg_repair'] / mech_stats['global_avg_repair'] - 1) * 100,
                        0
                    )
                    
                    # Filter to significant variances
                    response_outliers = mech_stats[mech_stats['response_pct_diff'] > MECHANIC_VARIANCE_THRESHOLD_PCT]
                    repair_outliers = mech_stats[mech_stats['repair_pct_diff'] > MECHANIC_VARIANCE_THRESHOLD_PCT]
                    
                    print(f"TIME_SERIES_INTERPRETER: Found {len(response_outliers)} mechanic response time daily outliers")
                    print(f"TIME_SERIES_INTERPRETER: Found {len(repair_outliers)} mechanic repair time daily outliers")
                    
                    # Get day names if not already in dataframe
                    day_names = {
                        0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 
                        3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'
                    }
                    
                    # Create findings for response time outliers
                    for _, row in response_outliers.iterrows():
                        mechanic_name = row['mechanic_name']
                        mechanic_id = row['mechanic_id']
                        employee_num = mechanic_dict.get(mechanic_name, 'Unknown')
                        day_of_week = row['day_of_week']
                        
                        if 'day_name' in row:
                            day_name = row['day_name']
                        else:
                            day_name = day_names.get(day_of_week, f"Day {day_of_week}")
                            
                        pct_diff = row['response_pct_diff']
                        avg_time = row['avg_response']
                        global_avg = row['global_avg_response']
                        
                        finding_summary = (
                            f"MECHANIC DAILY RESPONSE: {mechanic_name} (#{employee_num}) has {pct_diff:.1f}% "
                            f"longer response times on {day_name} ({avg_time:.1f} min vs. team average of {global_avg:.1f} min)."
                        )
                        
                        finding_details = {
                            "mechanic_id": mechanic_name,
                            "employee_number": employee_num,
                            "metric": "mechanic_daily_response",
                            "day_of_week": day_of_week,
                            "day_name": day_name,
                            "value": round(avg_time, 1),
                            "mean_value": round(global_avg, 1),
                            "pct_diff": round(pct_diff, 1),
                            "threshold": MECHANIC_VARIANCE_THRESHOLD_PCT,
                            "context": f"Mechanic Response Time on {day_name}"
                        }
                        
                        finding = {
                            "analysis_type": "time_series_mechanic_daily_response",
                            "finding_summary": finding_summary,
                            "finding_details": finding_details
                        }
                        findings.append(finding)
                    
                    # Create findings for repair time outliers
                    for _, row in repair_outliers.iterrows():
                        mechanic_name = row['mechanic_name']
                        mechanic_id = row['mechanic_id']
                        employee_num = mechanic_dict.get(mechanic_name, 'Unknown')
                        day_of_week = row['day_of_week']
                        
                        if 'day_name' in row:
                            day_name = row['day_name']
                        else:
                            day_name = day_names.get(day_of_week, f"Day {day_of_week}")
                            
                        pct_diff = row['repair_pct_diff']
                        avg_time = row['avg_repair']
                        global_avg = row['global_avg_repair']
                        
                        finding_summary = (
                            f"MECHANIC DAILY REPAIR: {mechanic_name} (#{employee_num}) has {pct_diff:.1f}% "
                            f"longer repair times on {day_name} ({avg_time:.1f} min vs. team average of {global_avg:.1f} min)."
                        )
                        
                        finding_details = {
                            "mechanic_id": mechanic_name,
                            "employee_number": employee_num,
                            "metric": "mechanic_daily_repair",
                            "day_of_week": day_of_week,
                            "day_name": day_name,
                            "value": round(avg_time, 1),
                            "mean_value": round(global_avg, 1),
                            "pct_diff": round(pct_diff, 1),
                            "threshold": MECHANIC_VARIANCE_THRESHOLD_PCT,
                            "context": f"Mechanic Repair Time on {day_name}"
                        }
                        
                        finding = {
                            "analysis_type": "time_series_mechanic_daily_repair",
                            "finding_summary": finding_summary,
                            "finding_details": finding_details
                        }
                        findings.append(finding)
            except Exception as e:
                print(f"TIME_SERIES_INTERPRETER: Error processing mechanic daily stats: {str(e)}")
        else:
            print("TIME_SERIES_INTERPRETER: No mechanic daily stats available")
        
        # 3. Check line-specific daily patterns
        if 'line_daily_outliers' in daily_data and daily_data['line_daily_outliers']:
            line_outliers = daily_data['line_daily_outliers']
            print(f"TIME_SERIES_INTERPRETER: Processing {len(line_outliers)} line daily outliers")
            
            for line_outlier in line_outliers:
                line_id = line_outlier.get('line_id')
                day_name = line_outlier.get('day_name')
                pct_diff = line_outlier.get('pct_diff')
                avg_downtime = line_outlier.get('avg_downtime_min')
                global_avg = line_outlier.get('global_avg_downtime')
                
                finding_summary = (
                    f"LINE-SPECIFIC DAILY PATTERN: Line {line_id} has {pct_diff:.1f}% "
                    f"more downtime on {day_name} ({avg_downtime:.1f} min vs. average {global_avg:.1f} min)."
                )
                
                finding_details = {
                    "metric": "line_daily_pattern",
                    "line_id": line_id,
                    "day_name": day_name,
                    "value": round(avg_downtime, 1),
                    "mean_value": round(global_avg, 1),
                    "pct_diff": round(pct_diff, 1),
                    "threshold": LINE_VARIANCE_THRESHOLD_PCT,
                    "context": f"Line-Specific Pattern on {day_name}"
                }
                
                finding = {
                    "analysis_type": "time_series_line_daily",
                    "finding_summary": finding_summary,
                    "finding_details": finding_details
                }
                findings.append(finding)
        else:
            print("TIME_SERIES_INTERPRETER: No line daily outliers found")
    
    # --- Deduplicate findings ---
    # Create a simple hash for each finding to identify duplicates
    finding_hashes = set()
    deduplicated_findings = []
    
    for finding in findings:
        # Create a simple hash of key elements
        hash_elements = [
            finding['analysis_type'],
            str(finding['finding_details'].get('hour', '')),
            finding['finding_details'].get('day_name', ''),
            str(finding['finding_details'].get('line_id', '')),
            finding['finding_details'].get('mechanic_id', ''),
            finding['finding_details'].get('metric', '')
        ]
        finding_hash = '_'.join(hash_elements)
        
        if finding_hash not in finding_hashes:
            finding_hashes.add(finding_hash)
            deduplicated_findings.append(finding)
    
    print(f"TIME_SERIES_INTERPRETER: Identified {len(findings)} potential findings, deduplicated to {len(deduplicated_findings)}")
    return deduplicated_findings

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
        print(f"TIME_SERIES_INTERPRETER: Loaded {len(mechanic_dict)} mechanic records")
        return mechanic_dict
    except Exception as e:
        print(f"TIME_SERIES_INTERPRETER: Error loading mechanic data: {e}")
        return {}  # Return empty dict on error

# Test function for direct execution
if __name__ == '__main__':
    import json
    import argparse
    
    parser = argparse.ArgumentParser(description='Interpret time series analysis results')
    parser.add_argument('--hourly-file', help='Path to JSON file containing hourly analysis results')
    parser.add_argument('--daily-file', help='Path to JSON file containing daily analysis results')
    args = parser.parse_args()
    
    test_summary = {}
    
    # Load hourly data if provided
    if args.hourly_file and os.path.exists(args.hourly_file):
        with open(args.hourly_file, 'r') as f:
            test_summary['hourly'] = json.load(f)
    
    # Load daily data if provided
    if args.daily_file and os.path.exists(args.daily_file):
        with open(args.daily_file, 'r') as f:
            test_summary['daily'] = json.load(f)
    
    if test_summary:
        findings = interpret_time_series_results(test_summary)
        print(f"\n--- Time Series Interpreter Test Results ---")
        print(f"Found {len(findings)} potential findings:")
        for i, f in enumerate(findings):
            if i < 10:  # Only show first 10 findings to avoid overwhelming output
                print(f"- {f['finding_summary']}")
            elif i == 10:
                print(f"... and {len(findings) - 10} more findings.")
    else:
        print("Please provide hourly and/or daily analysis files for testing.")