# src/agents/maintenance/analytics/time_series_tool/hourly_pattern_interpreter.py
import sys
import os
import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Any

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Define your statistical thresholds ---
Z_SCORE_THRESHOLD = 1.5  # Flag time patterns with Z-score > 1.5 (beyond 1.5 standard deviations)
LINE_VARIANCE_THRESHOLD_PCT = 20.0  # Flag line-specific patterns with variance > 20%
MECHANIC_VARIANCE_THRESHOLD_PCT = 25.0  # Flag mechanic-specific patterns with variance > 25%

def interpret_hourly_findings(hourly_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Interpret hourly analysis results and create findings
    
    Args:
        hourly_summary: Dictionary with hourly analysis results
        
    Returns:
        list: Findings generated from hourly analysis
    """
    findings = []
    
    # Check if we have valid data
    if not hourly_summary:
        logger.warning("No hourly summary data to interpret")
        return findings
        
    logger.info(f"HOURLY_INTERPRETER: Interpreting hourly summary with keys: {list(hourly_summary.keys())}")
    
    # 1. Handle statistical outliers (hours with unusually high incident counts)
    if 'statistical_outliers' in hourly_summary and hourly_summary['statistical_outliers']:
        outliers = hourly_summary['statistical_outliers']
        logger.info(f"HOURLY_INTERPRETER: Found {len(outliers)} hourly statistical outliers")
        
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
                logger.info(f"HOURLY_INTERPRETER: Added hourly pattern finding for hour {hour}:00")
    else:
        logger.info("HOURLY_INTERPRETER: No hourly statistical outliers found")
    
    # 2. Handle mechanic hourly performance patterns
    if 'mechanic_hourly_stats' in hourly_summary and hourly_summary['mechanic_hourly_stats']:
        try:
            mech_stats_raw = hourly_summary['mechanic_hourly_stats']
            logger.info(f"HOURLY_INTERPRETER: Processing {len(mech_stats_raw)} mechanic hourly stats")
            
            # Convert to DataFrame for easier analysis
            mech_df = pd.DataFrame(mech_stats_raw)
            
            # Check required columns
            required_cols = ['mechanic_id', 'mechanic_name', 'hour_of_day', 'avg_repair', 'avg_response', 'incident_count']
            if not all(col in mech_df.columns for col in required_cols):
                # Try alternate field names
                rename_map = {
                    'avg_downtime': 'avg_repair',  # In case field names differ
                    'mechanicId': 'mechanic_id',
                    'mechanicName': 'mechanic_name',
                    'count': 'incident_count',
                }
                mech_df = mech_df.rename(columns={k: v for k, v in rename_map.items() if k in mech_df.columns})
                logger.warning(f"HOURLY_INTERPRETER: Some columns missing or renamed. Available columns: {list(mech_df.columns)}")
            
            # Filter to rows with enough incidents
            min_incidents = 2
            filtered_mech_df = mech_df[mech_df['incident_count'] >= min_incidents].copy()
            logger.info(f"HOURLY_INTERPRETER: Filtered from {len(mech_df)} to {len(filtered_mech_df)} mechanic-hour stats with {min_incidents}+ incidents")
            
            if not filtered_mech_df.empty:
                # Calculate global average repair and response times per hour
                hour_avgs = filtered_mech_df.groupby('hour_of_day').agg({
                    'avg_repair': 'mean',
                    'avg_response': 'mean',
                }).reset_index()
                
                # Merge back to get global averages
                merged_df = pd.merge(
                    filtered_mech_df,
                    hour_avgs,
                    on='hour_of_day',
                    suffixes=('', '_global')
                )
                
                # Calculate percentage differences
                merged_df['repair_pct_diff'] = ((merged_df['avg_repair'] / merged_df['avg_repair_global']) - 1) * 100
                merged_df['response_pct_diff'] = ((merged_df['avg_response'] / merged_df['avg_response_global']) - 1) * 100
                
                # Find outliers
                repair_outliers = merged_df[merged_df['repair_pct_diff'] > MECHANIC_VARIANCE_THRESHOLD_PCT]
                response_outliers = merged_df[merged_df['response_pct_diff'] > MECHANIC_VARIANCE_THRESHOLD_PCT]
                
                logger.info(f"HOURLY_INTERPRETER: Found {len(repair_outliers)} repair time outliers and {len(response_outliers)} response time outliers")
                
                # Get mechanic info from database (employee numbers)
                mechanic_dict = get_mechanic_info()
                
                # Convert repair outliers to findings
                for _, row in repair_outliers.iterrows():
                    mechanic_name = row['mechanic_name']
                    mechanic_id = row['mechanic_id']
                    employee_num = mechanic_dict.get(mechanic_name, mechanic_id)
                    hour = row['hour_of_day']
                    
                    finding_summary = (
                        f"MECHANIC HOURLY REPAIR: {mechanic_name} (#{employee_num}) has {row['repair_pct_diff']:.1f}% "
                        f"longer repair times at hour {hour}:00 ({row['avg_repair']:.1f} min vs. team average of {row['avg_repair_global']:.1f} min)."
                    )
                    
                    finding_details = {
                        "mechanic_id": mechanic_name,
                        "employee_number": employee_num,
                        "metric": "mechanic_hourly_repair",
                        "hour": hour,
                        "value": round(row['avg_repair'], 1),
                        "mean_value": round(row['avg_repair_global'], 1),
                        "pct_diff": round(row['repair_pct_diff'], 1),
                        "threshold": MECHANIC_VARIANCE_THRESHOLD_PCT,
                        "context": f"Mechanic Repair Time at Hour {hour}:00"
                    }
                    
                    finding = {
                        "analysis_type": "time_series_mechanic_hourly_repair",
                        "finding_summary": finding_summary,
                        "finding_details": finding_details
                    }
                    findings.append(finding)
                    logger.info(f"HOURLY_INTERPRETER: Added mechanic hourly repair finding for {mechanic_name} at hour {hour}:00")
                
                # Convert response outliers to findings
                for _, row in response_outliers.iterrows():
                    mechanic_name = row['mechanic_name']
                    mechanic_id = row['mechanic_id']
                    employee_num = mechanic_dict.get(mechanic_name, mechanic_id)
                    hour = row['hour_of_day']
                    
                    finding_summary = (
                        f"MECHANIC HOURLY RESPONSE: {mechanic_name} (#{employee_num}) has {row['response_pct_diff']:.1f}% "
                        f"longer response times at hour {hour}:00 ({row['avg_response']:.1f} min vs. team average of {row['avg_response_global']:.1f} min)."
                    )
                    
                    finding_details = {
                        "mechanic_id": mechanic_name,
                        "employee_number": employee_num,
                        "metric": "mechanic_hourly_response",
                        "hour": hour,
                        "value": round(row['avg_response'], 1),
                        "mean_value": round(row['avg_response_global'], 1),
                        "pct_diff": round(row['response_pct_diff'], 1),
                        "threshold": MECHANIC_VARIANCE_THRESHOLD_PCT,
                        "context": f"Mechanic Response Time at Hour {hour}:00"
                    }
                    
                    finding = {
                        "analysis_type": "time_series_mechanic_hourly_response",
                        "finding_summary": finding_summary,
                        "finding_details": finding_details
                    }
                    findings.append(finding)
                    logger.info(f"HOURLY_INTERPRETER: Added mechanic hourly response finding for {mechanic_name} at hour {hour}:00")
            else:
                logger.warning("HOURLY_INTERPRETER: No mechanic-hour combinations with sufficient incidents for analysis")
        except Exception as e:
            logger.error(f"HOURLY_INTERPRETER: Error processing mechanic hourly stats: {e}")
            logger.error(f"HOURLY_INTERPRETER: Error details: {str(e)}")
    else:
        logger.info("HOURLY_INTERPRETER: No mechanic hourly stats available")
    
    # 3. Handle line-specific hourly patterns
    if 'line_hourly_outliers' in hourly_summary and hourly_summary['line_hourly_outliers']:
        line_outliers = hourly_summary['line_hourly_outliers']
        logger.info(f"HOURLY_INTERPRETER: Processing {len(line_outliers)} line hourly outliers")
        
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
            logger.info(f"HOURLY_INTERPRETER: Added line hourly finding for Line {line_id} at hour {hour}:00")
    else:
        logger.info("HOURLY_INTERPRETER: No line hourly outliers found")
        
    logger.info(f"HOURLY_INTERPRETER: Generated {len(findings)} findings from hourly analysis")
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
        logger.info(f"HOURLY_INTERPRETER: Loaded {len(mechanic_dict)} mechanic records")
        return mechanic_dict
    except Exception as e:
        logger.error(f"HOURLY_INTERPRETER: Error loading mechanic data: {e}")
        return {}  # Return empty dict on error

# For testing the interpreter directly
if __name__ == "__main__":
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Test the hourly pattern interpreter")
    parser.add_argument("--input", required=True, help="Path to hourly analysis results JSON file")
    parser.add_argument("--output", help="Path to save findings JSON file (optional)")
    
    args = parser.parse_args()
    
    try:
        with open(args.input, 'r') as f:
            hourly_summary = json.load(f)
        
        findings = interpret_hourly_findings(hourly_summary)
        
        print(f"\nInterpreter generated {len(findings)} findings from hourly analysis")
        
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