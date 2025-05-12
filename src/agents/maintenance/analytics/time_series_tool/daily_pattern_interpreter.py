# src/agents/maintenance/analytics/time_series_tool/daily_pattern_interpreter.py
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
LINE_VARIANCE_THRESHOLD_PCT = 25.0  # Flag line-specific patterns with variance > 25%
MECHANIC_VARIANCE_THRESHOLD_PCT = 25.0  # Flag mechanic-specific patterns with variance > 25%

def interpret_daily_findings(daily_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Interpret daily analysis results and create findings
    
    Args:
        daily_summary: Dictionary with daily analysis results
        
    Returns:
        list: Findings generated from daily analysis
    """
    findings = []
    
    # Check if we have valid data
    if not daily_summary:
        logger.warning("No daily summary data to interpret")
        return findings
        
    logger.info(f"DAILY_INTERPRETER: Interpreting daily summary with keys: {list(daily_summary.keys())}")
    
    # 1. Handle statistical outliers (days with unusually high incident counts)
    if 'statistical_outliers' in daily_summary and daily_summary['statistical_outliers']:
        outliers = daily_summary['statistical_outliers']
        logger.info(f"DAILY_INTERPRETER: Found {len(outliers)} daily statistical outliers")
        
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
                logger.info(f"DAILY_INTERPRETER: Added daily pattern finding for {day_name}")
    else:
        logger.info("DAILY_INTERPRETER: No daily statistical outliers found")
    
    # 2. Handle peak days
    if 'peak_breakdown_days' in daily_summary and daily_summary['peak_breakdown_days']:
        peak_days = daily_summary['peak_breakdown_days']
        logger.info(f"DAILY_INTERPRETER: Found {len(peak_days)} peak days")
        
        for peak_day in peak_days:
            day_name = peak_day.get('day_name')
            count = peak_day.get('incident_count')
            pct_of_total = peak_day.get('pct_of_total', 0)
            vs_expected_pct = peak_day.get('vs_expected_pct', 0)
            
            # Only include meaningful peaks (at least 15% of total or 20% above expected)
            if pct_of_total >= 15 or (vs_expected_pct is not None and vs_expected_pct >= 20):
                finding_summary = (
                    f"PEAK DAY ALERT: {day_name} has {count} incidents "
                    f"({pct_of_total:.1f}% of total)"
                )
                
                if vs_expected_pct is not None:
                    finding_summary += f", {vs_expected_pct:.1f}% higher than expected for this day type."
                else:
                    finding_summary += "."
                
                finding_details = {
                    "metric": "peak_day",
                    "day_name": day_name,
                    "incident_count": count,
                    "pct_of_total": pct_of_total,
                    "vs_expected_pct": vs_expected_pct,
                    "context": "Peak Day Analysis"
                }
                
                finding = {
                    "analysis_type": "time_series_peak_day",
                    "finding_summary": finding_summary,
                    "finding_details": finding_details
                }
                findings.append(finding)
                logger.info(f"DAILY_INTERPRETER: Added peak day finding for {day_name}")
    else:
        logger.info("DAILY_INTERPRETER: No peak days found")
    
    # 3. Handle mechanic daily performance patterns
    if 'mechanic_daily_stats' in daily_summary and daily_summary['mechanic_daily_stats']:
        try:
            mech_stats = daily_summary['mechanic_daily_stats']
            logger.info(f"DAILY_INTERPRETER: Processing {len(mech_stats)} mechanic daily stats")
            
            # Convert to DataFrame for easier analysis
            mech_df = pd.DataFrame(mech_stats)
            
            # Check required columns
            required_cols = ['mechanicId', 'mechanicName', 'day_of_week', 'avg_repair', 'avg_response', 'incident_count']
            if not all(col in mech_df.columns for col in required_cols):
                # Try alternate field names
                rename_map = {
                    'avg_downtime': 'avg_repair',  # In case field names differ
                    'mechanicId': 'mechanic_id',
                    'mechanicName': 'mechanic_name',
                    'count': 'incident_count',
                    'day_name': 'day_name'
                }
                mech_df = mech_df.rename(columns={k: v for k, v in rename_map.items() if k in mech_df.columns})
                logger.warning(f"DAILY_INTERPRETER: Some columns missing or renamed. Available columns: {list(mech_df.columns)}")
            
            # Ensure we have day_name
            if 'day_name' not in mech_df.columns and 'day_of_week' in mech_df.columns:
                # Map day_of_week to day_name
                day_names = {
                    0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 
                    3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'
                }
                mech_df['day_name'] = mech_df['day_of_week'].map(lambda x: day_names.get(x, f"Day {x}"))
            
            # Filter to rows with enough incidents
            min_incidents = 2
            filtered_mech_df = mech_df[mech_df['incident_count'] >= min_incidents].copy()
            logger.info(f"DAILY_INTERPRETER: Filtered from {len(mech_df)} to {len(filtered_mech_df)} mechanic-day stats with {min_incidents}+ incidents")
            
            if not filtered_mech_df.empty:
                # Calculate global average repair and response times per day
                day_avgs = filtered_mech_df.groupby('day_of_week').agg({
                    'avg_repair': 'mean',
                    'avg_response': 'mean',
                    'day_name': 'first'  # Keep day name for reference
                }).reset_index()
                
                # Merge back to get global averages
                merged_df = pd.merge(
                    filtered_mech_df,
                    day_avgs,
                    on='day_of_week',
                    suffixes=('', '_global')
                )
                
                # Calculate percentage differences
                merged_df['repair_pct_diff'] = ((merged_df['avg_repair'] / merged_df['avg_repair_global']) - 1) * 100
                merged_df['response_pct_diff'] = ((merged_df['avg_response'] / merged_df['avg_response_global']) - 1) * 100
                
                # Find outliers
                mechanic_variance_threshold = MECHANIC_VARIANCE_THRESHOLD_PCT
                repair_outliers = merged_df[merged_df['repair_pct_diff'] > mechanic_variance_threshold]
                response_outliers = merged_df[merged_df['response_pct_diff'] > mechanic_variance_threshold]
                
                logger.info(f"DAILY_INTERPRETER: Found {len(repair_outliers)} repair time outliers and {len(response_outliers)} response time outliers")
                
                # Get mechanic info from database (employee numbers)
                mechanic_dict = get_mechanic_info()
                
                # Convert repair outliers to findings
                for _, row in repair_outliers.iterrows():
                    mechanic_name = row['mechanicName']
                    mechanic_id = row['mechanicId']
                    employee_num = mechanic_dict.get(mechanic_name, mechanic_id)
                    day_name = row['day_name']
                    
                    finding_summary = (
                        f"MECHANIC DAILY REPAIR: {mechanic_name} (#{employee_num}) has {row['repair_pct_diff']:.1f}% "
                        f"longer repair times on {day_name} ({row['avg_repair']:.1f} min vs. team average of {row['avg_repair_global']:.1f} min)."
                    )
                    
                    finding_details = {
                        "mechanic_id": mechanic_name,
                        "employee_number": employee_num,
                        "metric": "mechanic_daily_repair",
                        "day_of_week": int(row['day_of_week']),
                        "day_name": day_name,
                        "value": round(row['avg_repair'], 1),
                        "mean_value": round(row['avg_repair_global'], 1),
                        "pct_diff": round(row['repair_pct_diff'], 1),
                        "threshold": mechanic_variance_threshold,
                        "context": f"Mechanic Repair Time on {day_name}"
                    }
                    
                    finding = {
                        "analysis_type": "time_series_mechanic_daily_repair",
                        "finding_summary": finding_summary,
                        "finding_details": finding_details
                    }
                    findings.append(finding)
                    logger.info(f"DAILY_INTERPRETER: Added mechanic daily repair finding for {mechanic_name} on {day_name}")
                
                # Convert response outliers to findings
                for _, row in response_outliers.iterrows():
                    mechanic_name = row['mechanicName']
                    mechanic_id = row['mechanicId']
                    employee_num = mechanic_dict.get(mechanic_name, mechanic_id)
                    day_name = row['day_name']
                    
                    finding_summary = (
                        f"MECHANIC DAILY RESPONSE: {mechanic_name} (#{employee_num}) has {row['response_pct_diff']:.1f}% "
                        f"longer response times on {day_name} ({row['avg_response']:.1f} min vs. team average of {row['avg_response_global']:.1f} min)."
                    )
                    
                    finding_details = {
                        "mechanic_id": mechanic_name,
                        "employee_number": employee_num,
                        "metric": "mechanic_daily_response",
                        "day_of_week": int(row['day_of_week']),
                        "day_name": day_name,
                        "value": round(row['avg_response'], 1),
                        "mean_value": round(row['avg_response_global'], 1),
                        "pct_diff": round(row['response_pct_diff'], 1),
                        "threshold": mechanic_variance_threshold,
                        "context": f"Mechanic Response Time on {day_name}"
                    }
                    
                    finding = {
                        "analysis_type": "time_series_mechanic_daily_response",
                        "finding_summary": finding_summary,
                        "finding_details": finding_details
                    }
                    findings.append(finding)
                    logger.info(f"DAILY_INTERPRETER: Added mechanic daily response finding for {mechanic_name} on {day_name}")
            else:
                logger.warning("DAILY_INTERPRETER: No mechanic-day combinations with sufficient incidents for analysis")
        except Exception as e:
            logger.error(f"DAILY_INTERPRETER: Error processing mechanic daily stats: {e}")
            logger.error(f"DAILY_INTERPRETER: Error details: {str(e)}")
    else:
        logger.info("DAILY_INTERPRETER: No mechanic daily stats available")
    
    # 4. Handle line-specific daily patterns
    if 'line_daily_outliers' in daily_summary and daily_summary['line_daily_outliers']:
        line_outliers = daily_summary['line_daily_outliers']
        logger.info(f"DAILY_INTERPRETER: Processing {len(line_outliers)} line daily outliers")
        
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
            logger.info(f"DAILY_INTERPRETER: Added line daily finding for Line {line_id} on {day_name}")
    else:
        logger.info("DAILY_INTERPRETER: No line daily outliers found")
        
    logger.info(f"DAILY_INTERPRETER: Generated {len(findings)} findings from daily analysis")
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
        logger.info(f"DAILY_INTERPRETER: Loaded {len(mechanic_dict)} mechanic records")
        return mechanic_dict
    except Exception as e:
        logger.error(f"DAILY_INTERPRETER: Error loading mechanic data: {e}")
        return {}  # Return empty dict on error

# For testing the interpreter directly
if __name__ == "__main__":
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Test the daily pattern interpreter")
    parser.add_argument("--input", required=True, help="Path to daily analysis results JSON file")
    parser.add_argument("--output", help="Path to save findings JSON file (optional)")
    
    args = parser.parse_args()
    
    try:
        with open(args.input, 'r') as f:
            daily_summary = json.load(f)
        
        findings = interpret_daily_findings(daily_summary)
        
        print(f"\nInterpreter generated {len(findings)} findings from daily analysis")
        
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