"""
Daily Pattern Analysis for Maintenance AI Agent

This script analyzes maintenance data by day of week to identify:
1. Days with higher breakdown frequencies
2. Days with unusual response/repair times
3. Mechanic performance patterns by day

Usage:
    python daily_analysis.py --input maintenance_data.json --output daily_results.json
"""

import os
import json
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
import traceback
from scipy import stats

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run daily pattern analysis on maintenance data.")
    parser.add_argument("--input", type=str, default="maintenance_data.json", help="Input JSON file path")
    parser.add_argument("--output", type=str, default="daily_results.json", help="Output file path")
    parser.add_argument("--start-date", type=str, default="2024-11-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--work-hours-only", action="store_true", help="Analyze only working hours (7:00-17:00)")
    parser.add_argument("--min-incidents", type=int, default=3, help="Minimum incidents for statistical significance")
    parser.add_argument("--z-threshold", type=float, default=1.5, help="Z-score threshold for outlier detection")
    return parser.parse_args()

def load_data(file_path):
    """Load data from JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def convert_to_dataframe(records):
    """Convert JSON records to pandas DataFrame with proper types."""
    df = pd.DataFrame(records)
    
    # Convert timestamp strings to datetime objects
    for col in ['createdAt', 'resolvedAt']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Convert time values from milliseconds to minutes
    time_columns = ['totalDowntime', 'totalRepairTime', 'totalResponseTime']
    for col in time_columns:
        if col in df.columns:
            # Here we assume the times are in milliseconds.
            df[col] = pd.to_numeric(df[col], errors='coerce') / 60000  
    return df

def filter_date_range(df, start_date=None, end_date=None):
    """Filter data to only include records within the specified date range."""
    if start_date and len(df) > 0:
        if df['createdAt'].dt.tz is not None:
            start_dt = pd.to_datetime(start_date).tz_localize('UTC')
        else:
            start_dt = pd.to_datetime(start_date)
        df = df[df['createdAt'] >= start_dt].copy()
        print(f"Filtered to records after {start_date}")
    if end_date and len(df) > 0:
        if df['createdAt'].dt.tz is not None:
            end_dt = pd.to_datetime(end_date).tz_localize('UTC')
        else:
            end_dt = pd.to_datetime(end_date)
        df = df[df['createdAt'] <= end_dt].copy()
        print(f"Filtered to records before {end_date}")
    print(f"Total {len(df)} records within date range")
    return df

def add_time_features(df, work_hours_only=False):
    """Add time-related features to the DataFrame."""
    df['hour'] = df['createdAt'].dt.hour
    df['day_of_week'] = df['createdAt'].dt.dayofweek
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df['day_name'] = df['day_of_week'].apply(lambda x: days[x])
    df['month'] = df['createdAt'].dt.month
    df['month_name'] = df['createdAt'].dt.strftime('%B')
    df['date'] = df['createdAt'].dt.date
    if work_hours_only:
        df = df[(df['hour'] >= 7) & (df['hour'] < 17)].copy()
        print(f"Filtered to working hours (7:00-17:00): {len(df)} records")
    return df

def analyze_daily_breakdown_frequency(df, z_threshold=1.5):
    """Analyze machine breakdown frequency by day of week with statistical analysis."""
    daily_counts = df.groupby(['day_of_week', 'day_name'])['id'].count().reset_index()
    daily_counts.columns = ['day_of_week', 'day_name', 'incident_count']
    total_count = daily_counts['incident_count'].sum()
    daily_counts['pct_of_total'] = (daily_counts['incident_count'] / total_count * 100).round(1)
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    all_days_df = pd.DataFrame({'day_of_week': range(7), 'day_name': days})
    daily_counts = pd.merge(all_days_df, daily_counts, on=['day_of_week', 'day_name'], how='left')
    daily_counts['incident_count'] = daily_counts['incident_count'].fillna(0).astype(int)
    daily_counts['pct_of_total'] = daily_counts['pct_of_total'].fillna(0).round(1)
    weekday_counts = daily_counts[daily_counts['day_of_week'] < 5]
    weekend_counts = daily_counts[daily_counts['day_of_week'] >= 5]
    if not weekday_counts.empty:
        expected_weekday = weekday_counts['incident_count'].sum() / 5
        weekday_counts['expected_count'] = expected_weekday
        weekday_counts['vs_expected_pct'] = ((weekday_counts['incident_count'] / expected_weekday) - 1) * 100
    if not weekend_counts.empty:
        expected_weekend = weekend_counts['incident_count'].sum() / 2
        weekend_counts['expected_count'] = expected_weekend
        weekend_counts['vs_expected_pct'] = ((weekend_counts['incident_count'] / expected_weekend) - 1) * 100
    daily_counts = pd.concat([weekday_counts, weekend_counts]).sort_values('day_of_week')
    daily_counts['expected_count'] = daily_counts['expected_count'].fillna(0)
    daily_counts['vs_expected_pct'] = daily_counts['vs_expected_pct'].fillna(0)
    if len(daily_counts[daily_counts['incident_count'] > 0]) >= 3:
        days_with_data = daily_counts[daily_counts['incident_count'] > 0]
        mean_count = days_with_data['incident_count'].mean()
        std_count = days_with_data['incident_count'].std()
        if std_count > 0:
            daily_counts['z_score'] = (daily_counts['incident_count'] - mean_count) / std_count
        else:
            daily_counts['z_score'] = 0
    else:
        daily_counts['z_score'] = 0
    daily_counts['is_outlier'] = abs(daily_counts['z_score']) > z_threshold
    daily_counts['trend_direction'] = np.where(
        daily_counts['incident_count'] > daily_counts['expected_count'], 
        'HIGHER', 
        np.where(daily_counts['incident_count'] < daily_counts['expected_count'], 'LOWER', 'NORMAL')
    )
    daily_counts['significance'] = np.where(daily_counts['is_outlier'], 'SIGNIFICANT', 'NORMAL')
    daily_counts = daily_counts.sort_values('day_of_week')
    peak_days = daily_counts.sort_values('incident_count', ascending=False).head(3)
    outlier_days = daily_counts[daily_counts['is_outlier']]
    return {
        'daily_breakdown_counts': daily_counts.to_dict('records'),
        'peak_breakdown_days': peak_days.to_dict('records'),
        'statistical_outliers': outlier_days.to_dict('records'),
        'total_breakdowns': int(total_count),
        'weekday_vs_weekend': {
            'weekday_count': int(weekday_counts['incident_count'].sum() if not weekday_counts.empty else 0),
            'weekend_count': int(weekend_counts['incident_count'].sum() if not weekend_counts.empty else 0),
            'weekday_pct': float(weekday_counts['incident_count'].sum() / total_count * 100 if not weekday_counts.empty and total_count > 0 else 0),
            'weekend_pct': float(weekend_counts['incident_count'].sum() / total_count * 100 if not weekend_counts.empty and total_count > 0 else 0)
        }
    }

def analyze_daily_response_repair_times(df, min_incidents=3, z_threshold=1.5):
    """Analyze response and repair times by day of week with statistical analysis."""
    daily_times = df.groupby(['day_of_week', 'day_name']).agg({
        'id': 'count',
        'totalResponseTime': ['mean', 'median', 'std', 'min', 'max'],
        'totalRepairTime': ['mean', 'median', 'std', 'min', 'max']
    }).reset_index()
    daily_times.columns = [
        'day_of_week', 'day_name', 'incident_count', 
        'avg_response_time', 'median_response_time', 'std_response_time', 'min_response_time', 'max_response_time',
        'avg_repair_time', 'median_repair_time', 'std_repair_time', 'min_repair_time', 'max_repair_time'
    ]
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    all_days_df = pd.DataFrame({'day_of_week': range(7), 'day_name': days})
    daily_times = pd.merge(all_days_df, daily_times, on=['day_of_week', 'day_name'], how='left')
    daily_times['incident_count'] = daily_times['incident_count'].fillna(0).astype(int)
    valid_response_data = daily_times[daily_times['incident_count'] >= min_incidents]
    global_avg_response = valid_response_data['avg_response_time'].mean() if not valid_response_data.empty else None
    global_avg_repair = valid_response_data['avg_repair_time'].mean() if not valid_response_data.empty else None
    if global_avg_response is not None and global_avg_repair is not None:
        mask = daily_times['incident_count'] >= min_incidents
        daily_times.loc[mask, 'response_vs_global_pct'] = ((daily_times.loc[mask, 'avg_response_time'] / global_avg_response) - 1) * 100
        daily_times.loc[mask, 'repair_vs_global_pct'] = ((daily_times.loc[mask, 'avg_repair_time'] / global_avg_repair) - 1) * 100
    daily_times_filtered = daily_times[daily_times['incident_count'] >= min_incidents].copy()
    if len(daily_times_filtered) >= 3:
        mean_response = daily_times_filtered['avg_response_time'].mean()
        std_response = daily_times_filtered['avg_response_time'].std()
        if std_response > 0:
            daily_times_filtered['response_z_score'] = (daily_times_filtered['avg_response_time'] - mean_response) / std_response
        else:
            daily_times_filtered['response_z_score'] = 0
        mean_repair = daily_times_filtered['avg_repair_time'].mean()
        std_repair = daily_times_filtered['avg_repair_time'].std()
        if std_repair > 0:
            daily_times_filtered['repair_z_score'] = (daily_times_filtered['avg_repair_time'] - mean_repair) / std_repair
        else:
            daily_times_filtered['repair_z_score'] = 0
    else:
        daily_times_filtered['response_z_score'] = 0
        daily_times_filtered['repair_z_score'] = 0
    daily_times_filtered['response_is_outlier'] = abs(daily_times_filtered['response_z_score']) > z_threshold
    daily_times_filtered['repair_is_outlier'] = abs(daily_times_filtered['repair_z_score']) > z_threshold
    daily_times_filtered['response_trend'] = np.where(
        daily_times_filtered['avg_response_time'] > mean_response if 'mean_response' in locals() else 0, 
        'SLOWER', 
        'FASTER'
    )
    daily_times_filtered['repair_trend'] = np.where(
        daily_times_filtered['avg_repair_time'] > mean_repair if 'mean_repair' in locals() else 0, 
        'SLOWER', 
        'FASTER'
    )
    daily_times_filtered['response_significance'] = np.where(
        daily_times_filtered['response_is_outlier'],
        'SIGNIFICANT',
        'NORMAL'
    )
    daily_times_filtered['repair_significance'] = np.where(
        daily_times_filtered['repair_is_outlier'],
        'SIGNIFICANT',
        'NORMAL'
    )
    daily_times = daily_times.sort_values('day_of_week')
    daily_times_filtered = daily_times_filtered.sort_values('day_of_week')
    response_outliers = daily_times_filtered[daily_times_filtered['response_is_outlier']]
    repair_outliers = daily_times_filtered[daily_times_filtered['repair_is_outlier']]
    slowest_response_days = daily_times_filtered.sort_values('avg_response_time', ascending=False).head(2)
    fastest_response_days = daily_times_filtered.sort_values('avg_response_time').head(2)
    slowest_repair_days = daily_times_filtered.sort_values('avg_repair_time', ascending=False).head(2)
    fastest_repair_days = daily_times_filtered.sort_values('avg_repair_time').head(2)
    return {
        'daily_response_repair_times': daily_times.to_dict('records'),
        'daily_times_filtered': daily_times_filtered.to_dict('records'),
        'response_outliers': response_outliers.to_dict('records'),
        'repair_outliers': repair_outliers.to_dict('records'),
        'slowest_response_days': slowest_response_days.to_dict('records'),
        'fastest_response_days': fastest_response_days.to_dict('records'),
        'slowest_repair_days': slowest_repair_days.to_dict('records'),
        'fastest_repair_days': fastest_repair_days.to_dict('records'),
        'global_avg_response_time': float(global_avg_response) if global_avg_response is not None else None,
        'global_avg_repair_time': float(global_avg_repair) if global_avg_repair is not None else None
    }

def analyze_mechanic_daily_performance(df, min_incidents=3, z_threshold=1.5):
    """Analyze mechanic performance by day of week with statistical analysis."""
    if 'mechanicId' not in df.columns or df['mechanicId'].isna().all():
        return { 'error': 'No mechanic data available for analysis' }
    mechanic_overall = df.groupby(['mechanicId', 'mechanicName']).agg({
        'id': 'count',
        'totalResponseTime': 'mean',
        'totalRepairTime': 'mean'
    }).reset_index()
    mechanic_overall.columns = [
        'mechanic_id', 'mechanic_name', 'total_incidents',
        'avg_response_time', 'avg_repair_time'
    ]
    global_avg_response = df['totalResponseTime'].mean()
    global_avg_repair = df['totalRepairTime'].mean()
    mechanic_overall['response_vs_global_pct'] = ((mechanic_overall['avg_response_time'] / global_avg_response) - 1) * 100
    mechanic_overall['repair_vs_global_pct'] = ((mechanic_overall['avg_repair_time'] / global_avg_repair) - 1) * 100
    mechanic_daily = df.groupby(['mechanicId', 'mechanicName', 'day_of_week', 'day_name']).agg({
        'id': 'count',
        'totalResponseTime': 'mean',
        'totalRepairTime': 'mean'
    }).reset_index()
    mechanic_daily.columns = [
        'mechanic_id', 'mechanic_name', 'day_of_week', 'day_name', 
        'incident_count', 'avg_response_time', 'avg_repair_time'
    ]
    mechanic_daily_filtered = mechanic_daily[mechanic_daily['incident_count'] >= min_incidents].copy()
    mechanic_daily_analysis = []
    for mechanic_id in mechanic_overall['mechanic_id'].unique():
        mech_data = mechanic_overall[mechanic_overall['mechanic_id'] == mechanic_id]
        if len(mech_data) == 0:
            continue
        mech_data = mech_data.iloc[0]
        mechanic_name = mech_data['mechanic_name']
        mech_avg_response = mech_data['avg_response_time']
        mech_avg_repair = mech_data['avg_repair_time']
        daily_data = mechanic_daily_filtered[mechanic_daily_filtered['mechanic_id'] == mechanic_id]
        if len(daily_data) < 2:
            continue
        if len(daily_data) >= 3:
            mean_response = daily_data['avg_response_time'].mean()
            std_response = daily_data['avg_response_time'].std()
            if std_response > 0:
                daily_data['response_z_score'] = (daily_data['avg_response_time'] - mean_response) / std_response
            else:
                daily_data['response_z_score'] = 0
            mean_repair = daily_data['avg_repair_time'].mean()
            std_repair = daily_data['avg_repair_time'].std()
            if std_repair > 0:
                daily_data['repair_z_score'] = (daily_data['avg_repair_time'] - mean_repair) / std_repair
            else:
                daily_data['repair_z_score'] = 0
        else:
            daily_data['response_z_score'] = 0
            daily_data['repair_z_score'] = 0
        daily_data['response_vs_self_pct'] = ((daily_data['avg_response_time'] / mech_avg_response) - 1) * 100
        daily_data['repair_vs_self_pct'] = ((daily_data['avg_repair_time'] / mech_avg_repair) - 1) * 100
        daily_data['response_is_outlier'] = abs(daily_data['response_z_score']) > z_threshold
        daily_data['repair_is_outlier'] = abs(daily_data['repair_z_score']) > z_threshold
        daily_data['response_trend'] = np.where(
            daily_data['avg_response_time'] > mech_avg_response, 
            'SLOWER', 
            'FASTER'
        )
        daily_data['repair_trend'] = np.where(
            daily_data['avg_repair_time'] > mech_avg_repair, 
            'SLOWER', 
            'FASTER'
        )
        daily_data['response_significance'] = np.where(
            daily_data['response_is_outlier'],
            'SIGNIFICANT',
            'NORMAL'
        )
        daily_data['repair_significance'] = np.where(
            daily_data['repair_is_outlier'],
            'SIGNIFICANT',
            'NORMAL'
        )
        for _, row in daily_data.iterrows():
            mechanic_daily_analysis.append({
                'mechanic_id': mechanic_id,
                'mechanic_name': mechanic_name,
                'day_of_week': int(row['day_of_week']),
                'day_name': row['day_name'],
                'incident_count': int(row['incident_count']),
                'avg_response_time': float(row['avg_response_time']),
                'avg_repair_time': float(row['avg_repair_time']),
                'response_vs_self_pct': float(row['response_vs_self_pct']),
                'repair_vs_self_pct': float(row['repair_vs_self_pct']),
                'response_z_score': float(row['response_z_score']) if 'response_z_score' in row else 0,
                'repair_z_score': float(row['repair_z_score']) if 'repair_z_score' in row else 0,
                'response_is_outlier': bool(row['response_is_outlier']) if 'response_is_outlier' in row else False,
                'repair_is_outlier': bool(row['repair_is_outlier']) if 'repair_is_outlier' in row else False,
                'response_trend': row['response_trend'] if 'response_trend' in row else '',
                'repair_trend': row['repair_trend'] if 'repair_trend' in row else '',
                'response_significance': row['response_significance'] if 'response_significance' in row else '',
                'repair_significance': row['repair_significance'] if 'repair_significance' in row else ''
            })
    significant_variations = [
        item for item in mechanic_daily_analysis
        if (abs(item['response_vs_self_pct']) > 30 or abs(item['repair_vs_self_pct']) > 30)
        and item['incident_count'] >= min_incidents
    ]
    significant_variations.sort(key=lambda x: max(abs(x['response_vs_self_pct']), abs(x['repair_vs_self_pct'])), reverse=True)
    day_consistency = {}
    for day in range(7):
        day_data = [item for item in mechanic_daily_analysis if item['day_of_week'] == day]
        if len(day_data) >= 3:
            response_variation = np.std([item['avg_response_time'] for item in day_data])
            repair_variation = np.std([item['avg_repair_time'] for item in day_data])
            day_consistency[day] = {
                'day_of_week': day,
                'day_name': day_data[0]['day_name'],
                'mechanics_count': len(day_data),
                'response_std_dev': float(response_variation),
                'repair_std_dev': float(repair_variation),
                'response_cv': float(response_variation / np.mean([item['avg_response_time'] for item in day_data]) if np.mean([item['avg_response_time'] for item in day_data]) > 0 else 0),
                'repair_cv': float(repair_variation / np.mean([item['avg_repair_time'] for item in day_data]) if np.mean([item['avg_repair_time'] for item in day_data]) > 0 else 0)
            }
    consistent_days = sorted(day_consistency.values(), key=lambda x: x['response_cv'])
    inconsistent_days = sorted(day_consistency.values(), key=lambda x: x['response_cv'], reverse=True)
    return {
        'mechanic_overall_stats': mechanic_overall.to_dict('records'),
        'mechanic_daily_stats': mechanic_daily.to_dict('records'),
        'mechanic_daily_filtered': mechanic_daily_filtered.to_dict('records'),
        'mechanic_daily_analysis': mechanic_daily_analysis,
        'significant_variations': significant_variations,
        'most_consistent_days': consistent_days[:2] if consistent_days else [],
        'most_inconsistent_days': inconsistent_days[:2] if inconsistent_days else []
    }

def run_daily_analysis(data, start_date=None, end_date=None, work_hours_only=False, min_incidents=3, z_threshold=1.5):
    """Run comprehensive daily pattern analysis."""
    try:
        print("Starting daily pattern analysis...")
        print("Converting data to DataFrame...")
        df = convert_to_dataframe(data)
        if 'createdAt' in df.columns and not df['createdAt'].empty:
            print(f"Date column timezone: {df['createdAt'].dt.tz}")
            print(f"Date range in data: {df['createdAt'].min()} to {df['createdAt'].max()}")
        if start_date or end_date:
            df = filter_date_range(df, start_date, end_date)
        if len(df) < 10:
            return {
                "error": "Not enough data for meaningful daily analysis (need at least 10 records)",
                "record_count": len(df)
            }
        df = add_time_features(df, work_hours_only)
        if len(df) < 10:
            return {
                "error": "Not enough data after filtering for meaningful analysis",
                "record_count": len(df),
                "work_hours_only": work_hours_only
            }
        print("Analyzing daily breakdown frequency...")
        breakdown_results = analyze_daily_breakdown_frequency(df, z_threshold)
        print("Analyzing daily response and repair times...")
        time_results = analyze_daily_response_repair_times(df, min_incidents, z_threshold)
        print("Analyzing mechanic performance by day...")
        mechanic_results = analyze_mechanic_daily_performance(df, min_incidents, z_threshold)
        results = {
            "record_count": len(df),
            "date_range": {
                "start": df['createdAt'].min().isoformat() if not df['createdAt'].empty else None,
                "end": df['createdAt'].max().isoformat() if not df['createdAt'].empty else None
            },
            "analysis_parameters": {
                "work_hours_only": work_hours_only,
                "min_incidents_for_comparison": min_incidents,
                "z_score_threshold": z_threshold
            },
            "daily_breakdown_frequency": breakdown_results,
            "daily_response_repair_times": time_results,
            "mechanic_daily_performance": mechanic_results
        }
        return results
    except Exception as e:
        print(f"Error in analysis: {str(e)}")
        traceback.print_exc()
        return {
            "error": f"Error in analysis: {str(e)}",
            "traceback": traceback.format_exc()
        }

# ============================
# Main block:
# Run the analysis, print a summary of the daily breakdown (including outlier information),
# and save the full results to a JSON file.
# ============================

if __name__ == "__main__":
    args = parse_arguments()
    data = load_data(args.input)
    results = run_daily_analysis(
        data,
        start_date=args.start_date,
        end_date=args.end_date,
        work_hours_only=args.work_hours_only,
        min_incidents=args.min_incidents,
        z_threshold=args.z_threshold
    )
    
    # Print a summary of daily breakdown counts to the terminal
    print("\nDaily Breakdown Summary:")
    if "daily_breakdown_frequency" in results:
        breakdown = results["daily_breakdown_frequency"]
        for day in breakdown.get("daily_breakdown_counts", []):
            print(f"{day['day_name']}: {day['incident_count']} incidents ({day['pct_of_total']}% of total incidents)")
        
        # Additionally, print outlier days (e.g., if Thursday is an outlier)
        print("\nOutlier Days in Daily Breakdown:")
        outliers = breakdown.get("statistical_outliers", [])
        if outliers:
            for day in outliers:
                # Print day name, incident count, and z score (if present)
                z_score = day.get('z_score', None)
                if z_score is not None:
                    print(f"{day['day_name']} is an outlier with z-score {z_score:.2f} and {day['incident_count']} incidents.")
                else:
                    print(f"{day['day_name']} is an outlier with {day['incident_count']} incidents.")
        else:
            print("No outlier days found.")
    else:
        print("No daily breakdown data available.")
    
    # Save the complete analysis results to JSON file
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nFull analysis results have been saved to: {args.output}")
