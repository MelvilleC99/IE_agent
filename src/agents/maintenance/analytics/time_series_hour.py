"""
Working Hours Analysis for Maintenance AI Agent

This script provides comprehensive hourly analysis focused on standard working hours (7:00 AM to 5:00 PM):
1. Breakdown frequency by hour
2. Response and repair times by hour
3. Mechanic performance by hour

The working hours can be easily adjusted by changing the parameters:
--work-hours-start and --work-hours-end

Usage:
1. Run with default working hours (7:00 AM - 5:00 PM):
   python working_hours_analysis.py --input maintenance_data.json --output hourly_results.json

2. Run with custom working hours:
   python working_hours_analysis.py --work-hours-start 6 --work-hours-end 22 --input maintenance_data.json
"""

import os
import json
import argparse
import pandas as pd
from datetime import datetime
import traceback

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run working hours analysis on maintenance data.")
    parser.add_argument("--input", type=str, default="maintenance_data.json", help="Input JSON file path")
    parser.add_argument("--output", type=str, default="hourly_results.json", help="Output file path")
    parser.add_argument("--start-date", type=str, default="2024-11-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--work-hours-start", type=int, default=7, help="Start of working hours (24-hour format)")
    parser.add_argument("--work-hours-end", type=int, default=17, help="End of working hours (24-hour format)")
    parser.add_argument("--min-incidents", type=int, default=2, help="Minimum incidents for comparison")
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
            df[col] = pd.to_numeric(df[col], errors='coerce') / 60000  # Convert to minutes
    
    return df

def filter_date_range(df, start_date=None, end_date=None):
    """Filter data to only include records within the specified date range."""
    if start_date and len(df) > 0:
        # Convert to timezone-aware datetime using the same timezone as the DataFrame
        if df['createdAt'].dt.tz is not None:
            start_dt = pd.to_datetime(start_date).tz_localize('UTC')
        else:
            start_dt = pd.to_datetime(start_date)
            
        df = df[df['createdAt'] >= start_dt].copy()
        print(f"Filtered to records after {start_date}")
    
    if end_date and len(df) > 0:
        # Convert to timezone-aware datetime using the same timezone as the DataFrame
        if df['createdAt'].dt.tz is not None:
            end_dt = pd.to_datetime(end_date).tz_localize('UTC')
        else:
            end_dt = pd.to_datetime(end_date)
            
        df = df[df['createdAt'] <= end_dt].copy()
        print(f"Filtered to records before {end_date}")
    
    print(f"Total {len(df)} records within date range")
    return df

def add_time_features(df):
    """Add time-related features to the DataFrame."""
    # Add hour of day
    df['hour'] = df['createdAt'].dt.hour
    return df

def filter_working_hours(df, work_hours_start, work_hours_end):
    """Filter data to only include records within working hours."""
    # Create a working hours flag
    df['is_work_hours'] = df['hour'].between(work_hours_start, work_hours_end - 1)
    
    # Filter to only include working hours
    df_working = df[df['is_work_hours']].copy()
    
    print(f"Filtered from {len(df)} total records to {len(df_working)} records during working hours ({work_hours_start}:00-{work_hours_end}:00)")
    return df_working

def analyze_hourly_breakdown_frequency(df, work_hours_start, work_hours_end):
    """Analyze machine breakdown frequency by hour within working hours."""
    # Count incidents by hour
    hourly_counts = df.groupby('hour')['id'].count().reset_index()
    hourly_counts.columns = ['hour', 'incident_count']
    
    # Calculate percentages
    total_count = hourly_counts['incident_count'].sum()
    hourly_counts['pct_of_total'] = (hourly_counts['incident_count'] / total_count * 100).round(1)
    
    # Add hour labels for readability
    hourly_counts['hour_label'] = hourly_counts['hour'].apply(lambda h: f"{h:02d}:00-{(h+1)%24:02d}:00")
    
    # Ensure all working hours are represented (including zeros)
    all_hours = pd.DataFrame({'hour': range(work_hours_start, work_hours_end)})
    hourly_counts = pd.merge(all_hours, hourly_counts, on='hour', how='left').fillna(0)
    hourly_counts['hour_label'] = hourly_counts['hour'].apply(lambda h: f"{h:02d}:00-{(h+1)%24:02d}:00")
    
    # Sort by hour for readability
    hourly_counts = hourly_counts.sort_values('hour')
    
    # Find peak hours
    peak_hours = hourly_counts.sort_values('incident_count', ascending=False).head(3)
    
    # Find hours with no incidents
    zero_incident_hours = hourly_counts[hourly_counts['incident_count'] == 0]
    
    return {
        'hourly_breakdown_counts': hourly_counts.to_dict('records'),
        'peak_breakdown_hours': peak_hours.to_dict('records'),
        'zero_incident_hours': zero_incident_hours.to_dict('records'),
        'total_breakdowns': int(total_count),
        'working_hours': f"{work_hours_start}:00-{work_hours_end}:00"
    }

def analyze_hourly_response_repair_times(df, min_incidents, work_hours_start, work_hours_end):
    """Analyze response and repair times by hour within working hours."""
    # Calculate hourly averages
    hourly_times = df.groupby('hour').agg({
        'id': 'count',
        'totalResponseTime': ['mean', 'median', 'std', 'min', 'max'],
        'totalRepairTime': ['mean', 'median', 'std', 'min', 'max']
    }).reset_index()
    
    # Flatten the column names
    hourly_times.columns = [
        'hour', 'incident_count', 
        'avg_response_time', 'median_response_time', 'std_response_time', 'min_response_time', 'max_response_time',
        'avg_repair_time', 'median_repair_time', 'std_repair_time', 'min_repair_time', 'max_repair_time'
    ]
    
    # Add hour labels
    hourly_times['hour_label'] = hourly_times['hour'].apply(lambda h: f"{h:02d}:00-{(h+1)%24:02d}:00")
    
    # Ensure all working hours are represented
    all_hours = pd.DataFrame({'hour': range(work_hours_start, work_hours_end)})
    hourly_times = pd.merge(all_hours, hourly_times, on='hour', how='left').fillna(0)
    hourly_times['hour_label'] = hourly_times['hour'].apply(lambda h: f"{h:02d}:00-{(h+1)%24:02d}:00")
    
    # Sort by hour
    hourly_times = hourly_times.sort_values('hour')
    
    # Calculate global averages
    global_avg_response = df['totalResponseTime'].mean()
    global_avg_repair = df['totalRepairTime'].mean()
    
    # Filter for meaningful comparisons (enough incidents)
    hourly_times_filtered = hourly_times[hourly_times['incident_count'] >= min_incidents].copy()
    
    # Calculate difference from average (percentage)
    for idx, row in hourly_times_filtered.iterrows():
        if global_avg_response > 0:
            hourly_times_filtered.at[idx, 'response_vs_avg_pct'] = ((row['avg_response_time'] / global_avg_response) - 1) * 100
        else:
            hourly_times_filtered.at[idx, 'response_vs_avg_pct'] = 0
            
        if global_avg_repair > 0:
            hourly_times_filtered.at[idx, 'repair_vs_avg_pct'] = ((row['avg_repair_time'] / global_avg_repair) - 1) * 100
        else:
            hourly_times_filtered.at[idx, 'repair_vs_avg_pct'] = 0
    
    # Sort for rankings
    response_ranking = hourly_times_filtered.sort_values('avg_response_time').reset_index(drop=True)
    response_ranking['response_time_rank'] = response_ranking.index + 1
    
    repair_ranking = hourly_times_filtered.sort_values('avg_repair_time').reset_index(drop=True)
    repair_ranking['repair_time_rank'] = repair_ranking.index + 1
    
    # Find slowest and fastest hours
    slowest_response_hours = response_ranking.sort_values('avg_response_time', ascending=False).head(3)
    fastest_response_hours = response_ranking.sort_values('avg_response_time').head(3)
    
    slowest_repair_hours = repair_ranking.sort_values('avg_repair_time', ascending=False).head(3)
    fastest_repair_hours = repair_ranking.sort_values('avg_repair_time').head(3)
    
    return {
        'hourly_response_repair_times': hourly_times.to_dict('records'),
        'hourly_filtered': hourly_times_filtered.to_dict('records'),
        'response_time_ranking': response_ranking.to_dict('records'),
        'repair_time_ranking': repair_ranking.to_dict('records'),
        'slowest_response_hours': slowest_response_hours.to_dict('records'),
        'fastest_response_hours': fastest_response_hours.to_dict('records'),
        'slowest_repair_hours': slowest_repair_hours.to_dict('records'),
        'fastest_repair_hours': fastest_repair_hours.to_dict('records'),
        'global_avg_response_time': float(global_avg_response),
        'global_avg_repair_time': float(global_avg_repair),
        'working_hours': f"{work_hours_start}:00-{work_hours_end}:00"
    }

def analyze_mechanic_hourly_performance(df, min_incidents, work_hours_start, work_hours_end):
    """Analyze mechanic performance by hour within working hours."""
    if 'mechanicId' not in df.columns or df['mechanicId'].isna().all():
        return {
            'error': 'No mechanic data available for analysis'
        }
        
    # Calculate overall mechanic metrics
    mechanic_overall = df.groupby(['mechanicId', 'mechanicName']).agg({
        'id': 'count',
        'totalResponseTime': ['mean', 'median', 'std', 'min', 'max'],
        'totalRepairTime': ['mean', 'median', 'std', 'min', 'max']
    }).reset_index()
    
    # Flatten column names
    mechanic_overall.columns = [
        'mechanic_id', 'mechanic_name', 'total_incidents',
        'avg_response_time', 'median_response_time', 'std_response_time', 'min_response_time', 'max_response_time',
        'avg_repair_time', 'median_repair_time', 'std_repair_time', 'min_repair_time', 'max_repair_time'
    ]
    
    # Global averages for comparison
    global_avg_response = df['totalResponseTime'].mean()
    global_avg_repair = df['totalRepairTime'].mean()
    
    # Add comparison to global average
    for idx, row in mechanic_overall.iterrows():
        if global_avg_response > 0:
            mechanic_overall.at[idx, 'response_vs_global_pct'] = ((row['avg_response_time'] / global_avg_response) - 1) * 100
        else:
            mechanic_overall.at[idx, 'response_vs_global_pct'] = 0
            
        if global_avg_repair > 0:
            mechanic_overall.at[idx, 'repair_vs_global_pct'] = ((row['avg_repair_time'] / global_avg_repair) - 1) * 100
        else:
            mechanic_overall.at[idx, 'repair_vs_global_pct'] = 0
    
    # Sort and rank mechanics
    response_ranking = mechanic_overall.sort_values('avg_response_time').reset_index(drop=True)
    response_ranking['response_time_rank'] = response_ranking.index + 1
    
    repair_ranking = mechanic_overall.sort_values('avg_repair_time').reset_index(drop=True)
    repair_ranking['repair_time_rank'] = repair_ranking.index + 1
    
    # Calculate hourly metrics by mechanic (within working hours)
    mechanic_hourly = df.groupby(['mechanicId', 'mechanicName', 'hour']).agg({
        'id': 'count',
        'totalResponseTime': 'mean',
        'totalRepairTime': 'mean'
    }).reset_index()
    
    mechanic_hourly.columns = [
        'mechanic_id', 'mechanic_name', 'hour', 'incident_count',
        'avg_response_time', 'avg_repair_time'
    ]
    
    # Add hour labels
    mechanic_hourly['hour_label'] = mechanic_hourly['hour'].apply(lambda h: f"{h:02d}:00-{(h+1)%24:02d}:00")
    
    # Filter for meaningful comparisons
    mechanic_hourly_filtered = mechanic_hourly[mechanic_hourly['incident_count'] >= min_incidents].copy()
    
    # Create hour-specific mechanic rankings (for each working hour)
    hour_rankings = []
    
    for hour in range(work_hours_start, work_hours_end):
        hour_data = mechanic_hourly_filtered[mechanic_hourly_filtered['hour'] == hour]
        
        if len(hour_data) < 2:  # Need at least 2 mechanics for comparison
            continue
            
        hour_label = f"{hour:02d}:00-{(hour+1)%24:02d}:00"
        
        # Calculate hour average
        hour_avg_response = hour_data['avg_response_time'].mean()
        hour_avg_repair = hour_data['avg_repair_time'].mean()
        
        # Rank mechanics for this hour
        hour_response_ranking = hour_data.sort_values('avg_response_time').reset_index(drop=True)
        hour_response_ranking['hour_response_rank'] = hour_response_ranking.index + 1
        
        # Add percentage comparison to hour average
        for idx, row in hour_response_ranking.iterrows():
            if hour_avg_response > 0:
                hour_response_ranking.at[idx, 'vs_hour_avg_pct'] = ((row['avg_response_time'] / hour_avg_response) - 1) * 100
            else:
                hour_response_ranking.at[idx, 'vs_hour_avg_pct'] = 0
        
        # Repair time ranking
        hour_repair_ranking = hour_data.sort_values('avg_repair_time').reset_index(drop=True)
        hour_repair_ranking['hour_repair_rank'] = hour_repair_ranking.index + 1
        
        # Add percentage comparison to hour average
        for idx, row in hour_repair_ranking.iterrows():
            if hour_avg_repair > 0:
                hour_repair_ranking.at[idx, 'vs_hour_avg_pct'] = ((row['avg_repair_time'] / hour_avg_repair) - 1) * 100
            else:
                hour_repair_ranking.at[idx, 'vs_hour_avg_pct'] = 0
        
        hour_rankings.append({
            'hour': hour,
            'hour_label': hour_label,
            'total_mechanics': len(hour_data),
            'hour_avg_response_time': float(hour_avg_response),
            'hour_avg_repair_time': float(hour_avg_repair),
            'response_time_ranking': hour_response_ranking.to_dict('records'),
            'repair_time_ranking': hour_repair_ranking.to_dict('records')
        })
    
    # Generate a comprehensive mechanic/hour matrix
    all_mechanics = mechanic_overall['mechanic_id'].unique()
    all_hours = range(work_hours_start, work_hours_end)
    
    # Prepare for pivot table - filter to working hours first
    mech_hour_data = mechanic_hourly[mechanic_hourly['hour'].isin(all_hours)]
    
    # Use pivot table to create the matrix - use fill_value=None to indicate missing data
    if not mech_hour_data.empty:
        mechanic_hour_matrix = mech_hour_data.pivot_table(
            values=['avg_response_time', 'avg_repair_time', 'incident_count'],
            index='mechanic_id',
            columns='hour',
            fill_value=None
        ).reset_index()
        
        # Flatten and format (pandas pivot tables create MultiIndex)
        mechanic_hour_matrix.columns = [
            f"{col[0]}_{col[1]}" if isinstance(col, tuple) else col
            for col in mechanic_hour_matrix.columns
        ]
        
        # Add mechanic names back
        mechanic_names = {m['mechanic_id']: m['mechanic_name'] for m in mechanic_overall.to_dict('records')}
        mechanic_hour_matrix['mechanic_name'] = mechanic_hour_matrix['mechanic_id'].map(mechanic_names)
        
        # Reorder columns to put ID and name first
        first_cols = ['mechanic_id', 'mechanic_name']
        other_cols = [c for c in mechanic_hour_matrix.columns if c not in first_cols]
        mechanic_hour_matrix = mechanic_hour_matrix[first_cols + other_cols]
        
        matrix_data = mechanic_hour_matrix.to_dict('records')
    else:
        matrix_data = []
    
    return {
        'mechanic_overall_stats': mechanic_overall.to_dict('records'),
        'response_time_ranking': response_ranking.to_dict('records'),
        'repair_time_ranking': repair_ranking.to_dict('records'),
        'mechanic_hourly_stats': mechanic_hourly.to_dict('records'),
        'mechanic_hour_matrix': matrix_data,
        'hourly_rankings': hour_rankings,
        'mechanics_count': len(mechanic_overall),
        'global_avg_response_time': float(global_avg_response),
        'global_avg_repair_time': float(global_avg_repair),
        'working_hours': f"{work_hours_start}:00-{work_hours_end}:00"
    }

def run_working_hours_analysis(data, start_date=None, end_date=None, work_hours_start=7, work_hours_end=17, min_incidents=2):
    """Run analysis focused on working hours."""
    try:
        print(f"Starting analysis for working hours ({work_hours_start}:00-{work_hours_end}:00)...")
        
        # Convert to DataFrame
        print("Converting data to DataFrame...")
        df = convert_to_dataframe(data)
        
        # Print basic info about date column
        if 'createdAt' in df.columns and not df['createdAt'].empty:
            print(f"Date column timezone: {df['createdAt'].dt.tz}")
            print(f"Date range in data: {df['createdAt'].min()} to {df['createdAt'].max()}")
        
        # Filter by date range
        if start_date or end_date:
            df = filter_date_range(df, start_date, end_date)
        
        # Check if we have enough data
        if len(df) < 5:
            return {
                "error": "Not enough data for meaningful analysis",
                "record_count": len(df)
            }
        
        # Add time features
        df = add_time_features(df)
        
        # Filter to working hours
        df_working = filter_working_hours(df, work_hours_start, work_hours_end)
        
        # Check again if we have enough data after filtering
        if len(df_working) < 5:
            return {
                "error": "Not enough data during working hours for meaningful analysis",
                "record_count": len(df_working),
                "working_hours": f"{work_hours_start}:00-{work_hours_end}:00"
            }
        
        # Run analyses
        print("Analyzing hourly breakdown frequency...")
        breakdown_results = analyze_hourly_breakdown_frequency(df_working, work_hours_start, work_hours_end)
        
        print("Analyzing hourly response and repair times...")
        time_results = analyze_hourly_response_repair_times(df_working, min_incidents, work_hours_start, work_hours_end)
        
        print("Analyzing mechanic performance by hour...")
        mechanic_results = analyze_mechanic_hourly_performance(df_working, min_incidents, work_hours_start, work_hours_end)
        
        # Combine results
        results = {
            "record_count": len(df_working),
            "date_range": {
                "start": df['createdAt'].min().isoformat() if not df['createdAt'].empty else None,
                "end": df['createdAt'].max().isoformat() if not df['createdAt'].empty else None
            },
            "analysis_parameters": {
                "working_hours": f"{work_hours_start}:00-{work_hours_end}:00",
                "min_incidents_for_comparison": min_incidents
            },
            "hourly_breakdown_frequency": breakdown_results,
            "hourly_response_repair_times": time_results,
            "mechanic_performance": mechanic_results
        }
        
        return results
    
    except Exception as e:
        print(f"Error in analysis: {str(e)}")
        traceback.print_exc()
        return {
            "error": f"Error in analysis: {str(e)}",
            "traceback": traceback.format_exc()
        }

def main():
    """Main function."""
    args = parse_arguments()
    
    try:
        # Set default end date to today if not provided
        end_date = args.end_date
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Load data
        print(f"Loading data from {args.input}...")
        data = load_data(args.input)
        print(f"Loaded {len(data)} records")
        
        # Run analysis
        results = run_working_hours_analysis(
            data, 
            start_date=args.start_date, 
            end_date=end_date,
            work_hours_start=args.work_hours_start,
            work_hours_end=args.work_hours_end,
            min_incidents=args.min_incidents
        )
        
        # Save results
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"Analysis complete. Results saved to {args.output}")
        
        # Print a summary
        if 'error' in results:
            print(f"Analysis error: {results['error']}")
        else:
            print(f"\nWorking Hours Analysis Summary ({args.work_hours_start}:00-{args.work_hours_end}:00):")
            print(f"- Analyzed {results['record_count']} records from {results['date_range']['start']} to {results['date_range']['end']}")
            
            # Breakdown frequency summary
            peak_hours = results['hourly_breakdown_frequency']['peak_breakdown_hours']
            if peak_hours:
                print("\nPeak Breakdown Hours:")
                for i, hour in enumerate(peak_hours):
                    print(f"  {i+1}. {hour['hour_label']}: {hour['incident_count']} incidents ({hour['pct_of_total']}% of total)")
            
            # Mechanic summary
            if 'mechanic_performance' in results and 'response_time_ranking' in results['mechanic_performance']:
                mechanics = results['mechanic_performance']['response_time_ranking']
                if mechanics:
                    print(f"\nMechanic Response Time Ranking (out of {len(mechanics)}):")
                    for mech in mechanics[:3]:  # Show top 3
                        print(f"  {mech['response_time_rank']}. {mech['mechanic_name']}: {mech['avg_response_time']:.1f} min")
                    
                    # Also show bottom 3 if there are enough mechanics
                    if len(mechanics) > 6:
                        print("  ...")
                        for mech in mechanics[-3:]:  # Show bottom 3
                            print(f"  {mech['response_time_rank']}. {mech['mechanic_name']}: {mech['avg_response_time']:.1f} min")
    
    except Exception as e:
        print(f"Error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()