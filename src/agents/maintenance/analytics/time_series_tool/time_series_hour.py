# src/agents/maintenance/analytics/time_series_tool/time_series_hour.py
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional
from shared_services.supabase_client import SupabaseClient

def run_working_hours_analysis(
    work_hours_start: int = 7,
    work_hours_end: int = 17,
    min_incidents: int = 2,
    line_variance_pct: float = 20.0,
    period_start: Optional[datetime] = None,
    period_end: Optional[datetime] = None
) -> dict:
    """
    Analyze downtime by hour of day focusing on working hours.
    
    Steps:
      1. Fetch all rows from downtime_detail via SupabaseClient.query_table
      2. Parse 'created_at' to datetime, convert durations (seconds→minutes)
      3. Filter to working hours (default 7-17)
      4. Compute:
         - hourly incident counts + % of total
         - peak hours (top 3)
         - z-score outliers on counts
         - per-mechanic hourly stats
         - per-line hourly variance outliers
      5. Return summary dict
      
    Args:
        work_hours_start: Start hour for working hours filter
        work_hours_end: End hour for working hours filter
        min_incidents: Minimum incidents required for analysis
        line_variance_pct: Percentage threshold for line variance outliers
        period_start: Optional start date for the analysis period
        period_end: Optional end date for the analysis period
    """
    print("HOURS_ANALYSIS: Starting hourly analysis...")
    
    # Generate period info for results
    period_info = {}
    if period_start:
        period_info['period_start'] = period_start.isoformat() if isinstance(period_start, datetime) else period_start
    if period_end:
        period_info['period_end'] = period_end.isoformat() if isinstance(period_end, datetime) else period_end
    
    # 1. Fetch data with date filtering
    db = SupabaseClient()
    
    # Create filters based on date range
    filters = {}
    if period_start and period_end:
        # Convert datetime objects to strings if needed
        start_date_str = period_start.isoformat() if isinstance(period_start, datetime) else period_start
        end_date_str = period_end.isoformat() if isinstance(period_end, datetime) else period_end
        
        # Add date filters
        filters['resolved_at.gte'] = start_date_str
        filters['resolved_at.lte'] = end_date_str
        
        print(f"HOURS_ANALYSIS: Filtering records by date range: {start_date_str} to {end_date_str}")
    elif period_start:
        start_date_str = period_start.isoformat() if isinstance(period_start, datetime) else period_start
        filters['resolved_at.gte'] = start_date_str
        print(f"HOURS_ANALYSIS: Filtering records from {start_date_str} onwards")
    elif period_end:
        end_date_str = period_end.isoformat() if isinstance(period_end, datetime) else period_end
        filters['resolved_at.lte'] = end_date_str
        print(f"HOURS_ANALYSIS: Filtering records up to {end_date_str}")
    
    records = db.query_table(
        table_name="downtime_detail",
        columns="*",
        filters=filters,
        limit=1000
    )
    
    print(f"HOURS_ANALYSIS: Retrieved {len(records)} records from database")
    
    # Check if we have data to analyze
    if not records:
        print("HOURS_ANALYSIS: No records found for the specified period")
        return {
            'hourly_breakdown_counts': [],
            'peak_breakdown_hours': [],
            'statistical_outliers': [],
            'mechanic_hourly_stats': [],
            'line_hourly_outliers': [],
            'total_records': 0,
            **period_info
        }

    # 2. Load into DataFrame & parse
    df = pd.DataFrame(records)
    if 'created_at' not in df.columns:
        raise KeyError("Expected 'created_at' column in downtime_detail")
    df['ts'] = pd.to_datetime(df['created_at'], errors='coerce')

    # Convert durations (sec→min)
    df['downtime_min'] = pd.to_numeric(df['total_downtime'], errors='coerce') / 60.0
    df['response_min'] = pd.to_numeric(df['total_response_time'], errors='coerce') / 60.0
    df['repair_min'] = pd.to_numeric(df['total_repair_time'], errors='coerce') / 60.0

    # 3. Filter to working hours
    original_count = len(df)
    df = df[df['ts'].dt.hour.between(work_hours_start, work_hours_end)].copy()
    filtered_count = len(df)
    print(f"HOURS_ANALYSIS: Filtered to {filtered_count} records within hours {work_hours_start}-{work_hours_end} (from {original_count})")

    # 4a. Annotate hour of day
    df['hour_of_day'] = df['ts'].dt.hour

    # 4b. Hourly incident breakdown
    hourly = (
        df.groupby(['hour_of_day'])
          .agg(incident_count=('id', 'count'))
          .reset_index()
    )
    
    if hourly.empty:
        print("HOURS_ANALYSIS: Warning - No hourly data available after filtering")
        return {
            'hourly_breakdown_counts': [],
            'peak_breakdown_hours': [],
            'statistical_outliers': [],
            'mechanic_hourly_stats': [],
            'line_hourly_outliers': [],
            'total_records': 0,
            **period_info
        }
    
    total_inc = hourly['incident_count'].sum() or 1
    hourly['pct_of_total'] = (hourly['incident_count'] / total_inc * 100).round(1)
    print(f"HOURS_ANALYSIS: Calculated incidents per hour across {len(hourly)} hours")

    # 4c. Z-score outlier detection on counts
    mean_cnt = hourly['incident_count'].mean()
    std_cnt = hourly['incident_count'].std()
    
    # Use numpy where() instead of and/or operators
    if std_cnt > 0:
        hourly['z_score'] = (hourly['incident_count'] - mean_cnt) / std_cnt
    else:
        hourly['z_score'] = 0
        
    hourly['is_outlier'] = hourly['z_score'].abs() > 1.5  # Standard Z-threshold
    
    print(f"HOURS_ANALYSIS: Mean hourly incidents: {mean_cnt:.1f}, StdDev: {std_cnt:.1f}")
    print(f"HOURS_ANALYSIS: Identified {hourly['is_outlier'].sum()} hourly outliers")

    peak_hours = hourly.nlargest(3, 'incident_count').to_dict('records')
    outliers = hourly[hourly['is_outlier']].to_dict('records')

    # 4d. Mechanic performance by hour
    mech_stats = []
    if {'mechanic_id', 'mechanic_name'}.issubset(df.columns):
        print("HOURS_ANALYSIS: Analyzing mechanic hourly performance")
        md = (
            df.groupby(['mechanic_id', 'mechanic_name', 'hour_of_day'])
              .agg(
                  incident_count=('id', 'count'),
                  avg_downtime=('downtime_min', 'mean'),
                  avg_response=('response_min', 'mean'),
                  avg_repair=('repair_min', 'mean')
              )
              .reset_index()
        )
        # Filter to minimum number of incidents
        if min_incidents > 1:
            md = md[md['incident_count'] >= min_incidents]
            
        mech_stats = md.to_dict('records')
        print(f"HOURS_ANALYSIS: Generated {len(mech_stats)} mechanic-hour statistics")

    # 4e. Line variance detection by hour
    line_stats = []
    if 'line_id' in df.columns:
        print("HOURS_ANALYSIS: Analyzing line-specific hourly patterns")
        ld = (
            df.groupby(['line_id', 'hour_of_day'])
              .agg(
                  incident_count=('id', 'count'),
                  avg_downtime=('downtime_min', 'mean')
              )
              .reset_index()
        )
        # Only analyze hours with minimum incidents
        ld = ld[ld['incident_count'] >= min_incidents]
        
        if not ld.empty:
            global_hour = (
                ld.groupby('hour_of_day')['avg_downtime']
                .mean()
                .reset_index(name='global_avg_downtime')
            )
            ld = ld.merge(global_hour, on='hour_of_day')
            ld['pct_diff'] = (ld['avg_downtime'] / ld['global_avg_downtime'] - 1) * 100
            out = ld[ld['pct_diff'].abs() > line_variance_pct]
            for row in out.to_dict('records'):
                line_stats.append({
                    'type': 'line_hourly_outlier',
                    'line_id': row['line_id'],
                    'hour_of_day': int(row['hour_of_day']),
                    'avg_downtime_min': float(row['avg_downtime']),
                    'global_avg_downtime': float(row['global_avg_downtime']),
                    'pct_diff': float(row['pct_diff']),
                    'message': f"Line {row['line_id']} has {row['pct_diff']:.1f}% more downtime than average at hour {row['hour_of_day']}."
                })
            print(f"HOURS_ANALYSIS: Found {len(line_stats)} line-specific hourly outliers")

    # 5. Return summary
    result = {
        'hourly_breakdown_counts': hourly.to_dict('records'),
        'peak_breakdown_hours': peak_hours,
        'statistical_outliers': outliers,
        'mechanic_hourly_stats': mech_stats,
        'line_hourly_outliers': line_stats,
        'total_records': int(len(df)),
        **period_info
    }
    
    print(f"HOURS_ANALYSIS: Analysis complete - {len(outliers)} hourly outliers, {len(peak_hours)} peak hours identified")
    return result