# src/agents/maintenance/analytics/time_series_tool/time_series_day.py
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional
from shared_services.supabase_client import SupabaseClient


def run_daily_pattern_analysis(
    work_hours_only: bool = False,
    z_threshold: float = 1.5,
    line_variance_pct: float = 25.0,
    period_start: Optional[datetime] = None,
    period_end: Optional[datetime] = None
) -> dict:
    """
    Analyze downtime by day of week using all records, including per-line variance.
    
    Args:
        work_hours_only: Whether to filter to only include work hours
        z_threshold: Threshold for Z-score outlier detection
        line_variance_pct: Threshold percentage for line variance outlier detection
        period_start: Optional start date for the analysis period
        period_end: Optional end date for the analysis period
    """
    # 1. Fetch all records with date filtering
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
        
        print(f"Filtering records by date range: {start_date_str} to {end_date_str}")
    elif period_start:
        start_date_str = period_start.isoformat() if isinstance(period_start, datetime) else period_start
        filters['resolved_at.gte'] = start_date_str
        print(f"Filtering records from {start_date_str} onwards")
    elif period_end:
        end_date_str = period_end.isoformat() if isinstance(period_end, datetime) else period_end
        filters['resolved_at.lte'] = end_date_str
        print(f"Filtering records up to {end_date_str}")
    
    records = db.query_table(
        table_name="downtime_detail",
        columns="*",
        filters=filters,
        limit=1000
    )
    
    # Add period info to results for reference
    period_info = {}
    if period_start:
        period_info['period_start'] = period_start.isoformat() if isinstance(period_start, datetime) else period_start
    if period_end:
        period_info['period_end'] = period_end.isoformat() if isinstance(period_end, datetime) else period_end

    # Check if we have data to analyze
    if not records:
        print("No records found for the specified period")
        return {
            'daily_breakdown_counts': [],
            'peak_breakdown_days': [],
            'statistical_outliers': [],
            'mechanic_daily_stats': [],
            'line_daily_outliers': [],
            'total_records': 0,
            **period_info
        }

    # 2. Build DataFrame
    df = pd.DataFrame(records)

    # 3. Normalize timestamp
    if 'created_at' not in df.columns:
        raise KeyError("Expected 'created_at' column in downtime_detail")
    df['ts'] = pd.to_datetime(df['created_at'], errors='coerce')

    # 4. Convert durations to minutes
    df['downtime_min']  = pd.to_numeric(df['total_downtime'],     errors='coerce') / 60.0
    df['response_min']  = pd.to_numeric(df['total_response_time'], errors='coerce') / 60.0
    df['repair_min']    = pd.to_numeric(df['total_repair_time'],   errors='coerce') / 60.0

    # 5. Optional working‐hours filter
    if work_hours_only:
        df = df[df['ts'].dt.hour.between(7,16)].copy()

    # 6. Annotate day of week
    df['day_of_week'] = df['ts'].dt.dayofweek
    df['day_name']    = df['ts'].dt.day_name()

    # --- Daily breakdown counts & percentages ---
    daily = (
        df.groupby(['day_of_week','day_name'])
          .agg(incident_count=('id','count'))
          .reset_index()
    )
    total_inc = daily['incident_count'].sum() or 1
    daily['pct_of_total'] = (daily['incident_count'] / total_inc * 100).round(1)

    # --- Expected vs actual (weekday/weekend) ---
    weekday = daily[daily['day_of_week'] < 5].copy()
    weekend = daily[daily['day_of_week'] >= 5].copy()
    if not weekday.empty:
        exp_wd = weekday['incident_count'].sum() / 5
        weekday['vs_expected_pct'] = ((weekday['incident_count'] / exp_wd) - 1) * 100
    if not weekend.empty:
        exp_we = weekend['incident_count'].sum() / 2
        weekend['vs_expected_pct'] = ((weekend['incident_count'] / exp_we) - 1) * 100
    daily = pd.concat([weekday, weekend]).sort_values('day_of_week')

    # --- Z-score outlier detection on counts ---
    mean_cnt = daily['incident_count'].mean()
    std_cnt  = daily['incident_count'].std()
    
    # FIX: Use numpy where() instead of and/or operators
    if std_cnt > 0:
        daily['z_score'] = (daily['incident_count'] - mean_cnt) / std_cnt
    else:
        daily['z_score'] = 0
        
    daily['is_outlier'] = daily['z_score'].abs() > z_threshold

    peak_days = daily.nlargest(3, 'incident_count').to_dict('records')
    outliers  = daily[daily['is_outlier']].to_dict('records')

    # --- Mechanic performance by day ---
    mech_stats = []
    if {'mechanic_id','mechanic_name'}.issubset(df.columns):
        md = (
            df.groupby(['mechanic_id','mechanic_name','day_of_week'])
              .agg(
                  incident_count=('id','count'),
                  avg_downtime=('downtime_min','mean'),
                  avg_response=('response_min','mean'),
                  avg_repair=('repair_min','mean')
              )
              .reset_index()
        )
        mech_stats = md.to_dict('records')

    # --- Line variance detection by day ---
    line_stats = []
    if 'line_id' in df.columns:
        ld = (
            df.groupby(['line_id','day_of_week'])
              .agg(
                  incident_count=('id','count'),
                  avg_downtime=('downtime_min','mean')
              )
              .reset_index()
        )
        global_day = (
            ld.groupby('day_of_week')['avg_downtime']
              .mean()
              .reset_index(name='global_avg_downtime')
        )
        ld = ld.merge(global_day, on='day_of_week')
        ld['pct_diff'] = (ld['avg_downtime'] / ld['global_avg_downtime'] - 1) * 100
        out = ld[ld['pct_diff'].abs() > line_variance_pct]
        for row in out.to_dict('records'):
            line_stats.append({
                'type':               'line_daily_outlier',
                'line_id':            row['line_id'],
                'day_of_week':        int(row['day_of_week']),
                'day_name':           df[df['day_of_week'] == row['day_of_week']]['day_name'].iloc[0] if not df.empty else '',
                'avg_downtime_min':   float(row['avg_downtime']),
                'global_avg_downtime':float(row['global_avg_downtime']),
                'pct_diff':           float(row['pct_diff']),
                'message':            f"Line {row['line_id']} has {row['pct_diff']:.1f}% more downtime than the daily average on day {row['day_of_week']}."
            })

    return {
        'daily_breakdown_counts': daily.to_dict('records'),
        'peak_breakdown_days':    peak_days,
        'statistical_outliers':   outliers,
        'mechanic_daily_stats':   mech_stats,
        'line_daily_outliers':    line_stats,
        'total_records':          int(len(df)),
        **period_info
    }