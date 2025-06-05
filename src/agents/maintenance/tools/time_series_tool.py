# src/agents/maintenance/tools/time_series_tool.py
import json
import os
import re
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging
from dotenv import load_dotenv

# Load environment variables
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
env_path = os.path.join(project_root, '.env.local')
load_dotenv(dotenv_path=env_path)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("time_series_tool")

# Import the workflows and utilities
from ..workflows.daily_analysis_workflow import DailyAnalysisWorkflow
from ..workflows.hourly_analysis_workflow import HourlyAnalysisWorkflow
from ..tools.date_selector import DateSelector
from ..utils.tool_run_manager import ToolRunManager
from shared_services.supabase_client import get_shared_supabase_client

def time_series_analysis_tool(
    analysis_type: str = "both",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    mode: str = "interactive",
    force: bool = False
) -> str:
    """
    Tool for running time series analysis to identify daily and hourly patterns.
    
    Args:
        analysis_type: Type of analysis ('daily', 'hourly', or 'both')
        start_date: Optional start date for analysis (YYYY-MM-DD)
        end_date: Optional end date for analysis (YYYY-MM-DD)
        mode: Date selection mode ('interactive' or 'args')
        force: Override 30-day frequency limit (default: False)
        
    Returns:
        JSON string with the results
    """
    try:
        # Clean the analysis_type input
        logger.info(f"Time series analysis requested: {analysis_type}")
        
        if isinstance(analysis_type, str):
            analysis_type = analysis_type.strip().lower()
            
        # Validate analysis type
        valid_types = ["daily", "hourly", "both"]
        if analysis_type not in valid_types:
            return json.dumps({
                "status": "error",
                "message": f"Invalid analysis_type: {analysis_type}. Must be one of: {valid_types}"
            })
        
        # Initialize database and run manager
        db = get_shared_supabase_client()
        run_manager = ToolRunManager(db)
        
        # Check frequency limits (30-day minimum)
        can_run, last_run_date = run_manager.can_run_tool("time_series_analysis", min_days=30)
        
        if not can_run and not force and last_run_date:
            days_since = (datetime.now() - last_run_date).days
            warning_msg = f"Last time series analysis was {days_since} days ago (on {last_run_date.date()}). Minimum 30 days required between analyses."
            logger.warning(warning_msg)
            
            return json.dumps({
                "status": "frequency_warning",
                "message": warning_msg,
                "last_run_date": last_run_date.isoformat(),
                "days_since_last_run": days_since,
                "suggestion": "Use force=True to override, or wait for the frequency limit to pass"
            })
        
        if force and not can_run:
            logger.info(f"Force parameter enabled - overriding 30-day frequency limit (last run: {last_run_date.date() if last_run_date else 'never'})")
        
        # Handle date selection
        if start_date and end_date:
            try:
                period_start = datetime.strptime(start_date, "%Y-%m-%d")
                period_end = datetime.strptime(end_date, "%Y-%m-%d")
                logger.info(f"Using provided date range: {period_start.date()} to {period_end.date()}")
            except ValueError as e:
                return json.dumps({
                    "status": "error", 
                    "message": f"Invalid date format: {e}. Use YYYY-MM-DD format."
                })
        else:
            # Use date selector
            start_date_str, end_date_str = DateSelector.get_date_range(mode=mode)
            period_start = datetime.strptime(start_date_str, "%Y-%m-%d")
            period_end = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        # Set end date to end of day
        period_end = period_end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # Start tool run logging
        run_id = run_manager.log_tool_start(
            tool_name="time_series_analysis",
            period_start=period_start,
            period_end=period_end,
            summary=f"{analysis_type.title()} time series analysis"
        )
        logger.info(f"Started time series analysis with run ID: {run_id}")
        
        # Initialize results
        results = {
            "analysis_type": analysis_type,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "daily_analysis": None,
            "hourly_analysis": None,
            "patterns_flagged": 0,
            "status": "success"
        }
        
        try:
            # Run daily analysis if requested
            if analysis_type in ["daily", "both"]:
                logger.info("Running daily analysis...")
                daily_workflow = DailyAnalysisWorkflow()
                daily_result = daily_workflow.run(period_start=period_start, period_end=period_end)
                results["daily_analysis"] = {
                    "success": daily_result.get("daily_analysis_success", False),
                    "findings_count": daily_result.get("findings_count", 0),
                    "findings_saved": daily_result.get("findings_saved", 0)
                }
                
                # Process daily patterns and save to time_series_results
                if daily_result.get("daily_analysis_success"):
                    daily_patterns = _process_daily_patterns(daily_result, run_id, period_start, period_end)
                    _save_time_series_results(db, daily_patterns)
                    results["patterns_flagged"] += len(daily_patterns)
            
            # Run hourly analysis if requested  
            if analysis_type in ["hourly", "both"]:
                logger.info("Running hourly analysis...")
                hourly_workflow = HourlyAnalysisWorkflow()
                hourly_result = hourly_workflow.run(period_start=period_start, period_end=period_end)
                results["hourly_analysis"] = {
                    "success": hourly_result.get("hourly_analysis_success", False),
                    "findings_count": hourly_result.get("findings_count", 0),
                    "findings_saved": hourly_result.get("findings_saved", 0)
                }
                
                # Process hourly patterns and save to time_series_results
                if hourly_result.get("hourly_analysis_success"):
                    hourly_patterns = _process_hourly_patterns(hourly_result, run_id, period_start, period_end)
                    _save_time_series_results(db, hourly_patterns)
                    results["patterns_flagged"] += len(hourly_patterns)
            
            # Complete tool run logging
            run_manager.log_tool_complete(
                run_id=run_id,
                items_processed=results.get("patterns_flagged", 0),
                items_created=results.get("patterns_flagged", 0),
                summary=f"Time series analysis completed - {results['patterns_flagged']} patterns flagged",
                metadata=results
            )
            
            # Send completion notification
            _send_completion_notification(db, run_id, results["patterns_flagged"], analysis_type)
            
            logger.info(f"Time series analysis completed successfully. {results['patterns_flagged']} patterns flagged.")
            
            return json.dumps(results, indent=2)
            
        except Exception as e:
            error_msg = f"Error during analysis execution: {str(e)}"
            logger.error(error_msg)
            run_manager.log_tool_error(run_id, error_msg)
            results["status"] = "error"
            results["error"] = error_msg
            return json.dumps(results, indent=2)
        
    except Exception as e:
        logger.error(f"Error in time_series_analysis_tool: {str(e)}", exc_info=True)
        return json.dumps({
            "status": "error",
            "message": str(e)
        }, indent=2)


def _process_daily_patterns(daily_result: Dict[str, Any], run_id: str, period_start: datetime, period_end: datetime) -> List[Dict[str, Any]]:
    """Process daily analysis results and extract flagged patterns with meaningful context."""
    patterns = []
    
    # Get the daily summary data
    daily_summary = daily_result.get("daily_summary", {})
    total_records = daily_summary.get("total_records", 0)
    
    if total_records == 0:
        logger.info("No records to analyze for daily patterns")
        return patterns
    
    # Calculate overall averages for comparison
    daily_breakdown = daily_summary.get("daily_breakdown_counts", [])
    if not daily_breakdown:
        logger.info("No daily breakdown data available")
        return patterns
    
    # Calculate average incidents per day
    total_incidents = sum(day.get("incident_count", 0) for day in daily_breakdown)
    avg_incidents_per_day = total_incidents / len(daily_breakdown) if daily_breakdown else 0
    
    # Process statistical outliers (days significantly different from normal)
    outliers = daily_summary.get("statistical_outliers", [])
    for outlier in outliers:
        day_name = outlier.get("day_name", "Unknown")
        incident_count = outlier.get("incident_count", 0)
        pct_of_total = outlier.get("pct_of_total", 0)
        z_score = outlier.get("z_score", 0)
        
        # Determine if it's high or low
        if incident_count > avg_incidents_per_day:
            description = f"High incident volume on {day_name}s"
            variance_type = "higher"
        else:
            description = f"Low incident volume on {day_name}s" 
            variance_type = "lower"
        
        variance_pct = round(((incident_count / avg_incidents_per_day) - 1) * 100) if avg_incidents_per_day > 0 else 0
        
        pattern = {
            "tool_run_id": run_id,
            "analysis_type": "daily",
            "pattern_type": "incident_count",
            "entity_type": "overall",
            "entity_id": None,
            "time_dimension": "day_of_week",
            "time_value": day_name,
            "description": description,
            "severity": _determine_severity(abs(z_score)),
            "context_data": {
                "flagged_day_incidents": incident_count,
                "average_daily_incidents": round(avg_incidents_per_day, 1),
                "percentage_of_total": f"{pct_of_total:.1f}%",
                "variance": f"{'+' if variance_pct > 0 else ''}{variance_pct}%",
                "z_score": round(z_score, 2),
                "total_records_analyzed": total_records
            },
            "analysis_period_start": period_start.date().isoformat(),
            "analysis_period_end": period_end.date().isoformat()
        }
        patterns.append(pattern)
    
    # Process peak days (highest volume days even if not statistical outliers)
    peak_days = daily_summary.get("peak_breakdown_days", [])[:2]  # Top 2 peak days
    for peak_day in peak_days:
        day_name = peak_day.get("day_name", "Unknown")
        incident_count = peak_day.get("incident_count", 0)
        pct_of_total = peak_day.get("pct_of_total", 0)
        
        # Skip if already processed as outlier
        if any(p.get("time_value") == day_name for p in patterns):
            continue
            
        # Only flag if significantly above average
        if incident_count > avg_incidents_per_day * 1.2:  # 20% above average
            variance_pct = round(((incident_count / avg_incidents_per_day) - 1) * 100) if avg_incidents_per_day > 0 else 0
            
            pattern = {
                "tool_run_id": run_id,
                "analysis_type": "daily",
                "pattern_type": "incident_count",
                "entity_type": "overall", 
                "entity_id": None,
                "time_dimension": "day_of_week",
                "time_value": day_name,
                "description": f"Peak incident volume on {day_name}s",
                "severity": "medium",
                "context_data": {
                    "peak_day_incidents": incident_count,
                    "average_daily_incidents": round(avg_incidents_per_day, 1),
                    "percentage_of_total": f"{pct_of_total:.1f}%",
                    "variance": f"+{variance_pct}%",
                    "rank": "peak day"
                },
                "analysis_period_start": period_start.date().isoformat(),
                "analysis_period_end": period_end.date().isoformat()
            }
            patterns.append(pattern)
    
    # Process mechanic-specific daily patterns
    mechanic_stats = daily_summary.get("mechanic_daily_stats", [])
    if mechanic_stats:
        patterns.extend(_process_mechanic_daily_patterns(mechanic_stats, run_id, period_start, period_end))
    
    logger.info(f"Processed {len(patterns)} daily patterns")
    return patterns


def _process_mechanic_daily_patterns(mechanic_stats: List[Dict], run_id: str, period_start: datetime, period_end: datetime) -> List[Dict[str, Any]]:
    """Process mechanic-specific daily patterns."""
    patterns = []
    
    # Group by mechanic to compare their different days
    mechanic_data = {}
    for stat in mechanic_stats:
        mechanic_name = stat.get("mechanic_name", "Unknown")
        if mechanic_name not in mechanic_data:
            mechanic_data[mechanic_name] = []
        mechanic_data[mechanic_name].append(stat)
    
    # Analyze each mechanic's daily patterns
    for mechanic_name, days_data in mechanic_data.items():
        if len(days_data) < 2:  # Need at least 2 days to compare
            continue
            
        # Calculate mechanic's averages across all their days
        total_response = sum(day.get("avg_response", 0) for day in days_data)
        total_repair = sum(day.get("avg_repair", 0) for day in days_data) 
        total_downtime = sum(day.get("avg_downtime", 0) for day in days_data)
        
        avg_response = total_response / len(days_data)
        avg_repair = total_repair / len(days_data)
        avg_downtime = total_downtime / len(days_data)
        
        # Check each day for significant deviations
        for day_stat in days_data:
            day_name = _get_day_name(day_stat.get("day_of_week", 0))
            day_response = day_stat.get("avg_response", 0)
            day_repair = day_stat.get("avg_repair", 0)
            day_downtime = day_stat.get("avg_downtime", 0)
            
            # Check for response time issues (>50% worse than mechanic's average)
            if day_response > avg_response * 1.5 and day_response > 5:  # At least 5 minutes
                variance_pct = round(((day_response / avg_response) - 1) * 100) if avg_response > 0 else 0
                
                pattern = {
                    "tool_run_id": run_id,
                    "analysis_type": "daily",
                    "pattern_type": "response_time",
                    "entity_type": "mechanic",
                    "entity_id": mechanic_name,
                    "time_dimension": "day_of_week", 
                    "time_value": day_name,
                    "description": f"Slow response time on {day_name}s",
                    "severity": "medium" if variance_pct > 100 else "low",
                    "context_data": {
                        "flagged_avg_response": f"{day_response:.1f} min",
                        "mechanic_normal_avg": f"{avg_response:.1f} min",
                        "variance_vs_normal": f"+{variance_pct}%",
                        "incident_count": day_stat.get("incident_count", 0)
                    },
                    "analysis_period_start": period_start.date().isoformat(),
                    "analysis_period_end": period_end.date().isoformat()
                }
                patterns.append(pattern)
            
            # Check for repair time issues
            if day_repair > avg_repair * 1.5 and day_repair > 5:  # At least 5 minutes
                variance_pct = round(((day_repair / avg_repair) - 1) * 100) if avg_repair > 0 else 0
                
                pattern = {
                    "tool_run_id": run_id,
                    "analysis_type": "daily",
                    "pattern_type": "repair_time",
                    "entity_type": "mechanic",
                    "entity_id": mechanic_name,
                    "time_dimension": "day_of_week",
                    "time_value": day_name, 
                    "description": f"Slow repair time on {day_name}s",
                    "severity": "medium" if variance_pct > 100 else "low",
                    "context_data": {
                        "flagged_avg_repair": f"{day_repair:.1f} min",
                        "mechanic_normal_avg": f"{avg_repair:.1f} min", 
                        "variance_vs_normal": f"+{variance_pct}%",
                        "incident_count": day_stat.get("incident_count", 0)
                    },
                    "analysis_period_start": period_start.date().isoformat(),
                    "analysis_period_end": period_end.date().isoformat()
                }
                patterns.append(pattern)
    
    return patterns


def _get_day_name(day_of_week: float) -> str:
    """Convert day_of_week number to day name."""
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    try:
        return day_names[int(day_of_week)]
    except (ValueError, IndexError):
        return "Unknown"

def _process_hourly_patterns(hourly_result: Dict[str, Any], run_id: str, period_start: datetime, period_end: datetime) -> List[Dict[str, Any]]:
    """Process hourly analysis results and extract flagged patterns."""
    patterns = []
    
    # Get the hourly summary data
    hourly_summary = hourly_result.get("hourly_summary", {})
    
    # Process statistical outliers
    outliers = hourly_summary.get("statistical_outliers", [])
    for outlier in outliers:
        pattern = {
            "tool_run_id": run_id,
            "analysis_type": "hourly",
            "pattern_type": "incident_count",
            "entity_type": "overall", 
            "entity_id": None,
            "time_dimension": "hour",
            "time_value": str(outlier.get("hour", "unknown")),
            "description": f"High incident volume at {outlier.get('hour', 'unknown')}:00",
            "severity": _determine_severity(outlier.get("z_score", 0)),
            "context_data": {
                "flagged_hour_incidents": outlier.get("incident_count", 0),
                "average_hourly_incidents": round(hourly_summary.get("hourly_avg_incidents", 0), 1),
                "z_score": outlier.get("z_score", 0),
                "variance": f"+{round((outlier.get('incident_count', 0) / max(hourly_summary.get('hourly_avg_incidents', 1), 1) - 1) * 100)}%"
            },
            "analysis_period_start": period_start.date().isoformat(),
            "analysis_period_end": period_end.date().isoformat()
        }
        patterns.append(pattern)
    
    # Process mechanic hourly stats
    mechanic_stats = hourly_summary.get("mechanic_hourly_stats", [])
    for stat in mechanic_stats:
        if stat.get("response_time_outliers"):
            for outlier_hour in stat["response_time_outliers"]:
                pattern = {
                    "tool_run_id": run_id,
                    "analysis_type": "hourly",
                    "pattern_type": "response_time", 
                    "entity_type": "mechanic",
                    "entity_id": stat.get("mechanic_name"),
                    "time_dimension": "hour",
                    "time_value": str(outlier_hour.get("hour")),
                    "description": f"Slow response time at {outlier_hour.get('hour')}:00",
                    "severity": _determine_severity(outlier_hour.get("z_score", 0)),
                    "context_data": {
                        "flagged_avg": f"{outlier_hour.get('avg_response_time', 0):.1f} min",
                        "normal_avg": f"{stat.get('overall_avg_response_time', 0):.1f} min", 
                        "team_avg": f"{hourly_summary.get('team_avg_response_time', 0):.1f} min",
                        "z_score": outlier_hour.get("z_score", 0),
                        "variance_vs_normal": f"+{round((outlier_hour.get('avg_response_time', 0) / max(stat.get('overall_avg_response_time', 1), 1) - 1) * 100)}%"
                    },
                    "analysis_period_start": period_start.date(),
                    "analysis_period_end": period_end.date()
                }
                patterns.append(pattern)
    
    logger.info(f"Processed {len(patterns)} hourly patterns")
    return patterns


def _save_time_series_results(db, patterns: List[Dict[str, Any]]):
    """Save flagged patterns to time_series_results table."""
    if not patterns:
        logger.info("No patterns to save")
        return
    
    try:
        for i, pattern in enumerate(patterns):
            try:
                # Ensure all values are JSON serializable
                pattern_copy = _ensure_json_serializable(pattern)
                
                # Insert into time_series_results table
                db.insert_data("time_series_results", pattern_copy)
                logger.debug(f"Saved pattern {i+1}: {pattern_copy.get('description', 'Unknown')}")
                
            except Exception as e:
                logger.error(f"Error saving individual pattern {i+1}: {e}")
                logger.debug(f"Problem pattern: {pattern}")
                continue
        
        logger.info(f"Saved {len(patterns)} patterns to time_series_results table")
        
    except Exception as e:
        logger.error(f"Error saving patterns to database: {e}")
        raise


def _ensure_json_serializable(obj):
    """Recursively ensure all values in a dict/list are JSON serializable."""
    import json
    from datetime import date, datetime
    
    if isinstance(obj, dict):
        return {k: _ensure_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_ensure_json_serializable(item) for item in obj]
    elif isinstance(obj, (date, datetime)):
        return obj.isoformat()
    elif isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    else:
        # Try to convert to string as fallback
        return str(obj)


def _determine_severity(z_score: float) -> str:
    """Determine severity level based on z-score."""
    abs_z = abs(z_score)
    if abs_z >= 2.5:
        return "high"
    elif abs_z >= 1.5:
        return "medium"
    else:
        return "low"


def _send_completion_notification(db, tool_run_id: str, patterns_count: int, analysis_type: str):
    """Send completion notification to notification_logs table (existing table)."""
    try:
        notification = {
            "recipient": "maintenance_manager",
            "notification_type": "dashboard", 
            "subject": "Time Series Analysis Completed",
            "message": f"{analysis_type.title()} time series analysis completed. {patterns_count} patterns flagged for attention.",
            "status": "sent",
            "sent_at": datetime.now().isoformat(),
            "created_at": datetime.now().isoformat()
        }
        
        db.insert_data("notification_logs", notification)
        logger.info(f"Sent completion notification for time series analysis")
        
    except Exception as e:
        logger.warning(f"Failed to send completion notification: {e}")
