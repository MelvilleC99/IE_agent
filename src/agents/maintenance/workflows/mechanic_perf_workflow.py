#!/usr/bin/env python3
import sys
import os
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env.local")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Import components
from agents.maintenance.analytics.Mechanic_performance_tool.mechanic_repair_analyzer import run_mechanic_analysis
from agents.maintenance.analytics.Mechanic_performance_tool.mechanic_repair_interpreter import interpret_analysis_results
from agents.maintenance.analytics.Mechanic_performance_tool.write_analysis import AnalysisWriter
from agents.maintenance.analytics.Mechanic_performance_tool.write_findings import FindingsWriter
from agents.maintenance.analytics.Mechanic_performance_tool.write_watchlist import WatchlistWriter
from agents.maintenance.analytics.Mechanic_performance_tool.notification_handler import NotificationHandler
from agents.maintenance.tools.date_selector import DateSelector
from shared_services.db_client import get_connection

def run_mechanic_performance_workflow(start_date=None, end_date=None):
    """
    Run the complete mechanic performance analysis workflow:
    1. Analyze mechanic performance data from database
    2. Write analysis results to database for historical reference
    3. Interpret analysis and identify issues
    4. Write findings to database
    5. Create watchlist items from findings
    6. Send notification
    
    Args:
        start_date: Start date for analysis period (optional)
        end_date: End date for analysis period (optional)
        
    Returns:
        dict: Summary of workflow results
    """
    start_time = datetime.now()
    logger.info("=== Starting Mechanic Performance Analysis Workflow ===")
    
    if start_date and end_date:
        logger.info(f"Analysis period: {start_date} to {end_date}")
    
    results = {
        "workflow_start": start_time.isoformat(),
        "status": "running",
        "steps": {
            "analysis": {"status": "pending", "details": None},
            "write_analysis": {"status": "pending", "details": None},
            "interpretation": {"status": "pending", "details": None},
            "write_findings": {"status": "pending", "details": None},
            "create_watchlist_items": {"status": "pending", "details": None},
            "notification": {"status": "pending", "details": None}
        },
        "workflow_end": None,
        "execution_time_seconds": None
    }
    
    try:
        # Step 1: Analyze mechanic performance data
        logger.info("Step 1: Running mechanic performance analysis")
        analysis_results = run_mechanic_analysis(start_date=start_date, end_date=end_date)
        
        if not analysis_results:
            logger.error("Analysis failed or returned empty results") 
            results["status"] = "failed"
            results["steps"]["analysis"]["status"] = "failed"
            return results
            
        results["steps"]["analysis"]["status"] = "completed"
        results["steps"]["analysis"]["details"] = {
            "dimensions": list(analysis_results.keys())
        }
        logger.info("Analysis completed successfully")
        
        # Step 2: Write analysis results to database
        logger.info("Step 2: Writing analysis results to database")
        analysis_writer = AnalysisWriter()
        analysis_records = analysis_writer.write_analysis_results(analysis_results)
        
        results["steps"]["write_analysis"]["status"] = "completed"
        results["steps"]["write_analysis"]["details"] = {
            "records_written": len(analysis_records) if analysis_records else 0
        }
        logger.info(f"Wrote {len(analysis_records) if analysis_records else 0} analysis records to database")
        
        # Step 3: Interpret analysis and identify issues
        logger.info("Step 3: Interpreting analysis results")
        findings = interpret_analysis_results(analysis_results)
        
        results["steps"]["interpretation"]["status"] = "completed"
        results["steps"]["interpretation"]["details"] = {
            "findings_identified": len(findings)
        }
        logger.info(f"Identified {len(findings)} potential findings")
        
        # Step 4: Write findings to database
        logger.info("Step 4: Writing findings to database")
        findings_writer = FindingsWriter()
        saved_findings = findings_writer.save_findings(findings)
        
        results["steps"]["write_findings"]["status"] = "completed"
        results["steps"]["write_findings"]["details"] = {
            "findings_saved": len(saved_findings)
        }
        logger.info(f"Saved {len(saved_findings)} findings to database")
        
        # Step 5: Create watchlist items from findings
        logger.info("Step 5: Creating watchlist items from findings")
        watchlist_writer = WatchlistWriter()
        watchlist_items = watchlist_writer.create_watchlist_items_from_findings()
        
        results["steps"]["create_watchlist_items"]["status"] = "completed"
        results["steps"]["create_watchlist_items"]["details"] = {
            "watchlist_items_created": len(watchlist_items)
        }
        logger.info(f"Created {len(watchlist_items)} watchlist items from findings")
        
        # Step 6: Send notification
        logger.info("Step 6: Sending notification")
        notification_handler = NotificationHandler()
        notification = notification_handler.create_workflow_completion_notification(len(watchlist_items))
        
        if notification:
            results["steps"]["notification"]["status"] = "completed"
            results["steps"]["notification"]["details"] = {
                "notification_id": notification.get("id")
            }
            logger.info(f"Created notification {notification.get('id')}")
            
            # Send pending notifications
            sent_notifications = notification_handler.send_pending_notifications()
            logger.info(f"Sent {len(sent_notifications)} notifications")
            
            # Add sent notifications to results
            results["steps"]["notification"]["details"]["notifications_sent"] = len(sent_notifications)
        else:
            results["steps"]["notification"]["status"] = "failed"
            logger.warning("Failed to create notification")
        
        # Mark workflow as completed
        results["status"] = "completed"
        
    except Exception as e:
        logger.exception(f"Error in workflow: {e}")
        results["status"] = "failed"
        
    # Record completion time and duration
    end_time = datetime.now()
    results["workflow_end"] = end_time.isoformat()
    results["execution_time_seconds"] = (end_time - start_time).total_seconds()
    
    logger.info(f"=== Workflow completed with status: {results['status']} ===")
    logger.info(f"Execution time: {results['execution_time_seconds']} seconds")
    
    return results

# For direct execution
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the mechanic performance analysis workflow")
    parser.add_argument("--start-date", help="Start date for analysis period (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for analysis period (YYYY-MM-DD)")
    parser.add_argument("--mode", choices=["interactive", "args"], default="interactive", 
                        help="Date selection mode (default: interactive)")
    args = parser.parse_args()
    
    # Use DateSelector if dates not provided via command line
    if not (args.start_date and args.end_date):
        start_date, end_date = DateSelector.get_date_range(mode=args.mode)
    else:
        start_date, end_date = args.start_date, args.end_date
    
    results = run_mechanic_performance_workflow(
        start_date=start_date,
        end_date=end_date
    )
    
    print("\nWorkflow Summary:")
    print(f"Status: {results['status']}")
    print(f"Start: {results['workflow_start']}")
    print(f"End: {results['workflow_end']}")
    print(f"Duration: {results['execution_time_seconds']} seconds")
    
    print("\nStep Results:")
    for step, details in results["steps"].items():
        print(f"- {step}: {details['status']}")
        if details['details']:
            for key, value in details['details'].items():
                print(f"  - {key}: {value}")
    
    if results["status"] != "completed":
        sys.exit(1)