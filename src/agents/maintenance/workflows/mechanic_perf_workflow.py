#!/usr/bin/env python3
import sys
import os
import json
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
from agents.maintenance.analytics.Mechanic_performance_tool.mechanic_repair_analyzer   import run_mechanic_analysis
from agents.maintenance.analytics.Mechanic_performance_tool.mechanic_repair_interpreter import interpret_analysis_results
from agents.maintenance.analytics.Mechanic_performance_tool.write_analysis               import AnalysisWriter
from agents.maintenance.analytics.Mechanic_performance_tool.write_findings               import FindingsWriter
from agents.maintenance.analytics.Mechanic_performance_tool.write_tasks                  import TaskWriter
from agents.maintenance.analytics.Mechanic_performance_tool.notification_handler         import NotificationHandler


def run_mechanic_performance_workflow(data_source=None):
    """
    Run the complete mechanic performance analysis workflow:
    1. Analyze mechanic performance data
    2. Write analysis results to database for historical reference
    3. Interpret analysis and identify issues
    4. Write findings to database
    5. Create tasks from findings
    6. Send notification
    
    Args:
        data_source: Path to the data source file
        
    Returns:
        dict: Summary of workflow results
    """
    start_time = datetime.now()
    logger.info("=== Starting Mechanic Performance Analysis Workflow ===")
    
    results = {
        "workflow_start": start_time.isoformat(),
        "status": "running",
        "steps": {
            "analysis": {"status": "pending", "details": None},
            "write_analysis": {"status": "pending", "details": None},
            "interpretation": {"status": "pending", "details": None},
            "write_findings": {"status": "pending", "details": None},
            "create_tasks": {"status": "pending", "details": None},
            "notification": {"status": "pending", "details": None}
        },
        "workflow_end": None,
        "execution_time_seconds": None
    }
    
    try:
        # Step 1: Analyze mechanic performance data
        logger.info("Step 1: Running mechanic performance analysis")
        if data_source is None:
            logger.error("No data source provided")
            results["status"] = "failed"
            return results
            
        analysis_results = run_mechanic_analysis(data_source)
        
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
        
        # Step 5: Create tasks from findings
        logger.info("Step 5: Creating tasks from findings")
        task_writer = TaskWriter()
        tasks = task_writer.create_tasks_from_findings()
        
        results["steps"]["create_tasks"]["status"] = "completed"
        results["steps"]["create_tasks"]["details"] = {
            "tasks_created": len(tasks)
        }
        logger.info(f"Created {len(tasks)} tasks from findings")
        
        # Step 6: Send notification
        logger.info("Step 6: Sending notification")
        notification_handler = NotificationHandler()
        notification = notification_handler.create_workflow_completion_notification(len(tasks))
        
        if notification:
            results["steps"]["notification"]["status"] = "completed"
            results["steps"]["notification"]["details"] = {
                "notification_id": notification.get("id")
            }
            logger.info(f"Created notification {notification.get('id')}")
            
            # Send pending notifications
            sent_count = notification_handler.send_pending_notifications()
            logger.info(f"Sent {sent_count} notifications")
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
    parser.add_argument("--data-source", required=True, help="Path to the maintenance data JSON file")
    args = parser.parse_args()
    
    if not os.path.exists(args.data_source):
        print(f"Error: Data source file not found at {args.data_source}")
        sys.exit(1)
    
    results = run_mechanic_performance_workflow(args.data_source)
    
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