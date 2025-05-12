import os
import sys
import logging
import json
from datetime import datetime
import traceback
import argparse

# Ensure src/ directory is on sys.path for absolute imports
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "../../.."))
src_root = os.path.join(project_root, "src")
if src_root not in sys.path:
    sys.path.insert(0, src_root)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import modules
try:
    # Repeat failure analysis
    from agents.maintenance.analytics.Repeat_failures.repeat_failure import run_analysis
    
    # Repeat failure interpreter
    from agents.maintenance.analytics.Repeat_failures.repeat_failure_interpreter import interpret_repeat_failure_findings
    
    # Shared writers for findings and insights
    from agents.maintenance.analytics.Mechanic_performance_tool.write_findings import FindingsWriter
    from agents.maintenance.analytics.Mechanic_performance_tool.write_watchlist import WatchlistWriter
    
    # Date selection utility
    from agents.maintenance.tools.date_selector import DateSelector
    
    logger.info("Successfully imported all required modules")
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error(traceback.format_exc())
    sys.exit(1)

class RepeatFailureWorkflow:
    """
    Repeat Failure Analysis Workflow for maintenance data.
    Identifies patterns in repeat failures within a short time window and creates findings.
    Specializes in detecting:
    1. Machines with multiple repeat failures
    2. Mechanics with multiple repeat failures
    3. Critical rapid repeat failures
    4. Common problems causing repeat failures
    """
    
    def __init__(self):
        """Initialize the repeat failure analysis workflow with writers for findings and tasks"""
        try:
            self.findings_writer = FindingsWriter()
            self.watchlist_writer = WatchlistWriter()
            logger.info("RepeatFailureWorkflow initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing RepeatFailureWorkflow: {e}")
            logger.error(traceback.format_exc())
            raise

    def run(self, threshold_minutes=120, period_start=None, period_end=None) -> dict:
        """
        Run the repeat failure analysis workflow:
        1. Fetch maintenance data
        2. Run repeat failure analysis
        3. Interpret findings
        4. Write findings to database
        5. Create tasks for new findings
        
        Args:
            threshold_minutes: The time window in minutes to consider for repeat failures
            period_start: Optional start date for the analysis period
            period_end: Optional end date for the analysis period
            
        Returns a summary of the workflow execution
        """
        result_summary = {
            'repeat_failure_analysis_success': False,
            'findings_count': 0,
            'findings_saved': 0,
            'tasks_created': 0,
            'period_start': period_start.isoformat() if isinstance(period_start, datetime) else period_start,
            'period_end': period_end.isoformat() if isinstance(period_end, datetime) else period_end,
            'errors': []
        }
        
        try:
            # Log analysis period if provided
            if period_start and period_end:
                logger.info(f"Analysis period: {period_start} to {period_end}")
            
            # --- Step 1: Fetch maintenance data ---
            logger.info("Fetching maintenance data...")
            from shared_services.supabase_client import SupabaseClient
            
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
                
                logger.info(f"Filtering records by date range: {start_date_str} to {end_date_str}")
            elif period_start:
                start_date_str = period_start.isoformat() if isinstance(period_start, datetime) else period_start
                filters['resolved_at.gte'] = start_date_str
                logger.info(f"Filtering records from {start_date_str} onwards")
            elif period_end:
                end_date_str = period_end.isoformat() if isinstance(period_end, datetime) else period_end
                filters['resolved_at.lte'] = end_date_str
                logger.info(f"Filtering records up to {end_date_str}")
            
            records = db.query_table(
                table_name="downtime_detail",
                columns="*",
                filters=filters,
                limit=1000
            )
            
            if not records:
                msg = "No maintenance records found in database for the specified period"
                logger.warning(msg)
                result_summary['errors'].append(msg)
                return result_summary
                
            logger.info(f"Retrieved {len(records)} maintenance records")
            
            # --- Step 2: Run repeat failure analysis ---
            logger.info(f"Running repeat failure analysis with {threshold_minutes} minute threshold...")
            repeat_failure_results = run_analysis(records, threshold_minutes)
            
            # Check for analysis errors
            if "error" in repeat_failure_results:
                msg = f"Error in repeat failure analysis: {repeat_failure_results['error']}"
                logger.error(msg)
                result_summary['errors'].append(msg)
                return result_summary
            
            # Log the analysis results
            if "repeat_failures" in repeat_failure_results:
                count = len(repeat_failure_results["repeat_failures"])
                logger.info(f"Found {count} repeat failures")
                if count == 0:
                    logger.info("No repeat failures found within the threshold time")
                    result_summary['repeat_failure_analysis_success'] = True
                    return result_summary
            else:
                msg = "Invalid results format from repeat failure analysis"
                logger.error(msg)
                result_summary['errors'].append(msg)
                return result_summary
            
            result_summary['repeat_failure_analysis_success'] = True
            
            # Save raw data to file for debugging
            try:
                debug_dir = os.path.join(project_root, "debug")
                os.makedirs(debug_dir, exist_ok=True)
                with open(os.path.join(debug_dir, "repeat_failure_raw.json"), "w") as f:
                    json.dump(repeat_failure_results, f, indent=2)
                logger.info(f"Saved raw repeat failure analysis to {os.path.join(debug_dir, 'repeat_failure_raw.json')}")
            except Exception as e:
                logger.warning(f"Failed to save raw repeat failure analysis: {e}")
            
            # --- Step 3: Interpret findings ---
            logger.info("Interpreting repeat failure findings...")
            findings = interpret_repeat_failure_findings(repeat_failure_results)
            result_summary['findings_count'] = len(findings)
            
            # Save findings to file for debugging
            try:
                with open(os.path.join(debug_dir, "repeat_failure_findings.json"), "w") as f:
                    json.dump(findings, f, indent=2)
                logger.info(f"Saved repeat failure findings to {os.path.join(debug_dir, 'repeat_failure_findings.json')}")
            except Exception as e:
                logger.warning(f"Failed to save repeat failure findings: {e}")
            
            # Log finding types
            finding_types = {}
            for f in findings:
                finding_type = f.get('analysis_type', 'unknown')
                finding_types[finding_type] = finding_types.get(finding_type, 0) + 1
            logger.info(f"Finding types: {finding_types}")
            
            # --- Step 4: Save findings to database ---
            if findings:
                logger.info("Saving findings to database...")
                # Add period information to findings
                for finding in findings:
                    finding['period_start'] = result_summary['period_start']
                    finding['period_end'] = result_summary['period_end']
                
                saved_findings = self.findings_writer.save_findings(findings)
                result_summary['findings_saved'] = len(saved_findings)
                logger.info(f"Saved {len(saved_findings)} findings")
                
                # --- Step 5: Create tasks for new findings ---
                logger.info("Creating tasks from findings...")
                tasks_created = 0
                for finding in saved_findings:
                    item = self.watchlist_writer.create_watchlist_item_from_finding(finding)
                    if item:
                        tasks_created += 1
                result_summary['tasks_created'] = tasks_created
                logger.info(f"Created {tasks_created} tasks")
            else:
                logger.info("No findings to save")
            
            # Add summary data for reference
            result_summary['repeat_failure_summary'] = {
                'total_repeat_failures': len(repeat_failure_results['repeat_failures']),
                'threshold_minutes': threshold_minutes,
                'explanation': repeat_failure_results.get('explanation', ''),
                'period_start': result_summary['period_start'],
                'period_end': result_summary['period_end']
            }
            
            return result_summary
            
        except Exception as e:
            error_msg = f"Error in workflow execution: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            result_summary['errors'].append(error_msg)
            return result_summary


def main():
    """Main entry point for running the repeat failure workflow from command line"""
    logger.info("Starting Repeat Failure Analysis Workflow")
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run Repeat Failure Analysis Workflow")
    parser.add_argument("--threshold", type=int, default=120, help="Threshold in minutes for repeat failures (default: 120)")
    parser.add_argument("--start_date", type=str, help="Start date for analysis (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, help="End date for analysis (YYYY-MM-DD)")
    parser.add_argument("--mode", choices=["interactive", "args"], default="interactive", 
                        help="Date selection mode (default: interactive)")
    args = parser.parse_args()
    
    try:
        # Handle date selection
        if args.start_date and args.end_date:
            # Use provided dates from command line
            period_start = datetime.strptime(args.start_date, "%Y-%m-%d")
            period_end = datetime.strptime(args.end_date, "%Y-%m-%d")
            logger.info(f"Using specified date range: {period_start.date()} to {period_end.date()}")
        else:
            # Use DateSelector for interactive selection
            start_date_str, end_date_str = DateSelector.get_date_range(mode=args.mode)
            period_start = datetime.strptime(start_date_str, "%Y-%m-%d")
            period_end = datetime.strptime(end_date_str, "%Y-%m-%d")
            logger.info(f"Selected date range: {period_start.date()} to {period_end.date()}")
        
        # For the end date, set it to the end of day (23:59:59) to include all records from that day
        period_end = period_end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # Run the workflow
        wf = RepeatFailureWorkflow()
        result = wf.run(
            threshold_minutes=args.threshold,
            period_start=period_start,
            period_end=period_end
        )
        
        # Print summary
        print("\n=== Repeat Failure Analysis Workflow Summary ===")
        print(f"Analysis period: {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}")
        print(f"Analysis: {'Success' if result.get('repeat_failure_analysis_success') else 'Failed'}")
        print(f"Findings generated: {result.get('findings_count', 0)}")
        print(f"Findings saved to database: {result.get('findings_saved', 0)}")
        print(f"Tasks created: {result.get('tasks_created', 0)}")
        
        if result.get('errors'):
            print("\nErrors encountered:")
            for error in result['errors']:
                print(f"- {error}")
        
        print("\nWorkflow execution complete.")
        return result
        
    except Exception as e:
        logger.error(f"Unhandled exception in main workflow: {e}")
        logger.error(traceback.format_exc())
        print(f"\nError running workflow: {e}")
        return {'status': 'failed', 'error': str(e)}


if __name__ == '__main__':
    main()