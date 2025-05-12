# src/agents/maintenance/workflows/hourly_analysis_workflow.py
import os
import sys
import logging
import json
from datetime import datetime, timedelta
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
    # Time series hourly analysis
    from agents.maintenance.analytics.time_series_tool.time_series_hour import run_working_hours_analysis
    
    # Hourly interpreter
    from agents.maintenance.analytics.time_series_tool.hourly_pattern_interpreter import interpret_hourly_findings
    
    # Shared writers for findings and insights
    from agents.maintenance.analytics.Mechanic_performance_tool.write_findings import FindingsWriter
    
    # Date selection utility
    from agents.maintenance.tools.date_selector import DateSelector
    
    logger.info("Successfully imported all required modules")
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error(traceback.format_exc())
    sys.exit(1)

class HourlyAnalysisWorkflow:
    """
    Hourly Analysis Workflow for maintenance data.
    Identifies patterns in hourly maintenance data and creates findings.
    Specializes in detecting:
    1. Hours with unusually high incident counts (statistical outliers)
    2. Mechanic-specific hourly patterns for repair/response times
    3. Line-specific hourly patterns for downtime
    """
    
    def __init__(self):
        """Initialize the hourly analysis workflow with writers for findings"""
        try:
            self.findings_writer = FindingsWriter()
            logger.info("HourlyAnalysisWorkflow initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing HourlyAnalysisWorkflow: {e}")
            logger.error(traceback.format_exc())
            raise

    def run(self, period_start=None, period_end=None) -> dict:
        """
        Run the hourly analysis workflow:
        1. Run hourly analysis
        2. Interpret findings
        3. Write findings to database
        
        Args:
            period_start: Optional start date for the analysis period
            period_end: Optional end date for the analysis period
            
        Returns a summary of the workflow execution
        """
        result_summary = {
            'hourly_analysis_success': False,
            'findings_count': 0,
            'findings_saved': 0,
            'period_start': period_start.isoformat() if isinstance(period_start, datetime) else period_start,
            'period_end': period_end.isoformat() if isinstance(period_end, datetime) else period_end,
            'errors': []
        }
        
        try:
            # Log analysis period if provided
            if period_start and period_end:
                logger.info(f"Analysis period: {period_start} to {period_end}")
            
            # --- Step 1: Hourly analysis ---
            logger.info("Starting hourly analysis...")
            hourly_summary = run_working_hours_analysis(
                work_hours_start=7,
                work_hours_end=17,
                min_incidents=2,
                line_variance_pct=20.0,
                period_start=period_start,
                period_end=period_end
            )
            
            # Log the hourly analysis results structure
            logger.info(f"Hourly analysis summary keys: {list(hourly_summary.keys())}")
            if 'statistical_outliers' in hourly_summary:
                logger.info(f"Statistical outliers found: {len(hourly_summary['statistical_outliers'])}")
            if 'line_hourly_outliers' in hourly_summary:
                logger.info(f"Line hourly outliers found: {len(hourly_summary['line_hourly_outliers'])}")
            if 'mechanic_hourly_stats' in hourly_summary:
                logger.info(f"Mechanic hourly stats found: {len(hourly_summary['mechanic_hourly_stats'])}")
                
            logger.info("Hourly analysis complete")
            result_summary['hourly_analysis_success'] = True
            
            # Save raw data to file for debugging
            try:
                debug_dir = os.path.join(project_root, "debug")
                os.makedirs(debug_dir, exist_ok=True)
                with open(os.path.join(debug_dir, "hourly_analysis_raw.json"), "w") as f:
                    json.dump(hourly_summary, f, indent=2)
                logger.info(f"Saved raw hourly analysis to {os.path.join(debug_dir, 'hourly_analysis_raw.json')}")
            except Exception as e:
                logger.warning(f"Failed to save raw hourly analysis: {e}")
            
            # --- Step 2: Interpret findings ---
            logger.info("Interpreting hourly findings...")
            findings = interpret_hourly_findings(hourly_summary)
            result_summary['findings_count'] = len(findings)
            
            # Save findings to file for debugging
            try:
                with open(os.path.join(debug_dir, "hourly_findings.json"), "w") as f:
                    json.dump(findings, f, indent=2)
                logger.info(f"Saved hourly findings to {os.path.join(debug_dir, 'hourly_findings.json')}")
            except Exception as e:
                logger.warning(f"Failed to save hourly findings: {e}")
            
            # Log finding types
            finding_types = {}
            for f in findings:
                finding_type = f.get('analysis_type', 'unknown')
                finding_types[finding_type] = finding_types.get(finding_type, 0) + 1
            logger.info(f"Finding types: {finding_types}")
            
            # --- Step 3: Save findings to database ---
            if findings:
                logger.info("Saving findings to database...")
                # Add period information to findings
                for finding in findings:
                    finding['period_start'] = result_summary['period_start']
                    finding['period_end'] = result_summary['period_end']
                    
                saved_findings = self.findings_writer.save_findings(findings)
                result_summary['findings_saved'] = len(saved_findings)
                logger.info(f"Saved {len(saved_findings)} findings")
            else:
                logger.info("No findings to save")
            
            # Add summary data for reference
            result_summary['hourly_summary'] = hourly_summary
            
            return result_summary
            
        except Exception as e:
            error_msg = f"Error in workflow execution: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            result_summary['errors'].append(error_msg)
            return result_summary


def main():
    """Main entry point for running the hourly analysis workflow from command line"""
    logger.info("Starting Hourly Analysis Workflow")
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run Hourly Analysis Workflow")
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
        wf = HourlyAnalysisWorkflow()
        result = wf.run(period_start=period_start, period_end=period_end)
        
        # Print summary
        print("\n=== Hourly Analysis Workflow Summary ===")
        print(f"Analysis period: {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}")
        print(f"Hourly analysis: {'Success' if result.get('hourly_analysis_success') else 'Failed'}")
        print(f"Findings generated: {result.get('findings_count', 0)}")
        print(f"Findings saved to database: {result.get('findings_saved', 0)}")
        
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