import os
import sys
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

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
    # Config
    from config.settings import SUPABASE_URL, SUPABASE_KEY
    
    # Pareto analysis
    from agents.maintenance.analytics.pareto.pareto_analyser import run_analysis
    
    # Pareto interpreter
    from agents.maintenance.analytics.pareto.pareto_interpreter import interpret_findings
    
    # Pareto summary
    from agents.maintenance.analytics.pareto.pareto_summary import generate_summary
    
    # Pareto writer
    from agents.maintenance.analytics.pareto.pareto_writer import ParetoWriter
    
    # Database client
    from shared_services.supabase_client import SupabaseClient
    
    logger.info("Successfully imported all required modules")
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error(traceback.format_exc())
    sys.exit(1)

class ParetoAnalysisWorkflow:
    """
    Pareto Analysis Workflow for maintenance data.
    Identifies the most significant factors contributing to downtime and creates findings.
    Specializes in detecting:
    1. Machines with highest downtime
    2. Most common failure reasons
    3. Production lines with most issues
    4. Product categories with highest impact
    """
    
    def __init__(self):
        """Initialize the Pareto analysis workflow"""
        try:
            # Set environment variables from settings
            if not SUPABASE_URL or not SUPABASE_KEY:
                raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in settings")
                
            os.environ["SUPABASE_URL"] = SUPABASE_URL
            os.environ["SUPABASE_KEY"] = SUPABASE_KEY
            
            self.db = SupabaseClient()
            self.writer = ParetoWriter(self.db)
            logger.info("ParetoAnalysisWorkflow initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing ParetoAnalysisWorkflow: {e}")
            logger.error(traceback.format_exc())
            raise

    def run(self, threshold: float = 80.0, period_start: Optional[datetime] = None, period_end: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Run the Pareto analysis workflow:
        1. Fetch maintenance data
        2. Run Pareto analysis
        3. Interpret findings
        4. Generate and display summary
        5. Save results to database
        
        Args:
            threshold: The cumulative percentage threshold for Pareto analysis (default: 80%)
            period_start: Optional start date for the analysis period
            period_end: Optional end date for the analysis period
            
        Returns a summary of the workflow execution
        """
        result_summary = {
            'pareto_analysis_success': False,
            'findings_count': 0,
            'errors': [],
            'analysis_id': None,
            'period_start': period_start.isoformat() if isinstance(period_start, datetime) else period_start,
            'period_end': period_end.isoformat() if isinstance(period_end, datetime) else period_end
        }
        
        try:
            # Log analysis period if provided
            if period_start and period_end:
                logger.info(f"Analysis period: {period_start} to {period_end}")
            
            # --- Step 1: Fetch maintenance data ---
            logger.info("Fetching maintenance data...")
            
            # Create filters based on date range
            filters = {}
            if period_start and period_end:
                # Convert datetime objects to strings if needed
                start_date_str = period_start.isoformat() if isinstance(period_start, datetime) else period_start
                end_date_str = period_end.isoformat() if isinstance(period_end, datetime) else period_end
                
                # Use the correct filter format for date ranges
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

            # Log the filters being applied
            if filters:
                logger.info(f"Applying filters: {filters}")

            # Query the database with filters
            records = self.db.query_table(
                table_name="downtime_detail",
                columns="*,resolved_at",
                filters=filters,
                limit=1000
            )
            
            if not records:
                msg = "No maintenance records found in database for the specified period"
                logger.warning(msg)
                result_summary['errors'].append(msg)
                return result_summary
                
            logger.info(f"Retrieved {len(records)} maintenance records")
            
            # --- Step 2: Run Pareto analysis ---
            logger.info(f"Running Pareto analysis with {threshold}% threshold...")
            pareto_results = run_analysis(records, threshold=threshold)
            
            # Check for analysis errors
            if "error" in pareto_results:
                msg = f"Error in Pareto analysis: {pareto_results['error']}"
                logger.error(msg)
                result_summary['errors'].append(msg)
                return result_summary
            
            # Log the analysis results
            if "dimensions" in pareto_results:
                count = len(pareto_results["dimensions"])
                logger.info(f"Analyzed {count} dimensions")
                if count == 0:
                    logger.info("No dimensions analyzed")
                    result_summary['pareto_analysis_success'] = True
                    return result_summary
            else:
                msg = "Invalid results format from Pareto analysis"
                logger.error(msg)
                result_summary['errors'].append(msg)
                return result_summary
            
            result_summary['pareto_analysis_success'] = True
            
            # --- Step 3: Interpret findings ---
            logger.info("Interpreting Pareto findings...")
            interpreted_results = interpret_findings(pareto_results)
            
            # Check for interpretation errors
            if 'error' in interpreted_results:
                msg = f"Error interpreting findings: {interpreted_results['error']}"
                logger.error(msg)
                result_summary['errors'].append(msg)
                return result_summary
            
            # Extract findings from interpreted results
            findings = []
            for dimension, dim_results in interpreted_results.get('interpreted_findings', {}).items():
                if 'findings' in dim_results:
                    findings.extend(dim_results['findings'])
            
            result_summary['findings_count'] = len(findings)
            
            # --- Step 4: Generate and display summary ---
            logger.info("Generating Pareto summary...")
            summary = generate_summary(interpreted_results)
            
            # Add period information to summary
            if period_start and period_end:
                period_info = f"\nAnalysis Period: {period_start.strftime('%Y-%m-%d') if isinstance(period_start, datetime) else period_start} to {period_end.strftime('%Y-%m-%d') if isinstance(period_end, datetime) else period_end}"
                summary = period_info + "\n" + summary
            
            # Print the summary to terminal
            print("\n" + summary)
            
            # Store summary in result
            result_summary['summary'] = summary
            
            # --- Step 5: Save results to database ---
            logger.info("Saving Pareto analysis results...")
            analysis_id = self.writer.save_analysis(
                analysis_results=pareto_results,
                interpreted_findings=interpreted_results,
                summary_text=summary,
                period_start=period_start,
                period_end=period_end
            )
            
            if analysis_id:
                logger.info(f"Successfully saved analysis with ID: {analysis_id}")
                result_summary['analysis_id'] = analysis_id
            else:
                logger.warning("Failed to save analysis results")
            
            return result_summary
            
        except Exception as e:
            error_msg = f"Error in workflow execution: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            result_summary['errors'].append(error_msg)
            return result_summary


def main():
    """Main entry point for running the Pareto analysis workflow from command line"""
    logger.info("Starting Pareto Analysis Workflow")
    
    import argparse
    parser = argparse.ArgumentParser(description="Run Pareto Analysis Workflow")
    parser.add_argument("--threshold", type=float, default=80.0, help="Threshold percentage for Pareto analysis (default: 80.0)")
    parser.add_argument("--start_date", type=str, help="Start date for analysis (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, help="End date for analysis (YYYY-MM-DD)")
    parser.add_argument("--mode", choices=["interactive", "args"], default="interactive", 
                        help="Date selection mode (default: interactive)")
    args = parser.parse_args()
    
    try:
        # Use DateSelector for date range selection
        from agents.maintenance.tools.date_selector import DateSelector
        
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
        
        # Initialize and run workflow
        wf = ParetoAnalysisWorkflow()
        result = wf.run(
            threshold=args.threshold,
            period_start=period_start,
            period_end=period_end
        )
        
        # Print summary
        print("\n=== Pareto Analysis Workflow Summary ===")
        print(f"Analysis period: {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}")
        print(f"Analysis status: {'Success' if result.get('pareto_analysis_success') else 'Failed'}")
        print(f"Findings generated: {result.get('findings_count', 0)}")
        if result.get('analysis_id'):
            print(f"Analysis saved with ID: {result['analysis_id']}")
        
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