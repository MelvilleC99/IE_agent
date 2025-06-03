# src/workflows/scheduled_maintenance_workflow.py
import os
import sys
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional, cast
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
logger = logging.getLogger("scheduled_maintenance_workflow")

# Import modules
try:
    # Config
    try:
        from config.settings import SUPABASE_URL, SUPABASE_KEY
    except ImportError:
        # Fallback to environment variables
        import os
        SUPABASE_URL = os.getenv("SUPABASE_URL")
        SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    
    # Machine clustering
    from agents.maintenance.analytics.Scheduled_Maintenance.MachineCluster import run_analysis
    from agents.maintenance.analytics.Scheduled_Maintenance.machine_cluster_interpreter import interpret_results
    
    # Maintenance scheduling
    from agents.maintenance.analytics.Scheduled_Maintenance.maintenance_task_scheduler import MaintenanceTaskScheduler
    from agents.maintenance.analytics.Scheduled_Maintenance.maintenance_task_writer import MaintenanceTaskWriter
    from agents.maintenance.analytics.Scheduled_Maintenance.maintenance_notifier import MaintenanceNotifier
    
    # Clustering results management
    from agents.maintenance.utils.tool_run_manager import ToolRunManager
    from agents.maintenance.utils.machine_candidates_saver import save_machine_candidates
    
    # Database client
    from shared_services.supabase_client import SupabaseClient
    
    logger.info("Successfully imported all required modules")
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error(traceback.format_exc())
    sys.exit(1)

def transform_records_for_clustering(records):
    """
    Transform database records to the format expected by MachineCluster.
    
    Args:
        records: List of records from downtime_detail table
        
    Returns:
        List of records in the format expected by MachineCluster
    """
    logger.info(f"Transforming {len(records)} records for clustering analysis")
    
    transformed_records = []
    
    for record in records:
        # Skip records without essential data
        if not record.get('machine_number'):
            continue
            
        # Handle null/empty purchase dates
        purchase_date = record.get('machine_purchase_date')
        if not purchase_date:
            # Use a default date if no purchase date (e.g., 5 years ago)
            purchase_date = '2019-01-01'
        
        # Ensure downtime is a valid number
        total_downtime = record.get('total_downtime', 0)
        try:
            total_downtime_ms = float(total_downtime) * 60000  # Convert minutes to milliseconds
        except (ValueError, TypeError):
            total_downtime_ms = 0
        
        # Transform each record to match MachineCluster expectations
        transformed_record = {
            'id': record.get('id'),
            'machineNumber': str(record.get('machine_number')),  # Ensure string
            'totalDowntime': total_downtime_ms,
            'machineData': {
                'purchaseDate': purchase_date,
                'make': record.get('machine_make', 'Unknown'),
                'type': record.get('machine_type', 'Unknown'),
                'model': record.get('machine_model', 'Unknown')
            },
            # Additional fields that might be useful
            'reason': record.get('reason'),
            'resolved_at': record.get('resolved_at'),
            'created_at': record.get('created_at')
        }
        
        transformed_records.append(transformed_record)
    
    logger.info(f"Transformed {len(transformed_records)} records successfully")
    
    # Log sample transformed record for debugging
    if transformed_records:
        sample = transformed_records[0]
        logger.debug(f"Sample transformed record: {sample}")
    
    return transformed_records

class ScheduledMaintenanceWorkflow:
    """
    Scheduled Maintenance Workflow for maintenance data.
    Analyzes machine data to identify maintenance needs and creates scheduled tasks.
    """
    
    def __init__(self):
        """Initialize the scheduled maintenance workflow"""
        try:
            # Set environment variables from settings
            if not SUPABASE_URL or not SUPABASE_KEY:
                raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in settings")
                
            os.environ["SUPABASE_URL"] = SUPABASE_URL
            os.environ["SUPABASE_KEY"] = SUPABASE_KEY
            
            self.db = SupabaseClient()
            self.scheduler = MaintenanceTaskScheduler(self.db)
            self.writer = MaintenanceTaskWriter(self.db)
            self.notifier = MaintenanceNotifier()
            self.run_manager = ToolRunManager(self.db)
            logger.info("ScheduledMaintenanceWorkflow initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing ScheduledMaintenanceWorkflow: {e}")
            logger.error(traceback.format_exc())
            raise

    def run(self, period_start: Optional[datetime] = None, period_end: Optional[datetime] = None, force: bool = False) -> Dict[str, Any]:
        """
        Run the scheduled maintenance workflow:
        1. Fetch machine data
        2. Transform data for clustering
        3. Run machine clustering analysis
        4. Interpret results
        5. Create maintenance tasks
        6. Send notifications
        
        Args:
            period_start: Optional start date for the analysis period
            period_end: Optional end date for the analysis period
            
        Returns a summary of the workflow execution
        """
        result_summary = {
            'analysis_success': False,
            'tasks_created': 0,
            'errors': [],
            'period_start': period_start.isoformat() if isinstance(period_start, datetime) else period_start,
            'period_end': period_end.isoformat() if isinstance(period_end, datetime) else period_end,
            'records_processed': 0,
            'machines_identified': 0
        }
        
        try:
            # --- Step 0: Check clustering frequency (30-day limit) ---
            logger.info("Checking clustering frequency...")
            can_run, last_run_date = self.run_manager.can_run_tool("scheduled_maintenance", min_days=30)
            
            if not can_run and not force and last_run_date:
                days_since = (datetime.now() - last_run_date).days
                warning_msg = f"Last scheduled maintenance analysis was {days_since} days ago (on {last_run_date.date()}). Minimum 30 days required between clustering runs."
                logger.warning(warning_msg)
                
                result_summary['status'] = 'frequency_warning'
                result_summary['message'] = warning_msg
                result_summary['last_run_date'] = last_run_date.isoformat()
                result_summary['days_since_last_run'] = days_since
                result_summary['suggestion'] = 'Use force=True to override, or wait for the frequency limit to pass'
                return result_summary
            
            if force and not can_run:
                logger.info(f"Force parameter enabled - overriding 30-day frequency limit (last run: {last_run_date.date() if last_run_date else 'never'})")
            else:
                logger.info("Clustering frequency check passed - proceeding with analysis")
            
            # Log the start of analysis
            run_id = self.run_manager.log_tool_start(
                tool_name="scheduled_maintenance",
                period_start=period_start or datetime.now().replace(day=1),
                period_end=period_end or datetime.now(),
                summary=f"Clustering analysis for maintenance scheduling"
            )
            
            # Log analysis period if provided
            if period_start and period_end:
                logger.info(f"Analysis period: {period_start} to {period_end}")
            
            # --- Step 1: Fetch machine data ---
            logger.info("Fetching machine data...")
            
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

            # Query the database with filters
            records = self.db.query_table(
                table_name="downtime_detail",
                columns="*",
                filters=filters,
                limit=1000
            )
            
            if not records:
                msg = "No machine records found in database for the specified period"
                logger.warning(msg)
                result_summary['errors'].append(msg)
                return result_summary
                
            logger.info(f"Retrieved {len(records)} machine records")
            result_summary['records_processed'] = len(records)
            
            # Log sample record for debugging
            if records:
                sample_record = records[0]
                logger.debug(f"Sample database record keys: {list(sample_record.keys())}")
                logger.debug(f"Sample machine_number: {sample_record.get('machine_number')}")
                logger.debug(f"Sample total_downtime: {sample_record.get('total_downtime')}")
            
            # --- Step 2: Transform data for clustering ---
            logger.info("Transforming data for clustering analysis...")
            transformed_records = transform_records_for_clustering(records)
            
            if not transformed_records:
                msg = "No valid records after transformation"
                logger.warning(msg)
                result_summary['errors'].append(msg)
                return result_summary
            
            logger.info(f"Successfully transformed {len(transformed_records)} records")
            
            # --- Step 3: Run machine clustering analysis ---
            logger.info("Running machine clustering analysis...")
            analysis_results = run_analysis(transformed_records)
            
            if not analysis_results:
                msg = "No results from machine clustering analysis"
                logger.warning(msg)
                result_summary['errors'].append(msg)
                return result_summary
            
            # Check for errors in analysis results
            if isinstance(analysis_results, dict) and 'error' in analysis_results:
                msg = f"Machine clustering analysis error: {analysis_results['error']}"
                logger.error(msg)
                result_summary['errors'].append(msg)
                return result_summary
            
            result_summary['analysis_success'] = True
            logger.info("Machine clustering analysis completed successfully")
            
            # --- Step 3.5: Save machine candidates ---
            logger.info("Saving machine candidates for progressive scheduling...")
            try:
                candidates_saved = save_machine_candidates(self.db, run_id, analysis_results)
                logger.info(f"Saved {candidates_saved} machine candidates")
            except Exception as e:
                logger.error(f"Error saving machine candidates: {e}")
                # Don't fail the workflow if candidate saving fails
            
            # Log analysis results summary
            if isinstance(analysis_results, dict):
                aggregated_data = analysis_results.get('aggregated_data', [])
                cluster_summary = analysis_results.get('cluster_summary', [])
                logger.info(f"Analysis produced {len(aggregated_data)} machine records and {len(cluster_summary)} clusters")
                
                # Log sample aggregated data
                if aggregated_data:
                    sample_machine = aggregated_data[0]
                    logger.debug(f"Sample aggregated machine: {sample_machine}")
            
            # --- Step 4: Interpret results ---
            logger.info("Interpreting clustering results...")
            machines_to_service = interpret_results(analysis_results)

            # Ensure correct type: List[Dict[str, Any]]
            if not isinstance(machines_to_service, list) or not all(isinstance(m, dict) for m in machines_to_service):
                logger.warning("machines_to_service is not a list of dicts. Filtering out invalid entries.")
                machines_to_service = [m for m in machines_to_service if isinstance(m, dict)]
            machines_to_service = cast(List[Dict[str, Any]], machines_to_service)

            if not machines_to_service:
                logger.info("No machines identified for maintenance")
                return result_summary
            
            logger.info(f"Identified {len(machines_to_service)} machines for maintenance")
            result_summary['machines_identified'] = len(machines_to_service)
            
            # Log the identified machines
            for i, machine in enumerate(machines_to_service[:5]):  # Log first 5
                logger.info(f"  {i+1}. Machine {machine.get('machineNumber', 'Unknown')}: "
                          f"{machine.get('failure_count', 0)} failures, "
                          f"{machine.get('priority', 'unknown')} priority")
            
            # --- Step 5: Create maintenance tasks ---
            logger.info("Creating maintenance tasks...")
            schedule_results = self.scheduler.schedule_maintenance_tasks(machines_to_service)
            write_results = self.writer.write_maintenance_tasks(schedule_results, self.scheduler)
            
            result_summary['tasks_created'] = write_results.get('tasks_created', 0)
            
            logger.info(f"Successfully created {result_summary['tasks_created']} maintenance tasks")
            
            # --- Step 6: Send notifications ---
            if result_summary['tasks_created'] > 0:
                logger.info("Sending maintenance notifications...")
                notification_result = self.notifier.send_notifications(machines_to_service)
                
                if notification_result.get('status') == 'success':
                    logger.info("Notifications sent successfully")
                else:
                    logger.warning(f"Issue with notifications: {notification_result}")
            
            # --- Step 7: Log analysis run in notification_logs ---
            try:
                logger.info("Logging clustering analysis run...")
                notification_entry = {
                    'type': 'clustering_analysis',
                    'subject': f'Clustering Analysis Completed - {result_summary["machines_identified"]} Machines Identified',
                    'message': f'Clustering analysis completed successfully. Analyzed {result_summary["records_processed"]} records, identified {result_summary["machines_identified"]} machines for maintenance, created {result_summary["tasks_created"]} tasks.',
                    'recipient': 'system',
                    'status': 'sent',
                    'created_at': datetime.now().isoformat(),
                    'metadata': {
                        'tool_run_id': run_id,
                        'records_processed': result_summary['records_processed'],
                        'machines_identified': result_summary['machines_identified'],
                        'tasks_created': result_summary['tasks_created'],
                        'analysis_period': {
                            'start': result_summary.get('period_start'),
                            'end': result_summary.get('period_end')
                        }
                    }
                }
                
                log_result = self.db.table('notification_logs').insert(notification_entry).execute()
                if log_result and hasattr(log_result, 'data') and log_result.data:
                    logger.info("Analysis run logged successfully in notification_logs")
                else:
                    logger.warning("Failed to log analysis run in notification_logs")
                    
            except Exception as e:
                logger.error(f"Error logging analysis run: {e}")
                # Don't fail the entire workflow if logging fails
            
            # --- Step 8: Complete tool run logging ---
            try:
                # Calculate summary data
                cluster_0_count = len([m for m in aggregated_data if m.get('cluster') == 0]) if aggregated_data else 0
                cluster_1_count = len([m for m in aggregated_data if m.get('cluster') == 1]) if aggregated_data else 0
                
                summary = f"Clustering analysis identified {result_summary['machines_identified']} machines for maintenance, created {result_summary['tasks_created']} tasks"
                
                metadata = {
                    "cluster_0_machines": cluster_0_count,
                    "cluster_1_machines": cluster_1_count,
                    "high_priority_count": result_summary['machines_identified'],
                    "analysis_period_days": (period_end - period_start).days if period_start and period_end else 0,
                    "force_override": force
                }
                
                self.run_manager.log_tool_complete(
                    run_id=run_id,
                    items_processed=result_summary['records_processed'], 
                    items_created=result_summary['tasks_created'],
                    summary=summary,
                    metadata=metadata
                )
                
                result_summary['run_id'] = run_id
                logger.info(f"Tool run logged successfully: {run_id}")
                
            except Exception as e:
                logger.error(f"Error completing tool run log: {e}")
                # Don't fail workflow if logging fails
            
            logger.info("Scheduled maintenance workflow completed successfully")
            return result_summary
            
        except Exception as e:
            # Log tool run error if we have a run_id
            if hasattr(self, 'run_manager') and 'run_id' in locals():
                try:
                    self.run_manager.log_tool_error(run_id, str(e))
                except:
                    pass  # Don't fail on logging error
                    
            error_msg = f"Error in workflow execution: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            result_summary['errors'].append(error_msg)
            return result_summary


def main():
    """Main entry point for running the scheduled maintenance workflow from command line"""
    logger.info("Starting Scheduled Maintenance Workflow")
    
    import argparse
    parser = argparse.ArgumentParser(description="Run Scheduled Maintenance Workflow")
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
        wf = ScheduledMaintenanceWorkflow()
        result = wf.run(
            period_start=period_start,
            period_end=period_end
        )
        
        # Print summary
        print("\n=== Scheduled Maintenance Workflow Summary ===")
        print(f"Analysis period: {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}")
        print(f"Records processed: {result.get('records_processed', 0)}")
        print(f"Analysis status: {'Success' if result.get('analysis_success') else 'Failed'}")
        print(f"Machines identified: {result.get('machines_identified', 0)}")
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