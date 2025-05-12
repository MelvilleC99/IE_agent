#!/usr/bin/env python3
import sys
import os
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Configuration & Environment Loading
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env.local")

# Structured logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Imports
from agents.maintenance.tracker.Performance_tracking.watchlist_monitor import WatchlistMonitorChecker
from agents.maintenance.tracker.Performance_tracking.daily_performance import DailyPerformanceMeasurement
from agents.maintenance.tracker.Performance_tracking.weekly_performance import WeeklyPerformanceMeasurement
from agents.maintenance.tracker.Performance_tracking.start_summary import SummaryStarter
from shared_services.db_client import get_connection

class PerformanceMonitoringWorkflow:
    """
    Performance Monitoring Workflow coordinates the watchlist monitoring process.
    Identifies watchlist items that need measurements and collects those measurements.
    Also identifies watchlist items ready for evaluation and marks them for the summary workflow.
    """
    def __init__(self):
        self.today = datetime.now().date()
        
        logger.info(f"Workflow initialized for {self.today}")
        
        # Initialize components
        self.watchlist_checker = WatchlistMonitorChecker()
        self.daily_processor = DailyPerformanceMeasurement()
        self.weekly_processor = WeeklyPerformanceMeasurement()
        self.summary_starter = SummaryStarter()
        
        # Initialize database connection
        try:
            self.supabase = get_connection()
            logger.info("Connected to database successfully")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            self.supabase = None
    
    def save_workflow_results(self, results):
        """
        Save workflow results to database
        
        Args:
            results: Dictionary with workflow results
            
        Returns:
            dict: The saved workflow record or None if save failed
        """
        if not self.supabase:
            logger.error("Cannot save workflow results - no database connection")
            return None
            
        try:
            # Create workflow record
            workflow_data = {
                "workflow_type": "performance_monitoring",
                "run_date": self.today.isoformat(),
                "status": results.get("status", "unknown"),
                "results": results,
                "created_at": datetime.now().isoformat()
            }
            
            result = self.supabase.table('workflow_logs').insert(workflow_data).execute()
            
            if result.data:
                workflow_id = result.data[0]['id']
                logger.info(f"Saved workflow results to database with ID {workflow_id}")
                return result.data[0]
            else:
                logger.error("Failed to save workflow results to database")
                return None
        except Exception as e:
            logger.error(f"Error saving workflow results: {e}")
            return None
    
    def process_daily_tasks(self, watchlist_items):
        """Process daily measurement tasks"""
        if not watchlist_items:
            logger.info("No daily watchlist items to process")
            return []
        
        logger.info(f"Processing {len(watchlist_items)} daily watchlist items")
        results = self.daily_processor.process_tasks(watchlist_items)
        
        # Log summary
        success_count = sum(1 for r in results if r and r.get('status') == 'measured')
        logger.info(f"Daily watchlist item processing complete. Successfully measured: {success_count}/{len(watchlist_items)}")
        
        return results
    
    def process_weekly_tasks(self, watchlist_items):
        """Process weekly measurement tasks"""
        if not watchlist_items:
            logger.info("No weekly watchlist items to process")
            return []
        
        logger.info(f"Processing {len(watchlist_items)} weekly watchlist items")
        results = self.weekly_processor.process_tasks(watchlist_items)
        
        # Log summary
        success_count = sum(1 for r in results if r and r.get('status') == 'measured')
        logger.info(f"Weekly watchlist item processing complete. Successfully measured: {success_count}/{len(watchlist_items)}")
        
        return results
    
    def process_evaluation_tasks(self, watchlist_items):
        """
        Start the summary process for watchlist items that have reached their end date
        
        Args:
            watchlist_items: List of watchlist items that need evaluation
            
        Returns:
            dict: Results from the summary starter
        """
        if not watchlist_items:
            logger.info("No watchlist items to process for evaluation")
            return {}
        
        logger.info(f"Starting summary process for {len(watchlist_items)} watchlist items")
        
        # Run the summary starter
        summary_results = self.summary_starter.run()
        
        logger.info(f"Summary process started for {summary_results.get('tasks_marked', 0)} watchlist items")
        
        return summary_results
    
    def run(self, run_daily=True, run_weekly=True, run_summary=True):
        """
        Run the performance monitoring workflow
        
        Args:
            run_daily: Whether to run daily measurements
            run_weekly: Whether to run weekly measurements
            run_summary: Whether to start the summary process for watchlist items at end date
            
        Returns:
            dict: Results from the workflow
        """
        logger.info("Starting performance monitoring workflow")
        
        start_time = datetime.now()
        
        # Step 1: Identify watchlist items that need attention
        watchlist_lists = self.watchlist_checker.run()
        
        daily_items = watchlist_lists.get('daily_items', [])
        weekly_items = watchlist_lists.get('weekly_items', [])
        evaluation_items = watchlist_lists.get('evaluation_items', [])
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'status': 'running',
            'daily_items_found': len(daily_items),
            'weekly_items_found': len(weekly_items),
            'evaluation_items_found': len(evaluation_items),
            'daily_results': None,
            'weekly_results': None,
            'summary_results': None,
            'workflow_end': None,
            'execution_time_seconds': None
        }
        
        try:
            # Step 2: Process daily items
            if run_daily and daily_items:
                results['daily_results'] = self.process_daily_tasks(daily_items)
            
            # Step 3: Process weekly items
            if run_weekly and weekly_items:
                results['weekly_results'] = self.process_weekly_tasks(weekly_items)
            
            # Step 4: Start summary process for evaluation items
            if run_summary and evaluation_items:
                results['summary_results'] = self.process_evaluation_tasks(evaluation_items)
            
            # Mark workflow as completed
            results['status'] = 'completed'
        except Exception as e:
            logger.exception(f"Error in workflow: {e}")
            results['status'] = 'failed'
            results['error'] = str(e)
        
        # Record completion time and duration
        end_time = datetime.now()
        results['workflow_end'] = end_time.isoformat()
        results['execution_time_seconds'] = (end_time - start_time).total_seconds()
        
        # Log summary
        total_items = len(daily_items) + len(weekly_items)
        logger.info(f"Workflow complete. Processed {total_items} items for measurement.")
        logger.info(f"- Daily items: {len(daily_items)}")
        logger.info(f"- Weekly items: {len(weekly_items)}")
        if evaluation_items:
            marked_count = results.get('summary_results', {}).get('tasks_marked', 0)
            logger.info(f"- Items marked for evaluation: {marked_count}/{len(evaluation_items)}")
        
        # Save workflow results to database
        self.save_workflow_results(results)
        
        return results


# CLI Entry Point
if __name__ == '__main__':
    try:
        import argparse
        
        parser = argparse.ArgumentParser(description='Run the performance monitoring workflow')
        parser.add_argument('--daily', action='store_true', help='Run daily measurements')
        parser.add_argument('--weekly', action='store_true', help='Run weekly measurements')
        parser.add_argument('--summary', action='store_true', help='Start summary process for watchlist items at end date')
        parser.add_argument('--all', action='store_true', help='Run all components')
        args = parser.parse_args()
        
        # If no specific flags are provided, run everything
        run_all = not (args.daily or args.weekly or args.summary) or args.all
        
        run_daily = args.daily or run_all
        run_weekly = args.weekly or run_all
        run_summary = args.summary or run_all
        
        workflow = PerformanceMonitoringWorkflow()
        results = workflow.run(run_daily, run_weekly, run_summary)
        
        print("\nWorkflow Summary:")
        print(f"Status: {results['status']}")
        print(f"Daily items: {results['daily_items_found']}")
        print(f"Weekly items: {results['weekly_items_found']}")
        print(f"Evaluation items: {results['evaluation_items_found']}")
        print(f"Execution time: {results['execution_time_seconds']} seconds")
        
        if results['status'] != 'completed':
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        logger.exception(f"Error in workflow: {e}")
        sys.exit(1)