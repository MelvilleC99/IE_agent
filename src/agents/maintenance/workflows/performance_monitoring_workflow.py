#!/usr/bin/env python3
import sys
import os
import json
import argparse
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
from agents.maintenance.tracker.Performance_tracking.task_monitor import TaskMonitorChecker
from agents.maintenance.tracker.Performance_tracking.daily_performance import DailyPerformanceMeasurement
from agents.maintenance.tracker.Performance_tracking.weekly_performance import WeeklyPerformanceMeasurement
from agents.maintenance.tracker.Performance_tracking.start_summary import SummaryStarter

class PerformanceMonitoringWorkflow:
    """
    Performance Monitoring Workflow coordinates the task monitoring process.
    Identifies tasks that need measurements and collects those measurements.
    Also identifies tasks ready for evaluation and marks them for the summary workflow.
    """
    def __init__(self):
        self.today = datetime.now().date()
        self.output_dir = Path(current_dir) / "output"
        self.output_dir.mkdir(exist_ok=True)
        
        logger.info(f"Workflow initialized for {self.today}")
        
        # Initialize components
        self.task_checker = TaskMonitorChecker()
        self.daily_processor = DailyPerformanceMeasurement()
        self.weekly_processor = WeeklyPerformanceMeasurement()
        self.summary_starter = SummaryStarter()
    
    def save_output(self, data, filename):
        """Save data to output file"""
        filepath = self.output_dir / filename
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved output to {filepath}")
        return filepath
    
    def process_daily_tasks(self, tasks):
        """Process daily measurement tasks"""
        if not tasks:
            logger.info("No daily tasks to process")
            return []
        
        logger.info(f"Processing {len(tasks)} daily tasks")
        results = self.daily_processor.process_tasks(tasks)
        
        # Save results
        self.save_output(results, f"daily_results_{self.today.isoformat()}.json")
        
        # Log summary
        success_count = sum(1 for r in results if r and r.get('status') == 'measured')
        logger.info(f"Daily task processing complete. Successfully measured: {success_count}/{len(tasks)}")
        
        return results
    
    def process_weekly_tasks(self, tasks):
        """Process weekly measurement tasks"""
        if not tasks:
            logger.info("No weekly tasks to process")
            return []
        
        logger.info(f"Processing {len(tasks)} weekly tasks")
        results = self.weekly_processor.process_tasks(tasks)
        
        # Save results
        self.save_output(results, f"weekly_results_{self.today.isoformat()}.json")
        
        # Log summary
        success_count = sum(1 for r in results if r and r.get('status') == 'measured')
        logger.info(f"Weekly task processing complete. Successfully measured: {success_count}/{len(tasks)}")
        
        return results
    
    def process_evaluation_tasks(self, tasks):
        """
        Start the summary process for tasks that have reached their end date
        
        Args:
            tasks: List of tasks that need evaluation
            
        Returns:
            dict: Results from the summary starter
        """
        if not tasks:
            logger.info("No tasks to process for evaluation")
            return {}
        
        logger.info(f"Starting summary process for {len(tasks)} tasks")
        
        # Run the summary starter
        summary_results = self.summary_starter.run()
        
        # Save results
        self.save_output(summary_results, f"summary_start_{self.today.isoformat()}.json")
        
        logger.info(f"Summary process started for {summary_results.get('tasks_marked', 0)} tasks")
        
        return summary_results
    
    def run(self, run_daily=True, run_weekly=True, run_summary=True):
        """
        Run the performance monitoring workflow
        
        Args:
            run_daily: Whether to run daily measurements
            run_weekly: Whether to run weekly measurements
            run_summary: Whether to start the summary process for tasks at end date
            
        Returns:
            dict: Results from the workflow
        """
        logger.info("Starting performance monitoring workflow")
        
        # Step 1: Identify tasks that need attention
        task_lists = self.task_checker.run()
        
        daily_tasks = task_lists.get('daily_tasks', [])
        weekly_tasks = task_lists.get('weekly_tasks', [])
        evaluation_tasks = task_lists.get('evaluation_tasks', [])
        
        # Save task lists for reference
        self.save_output(task_lists, f"task_lists_{self.today.isoformat()}.json")
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'daily_tasks_found': len(daily_tasks),
            'weekly_tasks_found': len(weekly_tasks),
            'evaluation_tasks_found': len(evaluation_tasks),
            'daily_results': None,
            'weekly_results': None,
            'summary_results': None
        }
        
        # Step 2: Process daily tasks
        if run_daily and daily_tasks:
            results['daily_results'] = self.process_daily_tasks(daily_tasks)
        
        # Step 3: Process weekly tasks
        if run_weekly and weekly_tasks:
            results['weekly_results'] = self.process_weekly_tasks(weekly_tasks)
        
        # Step 4: Start summary process for evaluation tasks
        if run_summary and evaluation_tasks:
            results['summary_results'] = self.process_evaluation_tasks(evaluation_tasks)
        
        # Save workflow results
        self.save_output(results, f"workflow_results_{self.today.isoformat()}.json")
        
        # Log summary
        total_tasks = len(daily_tasks) + len(weekly_tasks)
        logger.info(f"Workflow complete. Processed {total_tasks} tasks for measurement.")
        logger.info(f"- Daily tasks: {len(daily_tasks)}")
        logger.info(f"- Weekly tasks: {len(weekly_tasks)}")
        if evaluation_tasks:
            marked_count = results.get('summary_results', {}).get('tasks_marked', 0)
            logger.info(f"- Tasks marked for evaluation: {marked_count}/{len(evaluation_tasks)}")
        
        return results


# CLI Entry Point
if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Run the performance monitoring workflow')
        parser.add_argument('--daily', action='store_true', help='Run daily measurements')
        parser.add_argument('--weekly', action='store_true', help='Run weekly measurements')
        parser.add_argument('--summary', action='store_true', help='Start summary process for tasks at end date')
        parser.add_argument('--all', action='store_true', help='Run all components')
        args = parser.parse_args()
        
        # If no specific flags are provided, run everything
        run_all = not (args.daily or args.weekly or args.summary) or args.all
        
        run_daily = args.daily or run_all
        run_weekly = args.weekly or run_all
        run_summary = args.summary or run_all
        
        workflow = PerformanceMonitoringWorkflow()
        workflow.run(run_daily, run_weekly, run_summary)
        
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Error in workflow: {e}")
        sys.exit(1)