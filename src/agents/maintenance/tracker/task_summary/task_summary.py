#!/usr/bin/env python3
import sys
import os
import json
from datetime import datetime
from pathlib import Path
import argparse
from dotenv import load_dotenv

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env.local")

# Import the modular components
from src.agents.maintenance.tracker.task_summary.summary_data import SummaryDataCollector
from src.agents.maintenance.tracker.task_summary.summary_analyzer import SummaryAnalyzer
from src.agents.maintenance.tracker.task_summary.summary_writer import SummaryWriter

class TaskSummary:
    """
    Task Summary for end-of-monitoring-period analysis.
    Coordinates data collection, analysis, and storage of performance metrics.
    """
    def __init__(self):
        self.today = datetime.now().date()
        print(f"SUMMARY: Running for {self.today}")
        
        # Initialize components
        self.data_collector = SummaryDataCollector()
        self.analyzer = SummaryAnalyzer()
        self.writer = SummaryWriter()
    
    def summarize_task(self, task_id):
        """
        Generate a comprehensive performance summary for a task
        
        Args:
            task_id: ID of the task to summarize
            
        Returns:
            dict: Comprehensive summary with metrics and trend analysis
        """
        print(f"SUMMARY: Summarizing task ID {task_id}")
        
        # Step 1: Collect task data
        task_data = self.data_collector.collect_data_for_task(task_id)
        if not task_data:
            print(f"SUMMARY: Could not collect data for task {task_id}")
            return None
        
        # Step 2: Analyze the data
        summary = self.analyzer.analyze_task_data(task_data)
        
        # Step 3: Save the summary to the database
        saved_summary = self.writer.save_summary(summary, task_data['task'])
        
        # Step 4: Add the database ID to the summary if available
        if saved_summary and 'id' in saved_summary:
            summary['summary_id'] = saved_summary['id']
        
        print(f"SUMMARY: Completed summary for task ID {task_id}")
        if summary.get('status') != 'insufficient_data':
            improvement = summary.get('overall_metrics', {}).get('improvement_pct', 0)
            trend = summary.get('trend_analysis', {}).get('trend_description', 'unknown')
            print(f"SUMMARY: Improvement: {improvement:.2f}%, Trend: {trend}")
        
        return summary
    
    def process_tasks(self, tasks):
        """
        Generate summaries for multiple tasks
        
        Args:
            tasks: List of tasks to summarize
            
        Returns:
            list: Performance summaries for each task
        """
        if not tasks:
            print("SUMMARY: No tasks to summarize")
            return []
        
        print(f"SUMMARY: Processing {len(tasks)} tasks for summary")
        
        results = []
        for task in tasks:
            # Generate summary for this task
            task_id = task.get('id')
            summary = self.summarize_task(task_id)
            if summary:
                results.append(summary)
        
        # Print summary
        completed = len([r for r in results if r.get('status') == 'summarized'])
        insufficient = len([r for r in results if r.get('status') == 'insufficient_data'])
        
        print("\nSUMMARY: Summary Results:")
        print(f"- Tasks with complete summaries: {completed}")
        print(f"- Tasks with insufficient data: {insufficient}")
        
        return results
    
    def find_and_process_evaluation_tasks(self):
        """
        Find tasks that are ready for evaluation and process them
        
        Returns:
            list: Performance summaries for the evaluation-ready tasks
        """
        # Get tasks that have reached their end date
        evaluation_tasks = self.data_collector.get_tasks_for_evaluation()
        
        if not evaluation_tasks:
            print("SUMMARY: No tasks ready for evaluation")
            return []
        
        print(f"SUMMARY: Found {len(evaluation_tasks)} tasks ready for evaluation")
        
        # Process the tasks
        return self.process_tasks(evaluation_tasks)


# For testing this module directly
if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate performance summaries for tasks')
    parser.add_argument('--task-file', help='Path to JSON file with tasks')
    parser.add_argument('--task-id', help='Specific task ID to summarize')
    parser.add_argument('--find-ready', action='store_true', help='Find and process tasks ready for evaluation')
    parser.add_argument('--output-file', help='Output file for summaries (default: task_summaries_YYYYMMDD.json)')
    args = parser.parse_args()
    
    # Create summarizer
    summarizer = TaskSummary()
    
    # Process from file
    if args.task_file and os.path.exists(args.task_file):
        with open(args.task_file) as f:
            tasks = json.load(f)
        summaries = summarizer.process_tasks(tasks)
        
        # Save summaries to file
        if summaries:
            output_file = args.output_file or f"task_summaries_{datetime.now().strftime('%Y%m%d')}.json"
            with open(output_file, 'w') as f:
                json.dump(summaries, f, indent=2)
            print(f"Saved summaries to {output_file}")
    
    # Process specific task
    elif args.task_id:
        summary = summarizer.summarize_task(args.task_id)
        if summary:
            print("\nTask Summary:")
            print(json.dumps(summary, indent=2))
            
            # Save single task summary if output file specified
            if args.output_file:
                with open(args.output_file, 'w') as f:
                    json.dump(summary, f, indent=2)
                print(f"Saved summary to {args.output_file}")
    
    # Find and process tasks ready for evaluation
    elif args.find_ready:
        summaries = summarizer.find_and_process_evaluation_tasks()
        
        # Save summaries to file
        if summaries:
            output_file = args.output_file or f"task_summaries_{datetime.now().strftime('%Y%m%d')}.json"
            with open(output_file, 'w') as f:
                json.dump(summaries, f, indent=2)
            print(f"Saved summaries to {output_file}")
    
    else:
        print("This script generates performance summaries for tasks at the end of their monitoring period.")
        print("Run with --task-file to process multiple tasks or --task-id for a specific task.")
        print("Use --find-ready to automatically find and process tasks ready for evaluation.")
        print("Use --output-file to specify a custom output filename.")