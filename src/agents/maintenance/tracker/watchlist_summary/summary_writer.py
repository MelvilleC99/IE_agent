#!/usr/bin/env python3
import sys
import os
import json
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

from shared_services.db_client import get_connection

class SummaryWriter:
    """
    Handles saving performance summary data to the database.
    """
    def __init__(self):
        self.today = datetime.now().date()
        print(f"WRITER: Initializing for {self.today}")
        
        try:
            # Connect to the database
            self.supabase = get_connection()
            print("WRITER: Connected to Supabase")
        except Exception as e:
            print(f"WRITER: Error connecting to database: {e}")
            self.supabase = None
    
    def save_summary(self, summary, task):
        """
        Save the performance summary to the task_summaries table
        
        Args:
            summary: The analysis summary to save
            task: The original task object
            
        Returns:
            dict: The saved summary record with ID or None if failed
        """
        if not self.supabase:
            print("WRITER: No database connection available")
            return None
            
        try:
            # Determine if this is an insufficient data case
            is_insufficient = summary.get('status') == 'insufficient_data'
            
            # Get task details
            task_id = summary.get('task_id')
            extension_number = task.get('extension_count', 0)
            is_final = True  # Assume it's final unless extended later
            
            # Prepare common fields for summary record
            summary_record = {
                'task_id': task_id,
                'summary_date': self.today.isoformat(),
                'is_final': is_final,
                'extension_number': extension_number
            }
            
            # Add fields specific to the data status
            if is_insufficient:
                # Minimal record for insufficient data
                summary_record.update({
                    'period_start': task.get('monitor_start_date'),
                    'period_end': task.get('monitor_end_date'),
                    'measurements_count': 0,
                    'baseline_value': 0,
                    'latest_value': 0,
                    'raw_change_pct': 0,
                    'improvement_pct': 0,
                    'is_improved': False,
                    'trend_description': "Insufficient data for analysis",
                    'is_statistically_significant': False,
                    'recommendation': 'review',
                    'metrics_json': json.dumps({
                        'task_id': task_id,
                        'status': 'insufficient_data',
                        'message': summary.get('message', 'Insufficient data')
                    })
                })
            else:
                # Complete record for full analysis
                summary_record.update({
                    'period_start': summary['monitoring_period']['start'],
                    'period_end': summary['monitoring_period']['end'],
                    'measurements_count': summary['measurements_count'],
                    'baseline_value': float(summary['overall_metrics']['baseline_value']),
                    'latest_value': float(summary['overall_metrics']['latest_value']),
                    'raw_change_pct': float(summary['overall_metrics']['raw_change_pct']),
                    'improvement_pct': float(summary['overall_metrics']['improvement_pct']),
                    'is_improved': bool(summary['overall_metrics']['improved']),
                    'trend_slope': None if summary['trend_analysis'].get('slope') is None else float(summary['trend_analysis']['slope']),
                    'trend_r_squared': None if summary['trend_analysis'].get('r_squared') is None else float(summary['trend_analysis']['r_squared']),
                    'trend_p_value': None if summary['trend_analysis'].get('p_value') is None else float(summary['trend_analysis']['p_value']),
                    'trend_description': summary['trend_analysis'].get('trend_description'),
                    'is_statistically_significant': bool(summary['significance_test'].get('is_significant', False)),
                    'confidence_level': None if summary['significance_test'].get('confidence') is None else float(summary['significance_test']['confidence']),
                    'recommendation': '',  # No recommendation - that comes from evaluator
                    'metrics_json': json.dumps(summary)  # Store the full analysis as JSON
                })
            
            # Insert the record
            result = self.supabase.table('task_summaries').insert(summary_record).execute()
            
            if result.data:
                summary_id = result.data[0]['id']
                print(f"WRITER: Successfully saved summary to database with ID {summary_id}")
                # Add the ID to the returned summary
                summary['summary_id'] = summary_id
                return result.data[0]
            else:
                print(f"WRITER: Failed to save summary to database")
                return None
                
        except Exception as e:
            print(f"WRITER: Error saving summary to database: {e}")
            return None
    
    def update_summary_status(self, summary_id, is_final):
        """
        Update the is_final flag on a summary record
        Used when extending a task to mark previous summaries as non-final
        
        Args:
            summary_id: ID of summary to update
            is_final: New value for is_final flag
            
        Returns:
            bool: True if update succeeded, False otherwise
        """
        if not self.supabase:
            print("WRITER: No database connection available")
            return False
            
        try:
            result = (self.supabase.table('task_summaries')
                       .update({'is_final': is_final})
                       .eq('id', summary_id)
                       .execute())
            
            if result.data:
                print(f"WRITER: Updated summary {summary_id} is_final = {is_final}")
                return True
            else:
                print(f"WRITER: Failed to update summary {summary_id}")
                return False
        except Exception as e:
            print(f"WRITER: Error updating summary: {e}")
            return False
    
    def get_summary_by_id(self, summary_id):
        """
        Retrieve a summary by its ID
        
        Args:
            summary_id: ID of the summary to retrieve
            
        Returns:
            dict: The summary record or None if not found
        """
        if not self.supabase:
            print("WRITER: No database connection available")
            return None
            
        try:
            result = (self.supabase.table('task_summaries')
                       .select('*')
                       .eq('id', summary_id)
                       .execute())
            
            if result.data:
                return result.data[0]
            else:
                print(f"WRITER: No summary found with ID {summary_id}")
                return None
        except Exception as e:
            print(f"WRITER: Error retrieving summary: {e}")
            return None
    
    def get_latest_summary_for_task(self, task_id):
        """
        Get the most recent summary for a task
        
        Args:
            task_id: ID of the task to get summary for
            
        Returns:
            dict: The most recent summary or None if not found
        """
        if not self.supabase:
            print("WRITER: No database connection available")
            return None
            
        try:
            result = (self.supabase.table('task_summaries')
                       .select('*')
                       .eq('task_id', task_id)
                       .order('summary_date', option={'ascending': False})
                       .limit(1)
                       .execute())
            
            if result.data:
                return result.data[0]
            else:
                print(f"WRITER: No summaries found for task {task_id}")
                return None
        except Exception as e:
            print(f"WRITER: Error retrieving latest summary: {e}")
            return None


# For testing this module directly
if __name__ == '__main__':
    import argparse
    from summary_data import SummaryDataCollector
    from summary_analyzer import SummaryAnalyzer
    
    parser = argparse.ArgumentParser(description='Save task performance summaries to database')
    parser.add_argument('--task-id', help='ID of task to save summary for')
    parser.add_argument('--summary-file', help='Path to JSON file with analysis summary')
    parser.add_argument('--get-summary', help='Get summary by ID')
    parser.add_argument('--get-latest', help='Get latest summary for task ID')
    args = parser.parse_args()
    
    writer = SummaryWriter()
    
    if args.get_summary:
        summary = writer.get_summary_by_id(args.get_summary)
        if summary:
            print("\nSummary:")
            print(json.dumps(summary, indent=2))
        else:
            print(f"No summary found with ID {args.get_summary}")
            
    elif args.get_latest:
        summary = writer.get_latest_summary_for_task(args.get_latest)
        if summary:
            print("\nLatest Summary:")
            print(json.dumps(summary, indent=2))
        else:
            print(f"No summaries found for task {args.get_latest}")
            
    elif args.summary_file and os.path.exists(args.summary_file):
        # Load summary from file
        with open(args.summary_file) as f:
            summary = json.load(f)
            
        # Get task details for the summary
        collector = SummaryDataCollector()
        task = collector.get_task_details(summary['task_id'])
            
        if task:
            # Save summary to database
            saved_summary = writer.save_summary(summary, task)
            if saved_summary:
                print(f"Summary saved to database with ID {saved_summary['id']}")
            else:
                print("Failed to save summary to database")
        else:
            print(f"Could not find task {summary['task_id']}")
            
    elif args.task_id:
        # Get task data
        collector = SummaryDataCollector()
        task_data = collector.collect_data_for_task(args.task_id)
        
        if task_data:
            # Analyze the data
            analyzer = SummaryAnalyzer()
            summary = analyzer.analyze_task_data(task_data)
            
            # Save summary to database
            saved_summary = writer.save_summary(summary, task_data['task'])
            if saved_summary:
                print(f"Summary saved to database with ID {saved_summary['id']}")
                
                # Get the saved summary
                retrieved = writer.get_summary_by_id(saved_summary['id'])
                if retrieved:
                    print("\nRetrieved saved summary:")
                    print(f"- ID: {retrieved['id']}")
                    print(f"- Task ID: {retrieved['task_id']}")
                    print(f"- Date: {retrieved['summary_date']}")
                    print(f"- Measurements: {retrieved['measurements_count']}")
                    print(f"- Improvement: {retrieved['improvement_pct']}%")
            else:
                print("Failed to save summary to database")
        else:
            print(f"Could not collect data for task {args.task_id}")
    
    else:
        print("No action specified. Use --task-id, --summary-file, --get-summary, or --get-latest.")