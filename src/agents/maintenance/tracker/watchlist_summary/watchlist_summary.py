#!/usr/bin/env python3
import sys
import os
import json
from datetime import datetime
from pathlib import Path
import argparse
from dotenv import load_dotenv
from typing import Dict, Any, Optional, List

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env.local")

# Import the modular components
from agents.maintenance.tracker.watchlist_summary.watchlist_data import WatchlistDataCollector
from agents.maintenance.tracker.watchlist_summary.summary_analyzer import SummaryAnalyzer
from agents.maintenance.tracker.watchlist_summary.summary_writer import SummaryWriter

class WatchlistSummary:
    """
    Watchlist Summary for end-of-monitoring-period analysis.
    Coordinates data collection, analysis, and storage of performance metrics.
    """
    def __init__(self):
        self.today = datetime.now().date()
        print(f"SUMMARY: Running for {self.today}")
        
        # Initialize components
        self.data_collector = WatchlistDataCollector()
        self.analyzer = SummaryAnalyzer()
        self.writer = SummaryWriter()
    
    def summarize_watchlist(self, watchlist_id: str) -> Optional[Dict[str, Any]]:
        """
        Generate a comprehensive performance summary for a watchlist item
        
        Args:
            watchlist_id: ID of the watchlist item to summarize
            
        Returns:
            dict: Comprehensive summary with metrics and trend analysis
        """
        print(f"SUMMARY: Summarizing watchlist ID {watchlist_id}")
        
        # Step 1: Collect watchlist data
        watchlist_data = self.data_collector.collect_data_for_watchlist_item(watchlist_id)
        if not watchlist_data:
            print(f"SUMMARY: Could not collect data for watchlist item {watchlist_id}")
            return None
        
        # Step 2: Analyze the data
        summary = self.analyzer.analyze_watchlist_data(watchlist_data)
        if not summary:
            print(f"SUMMARY: Could not analyze data for watchlist item {watchlist_id}")
            return None
        
        # Step 3: Save the summary to the database
        saved_summary = self.writer.save_summary(summary, watchlist_data['watchlist'])
        
        # Step 4: Add the database ID to the summary if available
        if saved_summary and isinstance(saved_summary, dict) and 'id' in saved_summary:
            summary['summary_id'] = saved_summary['id']
        
        print(f"SUMMARY: Completed summary for watchlist ID {watchlist_id}")
        if summary and isinstance(summary, dict):
            status = summary.get('status')
            if status != 'insufficient_data':
                overall_metrics = summary.get('overall_metrics', {})
                trend_analysis = summary.get('trend_analysis', {})
                improvement = overall_metrics.get('improvement_pct', 0)
                trend = trend_analysis.get('trend_description', 'unknown')
                print(f"SUMMARY: Improvement: {improvement:.2f}%, Trend: {trend}")
        
        return summary
    
    def process_watchlists(self, watchlists):
        """
        Generate summaries for multiple watchlist items
        
        Args:
            watchlists: List of watchlist items to summarize
            
        Returns:
            list: Performance summaries for each watchlist item
        """
        if not watchlists:
            print("SUMMARY: No watchlist items to summarize")
            return []
        
        print(f"SUMMARY: Processing {len(watchlists)} watchlist items for summary")
        
        results = []
        for watchlist in watchlists:
            # Generate summary for this watchlist item
            watchlist_id = watchlist.get('id')
            summary = self.summarize_watchlist(watchlist_id)
            if summary:
                results.append(summary)
        
        # Print summary
        completed = len([r for r in results if r.get('status') == 'summarized'])
        insufficient = len([r for r in results if r.get('status') == 'insufficient_data'])
        
        print("\nSUMMARY: Summary Results:")
        print(f"- Watchlist items with complete summaries: {completed}")
        print(f"- Watchlist items with insufficient data: {insufficient}")
        
        return results
    
    def find_and_process_evaluation_watchlists(self):
        """
        Find watchlist items that are ready for evaluation and process them
        
        Returns:
            list: Performance summaries for the evaluation-ready watchlist items
        """
        # Get watchlist items that have reached their end date
        evaluation_watchlists = self.data_collector.get_watchlist_items_marked_for_evaluation()
        
        if not evaluation_watchlists:
            print("SUMMARY: No watchlist items ready for evaluation")
            return []
        
        print(f"SUMMARY: Found {len(evaluation_watchlists)} watchlist items ready for evaluation")
        
        # Process the watchlist items
        return self.process_watchlists(evaluation_watchlists)


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
    summarizer = WatchlistSummary()
    
    # Process from file
    if args.task_file and os.path.exists(args.task_file):
        with open(args.task_file) as f:
            tasks = json.load(f)
        summaries = summarizer.process_watchlists(tasks)
        
        # Save summaries to file
        if summaries:
            output_file = args.output_file or f"task_summaries_{datetime.now().strftime('%Y%m%d')}.json"
            with open(output_file, 'w') as f:
                json.dump(summaries, f, indent=2)
            print(f"Saved summaries to {output_file}")
    
    # Process specific task
    elif args.task_id:
        summary = summarizer.summarize_watchlist(args.task_id)
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
        summaries = summarizer.find_and_process_evaluation_watchlists()
        
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