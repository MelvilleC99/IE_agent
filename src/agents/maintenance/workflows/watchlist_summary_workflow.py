#!/usr/bin/env python3
import sys
import os
import json
import logging
from datetime import datetime
from pathlib import Path
import argparse
from dotenv import load_dotenv
import traceback
from typing import Dict, List, Any, Optional

# Ensure src/ directory is on sys.path for absolute imports
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "../../.."))
src_root = os.path.join(project_root, "src")
if src_root not in sys.path:
    sys.path.insert(0, src_root)

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env.local")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import modules
try:
    # Watchlist summary components
    from agents.maintenance.tracker.watchlist_summary.watchlist_data import WatchlistDataCollector
    from agents.maintenance.tracker.watchlist_summary.summary_analyzer import SummaryAnalyzer
    from agents.maintenance.tracker.watchlist_summary.summary_writer import SummaryWriter
    from agents.maintenance.tracker.watchlist_summary.watchlist_evaluator import WatchlistEvaluator
    from agents.maintenance.tracker.watchlist_summary.watchlist_updator import WatchlistUpdater
    from agents.maintenance.tracker.Performance_tracking.notification_handler import NotificationHandler
    
    logger.info("Successfully imported all required modules")
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error(traceback.format_exc())
    sys.exit(1)

class WatchlistSummaryWorkflow:
    """
    Watchlist Summary Workflow - Complete process for evaluating watchlist items at the end of their monitoring period.
    
    This workflow orchestrates the end-to-end process for watchlist evaluation:
    1. Find watchlist items marked for evaluation
    2. Collect and analyze performance data
    3. Save performance summaries
    4. Evaluate summaries and make decisions
    5. Update watchlist status based on evaluations
    6. Send notifications about outcomes
    """
    def __init__(self):
        self.today = datetime.now().date()
        logger.info(f"Initializing Watchlist Summary Workflow for {self.today}")
        
        # Create output directory
        self.output_dir = Path(script_dir) / "output"
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize components
        try:
            self.data_collector = WatchlistDataCollector()
            self.analyzer = SummaryAnalyzer()
            self.writer = SummaryWriter()
            self.evaluator = WatchlistEvaluator()
            self.updater = WatchlistUpdater()
            self.notification_handler = NotificationHandler()
            
            logger.info("All components initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing components: {e}")
            raise
        
        # Results tracking
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'watchlist_items_found': 0,
            'summaries_created': 0,
            'evaluations_made': 0,
            'watchlist_items_updated': 0,
            'notifications_sent': 0,
            'errors': []
        }
    
    def save_output(self, data, filename):
        """Save data to output file"""
        filepath = self.output_dir / filename
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved output to {filepath}")
        return filepath
    
    def find_watchlist_items_for_evaluation(self):
        """Find watchlist items marked for evaluation"""
        logger.info("Finding watchlist items marked for evaluation")
        try:
            # Use the new method we added to WatchlistDataCollector
            items = self.data_collector.get_watchlist_items_marked_for_evaluation()
            self.results['watchlist_items_found'] = len(items)
            
            if items:
                logger.info(f"Found {len(items)} watchlist items marked for evaluation")
                # Save item IDs for reference
                self.save_output([item['id'] for item in items], f"evaluation_items_{self.today.isoformat()}.json")
            else:
                logger.info("No watchlist items found marked for evaluation")
            
            return items
        except Exception as e:
            error_msg = f"Error finding watchlist items for evaluation: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.results['errors'].append(error_msg)
            return []
    
    def generate_summaries(self, items):
        """Generate performance summaries for watchlist items"""
        logger.info(f"Generating summaries for {len(items)} watchlist items")
        summaries = []
        
        for item in items:
            item_id = item['id']
            logger.info(f"Processing watchlist item {item_id}")
            
            try:
                # Step 1: Collect data for the item
                item_data = self.data_collector.collect_data_for_watchlist_item(item_id)
                if not item_data:
                    logger.warning(f"No data available for watchlist item {item_id}")
                    continue
                
                # Step 2: Analyze the data
                summary = self.analyzer.analyze_watchlist_data(item_data)
                if not summary:
                    logger.warning(f"Could not analyze data for watchlist item {item_id}")
                    continue
                
                # Step 3: Save the summary to the database
                saved_summary = self.writer.save_summary(summary, item_data['watchlist'])
                if saved_summary:
                    # Add the summary ID to the summary object
                    summary['summary_id'] = saved_summary['id']
                    summaries.append(summary)
                    logger.info(f"Created summary for watchlist item {item_id} with ID {saved_summary['id']}")
                else:
                    logger.warning(f"Failed to save summary for watchlist item {item_id}")
                
            except Exception as e:
                error_msg = f"Error processing watchlist item {item_id}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                self.results['errors'].append(error_msg)
        
        self.results['summaries_created'] = len(summaries)
        
        if summaries:
            # Save summaries for reference
            self.save_output(summaries, f"watchlist_summaries_{self.today.isoformat()}.json")
        
        logger.info(f"Created {len(summaries)} summaries")
        return summaries
    
    def evaluate_summaries(self):
        """Evaluate summaries and make decisions"""
        logger.info("Evaluating performance summaries")
        
        try:
            # Use WatchlistEvaluator to find and evaluate summaries
            evaluations = self.evaluator.find_and_evaluate_summaries()
            self.results['evaluations_made'] = len(evaluations)
            
            if evaluations:
                # Save evaluations for reference
                self.save_output(evaluations, f"watchlist_evaluations_{self.today.isoformat()}.json")
                logger.info(f"Made {len(evaluations)} evaluation decisions")
            else:
                logger.info("No evaluations were made")
            
            return evaluations
        except Exception as e:
            error_msg = f"Error evaluating summaries: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.results['errors'].append(error_msg)
            return []
    
    def update_watchlist_items(self):
        """Update watchlist items based on evaluations"""
        logger.info("Updating watchlist items based on evaluations")
        
        try:
            # Use WatchlistUpdater to find and process evaluations
            update_results = self.updater.find_and_process_evaluations()
            
            # Count only successfully processed updates
            processed_updates = [r for r in update_results if r['status'] == 'processed']
            self.results['watchlist_items_updated'] = len(processed_updates)
            
            if update_results:
                # Save update results for reference
                self.save_output(update_results, f"watchlist_updates_{self.today.isoformat()}.json")
                
                # Count actions by type
                actions = {}
                for result in processed_updates:
                    action = result['action']
                    actions[action] = actions.get(action, 0) + 1
                
                logger.info(f"Updated {len(processed_updates)} watchlist items")
                for action, count in actions.items():
                    logger.info(f"- {action}: {count} items")
            else:
                logger.info("No watchlist items were updated")
            
            return update_results
        except Exception as e:
            error_msg = f"Error updating watchlist items: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.results['errors'].append(error_msg)
            return []
    
    def send_notifications(self, update_results):
        """Send notifications about watchlist updates"""
        logger.info("Sending notifications")
        
        if not update_results:
            logger.info("No updates to send notifications for")
            return 0
        
        processed_updates = [r for r in update_results if r['status'] == 'processed']
        if not processed_updates:
            logger.info("No successfully processed updates to notify about")
            return 0
        
        notifications_created = 0
        
        try:
            for update in processed_updates:
                item_id = update['watchlist_id']
                action = update['action']
                evaluation_id = update.get('evaluation_id')
                
                # Get watchlist details
                item = self.updater.get_watchlist_details(item_id)
                if not item:
                    logger.warning(f"Could not find details for watchlist item {item_id}")
                    continue
                
                # Get evaluation details
                evaluation = self.evaluator.get_evaluation_details(evaluation_id) if evaluation_id else None
                if not evaluation and evaluation_id:
                    logger.warning(f"Could not find evaluation {evaluation_id}")
                    continue
                
                # Create notification using notification handler
                notification = self.notification_handler.create_notification(
                    watchlist_id=item_id,
                    evaluation_id=evaluation_id,
                    action=action,
                    message=self._create_notification_message(item, evaluation, action)
                )
                
                if notification:
                    notifications_created += 1
                    logger.info(f"Created notification for watchlist item {item_id} ({action})")
            
            # Process all pending notifications
            processed = self.notification_handler.process_evaluation_notifications()
            self.results['notifications_sent'] = len(processed)
            logger.info(f"Sent {len(processed)} notifications")
            return len(processed)
            
        except Exception as e:
            error_msg = f"Error sending notifications: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.results['errors'].append(error_msg)
            return 0
    
    def clear_evaluation_flags(self, item_ids):
        """Clear the needs_evaluation flag for processed watchlist items"""
        if not item_ids:
            logger.info("No watchlist items to clear evaluation flags for")
            return 0
        
        logger.info(f"Clearing evaluation flags for {len(item_ids)} watchlist items")
        cleared = 0
        
        try:
            if not self.data_collector.supabase:
                logger.error("No database connection available")
                return 0
                
            for item_id in item_ids:
                result = self.data_collector.supabase.table('watch_list').update({
                    'needs_evaluation': False,
                    'updated_at': datetime.now().isoformat()
                }).eq('id', item_id).execute()
                
                if result.data:
                    cleared += 1
            
            logger.info(f"Cleared evaluation flags for {cleared} watchlist items")
            return cleared
        except Exception as e:
            error_msg = f"Error clearing evaluation flags: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.results['errors'].append(error_msg)
            return 0
    
    def _create_notification_message(self, item, evaluation, action):
        """Create a notification message based on the action type"""
        item_title = item.get('title', f"Watchlist Item #{item['id']}")
        
        # Get explanation and recommendation from evaluation if available
        explanation = ""
        recommendation = ""
        if evaluation and isinstance(evaluation, dict):
            if 'explanation' in evaluation:
                explanation = evaluation['explanation']
            elif 'decision' in evaluation and isinstance(evaluation['decision'], dict):
                explanation = evaluation['decision'].get('explanation', '')
                recommendation = evaluation['decision'].get('recommendation', '')
        
        message = f"Watchlist Item: {item_title}\n\n"
        
        if action == 'close':
            message += f"This watchlist item has been closed. {explanation}\n\n"
            if recommendation:
                message += f"Recommendation: {recommendation}\n\n"
            message += f"Completed on: {self.today.isoformat()}"
        
        elif action == 'extend':
            # Get the extension details
            extension_count = item.get('extension_count', 0)
            end_date = item.get('monitor_end_date', 'unknown')
            
            message += f"This watchlist item has been extended (Extension #{extension_count}).\n\n"
            if explanation:
                message += f"Reason: {explanation}\n\n"
            message += f"New end date: {end_date}\n\n"
            if recommendation:
                message += f"Recommendation: {recommendation}"
        
        elif action == 'review':
            message += f"This watchlist item requires review. {explanation}\n\n"
            if recommendation:
                message += f"Recommendation: {recommendation}\n\n"
            message += "Please review the item details and performance data."
        
        elif action == 'intervene':
            message += f"URGENT: This watchlist item requires intervention. {explanation}\n\n"
            if recommendation:
                message += f"Recommendation: {recommendation}\n\n"
            message += "Immediate action is required to address performance issues."
        
        return message
    
    def _get_recipient_for_action(self, item, action):
        """Determine who should receive the notification"""
        # Default recipient based on item
        default_recipient = item.get('assigned_to', 'maintenance_manager')
        
        if action == 'intervene':
            # For intervention, always notify the manager
            return 'maintenance_manager'
        elif action == 'review':
            # For review, notify the manager
            return 'maintenance_manager'
        else:
            # For other actions, notify the assigned person
            return default_recipient
    
    def run(self):
        """
        Run the complete watchlist summary workflow
        
        Returns:
            dict: Results of the workflow execution
        """
        start_time = datetime.now()
        logger.info(f"Starting Watchlist Summary Workflow at {start_time}")
        
        try:
            # Step 1: Find watchlist items marked for evaluation
            items = self.find_watchlist_items_for_evaluation()
            if not items:
                logger.info("No watchlist items to process, workflow complete")
                return self.results
            
            # Step 2: Generate performance summaries
            summaries = self.generate_summaries(items)
            
            # Step 3: Evaluate summaries
            evaluations = self.evaluate_summaries()
            
            # Step 4: Update watchlist items based on evaluations
            update_results = self.update_watchlist_items()
            
            # Get item IDs that were successfully updated
            processed_item_ids = [r['watchlist_id'] for r in update_results if r['status'] == 'processed']
            
            # Step 5: Send notifications
            self.send_notifications(update_results)
            
            # Step 6: Clear evaluation flags for processed items
            self.clear_evaluation_flags(processed_item_ids)
            
            # Calculate execution time
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            logger.info(f"Workflow completed in {execution_time:.2f} seconds")
            logger.info(f"Processed {self.results['watchlist_items_found']} items, created {self.results['summaries_created']} summaries")
            logger.info(f"Made {self.results['evaluations_made']} evaluations, updated {self.results['watchlist_items_updated']} items")
            logger.info(f"Sent {self.results['notifications_sent']} notifications")
            
            if self.results['errors']:
                logger.warning(f"Encountered {len(self.results['errors'])} errors")
            
        except Exception as e:
            error_msg = f"Error in workflow execution: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.results['errors'].append(error_msg)
        
        # Save final results
        self.save_output(self.results, f"workflow_results_{self.today.isoformat()}.json")
        
        return self.results


# For direct execution
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run the Watchlist Summary Workflow')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--log-file', help='Log to the specified file')
    parser.add_argument('--output-dir', help='Output directory for result files')
    args = parser.parse_args()
    
    # Configure logging based on arguments
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger.setLevel(log_level)
    
    # Add file handler if log file specified
    if args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(file_handler)
    
    try:
        # Run the workflow
        workflow = WatchlistSummaryWorkflow()
        
        # Set custom output directory if specified
        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(exist_ok=True)
            workflow.output_dir = output_dir
        
        results = workflow.run()
        
        # Print summary to console
        print("\nWatchlist Summary Workflow Results:")
        print(f"- Items processed: {results['watchlist_items_found']}")
        print(f"- Summaries created: {results['summaries_created']}")
        print(f"- Evaluations made: {results['evaluations_made']}")
        print(f"- Items updated: {results['watchlist_items_updated']}")
        print(f"- Notifications sent: {results['notifications_sent']}")
        
        if results['errors']:
            print(f"- Errors encountered: {len(results['errors'])}")
            print(f"  First error: {results['errors'][0]}")
        
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Critical error in workflow: {e}")
        print(f"Critical error: {str(e)}")
        sys.exit(1)