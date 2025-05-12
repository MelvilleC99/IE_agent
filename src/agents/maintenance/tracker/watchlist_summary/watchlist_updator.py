#!/usr/bin/env python3
import sys
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any, Optional, List

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env.local")

from shared_services.db_client import get_connection
from agents.maintenance.tracker.watchlist_summary.summary_writer import SummaryWriter

class WatchlistUpdater:
    """
    Updates watchlist status in the database based on evaluation decisions.
    Handles watchlist extensions, closures, and status changes.
    """
    def __init__(self):
        self.today = datetime.now().date()
        print(f"UPDATER: Initializing for {self.today}")
        
        try:
            # Connect to the database
            self.supabase = get_connection()
            print("UPDATER: Connected to Supabase")
            
            # Initialize summary writer for updating summaries
            self.summary_writer = SummaryWriter()
        except Exception as e:
            print(f"UPDATER: Error initializing: {e}")
            self.supabase = None
            self.summary_writer = None
    
    def get_watchlist_details(self, watchlist_id):
        """
        Get details for a specific watchlist item.
        
        Args:
            watchlist_id: ID of the watchlist item to retrieve
            
        Returns:
            Watchlist details dictionary or None if not found
        """
        try:
            result = self.supabase.table('watch_list').select('*').eq('id', watchlist_id).execute()
            if result.data:
                return result.data[0]
            print(f"UPDATER: No watchlist item found with ID {watchlist_id}")
            return None
        except Exception as e:
            print(f"UPDATER: Error retrieving watchlist details: {e}")
            return None

    def get_evaluation_details(self, evaluation_id):
        """Get details for a specific evaluation."""
        try:
            result = self.supabase.table('watchlist_evaluations').select('*').eq('id', evaluation_id).execute()
            if result.data:
                return result.data[0]
            return None
        except Exception as e:
            print(f"UPDATER: Error retrieving evaluation details: {e}")
            return None

    def extend_watchlist(self, watchlist_id, original_end_date, reason, extension_days=14):
        """
        Extend a watchlist item's end date.
        
        Args:
            watchlist_id: ID of the watchlist item to extend
            original_end_date: Original end date
            reason: Reason for extension
            extension_days: Number of days to extend by
            
        Returns:
            Boolean indicating success
        """
        try:
            # Get current watchlist details
            watchlist = self.get_watchlist_details(watchlist_id)
            if not watchlist:
                return False
            
            # Calculate new end date
            new_end_date = datetime.fromisoformat(original_end_date) + timedelta(days=extension_days)
            
            # Update watchlist
            update_data = {
                'end_date': new_end_date.isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            update_result = self.supabase.table('watch_list').update(update_data).eq('id', watchlist_id).execute()
            if not update_result.data:
                print(f"UPDATER: Failed to update watchlist item {watchlist_id}")
                return False
            
            # Record extension
            extension_data = {
                'watch_id': watchlist_id,
                'original_end_date': original_end_date,
                'new_end_date': new_end_date.isoformat(),
                'reason': reason,
                'extension_days': extension_days,
                'created_at': datetime.now().isoformat()
            }
            
            extension_result = self.supabase.table('watchlist_extensions').insert(extension_data).execute()
            if not extension_result.data:
                print(f"UPDATER: Failed to record extension for watchlist item {watchlist_id}")
                return False
            
            print(f"UPDATER: Extended watchlist item {watchlist_id} to {new_end_date.isoformat()}")
            return True
            
        except Exception as e:
            print(f"UPDATER: Error extending watchlist item: {e}")
            return False

    def close_watchlist(self, watchlist_id, reason):
        """
        Close a watchlist item.
        
        Args:
            watchlist_id: ID of the watchlist item to close
            reason: Reason for closing
            
        Returns:
            Boolean indicating success
        """
        try:
            update_data = {
                'status': 'closed',
                'closed_at': datetime.now().isoformat(),
                'close_reason': reason,
                'updated_at': datetime.now().isoformat()
            }
            
            result = self.supabase.table('watch_list').update(update_data).eq('id', watchlist_id).execute()
            if result.data:
                print(f"UPDATER: Closed watchlist item {watchlist_id}")
                return True
            print(f"UPDATER: Failed to close watchlist item {watchlist_id}")
            return False
        except Exception as e:
            print(f"UPDATER: Error closing watchlist item: {e}")
            return False

    def update_watchlist_status(self, watchlist_id, status, notes=None):
        """
        Update a watchlist item's status.
        
        Args:
            watchlist_id: ID of the watchlist item to update
            status: New status
            notes: Optional notes about the status change
            
        Returns:
            Boolean indicating success
        """
        try:
            update_data = {
                'status': status,
                'updated_at': datetime.now().isoformat()
            }
            
            if notes:
                update_data['status_notes'] = notes
            
            result = self.supabase.table('watch_list').update(update_data).eq('id', watchlist_id).execute()
            if result.data:
                print(f"UPDATER: Updated watchlist item {watchlist_id} status to {status}")
                return True
            print(f"UPDATER: Failed to update watchlist item {watchlist_id}")
            return False
        except Exception as e:
            print(f"UPDATER: Error updating watchlist status: {e}")
            return False

    def process_evaluation(self, evaluation):
        """Process a watchlist evaluation."""
        try:
            watchlist_id = evaluation['watch_id']
            watchlist = self.get_watchlist_details(watchlist_id)
            if not watchlist:
                return False
            
            # Determine action based on evaluation
            if evaluation['action'] == 'close':
                explanation = f"Watchlist item closed based on evaluation: {evaluation['notes']}"
                result = self.close_watchlist(watchlist_id, explanation)
            elif evaluation['action'] == 'extend':
                explanation = f"Watchlist item extended based on evaluation: {evaluation['notes']}"
                result = self.extend_watchlist(
                    watchlist_id=watchlist_id,
                    original_end_date=watchlist['end_date'],
                    reason=explanation,
                    extension_days=evaluation.get('extension_days', 14)
                )
            elif evaluation['action'] == 'review':
                explanation = f"Watchlist item needs review: {evaluation['notes']}"
                result = self.update_watchlist_status(watchlist_id, 'needs_review', explanation)
            elif evaluation['action'] == 'intervene':
                explanation = f"Watchlist item needs intervention: {evaluation['notes']}"
                result = self.update_watchlist_status(watchlist_id, 'needs_intervention', explanation)
            else:
                print(f"UPDATER: Unknown evaluation action: {evaluation['action']}")
                return False
            
            # Mark evaluation as processed
            self.supabase.table('watchlist_evaluations').update({
                'processed_at': datetime.now().isoformat(),
                'processed_result': 'success' if result else 'failed'
            }).eq('id', evaluation['id']).execute()
            
            return result
            
        except Exception as e:
            print(f"UPDATER: Error processing evaluation: {e}")
            return False

    def get_pending_evaluations(self):
        """Get all pending evaluations."""
        try:
            result = self.supabase.table('watchlist_evaluations').select('*').is_('processed_at', 'null').execute()
            return result.data
        except Exception as e:
            print(f"UPDATER: Error getting pending evaluations: {e}")
            return []

    def find_and_process_evaluations(self):
        """
        Find and process all pending evaluations.
        
        Returns:
            list: List of evaluation processing results
        """
        try:
            # Get pending evaluations
            evaluations = self.get_pending_evaluations()
            if not evaluations:
                print("UPDATER: No pending evaluations found")
                return []
            
            results = []
            for evaluation in evaluations:
                try:
                    # Process the evaluation
                    success = self.process_evaluation(evaluation)
                    
                    # Record result
                    result = {
                        'watchlist_id': evaluation['watch_id'],
                        'evaluation_id': evaluation['id'],
                        'action': evaluation['action'],
                        'status': 'processed' if success else 'failed',
                        'processed_at': datetime.now().isoformat()
                    }
                    results.append(result)
                    
                except Exception as e:
                    print(f"UPDATER: Error processing evaluation {evaluation['id']}: {e}")
                    results.append({
                        'watchlist_id': evaluation['watch_id'],
                        'evaluation_id': evaluation['id'],
                        'action': evaluation['action'],
                        'status': 'error',
                        'error': str(e),
                        'processed_at': datetime.now().isoformat()
                    })
            
            return results
            
        except Exception as e:
            print(f"UPDATER: Error in find_and_process_evaluations: {e}")
            return []


# For testing this module directly
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Update watchlist items based on evaluations')
    parser.add_argument('--evaluation-id', help='ID of evaluation to process')
    parser.add_argument('--extend-watchlist', help='ID of watchlist item to extend')
    parser.add_argument('--close-watchlist', help='ID of watchlist item to close')
    parser.add_argument('--process-all', action='store_true', help='Process all unprocessed evaluations')
    args = parser.parse_args()
    
    updater = WatchlistUpdater()
    
    if args.evaluation_id:
        result = updater.process_evaluation(args.evaluation_id)
        if result:
            print(f"Processed evaluation {args.evaluation_id}")
            print(f"Action: {result['action']}, Status: {result['status']}")
    
    elif args.extend_watchlist:
        watchlist = updater.get_watchlist_details(args.extend_watchlist)
        if watchlist:
            result = updater.extend_watchlist(
                watchlist_id=args.extend_watchlist,
                original_end_date=watchlist['end_date'],
                reason="Manual extension"
            )
            if result:
                print(f"Extended watchlist item {args.extend_watchlist}")
                print(f"New end date: {result['end_date']}")
        else:
            print(f"Watchlist item {args.extend_watchlist} not found")
    
    elif args.close_watchlist:
        result = updater.close_watchlist(args.close_watchlist, "Manual closure")
        if result:
            print(f"Closed watchlist item {args.close_watchlist}")
    
    elif args.process_all:
        results = updater.get_pending_evaluations()
        print(f"Processed {len(results)} evaluations")
        
        # Print details
        for result in results:
            print(f"Evaluation {result['id']}: {result['action']} - {result['status']}")
    
    else:
        print("No action specified. Use --evaluation-id, --extend-watchlist, --close-watchlist, or --process-all.")