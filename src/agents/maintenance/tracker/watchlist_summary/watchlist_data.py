#!/usr/bin/env python3
import sys
import os
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

class WatchlistDataCollector:
    """
    Collects measurement data and watchlist details from the database
    for performance summary analysis.
    """
    def __init__(self):
        self.today = datetime.now().date()
        print(f"DATA: Initializing data collector for {self.today}")
        
        try:
            # Connect to the database
            self.supabase = get_connection()
            print("DATA: Connected to Supabase")
        except Exception as e:
            print(f"DATA: Error connecting to database: {e}")
            self.supabase = None
    
    def get_all_measurements(self, watchlist_id):
        """
        Get all measurements for a watchlist item in chronological order
        
        Args:
            watchlist_id: ID of the watchlist item to get measurements for
            
        Returns:
            list: List of measurement records or empty list if none found
        """
        if not self.supabase:
            print("DATA: No database connection available")
            return []
            
        try:
            result = (self.supabase.table('watchlist_measurements')
                       .select('*')
                       .eq('watch_id', watchlist_id)
                       .order('measurement_date')
                       .execute())
            
            if result.data:
                print(f"DATA: Retrieved {len(result.data)} measurements for watchlist item {watchlist_id}")
                return result.data
            
            print(f"DATA: No measurements found for watchlist item {watchlist_id}")
            return []
        except Exception as e:
            print(f"DATA: Error retrieving measurements: {e}")
            return []
    
    def get_watchlist_details(self, watchlist_id):
        """
        Get detailed watchlist item information
        
        Args:
            watchlist_id: ID of the watchlist item to get details for
            
        Returns:
            dict: Watchlist item details or None if not found
        """
        if not self.supabase:
            print("DATA: No database connection available")
            return None
            
        try:
            result = (self.supabase.table('watch_list')
                       .select('*')
                       .eq('id', watchlist_id)
                       .execute())
            
            if result.data:
                print(f"DATA: Retrieved watchlist item details for {watchlist_id}")
                return result.data[0]
                
            print(f"DATA: No watchlist item found with ID {watchlist_id}")
            return None
        except Exception as e:
            print(f"DATA: Error retrieving watchlist item details: {e}")
            return None
    
    def get_watchlist_items_for_evaluation(self):
        """
        Get all watchlist items that have reached their end date and need evaluation
        
        Returns:
            list: List of watchlist items ready for evaluation
        """
        if not self.supabase:
            print("DATA: No database connection available")
            return []
            
        try:
            # Get items where today is equal to or past the end date
            # and the status is still 'active'
            result = (self.supabase.table('watch_list')
                       .select('*')
                       .eq('monitor_status', 'active')
                       .lte('monitor_end_date', self.today.isoformat())
                       .execute())
            
            if result.data:
                print(f"DATA: Found {len(result.data)} watchlist items ready for evaluation")
                return result.data
                
            print("DATA: No watchlist items ready for evaluation")
            return []
        except Exception as e:
            print(f"DATA: Error retrieving watchlist items for evaluation: {e}")
            return []
    
    def get_watchlist_items_marked_for_evaluation(self):
        """
        Get watchlist items that have been marked as needing evaluation by the SummaryStarter
        
        Returns:
            list: List of watchlist items marked for evaluation
        """
        if not self.supabase:
            print("DATA: No database connection available")
            return []
            
        try:
            # Get items where needs_evaluation is True
            result = (self.supabase.table('watch_list')
                       .select('*')
                       .eq('needs_evaluation', True)
                       .execute())
            
            if result.data:
                print(f"DATA: Found {len(result.data)} watchlist items marked for evaluation")
                return result.data
                
            print("DATA: No watchlist items found marked for evaluation")
            return []
        except Exception as e:
            print(f"DATA: Error retrieving watchlist items marked for evaluation: {e}")
            return []
    
    def get_watchlist_items_to_process(self):
        """Get watchlist items that need processing."""
        try:
            result = (self.supabase.table('watch_list')
                     .select('*')
                     .eq('needs_evaluation', True)
                     .is_('evaluation_completed_at', 'null')
                     .execute())
            return result.data
        except Exception as e:
            print(f"COLLECTOR: Error getting watchlist items to process: {e}")
            return []

    def collect_data_for_watchlist_item(self, watchlist_id):
        """Collect all relevant data for a watchlist item."""
        try:
            # Get watchlist item details
            result = (self.supabase.table('watch_list')
                     .select('*')
                     .eq('id', watchlist_id)
                     .execute())
            
            if not result.data:
                print(f"COLLECTOR: No watchlist item found with ID {watchlist_id}")
                return None
            
            watchlist = result.data[0]
            
            # Get related data
            data = {
                'watchlist': watchlist,
                'measurements': self._get_measurements(watchlist_id),
                'evaluations': self._get_evaluations(watchlist_id),
                'extensions': self._get_extensions(watchlist_id)
            }
            
            return data
            
        except Exception as e:
            print(f"COLLECTOR: Error collecting data for watchlist item {watchlist_id}: {e}")
            return None

    def _get_measurements(self, watchlist_id):
        """Get measurements for a watchlist item."""
        try:
            result = (self.supabase.table('watchlist_measurements')
                     .select('*')
                     .eq('watch_id', watchlist_id)
                     .order('measurement_date', desc=True)
                     .execute())
            return result.data
        except Exception as e:
            print(f"COLLECTOR: Error getting measurements: {e}")
            return []

    def _get_evaluations(self, watchlist_id):
        """Get evaluations for a watchlist item."""
        try:
            result = (self.supabase.table('watchlist_evaluations')
                     .select('*')
                     .eq('watch_id', watchlist_id)
                     .order('created_at', desc=True)
                     .execute())
            return result.data
        except Exception as e:
            print(f"COLLECTOR: Error getting evaluations: {e}")
            return []

    def _get_extensions(self, watchlist_id):
        """Get extensions for a watchlist item."""
        try:
            result = (self.supabase.table('watchlist_extensions')
                     .select('*')
                     .eq('watch_id', watchlist_id)
                     .order('created_at', desc=True)
                     .execute())
            return result.data
        except Exception as e:
            print(f"COLLECTOR: Error getting extensions: {e}")
            return []


# For testing this module directly
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect data for watchlist evaluation')
    parser.add_argument('--watchlist-id', help='ID of watchlist item to collect data for')
    parser.add_argument('--find-ready', action='store_true', help='Find watchlist items ready for evaluation')
    parser.add_argument('--find-marked', action='store_true', help='Find watchlist items marked for evaluation')
    args = parser.parse_args()
    
    collector = WatchlistDataCollector()
    
    if args.watchlist_id:
        data = collector.collect_data_for_watchlist_item(args.watchlist_id)
        if data:
            watchlist = data['watchlist']
            measurements = data['measurements']
            print(f"\nWatchlist Item: {watchlist['title']} (ID: {watchlist['id']})")
            print(f"Measurements: {len(measurements)}")
            if measurements:
                print("First measurement:", measurements[0])
                print("Last measurement:", measurements[-1])
    
    if args.find_ready:
        items = collector.get_watchlist_items_for_evaluation()
        print(f"\nWatchlist items ready for evaluation: {len(items)}")
        for i, item in enumerate(items):
            print(f"{i+1}. Item ID {item['id']}: {item['title']}")
    
    if args.find_marked:
        items = collector.get_watchlist_items_marked_for_evaluation()
        print(f"\nWatchlist items marked for evaluation: {len(items)}")
        for i, item in enumerate(items):
            print(f"{i+1}. Item ID {item['id']}: {item['title']}")