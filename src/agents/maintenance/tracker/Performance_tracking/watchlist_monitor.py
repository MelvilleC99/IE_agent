import sys
import os
from datetime import datetime, timedelta
import calendar
import json

# Properly set up the path to find shared_services
current_dir = os.path.dirname(os.path.abspath(__file__))

# Go up to the src directory (assuming we're in src/agents/maintenance/tracker)
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Import the database client
from shared_services.db_client import get_connection

class WatchlistMonitorChecker:
    """
    Watchlist Monitor Checker identifies which watchlist items need measurements today
    and which watchlist items have reached their end date and need evaluation.
    """
    
    def __init__(self):
        """Initialize the watchlist monitor checker"""
        try:
            self.supabase = get_connection()
            if self.supabase:
                print("CHECKER: Connected to database successfully")
            else:
                print("CHECKER: Failed to connect to database")
            
            self.today = datetime.now().date()
            self.day_of_week = calendar.day_name[self.today.weekday()]
            print(f"CHECKER: Running for {self.today} ({self.day_of_week})")
        except Exception as e:
            print(f"CHECKER: Error connecting to database: {e}")
            self.supabase = None
    
    def should_measure_today(self, watchlist_item):
        """
        Determine if a watchlist item should be measured today based on its frequency
        
        Args:
            watchlist_item: The watchlist item record
            
        Returns:
            bool: True if the watchlist item should be measured today, False otherwise
        """
        if watchlist_item['monitor_status'] != 'active':
            return False
        
        # Check if we've passed the end date
        monitor_end_date = datetime.strptime(watchlist_item['monitor_end_date'], '%Y-%m-%d').date()
        if self.today > monitor_end_date:
            return False
        
        # Check if the watchlist item should be measured based on frequency
        frequency = watchlist_item['monitor_frequency']
        
        if frequency == 'daily':
            return True
        elif frequency == 'weekly':
            # For weekly, measure on Mondays
            return self.day_of_week == 'Monday'
        elif frequency == 'monthly':
            # For monthly, measure on the 1st of each month
            return self.today.day == 1
        
        return False
    
    def should_evaluate_today(self, watchlist_item):
        """
        Determine if a watchlist item has reached its end date and should be evaluated
        
        Args:
            watchlist_item: The watchlist item record
            
        Returns:
            bool: True if the watchlist item should be evaluated today, False otherwise
        """
        if watchlist_item['monitor_status'] != 'active':
            return False
        
        # Check if today is the end date or we've passed it
        monitor_end_date = datetime.strptime(watchlist_item['monitor_end_date'], '%Y-%m-%d').date()
        return self.today >= monitor_end_date
    
    def check_watchlist_items(self):
        """
        Check all active watchlist items to determine which ones need measurement or evaluation
        
        Returns:
            dict: Dictionary with lists of watchlist items for daily, weekly measurements and evaluation
        """
        if not self.supabase:
            print("CHECKER: No database connection, skipping watchlist check")
            return {
                'daily_items': [],
                'weekly_items': [],
                'evaluation_items': []
            }
        
        try:
            # Get all active monitoring watchlist items
            items_result = self.supabase.table('watchlist').select('*').eq('monitor_status', 'active').execute()
            
            if not items_result.data:
                print("CHECKER: No active monitoring watchlist items found")
                return {
                    'daily_items': [],
                    'weekly_items': [],
                    'evaluation_items': []
                }
            
            print(f"CHECKER: Found {len(items_result.data)} active monitoring watchlist items")
            
            # Initialize result lists
            daily_items = []
            weekly_items = []
            evaluation_items = []
            
            # Process each watchlist item
            for item in items_result.data:
                # Check if item needs evaluation (end of monitoring period)
                if self.should_evaluate_today(item):
                    evaluation_items.append(item)
                    print(f"CHECKER: Watchlist Item ID {item['id']} ({item['title']}) needs end-of-period evaluation")
                
                # Check if item needs measurement
                if self.should_measure_today(item):
                    if item['monitor_frequency'] == 'daily':
                        daily_items.append(item)
                        print(f"CHECKER: Watchlist Item ID {item['id']} ({item['title']}) needs daily measurement")
                    elif item['monitor_frequency'] == 'weekly':
                        weekly_items.append(item)
                        print(f"CHECKER: Watchlist Item ID {item['id']} ({item['title']}) needs weekly measurement")
            
            return {
                'daily_items': daily_items,
                'weekly_items': weekly_items, 
                'evaluation_items': evaluation_items
            }
            
        except Exception as e:
            print(f"CHECKER: Error checking watchlist items: {e}")
            return {
                'daily_items': [],
                'weekly_items': [],
                'evaluation_items': []
            }
    
    def run(self):
        """
        Run the watchlist monitor check and return the results
        
        Returns:
            dict: Dictionary with watchlist items that need attention
        """
        print("CHECKER: Starting watchlist monitor check...")
        
        # Check which watchlist items need attention
        items = self.check_watchlist_items()
        
        # Print summary
        print("\nCHECKER: Watchlist Monitor Check Summary:")
        print(f"- {len(items['daily_items'])} items need daily measurement")
        print(f"- {len(items['weekly_items'])} items need weekly measurement")
        print(f"- {len(items['evaluation_items'])} items need end-of-period evaluation")
        
        return items


# Example usage (for testing this script directly)
if __name__ == '__main__':
    # Print system path for debugging
    print(f"Current directory: {os.getcwd()}")
    print(f"Python path: {sys.path}")
    
    # Create the watchlist monitor checker
    checker = WatchlistMonitorChecker()
    
    # Run the check
    items_to_process = checker.run()
    
    # Output detailed results for verification
    print("\n--- Watchlist Monitor Check Details ---")
    
    # Show daily measurement items
    if items_to_process['daily_items']:
        print("\nDaily Measurement Items:")
        for i, item in enumerate(items_to_process['daily_items']):
            print(f"{i+1}. Item ID {item['id']}: {item['title']}")
            print(f"   Entity: {item['entity_id']}, Issue: {item['issue_type']}")
    else:
        print("\nNo daily measurement items for today")
    
    # Show weekly measurement items
    if items_to_process['weekly_items']:
        print("\nWeekly Measurement Items:")
        for i, item in enumerate(items_to_process['weekly_items']):
            print(f"{i+1}. Item ID {item['id']}: {item['title']}")
            print(f"   Entity: {item['entity_id']}, Issue: {item['issue_type']}")
    else:
        print("\nNo weekly measurement items for today")
    
    # Show evaluation items
    if items_to_process['evaluation_items']:
        print("\nEnd-of-Period Evaluation Items:")
        for i, item in enumerate(items_to_process['evaluation_items']):
            print(f"{i+1}. Item ID {item['id']}: {item['title']}")
            print(f"   Entity: {item['entity_id']}, Issue: {item['issue_type']}")
    else:
        print("\nNo items need evaluation today") 