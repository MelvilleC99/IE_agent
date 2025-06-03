#!/usr/bin/env python3
import argparse
from datetime import datetime, timedelta

try:
    import inquirer  # You'll need to pip install inquirer
    INQUIRER_AVAILABLE = True
except ImportError:
    INQUIRER_AVAILABLE = False
    print("Warning: inquirer not available, falling back to simple mode for interactive selection")

class DateSelector:
    """
    Utility class for date range selection for maintenance workflows.
    Provides interactive, argument-based, and API selection modes.
    """
    
    @staticmethod
    def get_date_range(mode='interactive'):
        """
        Get date range for analysis with multiple input modes.
        
        Args:
            mode: 'interactive', 'args', or 'api'
            
        Returns:
            tuple: (start_date, end_date) in YYYY-MM-DD format
        """
        if mode == 'interactive':
            return DateSelector._interactive_date_selection()
        elif mode == 'args':
            return DateSelector._parse_command_line_args()
        elif mode == 'api':
            # For usage in web APIs/UIs
            return DateSelector._default_range()
        else:
            # Default to interactive
            return DateSelector._interactive_date_selection()
    
    @staticmethod
    def _interactive_date_selection():
        """Interactive command-line date selection."""
        if not INQUIRER_AVAILABLE:
            print("Interactive mode requires 'inquirer' package. Using default range.")
            return DateSelector._default_range()
            
        # Predefined period options
        period_choices = [
            ('Previous Month', 'prev_month'),
            ('Current Month', 'curr_month'),
            ('Previous Quarter', 'prev_quarter'),
            ('Current Quarter', 'curr_quarter'),
            ('Last 30 Days', 'last_30'),
            ('Last 90 Days', 'last_90'),
            ('Year to Date', 'ytd'),
            ('Custom Range', 'custom')
        ]
        
        # Ask user to select a predefined period or custom range
        questions = [
            inquirer.List('period',
                message="Select time period for analysis",
                choices=period_choices,
            ),
        ]
        
        try:
            answers = inquirer.prompt(questions)
            if not answers:  # User hit Ctrl+C or similar
                print("Date selection canceled. Using default period.")
                return DateSelector._default_range()
                
            period = answers['period']
            
            today = datetime.now()
            
            if period == 'prev_month':
                first_day_curr = today.replace(day=1)
                last_day_prev = first_day_curr - timedelta(days=1)
                first_day_prev = last_day_prev.replace(day=1)
                return first_day_prev.strftime('%Y-%m-%d'), last_day_prev.strftime('%Y-%m-%d')
                
            elif period == 'curr_month':
                first_day = today.replace(day=1)
                return first_day.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
                
            elif period == 'prev_quarter':
                current_quarter = (today.month - 1) // 3 + 1
                prev_quarter = current_quarter - 1 if current_quarter > 1 else 4
                prev_quarter_year = today.year if current_quarter > 1 else today.year - 1
                
                if prev_quarter == 1:
                    return f"{prev_quarter_year}-01-01", f"{prev_quarter_year}-03-31"
                elif prev_quarter == 2:
                    return f"{prev_quarter_year}-04-01", f"{prev_quarter_year}-06-30"
                elif prev_quarter == 3:
                    return f"{prev_quarter_year}-07-01", f"{prev_quarter_year}-09-30"
                elif prev_quarter == 4:
                    return f"{prev_quarter_year}-10-01", f"{prev_quarter_year}-12-31"
                
            elif period == 'curr_quarter':
                current_quarter = (today.month - 1) // 3 + 1
                
                if current_quarter == 1:
                    return f"{today.year}-01-01", f"{today.year}-03-31"
                elif current_quarter == 2:
                    return f"{today.year}-04-01", f"{today.year}-06-30"
                elif current_quarter == 3:
                    return f"{today.year}-07-01", f"{today.year}-09-30"
                elif current_quarter == 4:
                    return f"{today.year}-10-01", f"{today.year}-12-31"
                
            elif period == 'last_30':
                end_date = today
                start_date = end_date - timedelta(days=30)
                return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
                
            elif period == 'last_90':
                end_date = today
                start_date = end_date - timedelta(days=90)
                return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
                
            elif period == 'ytd':
                start_date = datetime(today.year, 1, 1)
                return start_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
                
            elif period == 'custom':
                valid_date = False
                while not valid_date:
                    try:
                        start_date_str = input("Enter start date (YYYY-MM-DD): ")
                        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                        valid_date = True
                    except ValueError:
                        print("Invalid date format. Please use YYYY-MM-DD format.")
                
                valid_date = False
                while not valid_date:
                    try:
                        end_date_str = input("Enter end date (YYYY-MM-DD): ")
                        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                        
                        if end_date < start_date:
                            print("End date cannot be before start date. Please enter a valid end date.")
                        else:
                            valid_date = True
                    except ValueError:
                        print("Invalid date format. Please use YYYY-MM-DD format.")
                
                return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
        
        except Exception as e:
            print(f"Error during date selection: {e}. Using default period.")
            
        # Default fallback
        return DateSelector._default_range()
    
    @staticmethod
    def _parse_command_line_args():
        """Parse date range from command line arguments."""
        parser = argparse.ArgumentParser(description='Select date range for analysis')
        parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
        parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
        parser.add_argument('--period', choices=[
            'prev_month', 'curr_month', 'prev_quarter', 'curr_quarter', 
            'last_30', 'last_90', 'ytd', 'custom'
        ], help='Predefined period')
        
        args, _ = parser.parse_known_args()  # Parse only known args to avoid conflicts
        
        if args.period:
            # Handle predefined periods by simulating the interactive selection
            fake_answers = {'period': args.period}
            period = fake_answers['period']
            
            today = datetime.now()
            
            if period == 'prev_month':
                first_day_curr = today.replace(day=1)
                last_day_prev = first_day_curr - timedelta(days=1)
                first_day_prev = last_day_prev.replace(day=1)
                return first_day_prev.strftime('%Y-%m-%d'), last_day_prev.strftime('%Y-%m-%d')
            
            # Add other period handling similar to _interactive_date_selection
            # ...
        
        if args.start_date and args.end_date:
            try:
                # Validate dates
                start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
                end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
                
                if end_date < start_date:
                    print("End date cannot be before start date. Using default period.")
                    return DateSelector._default_range()
                    
                return args.start_date, args.end_date
            except ValueError:
                print("Invalid date format in arguments. Using default period.")
                
        return DateSelector._default_range()
    
    @staticmethod
    def _default_range():
        """Return default date range (previous month)."""
        today = datetime.now()
        first_day_curr = today.replace(day=1)
        last_day_prev = first_day_curr - timedelta(days=1)
        first_day_prev = last_day_prev.replace(day=1)
        return first_day_prev.strftime('%Y-%m-%d'), last_day_prev.strftime('%Y-%m-%d')

# Direct usage example
if __name__ == "__main__":
    start_date, end_date = DateSelector.get_date_range()
    print(f"Selected date range: {start_date} to {end_date}")