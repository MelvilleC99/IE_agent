import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Union

logger = logging.getLogger("date_utils")

class DateUtils:
    """Utility class for handling date operations consistently throughout the system."""
    
    @staticmethod
    def get_today() -> datetime:
        """Get today's date with time set to start of day."""
        now = datetime.now()
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    @staticmethod
    def get_tomorrow() -> datetime:
        """Get tomorrow's date with time set to start of day."""
        return DateUtils.get_today() + timedelta(days=1)
    
    @staticmethod
    def get_start_of_week(date: Optional[datetime] = None) -> datetime:
        """Get the start of the week (Monday) for a given date."""
        if date is None:
            date = DateUtils.get_today()
        # Monday is 0, Sunday is 6
        days_since_monday = date.weekday()
        start_of_week = date - timedelta(days=days_since_monday)
        return start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    
    @staticmethod
    def get_end_of_week(date: Optional[datetime] = None) -> datetime:
        """Get the end of the week (Sunday) for a given date."""
        start_of_week = DateUtils.get_start_of_week(date)
        return start_of_week + timedelta(days=6)
    
    @staticmethod
    def get_date_range_filter(period: str) -> Tuple[str, str]:
        """
        Convert a period string to date range filter strings.
        
        Args:
            period: Period string ('today', 'tomorrow', 'this_week', 'next_week')
            
        Returns:
            Tuple of (start_date, end_date) in YYYY-MM-DD format
        """
        today = DateUtils.get_today()
        
        if period == "today":
            start = today
            end = today + timedelta(days=1)
        elif period == "tomorrow":
            start = today + timedelta(days=1)
            end = today + timedelta(days=2)
        elif period == "this_week":
            start = DateUtils.get_start_of_week(today)
            end = start + timedelta(days=7)
        elif period == "next_week":
            this_week_start = DateUtils.get_start_of_week(today)
            start = this_week_start + timedelta(days=7)
            end = start + timedelta(days=7)
        else:
            # Default to today if unknown period
            start = today
            end = today + timedelta(days=1)
            
        return (start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
    
    @staticmethod
    def format_date_for_display(date_str: str) -> str:
        """
        Format a date string for user-friendly display.
        
        Args:
            date_str: Date string in various formats
            
        Returns:
            Formatted date string (e.g., "Mon, Jan 15, 2024")
        """
        try:
            # Handle ISO format with time
            if 'T' in date_str:
                date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                # Handle simple date format
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                
            return date_obj.strftime('%a, %b %d, %Y')
        except Exception as e:
            logger.error(f"Error formatting date {date_str}: {e}")
            return date_str
    
    @staticmethod
    def parse_date_input(date_input: str) -> Optional[datetime]:
        """
        Parse various date input formats.
        
        Args:
            date_input: Date string in various formats
            
        Returns:
            datetime object or None if parsing fails
        """
        # Common date formats to try
        formats = [
            '%Y-%m-%d',                    # 2024-01-15
            '%Y/%m/%d',                    # 2024/01/15
            '%d-%m-%Y',                    # 15-01-2024
            '%d/%m/%Y',                    # 15/01/2024
            '%Y-%m-%d %H:%M:%S',          # 2024-01-15 10:30:00
            '%Y-%m-%dT%H:%M:%S',          # ISO format
            '%Y-%m-%dT%H:%M:%S.%f',       # ISO with microseconds
            '%Y-%m-%dT%H:%M:%S.%fZ',      # ISO with microseconds and Z
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_input, fmt)
            except ValueError:
                continue
                
        # Try ISO format with timezone
        try:
            return datetime.fromisoformat(date_input.replace('Z', '+00:00'))
        except:
            pass
            
        logger.warning(f"Could not parse date: {date_input}")
        return None
    
    @staticmethod
    def calculate_time_difference(start: Union[str, datetime], end: Union[str, datetime]) -> Optional[timedelta]:
        """
        Calculate time difference between two dates.
        
        Args:
            start: Start date/time
            end: End date/time
            
        Returns:
            timedelta object or None if parsing fails
        """
        # Convert strings to datetime if needed
        start_dt: Optional[datetime] = None
        end_dt: Optional[datetime] = None
        
        if isinstance(start, str):
            start_dt = DateUtils.parse_date_input(start)
        else:
            start_dt = start
            
        if isinstance(end, str):
            end_dt = DateUtils.parse_date_input(end)
        else:
            end_dt = end
            
        if start_dt and end_dt:
            return end_dt - start_dt
        return None
    
    @staticmethod
    def format_duration(duration: Union[int, float, timedelta]) -> str:
        """
        Format a duration for display.
        
        Args:
            duration: Duration in milliseconds, seconds, or timedelta
            
        Returns:
            Human-readable duration string
        """
        if isinstance(duration, timedelta):
            total_seconds = duration.total_seconds()
        elif isinstance(duration, (int, float)):
            # Assume milliseconds if large number
            if duration > 1000000:
                total_seconds = duration / 1000
            else:
                total_seconds = duration
        else:
            return str(duration)
            
        if total_seconds < 60:
            return f"{total_seconds:.0f} seconds"
        elif total_seconds < 3600:
            minutes = total_seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = total_seconds / 3600
            return f"{hours:.1f} hours"
    
    @staticmethod
    def is_past_due(due_date_str: str, reference_date: Optional[datetime] = None) -> bool:
        """
        Check if a date is past due compared to a reference date.
        
        Args:
            due_date_str: Due date string
            reference_date: Reference date to compare against (default: today)
            
        Returns:
            True if past due, False otherwise
        """
        if reference_date is None:
            reference_date = DateUtils.get_today()
            
        due_date = DateUtils.parse_date_input(due_date_str)
        if due_date is None:
            return False
            
        return due_date < reference_date
    
    @staticmethod
    def days_until_due(due_date_str: str, reference_date: Optional[datetime] = None) -> Optional[int]:
        """
        Calculate days until a due date.
        
        Args:
            due_date_str: Due date string
            reference_date: Reference date to calculate from (default: today)
            
        Returns:
            Number of days until due (negative if past due)
        """
        if reference_date is None:
            reference_date = DateUtils.get_today()
            
        due_date = DateUtils.parse_date_input(due_date_str)
        if due_date is None:
            return None
            
        delta = due_date - reference_date
        return delta.days

# Create a global instance for easy access
date_utils = DateUtils()