import logging
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod
from datetime import datetime
import sys
import os

# Add project root to path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the shared Supabase client
from src.shared_services.supabase_client import get_shared_supabase_client
from src.MCP.agents.utils.date_utils import date_utils

logger = logging.getLogger("query_handler")

class QueryHandler(ABC):
    """
    Abstract base class for query handlers.
    Provides common functionality for query tools.
    """
    
    def __init__(self):
        """Initialize the query handler with shared database connection."""
        try:
            self.db_client = get_shared_supabase_client()
            logger.info(f"{self.__class__.__name__} initialized with shared database client")
        except Exception as e:
            logger.error(f"Error initializing database client: {e}")
            raise
    
    @abstractmethod
    def execute(self, query: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the query with given parameters.
        Must be implemented by subclasses.
        """
        pass
    
    def apply_time_filter(self, filters: Dict[str, Any], params: Dict[str, Any], date_column: str = "due_by"):
        """Apply time-based filters to the query."""
        time_filter = params.get("time_filter")
        
        if time_filter:
            start_date, end_date = date_utils.get_date_range_filter(time_filter)
            filters[f"{date_column}.gte"] = start_date
            filters[f"{date_column}.lt"] = end_date
            logger.info(f"Applied time filter: {time_filter} ({start_date} to {end_date})")
    
    def format_results(self, data: List[Dict[str, Any]], columns: List[str]) -> Dict[str, Any]:
        """
        Format query results for display.
        
        Args:
            data: Raw data from database
            columns: Columns to include in output
            
        Returns:
            Formatted result dictionary
        """
        if not data:
            return {
                "data": [],
                "count": 0,
                "format": "message",
                "message": "No records found matching your query."
            }
        
        # Filter data to only include specified columns
        formatted_data = []
        for row in data:
            formatted_row = {col: row.get(col, "") for col in columns}
            formatted_data.append(formatted_row)
        
        return {
            "data": formatted_data,
            "count": len(formatted_data),
            "format": "table",
            "columns": columns
        }
    
    def parse_date(self, date_str: str) -> Optional[str]:
        """Parse and format date strings consistently."""
        if not date_str:
            return None
            
        parsed_date = date_utils.parse_date_input(date_str)
        if parsed_date:
            return parsed_date.strftime('%Y-%m-%d')
        return date_str