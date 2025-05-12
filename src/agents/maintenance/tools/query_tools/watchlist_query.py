import logging
from typing import Dict, Any, List
import re

# Import the base handler
from MCP.query_handler import QueryHandler

logger = logging.getLogger("watchlist_query")

class WatchlistQueryTool(QueryHandler):
    """
    Query tool for watchlist/performance measurement queries.
    """
    
    def execute(self, query: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a watchlist query with the given parameters."""
        logger.info(f"Executing watchlist query with params: {params}")
        
        # Build filters
        filters = {}
        
        # Status filter (default to open)
        status = params.get("status", "open")
        filters["status"] = status
        
        # Issue type filter
        if "issue_type" in params:
            filters["issue_type"] = params["issue_type"]
        
        # Mechanic filter
        if "mechanic_name" in params:
            filters["mechanic_name"] = params["mechanic_name"]
        elif "mechanic_id" in params:
            filters["mechanic_id"] = params["mechanic_id"]
        
        # Time filter for review dates
        if "time_filter" in params:
            self.apply_time_filter(filters, params, "monitor_end_date")
        
        # Query the database
        try:
            data = self.db_client.query_table(
                table_name="watch_list",
                columns="*",
                filters=filters,
                limit=100
            )
            
            # Process the data
            formatted_data = self._format_watchlist_data(data)
            
            # Define columns to display
            display_columns = [
                "issue_type",
                "mechanic_info",
                "performance_detail",
                "status",
                "review_date"
            ]
            
            return self.format_results(formatted_data, display_columns)
            
        except Exception as e:
            logger.error(f"Error executing watchlist query: {e}")
            return {
                "data": [],
                "error": str(e),
                "format": "error"
            }
    
    def _format_watchlist_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format watchlist data for display."""
        formatted_data = []
        
        for row in data:
            # Extract performance details from notes
            notes = row.get("notes", "")
            performance_detail = self._extract_performance_detail(notes)
            
            formatted_row = {
                "issue_type": row.get("issue_type", "").replace("_", " ").title(),
                "mechanic_info": f"{row.get('mechanic_name', '')} (#{row.get('mechanic_id', '')})",
                "performance_detail": performance_detail,
                "status": row.get("status", ""),
                "review_date": self.parse_date(row.get("monitor_end_date", ""))
            }
            formatted_data.append(formatted_row)
        
        return formatted_data
    
    def _extract_performance_detail(self, notes: str) -> str:
        """Extract performance details from notes field."""
        if not notes:
            return "No details available"
        
        # Look for average time comparisons
        avg_match = re.search(r'average\s+(response|repair)\s+time\s+is\s+([\d.]+)\s*min.*?team\s+average\s+of\s+([\d.]+)\s*min', notes, re.IGNORECASE)
        
        if avg_match:
            metric_type = avg_match.group(1)
            individual_avg = avg_match.group(2)
            team_avg = avg_match.group(3)
            return f"{individual_avg}min vs team avg {team_avg}min"
        
        # Fallback to showing part of the notes
        return notes[:100] + "..." if len(notes) > 100 else notes