import logging
from typing import Dict, Any, List, Optional
import re
import sys
import os

# Add project root to path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the base handler
from src.MCP.query_handler import QueryHandler

logger = logging.getLogger("watchlist_query")

class WatchlistQueryTool(QueryHandler):
    """
    Query tool for watchlist/performance measurement queries.
    """
    
    def execute(self, query: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a watchlist query with the given parameters."""
        logger.info(f"Executing watchlist query: '{query}' with params: {params}")
        
        # Build filters
        filters = {}
        
        # Status filter (default to open)
        status = params.get("status", "open")
        filters["status"] = status
        
        # Issue type filter - exact match for database
        if "issue_type" in params:
            filters["issue_type"] = params["issue_type"]
            logger.info(f"Added issue_type filter: {params['issue_type']}")
        
        # Entity type filter
        if "entity_type" in params:
            filters["entity_type"] = params["entity_type"]
            logger.info(f"Added entity_type filter: {params['entity_type']}")
        
        # Mechanic filter
        if "mechanic_name" in params:
            # Use case-insensitive search for mechanic names
            filters["mechanic_name.ilike"] = f"%{params['mechanic_name']}%"
            logger.info(f"Added mechanic_name filter: {params['mechanic_name']}")
        elif "mechanic_id" in params:
            filters["mechanic_id"] = params["mechanic_id"]
        
        # Time filter for review dates
        if "time_filter" in params:
            self.apply_time_filter(filters, params, "monitor_end_date")
        
        logger.info(f"Final filters for watchlist query: {filters}")
        
        # Query the database
        try:
            data = self.db_client.query_table(
                table_name="watch_list",
                columns="*",
                filters=filters,
                limit=100
            )
            
            logger.info(f"Found {len(data)} watchlist items with filters: {filters}")
            
            # Log the actual items found for debugging
            if data:
                for item in data:
                    logger.info(f"  Item: ID={item.get('id')}, Issue={item.get('issue_type')}, Mechanic={item.get('mechanic_name')}, Status={item.get('status')}")
            
            # Process the data
            formatted_data = self._format_watchlist_data(data)
            
            # Define simplified columns per requirements
            display_columns = [
                "issue_type",
                "mechanic_name", 
                "status",
                "monitor_period"
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
        """Format watchlist data for simplified display."""
        formatted_data = []
        
        for row in data:
            formatted_row = {
                "issue_type": row.get("issue_type", "").replace("_", " ").title(),
                "mechanic_name": row.get("mechanic_name", ""),
                "status": row.get("status", ""),
                "monitor_period": f"{self.parse_date(row.get('monitor_start_date', ''))} to {self.parse_date(row.get('monitor_end_date', ''))}"
            }
            formatted_data.append(formatted_row)
        
        return formatted_data
    
    def get_item_details(self, item_id: Optional[str] = None, mechanic_name: Optional[str] = None, issue_type: Optional[str] = None) -> Dict[str, Any]:
        """Get detailed information about watchlist items."""
        filters = {"status": "open"}
        
        if item_id:
            filters["id"] = str(item_id)
        elif mechanic_name:
            # If only mechanic name provided, show details for ALL their items
            filters["mechanic_name.ilike"] = f"%{mechanic_name}%"
            if issue_type:
                # If issue type also provided, filter to specific type
                filters["issue_type"] = issue_type
        else:
            return {"error": "Must provide either item_id or mechanic_name"}
        
        try:
            data = self.db_client.query_table(
                table_name="watch_list",
                columns="*",
                filters=filters,
                limit=10  # Increased limit for showing multiple items
            )
            
            if not data:
                return {"error": "No matching watchlist items found"}
            
            # Handle multiple items (when showing all for a mechanic)
            if len(data) > 1:
                results = []
                for item in data:
                    item_detail = self._format_single_item_detail(item)
                    results.append(item_detail)
                
                return {
                    "multiple_items": True,
                    "mechanic": data[0].get("mechanic_name", ""),
                    "total_items": len(data),
                    "items": results
                }
            else:
                # Single item detail
                item = data[0]
                return self._format_single_item_detail(item)
                
        except Exception as e:
            return {"error": str(e)}
    
    def _format_single_item_detail(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Format a single item's details."""
        notes = item.get("notes", "")
        performance_detail = self._extract_detailed_performance(notes)
        
        return {
            "title": item.get("title", ""),
            "mechanic": f"{item.get('mechanic_name', '')} (#{item.get('mechanic_id', '')})",
            "issue_type": item.get("issue_type", "").replace("_", " ").title(),
            "status": item.get("status", ""),
            "monitor_period": f"{item.get('monitor_start_date', '')} to {item.get('monitor_end_date', '')}",
            "performance_details": performance_detail,
            "recommendation": item.get("recommendation", "No recommendation yet")
        }
    
    def _extract_detailed_performance(self, notes: str) -> str:
        """Extract detailed performance information from notes."""
        if not notes:
            return "No performance details available"
        
        # Updated patterns to handle spaced numbers like "12. 2 min"
        patterns = [
            r'average\s+(response|repair)\s+time\s+is\s+([\d.\s]+)\s*min.*?team\s+average\s+of\s+([\d.\s]+)\s*min',
            r'Z-score:\s*([\d.-]+)',
            r'([\d.\s]+)\s*standard\s+deviations?\s+(above|below)\s+(?:team\s+)?mean'
        ]
        
        details = []
        
        for pattern in patterns:
            match = re.search(pattern, notes, re.IGNORECASE)
            if match:
                if 'average' in pattern:
                    metric_type = match.group(1)
                    individual = match.group(2).replace(" ", "")  # Remove spaces from numbers
                    team_avg = match.group(3).replace(" ", "")    # Remove spaces from numbers
                    details.append(f"{metric_type.title()} time: {individual} min (team average: {team_avg} min)")
                elif 'Z-score' in pattern:
                    z_score = match.group(1).replace(" ", "")  # Remove spaces from Z-score
                    details.append(f"Z-score: {z_score}")
                elif 'standard' in pattern:
                    std_dev = match.group(1).replace(" ", "")  # Remove spaces from std dev
                    direction = match.group(2)
                    details.append(f"{std_dev} standard deviations {direction} team mean")
        
        return "; ".join(details) if details else notes[:200] + "..." if len(notes) > 200 else notes
    
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