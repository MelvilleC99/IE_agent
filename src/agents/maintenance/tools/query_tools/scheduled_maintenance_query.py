import logging
from typing import Dict, Any, List
import sys
import os

# Add project root to path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the base handler
from src.MCP.query_handler import QueryHandler

logger = logging.getLogger("scheduled_maintenance_query")

class ScheduledMaintenanceQueryTool(QueryHandler):
    """
    Query tool for scheduled maintenance task queries.
    """
    
    def execute(self, query: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a scheduled maintenance query with the given parameters."""
        logger.info(f"Executing scheduled maintenance query with params: {params}")
        
        # Build filters
        filters = {}
        
        # Status filter (default to open)
        status = params.get("status", "open")
        filters["status"] = status
        
        # Machine filters
        if "machine_id" in params:
            filters["machine_id"] = params["machine_id"]
        elif "machine_type" in params:
            filters["machine_type"] = params["machine_type"]
        
        # Mechanic filter
        if "mechanic_name" in params:
            mechanic_name = params["mechanic_name"]
            # Handle case-insensitive search by using ilike
            filters["mechanic_name.ilike"] = f"%{mechanic_name}%"
        elif "mechanic_id" in params:
            filters["assignee"] = params["mechanic_id"]
        
        # Time filter for due dates
        if "time_filter" in params:
            self.apply_time_filter(filters, params, "due_by")
        
        # Query the database
        try:
            data = self.db_client.query_table(
                table_name="scheduled_maintenance",
                columns="*",
                filters=filters,
                limit=100
            )
            
            # Process the data
            formatted_data = self._format_maintenance_data(data)
            
            # Define columns to display
            display_columns = [
                "machine_info",
                "assigned_to",
                "priority",
                "status",
                "due_date"
            ]
            
            return self.format_results(formatted_data, display_columns)
            
        except Exception as e:
            logger.error(f"Error executing scheduled maintenance query: {e}")
            return {
                "data": [],
                "error": str(e),
                "format": "error"
            }
    
    def _format_maintenance_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format maintenance data for display."""
        formatted_data = []
        
        for row in data:
            formatted_row = {
                "machine_info": f"{row.get('machine_type', '')} (#{row.get('machine_id', '')})",
                "assigned_to": row.get('mechanic_name', ''),
                "priority": row.get('priority', '').title(),
                "status": row.get('status', ''),
                "due_date": self.parse_date(row.get('due_by', ''))
            }
            formatted_data.append(formatted_row)
        
        return formatted_data