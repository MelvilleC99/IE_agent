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

logger = logging.getLogger("time_series_query")

class TimeSeriesQueryTool(QueryHandler):
    """
    Query tool for time series analysis results.
    """
    
    def execute(self, query: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a time series results query with the given parameters."""
        logger.info(f"Executing time series query: '{query}' with params: {params}")
        
        # Build filters
        filters = {}
        
        # Status filter (default to flagged)
        status = params.get("status", "flagged")
        filters["status"] = status
        
        # Analysis type filter
        if "analysis_type" in params:
            filters["analysis_type"] = params["analysis_type"]
            logger.info(f"Added analysis_type filter: {params['analysis_type']}")
        
        # Pattern type filter
        if "pattern_type" in params:
            filters["pattern_type"] = params["pattern_type"]
            logger.info(f"Added pattern_type filter: {params['pattern_type']}")
        
        # Entity type filter
        if "entity_type" in params:
            filters["entity_type"] = params["entity_type"]
            logger.info(f"Added entity_type filter: {params['entity_type']}")
        
        # Entity filter (mechanic name, etc.)
        if "entity_id" in params:
            filters["entity_id.ilike"] = f"%{params['entity_id']}%"
            logger.info(f"Added entity_id filter: {params['entity_id']}")
        
        # Time dimension filter
        if "time_dimension" in params:
            filters["time_dimension"] = params["time_dimension"]
            logger.info(f"Added time_dimension filter: {params['time_dimension']}")
        
        # Time value filter
        if "time_value" in params:
            filters["time_value"] = params["time_value"]
            logger.info(f"Added time_value filter: {params['time_value']}")
        
        # Severity filter
        if "severity" in params:
            filters["severity"] = params["severity"]
            logger.info(f"Added severity filter: {params['severity']}")
        
        logger.info(f"Final filters for time series query: {filters}")
        
        # Query the database
        try:
            data = self.db_client.query_table(
                table_name="time_series_results",
                columns="*",
                filters=filters,
                limit=50
            )
            
            logger.info(f"Found {len(data)} time series results with filters: {filters}")
            
            # Process the data
            formatted_data = self._format_time_series_data(data)
            
            # Define columns for display
            display_columns = [
                "pattern_description",
                "entity",
                "time_period", 
                "severity",
                "performance_context"
            ]
            
            return self.format_results(formatted_data, display_columns)
            
        except Exception as e:
            logger.error(f"Error executing time series query: {e}")
            return {
                "data": [],
                "error": str(e),
                "format": "error"
            }
    
    def _format_time_series_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format time series data for display."""
        formatted_data = []
        
        for row in data:
            # Extract context data for performance details
            context_data = row.get("context_data", {})
            
            # Format entity information
            entity_type = row.get("entity_type", "")
            entity_id = row.get("entity_id")
            if entity_type == "mechanic" and entity_id:
                entity = f"{entity_id} (mechanic)"
            elif entity_type == "overall":
                entity = "Factory Overall"
            elif entity_type == "line" and entity_id:
                entity = f"{entity_id} (line)"
            else:
                entity = entity_type.title()
            
            # Format time period
            time_dimension = row.get("time_dimension", "")
            time_value = row.get("time_value", "")
            if time_dimension == "day_of_week":
                time_period = f"{time_value}s"
            elif time_dimension == "hour":
                time_period = f"{time_value}:00 hour"
            else:
                time_period = f"{time_dimension}: {time_value}"
            
            # Format performance context from context_data
            performance_context = self._format_performance_context(context_data)
            
            formatted_row = {
                "pattern_description": row.get("description", ""),
                "entity": entity,
                "time_period": time_period,
                "severity": row.get("severity", "").title(),
                "performance_context": performance_context
            }
            formatted_data.append(formatted_row)
        
        return formatted_data
    
    def _format_performance_context(self, context_data: Dict[str, Any]) -> str:
        """Format the performance context from context_data JSON."""
        if not context_data:
            return "No details available"
        
        # Try to build a meaningful summary from the context data
        parts = []
        
        # Handle flagged vs normal averages
        if "flagged_avg" in context_data and "normal_avg" in context_data:
            flagged = context_data["flagged_avg"]
            normal = context_data["normal_avg"]
            parts.append(f"{flagged} vs normal {normal}")
        
        # Add variance information
        if "variance" in context_data:
            parts.append(f"({context_data['variance']})")
        elif "variance_vs_normal" in context_data:
            parts.append(f"({context_data['variance_vs_normal']} vs normal)")
        
        # Add team comparison if available
        if "team_avg" in context_data and "variance_vs_team" in context_data:
            team_avg = context_data["team_avg"]
            team_var = context_data["variance_vs_team"]
            parts.append(f"team avg: {team_avg} ({team_var})")
        
        return "; ".join(parts) if parts else "Performance issue detected"
