import logging
import re
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

logger = logging.getLogger("query_manager")

class QueryManager:
    """
    Routes queries to the appropriate query tool based on query analysis.
    """
    
    def __init__(self):
        """Initialize the query manager with available query tools."""
        self.query_tools = {}
        self.query_patterns = {
            'watchlist': [
                r'watch\s*list',
                r'performance\s+(?:measurement|monitoring|items?)',
                r'(?:monitoring|measuring|tracking)\s+(?:performance|issues?)',
                r'keeping\s+an\s+eye\s+on',
                r'review(?:ing)?\s+(?:performance|issues?)',
                r'response\s+time.*(?:monitor|watch|tracking|issues?)',
                r'repair\s+time.*(?:monitor|watch|tracking|issues?)',
                r'performance\s+(?:measurement|monitoring).*(?:tasks?|points?|items?)',
                r'(?:show|list|display|view).*(?:watchlist|watch\s+list)',
                r'(?:show|list|display|view).*(?:performance\s+(?:measurement|monitoring))',
                r'performance.*(?:monitoring|measurement).*(?:items?|list)',
                r'(?:z-score|statistical).*(?:performance|monitoring)'
            ],
            'scheduled_maintenance': [
                r'scheduled\s+maintenance(?!\s+measurement)',
                r'maintenance\s+(?:tasks?|schedule|plan)(?!\s+measurement)',
                r'preventative\s+maintenance',
                r'pm\s+(?:tasks?|schedule)',
                r'maintenance.*(?:open|due|assigned|scheduled)',
                r'(?:show|list|display|view).*maintenance.*(?:tasks?|schedule)',
                r'(?:items?|tasks?)\s+assigned\s+to.*(?:mechanic|person)',
                r'assigned.*(?:maintenance|tasks?)',
                r'(?:mechanic|person).*assigned.*(?:tasks?|items?)',
                r'(?:what|which|who).*assigned.*(?:maintenance|tasks?)',
                r'(?:what|which|who).*(?:maintenance|tasks?).*(?:for|assigned)',
                r'who.*(?:responsible|assigned).*maintenance'
            ]
        }
        
    def register_query_tool(self, name: str, tool_instance):
        """Register a query tool with the manager."""
        self.query_tools[name] = tool_instance
        logger.info(f"Registered query tool: {name}")
        
    def classify_query(self, query: str) -> Tuple[Optional[str], Dict[str, Any]]:
        """
        Classify a query and extract parameters.
        
        Args:
            query: The user's query
        
        Returns:
            Tuple of (query_type, parameters)
        """
        query_lower = query.lower()
        
        # Check patterns for each query type
        for query_type, patterns in self.query_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    # Extract parameters based on query type
                    params = self._extract_parameters(query, query_type)
                    logger.info(f"Classified query as '{query_type}' with params: {params}")
                    return query_type, params
        
        logger.info(f"Could not classify query: {query}")
        return None, {}
    
    def _extract_parameters(self, query: str, query_type: str) -> Dict[str, Any]:
        """Extract parameters from query based on query type."""
        params = {}
        query_lower = query.lower()
        
        # Extract common parameters
        if "this week" in query_lower:
            params["time_filter"] = "this_week"
        elif "next week" in query_lower:
            params["time_filter"] = "next_week"
        elif "today" in query_lower:
            params["time_filter"] = "today"
        elif "tomorrow" in query_lower:
            params["time_filter"] = "tomorrow"
            
        # Extract mechanic filter - enhanced patterns
        mechanic_patterns = [
            r'(?:for|mechanic)\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)\s*(?:#?(\d{3}))?',
            r'assigned\s+to\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)\s*(?:#?(\d{3}))?',
            r'(?:by|from)\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)\s*(?:#?(\d{3}))?'
        ]
        
        for pattern in mechanic_patterns:
            mechanic_match = re.search(pattern, query, re.IGNORECASE)
            if mechanic_match:
                params["mechanic_name"] = mechanic_match.group(1).strip().lower()
                if len(mechanic_match.groups()) > 1 and mechanic_match.group(2):
                    params["mechanic_id"] = mechanic_match.group(2)
                break
                
        # Extract machine filter for maintenance queries
        if query_type == "scheduled_maintenance":
            machine_match = re.search(r'(?:machine|asset)\s*(?:#|id)?\s*(\d{3})', query)
            if machine_match:
                params["machine_id"] = machine_match.group(1)
            else:
                # Check for machine type
                machine_types = ["plain machine", "overlocker", "button sew", "coverseam", "button hole", "bartack"]
                for machine_type in machine_types:
                    if machine_type in query_lower:
                        params["machine_type"] = machine_type.title()
                        break
                        
        # Extract issue type filter for watchlist with more precise matching
        if query_type == "watchlist":
            # Look for specific issue type mentions
            if re.search(r'\bresponse\s+time\b', query_lower):
                params["issue_type"] = "response_time"
            elif re.search(r'\brepair\s+time\b', query_lower):
                params["issue_type"] = "repair_time"
            
            # Also check for machine type or entity filters
            if re.search(r'\bmachine\b', query_lower):
                params["entity_type"] = "machine"
            elif re.search(r'\bmechanic\b', query_lower):
                params["entity_type"] = "mechanic"
                
        # Extract status filter
        if "open" in query_lower or "active" in query_lower:
            params["status"] = "open"
        elif "closed" in query_lower or "completed" in query_lower:
            params["status"] = "completed"
            
        return params
    
    def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a query by routing to the appropriate tool.
        
        Args:
            query: The user's natural language query
        
        Returns:
            Dict with 'success', 'data', and 'format' keys
        """
        query_type, params = self.classify_query(query)
        
        if query_type and query_type in self.query_tools:
            tool = self.query_tools[query_type]
            try:
                result = tool.execute(query, params)
                return {
                    "success": True,
                    "data": result["data"],
                    "format": result.get("format", "table"),
                    "query_type": query_type
                }
            except Exception as e:
                logger.error(f"Error executing query tool {query_type}: {e}")
                return {
                    "success": False,
                    "error": str(e),
                    "query_type": query_type
                }
        else:
            return {
                "success": False,
                "error": "Query type not recognized or no tool available",
                "query_type": None
            }