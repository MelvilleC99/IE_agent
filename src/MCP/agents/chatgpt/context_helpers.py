# /src/MCP/agents/chatgpt/context_helpers.py
"""
Context Management Utilities for ChatGPT Agent

Handles query metadata, context extraction, and helper functions.
"""

import logging
import re
from typing import Dict, Any, Optional

logger = logging.getLogger("context_helpers")

class ContextHelpers:
    """
    Provides context management utilities for the ChatGPT agent.
    
    This component handles:
    - Query metadata storage and retrieval
    - Mechanic name extraction from queries
    - Context analysis utilities
    """
    
    def __init__(self, context_manager):
        """Initialize the context helpers."""
        self.context_manager = context_manager
        logger.info("ContextHelpers initialized")
    
    def store_query_metadata(self, function_name: str, function_args: Dict[str, Any], result: Any):
        """
        Store simplified metadata about the executed query for context-aware follow-ups.
        
        Args:
            function_name: Name of the function that was executed
            function_args: Arguments passed to the function
            result: Result returned by the function
        """
        try:
            # Only store metadata for successful query operations
            if function_name == "quick_query" and isinstance(result, dict) and result.get("success"):
                query_type = result.get("query_type", "unknown")
                query_text = function_args.get("query", "").lower()
                
                # Extract simple filters from the query text
                mechanic_filter = self.extract_mechanic_from_query(query_text)
                
                # Store simple context
                self.context_manager.add_query_metadata(
                    query_type=query_type,
                    tool_name=function_name,
                    filters={"mechanic_name": mechanic_filter} if mechanic_filter else {},
                    table_shown=query_type
                )
                logger.info(f"Stored context: {query_type} query, mechanic: {mechanic_filter or 'none'}")
            
            # Clear context for non-query operations to avoid confusion
            elif function_name in ["get_tool_details", "run_scheduled_maintenance", "analyze_mechanic_performance"]:
                logger.info(f"Clearing context after {function_name} operation")
                self.context_manager.clear_query_metadata()
        
        except Exception as e:
            logger.warning(f"Failed to store query metadata: {e}")
    
    def extract_mechanic_from_query(self, query_text: str) -> Optional[str]:
        """Extract mechanic name from query text using simple patterns."""
        # Simple patterns for mechanic names
        patterns = [
            r'(?:for|by|from)\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
            r'([A-Za-z]+(?:\s+[A-Za-z]+)?)\s+(?:items?|tasks?|data)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, query_text, re.IGNORECASE)
            if match:
                name = match.group(1).strip().lower()
                # Basic validation - avoid common words
                if name not in ['the', 'all', 'any', 'some', 'show', 'list', 'view']:
                    return name
        
        return None
