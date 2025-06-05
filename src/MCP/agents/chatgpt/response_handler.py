# /src/MCP/agents/chatgpt/response_handler.py
"""
Response Processing and Formatting for ChatGPT Agent

Handles OpenAI response processing, function call results, and output formatting.
"""

import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("response_handler")

class ResponseHandler:
    """
    Processes OpenAI responses and formats output for users.
    
    This component handles:
    - Function call result processing
    - Response formatting and validation
    - Table data conversion to markdown
    - Error handling and fallbacks
    """
    
    def __init__(self, response_formatter):
        """Initialize the response handler."""
        self.response_formatter = response_formatter
        logger.info("ResponseHandler initialized")
    
    def format_function_result(self, result: Any, function_name: str) -> str:
        """
        Format a function call result for display.
        
        Args:
            result: The raw function result
            function_name: Name of the function that was called
            
        Returns:
            Formatted result string
        """
        try:
            # Use the response formatter to format the result
            formatted_result = self.response_formatter.format_tool_result(result, function_name)
            
            # Check if result is structured table data
            if isinstance(formatted_result, dict) and formatted_result.get("type") == "table":
                return self._convert_table_to_markdown(formatted_result)
            
            # Handle regular text responses
            return formatted_result if isinstance(formatted_result, str) else str(formatted_result)
            
        except Exception as e:
            logger.error(f"Error formatting function result: {e}")
            return f"Error formatting result: {str(e)}"
    
    def _convert_table_to_markdown(self, table_data: Dict[str, Any]) -> str:
        """Convert table data to markdown format."""
        headers = table_data.get("headers", [])
        rows = table_data.get("rows", [])
        
        if not headers or not rows:
            return f"Here are the {table_data.get('title', 'results')}, but no data was found."
        
        # Create header row
        header_row = "| " + " | ".join([h.get("label", h.get("key", "")) for h in headers]) + " |"
        separator = "|" + "|".join(["---" for _ in headers]) + "|"
        
        # Create data rows
        data_rows = []
        for row in rows:
            row_values = []
            for header in headers:
                key = header.get("key", "")
                value = str(row.get(key, ""))
                row_values.append(value)
            data_rows.append("| " + " | ".join(row_values) + " |")
        
        # Combine into markdown table
        markdown_table = "\n".join([header_row, separator] + data_rows)
        return f"Here are the {table_data.get('title', 'results')}:\n\n{markdown_table}"
