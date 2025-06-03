# /src/MCP/response_formatter.py

import logging
import json
import re
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime
import sys
import os

# Add project root to path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import date utilities
from src.MCP.agents.utils.date_utils import date_utils

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("mcp_response_formatter")

class MCPResponseFormatter:
    """
    Formats and processes responses within the Model Context Protocol (MCP).
    
    The response formatter handles parsing and structuring LLM responses, 
    extracting tool calls, and formatting final outputs to the user.
    """
    
    def __init__(self, tool_registry=None):
        """
        Initialize the response formatter.
        
        Args:
            tool_registry: Optional tool registry for executing extracted tool calls
        """
        self.tool_registry = tool_registry
        logger.info("Initialized MCP Response Formatter")
    
    def parse_llm_response(self, response: str) -> Dict[str, Any]:
        """
        Parse a raw LLM response into structured components.
        
        Args:
            response: Raw response from the LLM
            
        Returns:
            Structured response with message, tool calls, and reasoning
        """
        # Initialize the structured response
        structured_response = {
            "message": response,
            "tool_calls": [],
            "reasoning": "",
            "action_items": []
        }
        
        # Extract reasoning sections (text between ```reasoning and ```)
        reasoning_matches = re.findall(r'```reasoning\n(.*?)\n```', response, re.DOTALL)
        if reasoning_matches:
            structured_response["reasoning"] = reasoning_matches[0].strip()
            # Remove reasoning blocks from the message
            cleaned_message = re.sub(r'```reasoning\n.*?\n```', '', response, flags=re.DOTALL)
            structured_response["message"] = cleaned_message.strip()
        
        # Extract tool calls (text between ```tool and ```)
        tool_matches = re.findall(r'```tool\n(.*?)\n```', response, re.DOTALL)
        for tool_match in tool_matches:
            try:
                # Try to parse as JSON
                tool_call = json.loads(tool_match)
                structured_response["tool_calls"].append(tool_call)
            except json.JSONDecodeError:
                # If not valid JSON, try to parse as key-value pairs
                tool_call = self._parse_tool_text(tool_match)
                if tool_call:
                    structured_response["tool_calls"].append(tool_call)
            
        # Remove tool call blocks from the message
        cleaned_message = re.sub(r'```tool\n.*?\n```', '', structured_response["message"], flags=re.DOTALL)
        structured_response["message"] = cleaned_message.strip()
        
        # Extract action items
        action_items = re.findall(r'- \[ \] (.*?)$', structured_response["message"], re.MULTILINE)
        structured_response["action_items"] = action_items
        
        logger.debug(f"Parsed LLM response into {len(structured_response['tool_calls'])} tool calls")
        return structured_response
    
    def _parse_tool_text(self, tool_text: str) -> Optional[Dict[str, Any]]:
        """
        Parse tool call text that isn't in JSON format.
        
        Args:
            tool_text: Text describing a tool call
            
        Returns:
            Structured tool call or None if parsing fails
        """
        lines = tool_text.strip().split('\n')
        tool_call = {}
        
        # Extract tool name (usually the first line)
        for line in lines:
            if 'tool:' in line.lower() or 'name:' in line.lower():
                parts = line.split(':', 1)
                if len(parts) == 2:
                    tool_call["name"] = parts[1].strip()
                    break
        
        # Extract parameters
        parameters = {}
        current_param = None
        param_value = []
        
        for line in lines:
            if 'parameters:' in line.lower() or 'params:' in line.lower():
                continue
                
            # Check if this is a new parameter
            if ': ' in line and not line.startswith(' '):
                # Save the previous parameter if there was one
                if current_param and param_value:
                    parameters[current_param] = '\n'.join(param_value).strip()
                    param_value = []
                
                # Extract new parameter
                parts = line.split(':', 1)
                current_param = parts[0].strip()
                param_value.append(parts[1].strip())
            elif current_param:
                # Continue with current parameter
                param_value.append(line)
        
        # Save the last parameter
        if current_param and param_value:
            parameters[current_param] = '\n'.join(param_value).strip()
        
        # If we found a tool name and parameters, return the tool call
        if "name" in tool_call:
            tool_call["parameters"] = parameters
            return tool_call
            
        return None
    
    def execute_tool_calls(self, tool_calls: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute a list of tool calls using the tool registry.
        
        Args:
            tool_calls: List of tool calls to execute
            
        Returns:
            List of results from tool execution
        """
        if not self.tool_registry:
            logger.warning("No tool registry provided, cannot execute tool calls")
            return [{"error": "No tool registry available"} for _ in tool_calls]
            
        results = []
        
        for tool_call in tool_calls:
            tool_name = tool_call.get("name")
            parameters = tool_call.get("parameters", {})
            
            try:
                # Execute the tool
                result = self.tool_registry.execute_tool(tool_name, parameters)
                results.append({
                    "tool": tool_name,
                    "parameters": parameters,
                    "result": result
                })
            except Exception as e:
                logger.error(f"Error executing tool {tool_name}: {str(e)}", exc_info=True)
                results.append({
                    "tool": tool_name,
                    "parameters": parameters,
                    "error": str(e)
                })
        
        return results
    
    def format_table_data(self, data: List[Dict[str, Any]], columns: Optional[List[str]] = None) -> str:
        """
        Format data as a markdown table.
        
        Args:
            data: List of dictionaries with data
            columns: Optional list of columns to include
            
        Returns:
            Formatted markdown table string
        """
        if not data:
            return "No data found."
        
        # If no columns specified, use all columns from first row
        if not columns:
            columns = list(data[0].keys())
        
        # Create table header
        header = "| " + " | ".join(columns) + " |"
        separator = "|" + "|".join(["---" for _ in columns]) + "|"
        
        # Create table rows
        rows = []
        for row in data:
            row_values = []
            for col in columns:
                value = row.get(col, "")
                
                # Format dates nicely
                if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}', value):
                    value = date_utils.format_date_for_display(value)
                
                # Truncate long values
                if isinstance(value, str) and len(value) > 50:
                    value = value[:47] + "..."
                    
                row_values.append(str(value))
            
            rows.append("| " + " | ".join(row_values) + " |")
        
        # Combine everything
        table = "\n".join([header, separator] + rows)
        
        # Add count at the bottom
        table += f"\n\n*Total: {len(data)} records*"
        
        return table
    
    def format_list_data(self, data: List[Dict[str, Any]], key_fields: List[str]) -> str:
        """
        Format data as a numbered list.
        
        Args:
            data: List of dictionaries with data
            key_fields: Fields to include in each list item
            
        Returns:
            Formatted list string
        """
        if not data:
            return "No data found."
        
        result = []
        for i, item in enumerate(data, 1):
            item_text = f"{i}. "
            
            # Add primary field
            if key_fields:
                primary_value = item.get(key_fields[0], "")
                item_text += f"**{primary_value}**"
                
                # Add secondary fields
                for field in key_fields[1:]:
                    value = item.get(field, "")
                    
                    # Format dates nicely
                    if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}', value):
                        value = date_utils.format_date_for_display(value)
                    
                    field_name = field.replace('_', ' ').title()
                    item_text += f", {field_name}: {value}"
            
            result.append(item_text)
        
        # Join with newlines and add count
        formatted_list = "\n".join(result)
        formatted_list += f"\n\n*Total: {len(data)} items*"
        
        return formatted_list
    
    def format_data_adaptively(self, data: Union[List[Dict[str, Any]], Dict[str, Any]], 
                              query: str, data_type: Optional[str] = None) -> str:
        """
        Generic formatter that adapts the format based on data structure and content.
        
        Args:
            data: The data to format (list of items or single item)
            query: The original query for context
            data_type: Optional hint about the data type (tasks, mechanics, etc.)
            
        Returns:
            Formatted string representation of the data
        """
        # Handle empty data
        if not data:
            return "No data found matching your query."
        
        # Convert string to json if needed
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except:
                return data
        
        # Handle single items
        if not isinstance(data, list):
            return self._format_single_item(data)
        
        # For lists, determine the best format
        if len(data) > 10:
            # Use table for large datasets
            return self.format_table_data(data)
        else:
            # Use list for smaller datasets with key fields
            key_fields = self._determine_key_fields(data, data_type)
            return self.format_list_data(data, key_fields)
    
    def _format_single_item(self, item: Dict[str, Any]) -> str:
        """Format a single item with key-value pairs."""
        result = []
        
        # Exclude internal fields
        exclude_fields = ["id", "created_at", "updated_at", "timestamp"]
        
        for key, value in item.items():
            if key in exclude_fields:
                continue
            
            # Format times nicely
            if isinstance(value, (int, float)) and ("time" in key.lower() or "duration" in key.lower()):
                value = date_utils.format_duration(value)
            
            # Format dates nicely
            if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}', value):
                value = date_utils.format_date_for_display(value)
            
            # Format the key nicely
            formatted_key = key.replace('_', ' ').title()
            
            result.append(f"**{formatted_key}**: {value}")
        
        return "\n".join(result)
    
    def _determine_key_fields(self, data: List[Dict[str, Any]], data_type: Optional[str]) -> List[str]:
        """Determine the most important fields to display."""
        if not data:
            return []
        
        # Check common field patterns
        sample = data[0]
        
        # Watchlist data
        if "issue_type" in sample and "mechanic_info" in sample:
            return ["issue_type", "mechanic_info", "performance_detail", "review_date"]
        
        # Scheduled maintenance data
        if "machine_info" in sample and "assigned_to" in sample:
            return ["machine_info", "assigned_to", "priority", "due_date"]
        
        # Mechanic data
        if "name" in sample and "employee_number" in sample:
            return ["name", "employee_number", "specialty", "status"]
        
        # Default to first few non-id fields
        all_fields = list(sample.keys())
        exclude_fields = ["id", "created_at", "updated_at", "timestamp"]
        key_fields = [f for f in all_fields if f not in exclude_fields][:4]
        
        return key_fields
    
    def format_error_response(self, error: str) -> Dict[str, Any]:
        """
        Format an error response for the user.
        
        Args:
            error: Error message
            
        Returns:
            Formatted error response
        """
        return {
            "answer": f"I apologize, but I encountered an error: {error}",
            "error": error,
            "format": "error"
        }
    
    def format_tool_result(self, result: Any, tool_name: str) -> Union[str, Dict[str, Any]]:
        """
        Format a tool result for inclusion in a prompt.
        
        Args:
            result: The result from the tool
            tool_name: The name of the tool
            
        Returns:
            Formatted tool result (string or structured data for tables)
        """
        # Handle different result types
        if isinstance(result, str):
            return f"Result from {tool_name}:\n{result}"
        elif isinstance(result, dict):
            # Check if it's a structured query result
            if "data" in result and isinstance(result["data"], list):
                # Check if this should be a table
                if self._should_be_table(result["data"], tool_name):
                    return self._format_as_table_data(result["data"], tool_name)
                else:
                    return f"Result from {tool_name}:\n{self.format_data_adaptively(result['data'], tool_name)}"
            else:
                # Format as JSON for other dicts
                try:
                    json_str = json.dumps(result, indent=2)
                    return f"Result from {tool_name}:\n```json\n{json_str}\n```"
                except:
                    return f"Result from {tool_name}: {str(result)}"
        elif isinstance(result, list):
            # Check if this should be a table
            if self._should_be_table(result, tool_name):
                return self._format_as_table_data(result, tool_name)
            else:
                return f"Result from {tool_name}:\n{self.format_data_adaptively(result, tool_name)}"
        else:
            return f"Result from {tool_name}: {str(result)}"    
    def _should_be_table(self, data: List[Dict], tool_name: str) -> bool:
        """
        Determine if data should be displayed as a table.
        
        Args:
            data: List of data items
            tool_name: Name of the tool that returned the data
            
        Returns:
            True if data should be displayed as a table
        """
        if not data or not isinstance(data, list):
            return False
        
        # Table criteria
        table_indicators = [
            len(data) >= 3,  # Multiple items
            tool_name in ['quick_query', 'query_database'],  # Database queries
            'scheduled_maintenance' in str(data).lower(),  # Maintenance data
            any(key in data[0] for key in ['machine_id', 'assignee', 'due_date', 'priority', 'status']) if isinstance(data[0], dict) else False  # Table-like fields
        ]
        
        return any(table_indicators)
    
    def _format_as_table_data(self, data: List[Dict], tool_name: str) -> Dict[str, Any]:
        """
        Format data as structured table data for frontend rendering.
        
        Args:
            data: List of data items
            tool_name: Name of the tool that returned the data
            
        Returns:
            Structured table data
        """
        if not data:
            return {"type": "text", "content": "No data found."}
        
        # Extract table headers from first item
        first_item = data[0] if data else {}
        if not isinstance(first_item, dict):
            return {"type": "text", "content": self.format_data_adaptively(data, tool_name)}
        
        # Define column mappings for better display
        column_mappings = {
            'machine_id': 'Machine',
            'machine_type': 'Type', 
            'assignee': 'Assigned To',
            'assigned_mechanic': 'Assigned To',
            'priority': 'Priority',
            'due_date': 'Due Date',
            'status': 'Status',
            'created_at': 'Created',
            'description': 'Description',
            'task_type': 'Task Type'
        }
        
        # Get available columns
        available_columns = list(first_item.keys())
        
        # Order columns logically for maintenance data
        column_order = ['machine_id', 'machine_type', 'assignee', 'assigned_mechanic', 
                       'priority', 'status', 'due_date', 'created_at', 'description', 'task_type']
        
        # Create ordered column list
        ordered_columns = []
        for col in column_order:
            if col in available_columns:
                ordered_columns.append(col)
        
        # Add any remaining columns
        for col in available_columns:
            if col not in ordered_columns:
                ordered_columns.append(col)
        
        # Create headers
        headers = []
        for col in ordered_columns:
            display_name = column_mappings.get(col, col.replace('_', ' ').title())
            headers.append({
                'key': col,
                'label': display_name,
                'sortable': True
            })
        
        # Process data rows
        rows = []
        for item in data:
            row = {}
            for col in ordered_columns:
                value = item.get(col, '')
                
                # Format dates nicely
                if col in ['due_date', 'created_at'] and value:
                    if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}', value):
                        value = date_utils.format_date_for_display(value)
                
                # Format machine display
                if col == 'machine_id' and 'machine_type' in item:
                    machine_type = item.get('machine_type', '')
                    if machine_type:
                        value = f"{machine_type} (#{value})"
                
                row[col] = value
            rows.append(row)
        
        return {
            "type": "table",
            "headers": headers,
            "rows": rows,
            "total_count": len(rows),
            "title": self._get_table_title(tool_name, len(rows))
        }
    
    def _get_table_title(self, tool_name: str, count: int) -> str:
        """Generate appropriate title for table display."""
        if 'maintenance' in tool_name.lower():
            return f"Scheduled Maintenance Tasks ({count} items)"
        elif 'mechanic' in tool_name.lower():
            return f"Mechanics ({count} items)"
        else:
            return f"Results ({count} items)"
