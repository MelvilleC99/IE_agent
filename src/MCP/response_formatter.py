# /Users/melville/Documents/Industrial_Engineering_Agent/src/MCP/response_formatter.py

import logging
import json
import re
from typing import Dict, Any, List, Optional, Union, Tuple

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
    
    def format_user_response(
        self, 
        structured_response: Dict[str, Any], 
        tool_results: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Format the final response to send to the user.
        
        Args:
            structured_response: Structured response from parse_llm_response
            tool_results: Optional results from executed tool calls
            
        Returns:
            Formatted response for the user
        """
        # Start with the message from the LLM
        user_message = structured_response["message"]
        
        # Process tool results if available
        tools_used = []
        if tool_results:
            for result in tool_results:
                tools_used.append(result["tool"])
                
                # If there was an error, add it to the message
                if "error" in result:
                    user_message += f"\n\nError executing {result['tool']}: {result['error']}"
        
        # Create the final response
        formatted_response = {
            "answer": user_message,
            "tools_used": tools_used,
            "action_items": structured_response["action_items"],
            "has_reasoning": bool(structured_response.get("reasoning"))
        }
        
        return formatted_response
    
    def process_response(self, llm_response: str) -> Dict[str, Any]:
        """
        Process a raw LLM response, execute any tool calls, and format the final response.
        
        Args:
            llm_response: Raw response from the LLM
            
        Returns:
            Processed and formatted response
        """
        # Parse the LLM response
        structured_response = self.parse_llm_response(llm_response)
        
        # Execute tool calls if there are any and we have a tool registry
        tool_results = None
        if structured_response["tool_calls"] and self.tool_registry:
            tool_results = self.execute_tool_calls(structured_response["tool_calls"])
        
        # Format the final response
        formatted_response = self.format_user_response(structured_response, tool_results)
        
        # Look for format indicators in the response and apply appropriate formatting
        formatted_response["answer"] = self._enhance_formatting(formatted_response["answer"])
        
        return formatted_response
    
    def format_tool_result(self, result: Any, tool_name: str) -> str:
        """
        Format a tool result for inclusion in a prompt.
        
        Args:
            result: The result from the tool
            tool_name: The name of the tool
            
        Returns:
            Formatted tool result
        """
        # Handle different result types
        if isinstance(result, str):
            # For string results, just use them directly
            formatted_result = f"Result from {tool_name}:\n{result}"
        elif isinstance(result, dict):
            # For dictionaries, format as pretty JSON
            try:
                json_str = json.dumps(result, indent=2)
                formatted_result = f"Result from {tool_name}:\n```json\n{json_str}\n```"
            except:
                formatted_result = f"Result from {tool_name}: {str(result)}"
        elif isinstance(result, list):
            # For lists, format as pretty JSON
            try:
                json_str = json.dumps(result, indent=2)
                formatted_result = f"Result from {tool_name}:\n```json\n{json_str}\n```"
            except:
                formatted_result = f"Result from {tool_name}: {str(result)}"
        else:
            # For other types, convert to string
            formatted_result = f"Result from {tool_name}: {str(result)}"
        
        return formatted_result
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
        
        # Case 1: Single item with few fields (simple format)
        if not isinstance(data, list) and len(data) <= 5:
            return self._format_simple_item(data)
            
        # Case 2: List of items (tabular data)
        if isinstance(data, list):
            # Detect what kind of data we're dealing with based on fields
            if data_type is None:
                data_type = self._detect_data_type(data)
                
            # Count fields to determine if we should use a table or bullet points
            num_fields = len(self._get_common_fields(data))
            
            if num_fields <= 3:
                # Simple list with bullet points
                return self._format_simple_list(data, data_type, query)
            else:
                # More complex data - use table format
                return self._format_as_table(data, data_type, query)
                
        # Case 3: Complex single item (detailed format)
        return self._format_complex_item(data, data_type or "unknown")
    
    def _detect_data_type(self, data: List[Dict[str, Any]]) -> str:
        """Detect the type of data based on its fields."""
        if not data or not isinstance(data, list) or not data[0]:
            return "unknown"
            
        sample = data[0]
        
        # Check for task indicators
        if any(field in sample for field in ["machine_id", "due_by", "priority"]):
            return "scheduled_maintenance"
            
        # Check for performance task indicators
        if any(field in sample for field in ["monitor_frequency", "issue_type", "monitor_end_date"]):
            return "tasks"
            
        # Check for mechanic indicators
        if any(field in sample for field in ["name", "surname", "employee_number", "specialty"]):
            return "mechanics"
            
        # Check for performance indicators
        if any(field in sample for field in ["repair_time", "response_time", "z_score"]):
            return "performance"
            
        return "unknown"
    
    def _get_common_fields(self, data: List[Dict[str, Any]]) -> List[str]:
        """Extract common fields across all items in the dataset."""
        if not data:
            return []
            
        # Start with fields from first item
        common_fields = set(data[0].keys())
        
        # Find intersection with all other items
        for item in data[1:]:
            common_fields = common_fields.intersection(item.keys())
            
        # Exclude internal fields
        exclude_fields = ["id", "created_at", "updated_at", "timestamp"]
        common_fields = [f for f in common_fields if f not in exclude_fields]
        
        return list(common_fields)
    
    def _format_simple_item(self, item: Dict[str, Any]) -> str:
        """Format a simple item with key-value pairs."""
        result = ""
        
        # Exclude internal fields
        exclude_fields = ["id", "created_at", "updated_at", "timestamp"]
        
        for key, value in item.items():
            if key in exclude_fields:
                continue
                
            # Format times if needed
            if isinstance(value, (int, float)) and ("time" in key.lower() or "duration" in key.lower()):
                if value > 1000:  # Likely milliseconds
                    value = f"{value/60000:.2f} minutes"
                    
            # Format dates if needed
            if isinstance(value, str) and "T" in value and len(value) > 10:
                try:
                    value = value.split("T")[0]  # YYYY-MM-DD format
                except:
                    pass
                    
            # Add formatted key-value pair
            result += f"**{key.replace('_', ' ').title()}**: {value}\n"
            
        return result
    
    def _format_simple_list(self, data: List[Dict[str, Any]], data_type: str, query: str) -> str:
        """Format a simple list with bullet points."""
        # Determine key fields based on data type
        key_fields = self._get_key_fields(data_type)
        common_fields = self._get_common_fields(data)
        
        # If no key fields match, use all common fields
        fields_to_use = [f for f in key_fields if f in common_fields]
        if not fields_to_use:
            fields_to_use = common_fields[:3]  # Use up to 3 fields
            
        # Create header based on data type and query
        header = self._create_header(data_type, query)
        result = f"{header}\n\n"
        
        # Format each item
        for idx, item in enumerate(data, 1):
            result += f"{idx}. "
            
            # Add primary field with emphasis
            if fields_to_use:
                primary_field = fields_to_use[0]
                primary_value = item.get(primary_field, "")
                result += f"**{primary_value}**"
                
                # Add supporting fields
                for field in fields_to_use[1:]:
                    value = item.get(field, "")
                    
                    # Format times and dates
                    if isinstance(value, (int, float)) and ("time" in field.lower() or "duration" in field.lower()):
                        if value > 1000:  # Likely milliseconds
                            value = f"{value/60000:.2f} minutes"
                    
                    if isinstance(value, str) and "T" in value and len(value) > 10:
                        try:
                            value = value.split("T")[0]  # YYYY-MM-DD format
                        except:
                            pass
                    
                    field_name = field.replace('_', ' ').title()
                    result += f", {field_name}: {value}"
            else:
                # Fallback if no fields match
                result += str(item)
                
            result += "\n"
        
        # Add summary
        result += f"\nTotal items: {len(data)}"
        return result
    
    def _format_as_table(self, data: List[Dict[str, Any]], data_type: str, query: str) -> str:
        """Format data as a markdown table."""
        # Determine which fields to include
        key_fields = self._get_key_fields(data_type)
        common_fields = self._get_common_fields(data)
        
        # If no key fields match, use common fields (up to 5)
        fields_to_use = [f for f in key_fields if f in common_fields]
        if not fields_to_use:
            fields_to_use = common_fields[:5]
            
        # Create header based on data type and query
        header = self._create_header(data_type, query)
        result = f"{header}\n\n"
        
        # Create table header
        result += "| # | "
        for field in fields_to_use:
            field_name = field.replace('_', ' ').title()
            result += f"{field_name} | "
        result += "\n"
        
        # Add separator line
        result += "|---|" + "---|" * len(fields_to_use) + "\n"
        
        # Add table rows
        for idx, item in enumerate(data, 1):
            result += f"| {idx} | "
            
            for field in fields_to_use:
                value = item.get(field, "")
                
                # Format times and dates
                if isinstance(value, (int, float)) and ("time" in field.lower() or "duration" in field.lower()):
                    if value > 1000:  # Likely milliseconds
                        value = f"{value/60000:.2f} min"
                
                if isinstance(value, str) and "T" in value and len(value) > 10:
                    try:
                        value = value.split("T")[0]  # YYYY-MM-DD format
                    except:
                        pass
                        
                result += f"{value} | "
            
            result += "\n"
        
        # Add summary
        result += f"\nTotal items: {len(data)}"
        return result
    
    def _get_key_fields(self, data_type: str) -> List[str]:
        """Get the most important fields for a specific data type."""
        if data_type == "scheduled_maintenance":
            return ["machine_type", "machine_id", "priority", "status", "due_by", "mechanic_name"]
        elif data_type == "tasks":
            return ["title", "mechanic_name", "monitor_frequency", "monitor_end_date", "issue_type"]
        elif data_type == "mechanics":
            return ["name", "surname", "employee_number", "specialty", "active"]
        elif data_type == "performance":
            return ["mechanic_name", "repair_time", "response_time", "z_score", "machine_type"]
        else:
            return []
            
    def _create_header(self, data_type: str, query: str) -> str:
        """Create an appropriate header based on data type and query."""
        # Default headers
        headers = {
            "scheduled_maintenance": "Here are the scheduled maintenance tasks:",
            "tasks": "Here are the maintenance tasks:",
            "mechanics": "Here is the list of mechanics:",
            "performance": "Here is the performance data:"
        }
        
        # Get base header
        header = headers.get(data_type, "Here are the results:")
        
        # Customize based on query
        if data_type == "scheduled_maintenance" or data_type == "tasks":
            if "current" in query.lower() or "open" in query.lower() or "active" in query.lower():
                header = f"Here are the current open {data_type}:"
            elif "completed" in query.lower() or "closed" in query.lower() or "done" in query.lower():
                header = f"Here are the completed {data_type}:"
                
        elif data_type == "mechanics":
            if "active" in query.lower():
                header = "Here is the list of active mechanics:"
            elif "inactive" in query.lower():
                header = "Here is the list of inactive mechanics:"
                
        return header
    
    def _format_complex_item(self, item: Dict[str, Any], data_type: str) -> str:
        """Format a complex single item with sections."""
        # Create sections based on related fields
        result = ""
        
        # Organize fields into logical sections
        sections = self._organize_into_sections(item, data_type)
        
        # Format each section
        for section_name, section_fields in sections.items():
            if section_fields:
                result += f"## {section_name}\n\n"
                
                for field, value in section_fields:
                    # Format value appropriately
                    formatted_value = self._format_field_value(field, value)
                    field_name = field.replace('_', ' ').title()
                    result += f"**{field_name}**: {formatted_value}\n"
                    
                result += "\n"
                
        return result
    
    def _organize_into_sections(self, item: Dict[str, Any], data_type: str) -> Dict[str, List[Tuple[str, Any]]]:
        """Organize fields into logical sections based on data type."""
        sections = {
            "Basic Information": [],
            "Timing Information": [],
            "Status Information": [],
            "Related Information": []
        }
        
        # Exclude internal fields
        exclude_fields = ["id", "created_at", "updated_at", "timestamp"]
        
        for field, value in item.items():
            if field in exclude_fields:
                continue
                
            # Categorize field into a section
            if any(word in field.lower() for word in ["time", "duration", "date", "delay"]):
                sections["Timing Information"].append((field, value))
            elif any(word in field.lower() for word in ["status", "priority", "state", "active"]):
                sections["Status Information"].append((field, value))
            elif field in self._get_key_fields(data_type)[:2]:
                sections["Basic Information"].append((field, value))
            else:
                sections["Related Information"].append((field, value))
                
        # Remove empty sections
        return {k: v for k, v in sections.items() if v}
    
    def _format_field_value(self, field: str, value: Any) -> str:
        """Format a field value appropriately based on field name and value type."""
        # Format times
        if isinstance(value, (int, float)) and ("time" in field.lower() or "duration" in field.lower()):
            if value > 1000:  # Likely milliseconds
                return f"{value/60000:.2f} minutes"
                
        # Format dates
        if isinstance(value, str) and "T" in value and len(value) > 10:
            try:
                return value.split("T")[0]  # YYYY-MM-DD format
            except:
                pass
                
        # Format boolean values
        if isinstance(value, bool):
            return "Yes" if value else "No"
            
        return str(value)
        
    def _enhance_formatting(self, text: str) -> str:
        """
        Enhance the formatting of the response text based on format indicators.
        
        Args:
            text: The text to format
            
        Returns:
            Enhanced formatted text
        """
        # Look for format indicators
        table_sections = re.findall(r'#table\n(.*?)(?:\n#|$)', text, re.DOTALL)
        list_sections = re.findall(r'#list\n(.*?)(?:\n#|$)', text, re.DOTALL)
        details_sections = re.findall(r'#details\n(.*?)(?:\n#|$)', text, re.DOTALL)
        
        # Replace indicators with actual formatting
        for section in table_sections:
            # TODO: Implement table formatting if needed
            text = text.replace(f"#table\n{section}", section)
            
        for section in list_sections:
            # TODO: Implement list formatting if needed
            text = text.replace(f"#list\n{section}", section)
            
        for section in details_sections:
            # TODO: Implement details formatting if needed
            text = text.replace(f"#details\n{section}", section)
            
        # Remove any remaining format indicators
        text = re.sub(r'#(table|list|details)\n', '', text)
        
        # Remove any follow-up questions or suggestions at the end
        suggestions_pattern = r'\n\nWould you like(.*?)$'
        text = re.sub(suggestions_pattern, '', text, flags=re.DOTALL)
        
        suggestions_pattern2 = r'\n\nI can(.*?)$'
        text = re.sub(suggestions_pattern2, '', text, flags=re.DOTALL)
        
        return text