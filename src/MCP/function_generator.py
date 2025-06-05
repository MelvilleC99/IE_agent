# /src/MCP/function_generator.py
"""
Function Definition Generator for ChatGPT Agent
"""

import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger("function_generator")

class FunctionGenerator:
    """
    Generates ChatGPT function definitions from tool registry metadata.
    """
    
    def __init__(self, tool_registry=None):
        """Initialize the function generator."""
        self.tool_registry = tool_registry
        logger.info("FunctionGenerator initialized")
    
    def generate_function_definitions(self) -> List[Dict[str, Any]]:
        """Generate ChatGPT function definitions from all registered tools."""
        if not self.tool_registry:
            logger.warning("No tool registry provided")
            return []
        
        functions = []
        
        try:
            all_tools = self.tool_registry.get_all_tools()
            
            for tool_name, tool_info in all_tools.items():
                function_def = self._create_function_definition(tool_name, tool_info)
                if function_def:
                    functions.append(function_def)
                    
            logger.info(f"Generated {len(functions)} function definitions")
            return functions
            
        except Exception as e:
            logger.error(f"Error generating function definitions: {e}")
            return []

    def _create_function_definition(self, tool_name: str, tool_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a single ChatGPT function definition from tool metadata."""
        try:
            function_def = {
                "name": tool_name,
                "description": tool_info.get("description", f"Execute {tool_name} tool"),
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
            
            # Add parameters from tool info
            tool_parameters = tool_info.get("parameters", {})
            if tool_parameters:
                # Clean up the parameters - remove "required" from individual params
                cleaned_properties = {}
                required_params = []
                
                for param_name, param_info in tool_parameters.items():
                    if isinstance(param_info, dict):
                        # Copy param info but remove the "required" field
                        cleaned_param = {k: v for k, v in param_info.items() if k != "required"}
                        cleaned_properties[param_name] = cleaned_param
                        
                        # Check if this parameter is required
                        if param_info.get("required", False):
                            required_params.append(param_name)
                    else:
                        # Handle simple parameter definitions
                        cleaned_properties[param_name] = param_info
                
                function_def["parameters"]["properties"] = cleaned_properties
                
                if required_params:
                    function_def["parameters"]["required"] = required_params
            
            return function_def
            
        except Exception as e:
            logger.error(f"Error creating function definition for {tool_name}: {e}")
            return None
    
    def get_function_by_name(self, function_name: str) -> Optional[Dict[str, Any]]:
        """Get a specific function definition by name."""
        functions = self.generate_function_definitions()
        for func in functions:
            if func.get("name") == function_name:
                return func
        return None
    
    def get_function_names(self) -> List[str]:
        """Get list of all available function names."""
        functions = self.generate_function_definitions()
        return [func.get("name") for func in functions if func.get("name") is not None]
