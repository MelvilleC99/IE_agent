# /Users/melville/Documents/Industrial_Engineering_Agent/src/MCP/tool_registry.py

import logging
import inspect
from typing import Dict, Any, List, Callable, Optional, Union, TYPE_CHECKING
import json

if TYPE_CHECKING:
    from langchain.agents import Tool

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("mcp_tool_registry")

class MCPToolRegistry:
    """
    Registry for tools used in the Model Context Protocol (MCP).
    
    The tool registry manages tool definitions, documentation, and execution,
    providing a standardized interface for tools in the MCP system.
    """
    
    def __init__(self):
        """Initialize the tool registry."""
        self.tools = {}
        self.categories = {
            "data_retrieval": [],
            "analysis": [],
            "action": [],
            "maintenance": [],
            "notification": []
        }
        logger.info("Initialized MCP Tool Registry")
    
    def register_tool(
        self, 
        name: str, 
        function: Callable, 
        description: str, 
        category: str = "data_retrieval",
        parameters: Optional[Dict[str, Dict[str, Any]]] = None,
        examples: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """
        Register a tool with the registry.
        
        Args:
            name: The tool's name
            function: The function that implements the tool
            description: A description of what the tool does
            category: The tool category
            parameters: Parameter descriptions and types
            examples: Example usages of the tool
        """
        # Auto-generate parameters if not provided
        if parameters is None:
            parameters = self._generate_parameters(function)
        
        tool_info = {
            "name": name,
            "function": function,
            "description": description,
            "category": category,
            "parameters": parameters,
            "examples": examples or []
        }
        
        self.tools[name] = tool_info
        
        # Add to category list
        if category in self.categories:
            self.categories[category].append(name)
        else:
            self.categories[category] = [name]
            
        logger.info(f"Registered tool: {name} in category {category}")
    
    def _generate_parameters(self, function: Callable) -> Dict[str, Dict[str, Any]]:
        """
        Generate parameter information from function signature.
        
        Args:
            function: The function to analyze
            
        Returns:
            A dictionary of parameter information
        """
        params = {}
        signature = inspect.signature(function)
        
        for param_name, param in signature.parameters.items():
            # Skip 'self' parameter
            if param_name == 'self':
                continue
                
            param_info = {
                "type": "string",  # Default type
                "description": f"Parameter: {param_name}",
                "required": param.default == inspect.Parameter.empty
            }
            
            # Try to extract type annotation
            if param.annotation != inspect.Parameter.empty:
                if param.annotation == str:
                    param_info["type"] = "string"
                elif param.annotation == int:
                    param_info["type"] = "integer"
                elif param.annotation == float:
                    param_info["type"] = "number"
                elif param.annotation == bool:
                    param_info["type"] = "boolean"
                elif param.annotation == list or param.annotation == List:
                    param_info["type"] = "array"
                elif param.annotation == dict or param.annotation == Dict:
                    param_info["type"] = "object"
            
            params[param_name] = param_info
            
        return params
    
    def get_tool_info(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a tool.
        
        Args:
            name: The tool's name
            
        Returns:
            Tool information dictionary or None if not found
        """
        return self.tools.get(name)
    
    def execute_tool(self, name: str, parameters: Dict[str, Any]) -> Any:
        """
        Execute a tool with the given parameters.
        
        Args:
            name: The tool's name
            parameters: Tool parameters
            
        Returns:
            The tool's result
            
        Raises:
            ValueError: If the tool is not found
        """
        tool_info = self.tools.get(name)
        if not tool_info:
            logger.error(f"Tool not found: {name}")
            raise ValueError(f"Tool not found: {name}")
        
        function = tool_info["function"]
        
        try:
            # Log tool execution
            logger.info(f"Executing tool: {name} with parameters: {json.dumps(parameters, default=str)}")
            
            # Execute the tool
            result = function(**parameters)
            
            # Log success
            logger.info(f"Tool {name} executed successfully")
            
            return result
        except Exception as e:
            logger.error(f"Error executing tool {name}: {str(e)}", exc_info=True)
            raise
    
    def get_tools_by_category(self, category: str) -> List[str]:
        """
        Get all tool names in a category.
        
        Args:
            category: The category name
            
        Returns:
            List of tool names
        """
        return self.categories.get(category, [])
    
    def get_all_tools(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about all registered tools.
        
        Returns:
            Dictionary of all tools
        """
        # Return a copy without the function objects
        result = {}
        for name, info in self.tools.items():
            tool_copy = info.copy()
            tool_copy.pop("function", None)
            result[name] = tool_copy
            
        return result
    
    def get_tool(self, name: str) -> Optional[Callable]:
        """
        Get a tool function by name.
        
        Args:
            name: The tool's name
            
        Returns:
            The tool function or None if not found
        """
        tool_info = self.tools.get(name)
        return tool_info["function"] if tool_info else None
    
    def get_tool_names(self) -> List[str]:
        """
        Get names of all registered tools.
        
        Returns:
            List of tool names
        """
        return list(self.tools.keys())
    
    def get_tool_function(self, name: str) -> Optional[Callable]:
        """
        Get the function for a specific tool.
        
        Args:
            name: The tool's name
            
        Returns:
            The tool function or None if not found
        """
        tool_info = self.tools.get(name)
        if tool_info:
            return tool_info["function"]
        return None
    
    def get_langchain_tools(self) -> List['Tool']:
        """
        Convert MCP tools to Langchain Tool objects.
        
        Returns:
            List of Langchain Tool objects
        """
        try:
            from langchain.agents import Tool
            
            langchain_tools = []
            for name, tool_info in self.tools.items():
                langchain_tools.append(
                    Tool(
                        name=name,
                        func=tool_info["function"],
                        description=tool_info["description"]
                    )
                )
            
            logger.info(f"Converted {len(langchain_tools)} MCP tools to Langchain tools")
            return langchain_tools
            
        except ImportError:
            logger.error("Could not import langchain.agents.Tool")
            return []
    
    def generate_tool_descriptions(self) -> str:
        """
        Generate formatted descriptions of all tools.
        
        Returns:
            Formatted tool descriptions
        """
        descriptions = []
        
        for category, tools in self.categories.items():
            if not tools:
                continue
                
            descriptions.append(f"## {category.upper()} TOOLS")
            
            for tool_name in tools:
                tool = self.tools.get(tool_name)
                if not tool:
                    continue
                    
                descriptions.append(f"### {tool_name}")
                descriptions.append(tool["description"])
                
                if tool["parameters"]:
                    descriptions.append("Parameters:")
                    for param_name, param_info in tool["parameters"].items():
                        required = "Required" if param_info.get("required", False) else "Optional"
                        descriptions.append(f"- {param_name} ({param_info.get('type', 'string')}): {param_info.get('description', '')} [{required}]")
                
                descriptions.append("")
        
        return "\n".join(descriptions)


# Initialize the tool registry
tool_registry = MCPToolRegistry()

# Import existing tools
from src.agents.maintenance.maintenance_agent import (
    get_raw_maintenance_data,
    get_analysis_summary,
    get_mechanic_performance,
    compare_mechanics,
    get_machine_performance,
    get_machine_reason_data,
    run_scheduled_maintenance
)

# Import Supabase tools
from src.agents.maintenance.tools.supabase_tool import (
    query_database,
    get_schema_info,
    insert_or_update_data
)

# Register maintenance tools
def register_maintenance_tools():
    """Register maintenance tools with the registry."""
    
    # Register existing maintenance tools
    tool_registry.register_tool(
        name="get_raw_maintenance_data",
        function=get_raw_maintenance_data,
        description="Get raw maintenance data from the JSON file. Use 'sample' for first 5 records.",
        category="data_retrieval"
    )
    
    tool_registry.register_tool(
        name="get_analysis_summary",
        function=get_analysis_summary,
        description="Get analysis summary data. Use 'all' for entire summary or a specific section name.",
        category="analysis"
    )
    
    tool_registry.register_tool(
        name="get_mechanic_performance",
        function=get_mechanic_performance,
        description="Get performance data for a specific mechanic. Input should be the mechanic's name.",
        category="analysis",
        parameters={
            "mechanic_name": {
                "type": "string",
                "description": "Name of the mechanic to analyze",
                "required": True
            }
        }
    )
    
    tool_registry.register_tool(
        name="compare_mechanics",
        function=compare_mechanics,
        description="Compare all mechanics based on a metric.",
        category="analysis",
        parameters={
            "metric": {
                "type": "string",
                "description": "Metric to compare (e.g., 'repair_time', 'response_time')",
                "required": True
            }
        }
    )
    
    tool_registry.register_tool(
        name="get_machine_performance",
        function=get_machine_performance,
        description="Get performance data for a specific machine type.",
        category="analysis",
        parameters={
            "machine_type": {
                "type": "string",
                "description": "The type of machine to analyze",
                "required": True
            }
        }
    )
    
    tool_registry.register_tool(
        name="get_machine_reason_data",
        function=get_machine_reason_data,
        description="Get data for machine and reason combinations.",
        category="analysis",
        parameters={
            "combo": {
                "type": "string",
                "description": "Machine-reason combination in format 'MachineType_ReasonType'",
                "required": True
            }
        }
    )
    
    tool_registry.register_tool(
        name="run_scheduled_maintenance",
        function=run_scheduled_maintenance,
        description="Run the factory's scheduled maintenance workflow.",
        category="maintenance",
        parameters={
            "action": {
                "type": "string",
                "description": "Action to perform ('run', 'check', etc.)",
                "required": False
            }
        }
    )

# Register Supabase tools
def register_supabase_tools():
    """Register Supabase tools with the registry."""
    
    # Register the database query tool
    tool_registry.register_tool(
        name="query_database",
        function=query_database,
        description="Query the Supabase database based on provided parameters.",
        category="data_retrieval",
        parameters={
            "query_params": {
                "type": "string",
                "description": "Formatted string with query parameters in format: 'table_name:column1,column2;filter1=value1,filter2=value2;limit=100'",
                "required": True
            }
        },
        examples=[
            {
                "query_params": "maintenance_records:id,mechanic_name,repair_time",
                "description": "Get all maintenance records with ID, mechanic name, and repair time"
            },
            {
                "query_params": "mechanics:*;active=true",
                "description": "Get all fields for active mechanics"
            }
        ]
    )
    
    # Register the schema info tool
    tool_registry.register_tool(
        name="get_schema_info",
        function=get_schema_info,
        description="Get database schema information based on table name or search query.",
        category="data_retrieval",
        parameters={
            "query": {
                "type": "string",
                "description": "Table name or natural language query about database structure",
                "required": True
            }
        },
        examples=[
            {
                "query": "maintenance_records",
                "description": "Get schema for the maintenance_records table"
            },
            {
                "query": "Where are mechanic evaluations stored?",
                "description": "Natural language query about database structure"
            }
        ]
    )
    
    # Register the data modification tool
    tool_registry.register_tool(
        name="insert_or_update_data",
        function=insert_or_update_data,
        description="Insert or update data in the database.",
        category="action",
        parameters={
            "operation_str": {
                "type": "string",
                "description": "Formatted string with operation parameters in format: 'operation|table_name|json_data|match_column'",
                "required": True
            }
        },
        examples=[
            {
                "operation_str": "insert|mechanics|{\"name\":\"John Doe\",\"specialty\":\"Electrical\"}",
                "description": "Insert a new mechanic"
            },
            {
                "operation_str": "update|mechanics|{\"id\":5,\"active\":false}|id",
                "description": "Update mechanic with ID 5 to inactive status"
            }
        ]
    )

# Register all tools
def register_all_tools():
    """Register all tools with the registry."""
    register_maintenance_tools()
    register_supabase_tools()
    logger.info(f"Registered {len(tool_registry.get_tool_names())} tools")
    
# Initialize all tools when this module is imported
register_all_tools()