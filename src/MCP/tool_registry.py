# /src/MCP/tool_registry.py

import logging
import inspect
from typing import Dict, Any, List, Callable, Optional, Union, TYPE_CHECKING
import json
import sys
import os

# Add the project root to the path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

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

# Register Supabase tools
def register_supabase_tools():
    """Register Supabase tools with the registry."""
    from src.agents.maintenance.tools.supabase_tool import (
        query_database,
        get_schema_info,
        insert_or_update_data
    )
    
    # Register the database query tool for complex queries
    tool_registry.register_tool(
        name="query_database",
        function=query_database,
        description="Query the Supabase database for complex queries requiring LLM interpretation.",
        category="data_retrieval",
        parameters={
            "query_params": {
                "type": "string",
                "description": "Formatted string with query parameters in format: 'table_name:column1,column2;filter1=value1,filter2=value2;limit=100'",
                "required": True
            }
        }
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
        }
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
        }
    )

# Register maintenance tools
def register_maintenance_tools():
    """Register maintenance tools with the registry."""
    from src.agents.maintenance.tools.scheduled_maintenance_tool import scheduled_maintenance_tool
    from src.agents.maintenance.tools.mechanic_performance_tool import mechanic_performance_tool
    
    # Register the scheduled maintenance tool
    tool_registry.register_tool(
        name="run_scheduled_maintenance",
        function=scheduled_maintenance_tool,
        description="Run the factory's scheduled maintenance workflow with clustering analysis.",
        category="maintenance",
        parameters={
            "action": {
                "type": "string",
                "description": "Action to perform ('run', 'check', etc.)",
                "required": True
            },
            "start_date": {
                "type": "string",
                "description": "Start date for maintenance (YYYY-MM-DD)",
                "required": False
            },
            "end_date": {
                "type": "string",
                "description": "End date for maintenance (YYYY-MM-DD)",
                "required": False
            },
            "mode": {
                "type": "string",
                "description": "Mode of operation ('args' or 'interactive')",
                "required": False
            },
            "use_database": {
                "type": "boolean",
                "description": "Whether to use database or file storage",
                "required": False
            },
            "force": {
                "type": "boolean",
                "description": "Override 30-day clustering frequency limit",
                "required": False
            }
        }
    )
    
    # Register the mechanic performance tool
    tool_registry.register_tool(
        name="analyze_mechanic_performance",
        function=mechanic_performance_tool,
        description="Analyze mechanic performance including response times and repair times.",
        category="maintenance",
        parameters={
            "action": {
                "type": "string",
                "description": "Action to perform ('analyze', 'run')",
                "required": True
            },
            "start_date": {
                "type": "string",
                "description": "Start date for analysis (YYYY-MM-DD)",
                "required": False
            },
            "end_date": {
                "type": "string",
                "description": "End date for analysis (YYYY-MM-DD)",
                "required": False
            },
            "mode": {
                "type": "string", 
                "description": "Date selection mode ('args' or 'interactive')",
                "required": False
            },
            "force": {
                "type": "boolean",
                "description": "Override 30-day frequency limit",
                "required": False
            }
        }
    )

# Register query tools
def register_query_tools():
    """Register query tools with the registry."""
    from src.MCP.query_manager import QueryManager
    from src.agents.maintenance.tools.query_tools.watchlist_query import WatchlistQueryTool
    from src.agents.maintenance.tools.query_tools.scheduled_maintenance_query import ScheduledMaintenanceQueryTool
    
    # Create query manager instance
    query_manager = QueryManager()
    
    # Register individual query tools with the manager
    query_manager.register_query_tool("watchlist", WatchlistQueryTool())
    query_manager.register_query_tool("scheduled_maintenance", ScheduledMaintenanceQueryTool())
    
    # Register the query manager as a single tool
    tool_registry.register_tool(
        name="quick_query",
        function=query_manager.execute_query,
        description="Execute quick database queries for common requests (maintenance tasks, watchlist items).",
        category="data_retrieval",
        parameters={
            "query": {
                "type": "string",
                "description": "Natural language query about maintenance tasks or performance measurements",
                "required": True
            }
        }
    )

# Register all tools
def register_all_tools():
    """Register all tools with the registry."""
    register_supabase_tools()
    register_maintenance_tools()
    # Don't register query tools during module initialization to avoid circular imports
    # They will be registered when the orchestrator initializes
    logger.info(f"Registered {len(tool_registry.get_tool_names())} tools")

# Function to register query tools later (called by orchestrator)
def register_query_tools_if_needed():
    """Register query tools if they haven't been registered yet."""
    if "quick_query" not in tool_registry.get_tool_names():
        try:
            register_query_tools()
            logger.info("Query tools registered successfully")
        except Exception as e:
            logger.error(f"Failed to register query tools: {e}")
            # Register a minimal quick_query tool as fallback
            register_minimal_quick_query()

def register_minimal_quick_query():
    """Register a minimal quick_query tool that works directly."""
    try:
        from src.MCP.query_manager import QueryManager
        from src.agents.maintenance.tools.query_tools.scheduled_maintenance_query import ScheduledMaintenanceQueryTool
        from src.agents.maintenance.tools.query_tools.watchlist_query import WatchlistQueryTool
        
        # Create query manager instance
        query_manager = QueryManager()
        
        # Create watchlist tool instance for details
        watchlist_tool = WatchlistQueryTool()
        
        # Register individual query tools with the manager
        query_manager.register_query_tool("watchlist", watchlist_tool)
        query_manager.register_query_tool("scheduled_maintenance", ScheduledMaintenanceQueryTool())
        
        # Register the query manager as a single tool
        tool_registry.register_tool(
            name="quick_query",
            function=query_manager.execute_query,
            description="Execute quick database queries for common requests (maintenance tasks, watchlist items).",
            category="data_retrieval",
            parameters={
                "query": {
                    "type": "string",
                    "description": "Natural language query about maintenance tasks or performance measurements",
                    "required": True
                }
            }
        )
        
        # Register watchlist details as a separate tool
        tool_registry.register_tool(
            name="get_watchlist_details",
            function=watchlist_tool.get_item_details,
            description="Get detailed information about a specific watchlist item, including performance metrics and recommendations.",
            category="data_retrieval",
            parameters={
                "mechanic_name": {
                    "type": "string",
                    "description": "Name of the mechanic (required if item_id not provided)",
                    "required": False
                },
                "issue_type": {
                    "type": "string", 
                    "description": "Type of issue (response_time or repair_time, required if item_id not provided)",
                    "required": False
                },
                "item_id": {
                    "type": "integer",
                    "description": "Specific watchlist item ID (optional if mechanic_name and issue_type provided)",
                    "required": False
                }
            }
        )
        
        logger.info("Minimal quick_query and watchlist details tools registered successfully")
    except Exception as e:
        logger.error(f"Failed to register minimal quick_query tool: {e}")
        raise
    
# Initialize basic tools when this module is imported (but not query tools)
try:
    register_supabase_tools()
    logger.info("Registered Supabase tools")
except Exception as e:
    logger.error(f"Failed to register Supabase tools: {e}")

try:
    register_maintenance_tools()
    logger.info("Registered maintenance tools")
except Exception as e:
    logger.error(f"Failed to register maintenance tools: {e}")

logger.info(f"Registered {len(tool_registry.get_tool_names())} basic tools")