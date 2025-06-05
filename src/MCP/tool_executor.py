# /src/MCP/tool_executor.py
"""
Generic Tool Executor
"""

import logging
from typing import Dict, Any

logger = logging.getLogger("tool_executor")

class ToolExecutor:
    """
    Generic tool executor that routes tool calls to appropriate handlers.
    
    Replaces manual if/elif execution chains with simple, generic interface.
    """
    
    def __init__(self, tool_registry=None):
        """Initialize the tool executor."""
        self.tool_registry = tool_registry
        logger.info("ToolExecutor initialized")
    
    def execute(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute any tool by name with provided arguments."""
        if not self.tool_registry:
            logger.error("No tool registry available")
            return {"error": "Tool registry not available"}
        
        try:
            logger.info(f"Executing tool: {tool_name}")
            result = self.tool_registry.execute_tool(tool_name, arguments)
            logger.info(f"Tool {tool_name} executed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Error executing tool {tool_name}: {e}")
            return {"error": f"Tool execution failed: {str(e)}"}
    
    def is_tool_available(self, tool_name: str) -> bool:
        """Check if a tool is available for execution."""
        if not self.tool_registry:
            return False
        tool_info = self.tool_registry.get_tool_info(tool_name)
        return tool_info is not None
