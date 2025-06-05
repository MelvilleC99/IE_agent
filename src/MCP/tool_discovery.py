# /src/MCP/tool_discovery.py
"""
Tool Discovery and Auto-Registration
"""

import logging
import os
import importlib.util
from pathlib import Path

logger = logging.getLogger("tool_discovery")

class ToolDiscovery:
    """Discovers and auto-registers tools from the filesystem."""
    
    def __init__(self, tool_registry=None):
        """Initialize the tool discovery system."""
        self.tool_registry = tool_registry
        self.discovered_tools = {}
        logger.info("ToolDiscovery initialized")
    
    def discover_tools_in_directory(self, directory: str, category: str = "maintenance") -> int:
        """Discover and register tools in a directory."""
        if not os.path.exists(directory):
            logger.warning(f"Directory not found: {directory}")
            return 0
        
        tools_found = 0
        
        try:
            for file_path in Path(directory).glob("*.py"):
                if file_path.name.startswith("__"):
                    continue  # Skip __init__.py
                
                tools_in_file = self._scan_file_for_tools(str(file_path), category)
                tools_found += tools_in_file
                
            logger.info(f"Discovered {tools_found} tools in {directory}")
            return tools_found
            
        except Exception as e:
            logger.error(f"Error discovering tools: {e}")
            return 0

    def _scan_file_for_tools(self, file_path: str, category: str) -> int:
        """Scan a Python file for tool functions."""
        try:
            # Load module from file path
            spec = importlib.util.spec_from_file_location("tool_module", file_path)
            if not spec or not spec.loader:
                return 0
                
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            tools_found = 0
            
            # Look for tool functions (simple heuristic for now)
            for name, obj in vars(module).items():
                if callable(obj) and self._is_tool_function(name, obj):
                    self._register_discovered_tool(name, obj, category, file_path)
                    tools_found += 1
            
            return tools_found
            
        except Exception as e:
            logger.warning(f"Could not scan {file_path}: {e}")
            return 0
    
    def _is_tool_function(self, name: str, func) -> bool:
        """Check if a function is a tool function."""
        # Simple heuristics to identify tool functions
        if not callable(func):
            return False
        if name.startswith("_"):
            return False
        if name in ["main", "test", "setup"]:
            return False
        if name.endswith("_tool"):
            return True
        if "tool" in name.lower():
            return True
        return False
    
    def _register_discovered_tool(self, name: str, func, category: str, file_path: str):
        """Register a discovered tool with the tool registry."""
        if not self.tool_registry:
            return
            
        try:
            # Basic tool registration
            self.tool_registry.register_tool(
                name=name,
                function=func,
                description=f"Auto-discovered tool: {name}",
                category=category
            )
            
            self.discovered_tools[name] = {
                "function": func,
                "category": category,
                "file_path": file_path
            }
            
            logger.info(f"Auto-registered tool: {name} from {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to register tool {name}: {e}")
