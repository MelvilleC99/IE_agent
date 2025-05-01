# /Users/melville/Documents/Industrial_Engineering_Agent/src/MCP/orchestrator.py

import logging
import json
import time
import os
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("mcp_orchestrator")

from .protocol import MCPProtocol
from .context_manager import MCPContextManager
from .tool_registry import MCPToolRegistry
from .response_formatter import MCPResponseFormatter
from .fast_path_detector import FastPathDetector

class MCPOrchestrator:
    """
    Main orchestrator for the Model Context Protocol (MCP) architecture.
    
    This class coordinates:
    1. Query classification and routing
    2. Context management
    3. Tool selection and execution
    4. Response formatting
    """
    
    def __init__(self):
        """Initialize the MCP Orchestrator."""
        logger.info("Initializing MCP Orchestrator")
        
        # Initialize all components
        self.protocol = MCPProtocol()
        self.context_manager = MCPContextManager()
        self.tool_registry = MCPToolRegistry()
        self.response_formatter = MCPResponseFormatter(tool_registry=self.tool_registry)
        self.fast_path_detector = FastPathDetector()
        
        # Load additional prompts (to be done in implementation)
        self.tool_selection_prompt = ""
        self.system_prompt = ""
        
        logger.info("MCP Orchestrator initialized")
    
    def load_prompts(self, system_prompt_path: str, tool_selection_path: str):
        """Load system and tool selection prompts."""
        try:
            with open(system_prompt_path, 'r') as f:
                self.system_prompt = f.read()
            logger.info(f"Loaded system prompt from: {system_prompt_path}")
            
            if os.path.exists(tool_selection_path):
                with open(tool_selection_path, 'r') as f:
                    self.tool_selection_prompt = f.read()
                logger.info(f"Loaded tool selection prompt from: {tool_selection_path}")
        except Exception as e:
            logger.error(f"Error loading prompts: {e}")
    
    def process_query(self, query: str, user_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Process a user query through the MCP workflow.
        
        Args:
            query: The user's query
            user_info: Optional user information
            
        Returns:
            Formatted response
        """
        start_time = time.time()
        logger.info(f"Processing query: {query}")
        
        # Add to conversation context
        self.context_manager.add_message("user", query)
        
        # STEP 1: Check if this is a simple query for fast path
        if self.fast_path_detector.is_simple_query(query):
            direct_response = self.fast_path_detector.get_direct_response(query)
            if direct_response:
                # Add to context and return
                self.context_manager.add_message("assistant", direct_response)
                execution_time = time.time() - start_time
                logger.info(f"Fast path response provided in {execution_time:.2f} seconds")
                return {"answer": direct_response}
        
        # STEP 2: Check if this should be routed to a specific tool
        tool_name, tool_params = self.fast_path_detector.get_tool_routing(query)
        
        if tool_name and tool_params is not None:
            logger.info(f"Direct routing to tool: {tool_name} with params: {tool_params}")
            
            try:
                # Get the tool function
                tool_function = self.tool_registry.get_tool_function(tool_name)
                
                if tool_function:
                    # Execute the tool
                    tool_start_time = time.time()
                    result = tool_function(**tool_params)
                    tool_execution_time = time.time() - tool_start_time
                    logger.info(f"Tool executed in {tool_execution_time:.2f} seconds")
                    
                    # Format the result directly using our formatter, not the LLM
                    if tool_name == "QueryDatabase":
                        # Use our formatter directly
                        format_start_time = time.time()
                        try:
                            # Try to parse as JSON if it's not already
                            if isinstance(result, str):
                                result_data = json.loads(result)
                            else:
                                result_data = result
                                
                            # Use the adaptive formatter directly
                            response = self.response_formatter.format_data_adaptively(result_data, query)
                        except Exception as e:
                            logger.error(f"Error formatting database result: {e}", exc_info=True)
                            response = f"Here's the information from the database:\n\n{result}"
                        
                        format_time = time.time() - format_start_time
                        logger.info(f"Result formatted in {format_time:.2f} seconds")
                    elif tool_name == "RunScheduledMaintenance":
                        response = self._format_maintenance_result(result)
                    else:
                        response = f"Here are the results:\n\n{result}"
                    
                    # Add to context
                    self.context_manager.add_message("assistant", response, tools_used=[tool_name])
                    
                    execution_time = time.time() - start_time
                    logger.info(f"Tool routing response provided in {execution_time:.2f} seconds")
                    return {"answer": response}
            except Exception as e:
                logger.error(f"Error in tool routing: {e}", exc_info=True)
                # Fall back to LLM if tool execution fails
        
        # STEP 3: Prepare for LLM processing
        current_context = self.context_manager.get_context()
        
        mcp_message = self.protocol.format_message(
            query=query,
            user_info=user_info or current_context.get("user_info", {}),
            conversation_history=current_context.get("conversation_history", []),
            known_entities=current_context.get("known_entities", {}),
            primary_instruction="Respond to the user's maintenance query",
            available_tools=self.tool_registry.get_tool_names(),
            expected_tool_usage="Use tools when specific data is needed."
        )
        
        logger.info("Query requires LLM processing")
        return {
            "_requires_llm": True,
            "_mcp_message": mcp_message,
            "_context": current_context
        }
    
    def process_llm_response(self, response: str, tools_used: Optional[List[str]] = None) -> Dict[str, Any]:
        """Process a response from the LLM."""
        processed = self.response_formatter.process_response(response)
        
        # Add to context
        self.context_manager.add_message(
            "assistant", 
            processed["answer"],
            tools_used=tools_used or processed.get("tools_used", [])
        )
        
        return processed
    
    def _format_maintenance_result(self, result: str) -> str:
        """Format the maintenance scheduling result in a user-friendly way."""
        try:
            # Try to parse as JSON
            result_json = json.loads(result)
            
            # Check if we have a well-structured response
            if "current_schedule" in result_json:
                # Extract the schedule
                schedule = result_json.get("current_schedule", [])
                
                if not schedule:
                    return "I ran the scheduled maintenance workflow, but there are currently no scheduled maintenance tasks."
                
                # Use the generic formatter
                return self.response_formatter.format_data_adaptively(
                    schedule, 
                    "show scheduled maintenance", 
                    data_type="scheduled_maintenance"
                )
            else:
                # More generic formatting for other result structures
                return f"I've run the scheduled maintenance workflow. Here are the results:\n\n{result}"
        except:
            # If not JSON or if there's an error, return the raw result
            return f"I've run the scheduled maintenance workflow. Here are the results:\n\n{result}"