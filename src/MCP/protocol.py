# /Users/melville/Documents/Industrial_Engineering_Agent/src/MCP/protocol.py

import json
import logging
import time
from typing import Dict, Any, List, Optional, Union

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("mcp_protocol")

class MCPProtocol:
    """
    Model Context Protocol (MCP) handler for standardized LLM interactions.
    
    The MCP protocol structures communication with LLMs to provide clear context,
    instructions, and expected responses. This leads to more efficient token usage
    and more reliable model behavior.
    """
    
    def __init__(self, version: str = "1.0"):
        """Initialize the MCP protocol handler."""
        self.version = version
        self.conversation_id = f"conv-{int(time.time())}"
        logger.info(f"Initialized MCP Protocol v{version} with conversation ID: {self.conversation_id}")
    
    def format_message(
        self,
        query: str,
        user_info: Optional[Dict[str, Any]] = None,
        conversation_history: Optional[List[Dict[str, Any]]] = None,
        known_entities: Optional[Dict[str, List[str]]] = None,
        primary_instruction: Optional[str] = None,
        clarifications: Optional[List[str]] = None,
        available_tools: Optional[List[str]] = None,
        expected_tool_usage: Optional[str] = None,
        relevant_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Format a message according to the MCP specification.
        
        Args:
            query: The user's query
            user_info: Information about the user (role, permissions, etc.)
            conversation_history: Previous messages in the conversation
            known_entities: Entities that have been discussed (mechanics, machines, etc.)
            primary_instruction: The main instruction for the LLM
            clarifications: Additional instructions or clarifications
            available_tools: Names of tools that can be used
            expected_tool_usage: Guidance on when/how to use tools
            relevant_data: Any relevant data for the query
            
        Returns:
            A structured MCP message
        """
        # Create the MCP message structure
        message = {
            "meta": {
                "protocol_version": self.version,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "conversation_id": self.conversation_id
            },
            "context": {
                "user_info": user_info or {},
                "conversation_history": conversation_history or [],
                "known_entities": known_entities or {}
            },
            "instruction": {
                "primary": primary_instruction or "Respond to the user's query",
                "clarifications": clarifications or []
            },
            "tools": {
                "available": available_tools or [],
                "expected_usage": expected_tool_usage or ""
            },
            "content": {
                "query": query,
                "relevant_data": relevant_data or {}
            }
        }
        
        logger.debug(f"Created MCP message: {json.dumps(message, indent=2)}")
        return message
    
    def parse_response(self, response: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Parse a response from the LLM into a structured format.
        
        Args:
            response: The raw response from the LLM
            
        Returns:
            A structured response object
        """
        # Handle string responses
        if isinstance(response, str):
            parsed = {
                "message": response,
                "tool_calls": [],
                "reasoning": "",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }
            return parsed
            
        # Handle dictionary responses (already structured)
        if isinstance(response, dict):
            # Ensure required fields exist
            parsed = {
                "message": response.get("message", ""),
                "tool_calls": response.get("tool_calls", []),
                "reasoning": response.get("reasoning", ""),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }
            return parsed
            
        # Default fallback
        logger.warning(f"Unexpected response type: {type(response)}")
        return {
            "message": str(response),
            "tool_calls": [],
            "reasoning": "",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
    
    def generate_system_prompt(self, mcp_message: Dict[str, Any]) -> str:
        """
        Generate a system prompt from an MCP message.
        
        Args:
            mcp_message: A structured MCP message
            
        Returns:
            A system prompt string
        """
        # Extract key components
        primary_instruction = mcp_message["instruction"]["primary"]
        clarifications = mcp_message["instruction"]["clarifications"]
        available_tools = mcp_message["tools"]["available"]
        expected_usage = mcp_message["tools"]["expected_usage"]
        
        # Format the prompt
        prompt_parts = [
            f"# Primary Instruction\n{primary_instruction}",
        ]
        
        # Add clarifications if present
        if clarifications:
            clarification_text = "\n".join([f"- {c}" for c in clarifications])
            prompt_parts.append(f"# Clarifications\n{clarification_text}")
        
        # Add tools if present
        if available_tools:
            tools_text = "\n".join([f"- {t}" for t in available_tools])
            prompt_parts.append(f"# Available Tools\n{tools_text}")
            
            if expected_usage:
                prompt_parts.append(f"# Expected Tool Usage\n{expected_usage}")
        
        # Combine all parts
        system_prompt = "\n\n".join(prompt_parts)
        logger.debug(f"Generated system prompt: {system_prompt}")
        
        return system_prompt