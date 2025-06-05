# /src/MCP/agents/chatgpt/core_agent.py
"""
Core ChatGPT Agent - Stable Orchestrator

This is the main agent class that coordinates all the specialized components.
This file should remain stable and rarely need changes.
"""

import os
import sys
import logging
import json
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from openai import OpenAI
from openai.types.chat.chat_completion import ChatCompletion

# Add project root to path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import MCP components
from src.MCP.context_manager import ContextManager
from src.MCP.token_tracker import token_tracker
from src.MCP.response_formatter import MCPResponseFormatter
from src.MCP.function_generator import FunctionGenerator
from src.MCP.tool_executor import ToolExecutor

# Import specialized agent components
from .prompt_manager import PromptManager
from .message_builder import MessageBuilder
from .response_handler import ResponseHandler
from .session_detector import SessionDetector
from .context_helpers import ContextHelpers

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("chatgpt_agent")

class ChatGPTAgent:
    """
    Core ChatGPT Agent - Stable Orchestrator
    
    This agent coordinates specialized components to handle user queries.
    The core logic here should remain stable while components can be modified.
    """
    
    def __init__(self, tool_registry=None, context_manager=None):
        """
        Initialize the ChatGPT agent with all specialized components.
        
        Args:
            tool_registry: Optional tool registry for tool execution
            context_manager: Optional context manager for conversation history
        """
        # Core dependencies
        self.tool_registry = tool_registry
        self.context_manager = context_manager or ContextManager()
        
        # Initialize OpenAI client
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.error("OPENAI_API_KEY not found in environment variables")
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        
        try:
            self.client = OpenAI(api_key=api_key)
        except ImportError:
            logger.error("OpenAI package not installed. Run 'pip install openai'")
            raise ImportError("OpenAI package not installed. Run 'pip install openai'")
            
        self.model = "gpt-4o-mini"
        
        # Initialize specialized components
        self.prompt_manager = PromptManager()
        self.message_builder = MessageBuilder(self.prompt_manager)
        self.response_formatter = MCPResponseFormatter(tool_registry=tool_registry)
        self.response_handler = ResponseHandler(self.response_formatter)
        self.session_detector = SessionDetector()
        self.context_helpers = ContextHelpers(self.context_manager)
        
        # Initialize MCP components
        self.function_generator = FunctionGenerator(tool_registry=tool_registry)
        self.tool_executor = ToolExecutor(tool_registry=tool_registry)
        
        # Initialize usage tracker (will be set by orchestrator)
        self.usage_tracker = None
        
        logger.info(f"ChatGPT Agent initialized with model {self.model}")

    def set_usage_tracker(self, usage_tracker):
        """Set the usage tracker for cost tracking."""
        self.usage_tracker = usage_tracker
    
    def refresh_date(self):
        """Refresh the cached date - useful for long-running sessions that span midnight."""
        self.prompt_manager.refresh_date()
        logger.info("Date refreshed in ChatGPT agent")
    
    def define_functions(self) -> List[Dict[str, Any]]:
        """
        Define the functions that will be available to the GPT model.
        
        Returns:
            List of function definitions (auto-generated from tool registry)
        """
        try:
            functions = self.function_generator.generate_function_definitions()
            logger.info(f"Auto-generated {len(functions)} function definitions")
            return functions
        except Exception as e:
            logger.error(f"Error auto-generating functions, falling back to empty list: {e}")
            return []

    def process_query(self, query: str, conversation_history: Optional[List[Dict[str, str]]] = None, conversation_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a user query - STABLE CORE ORCHESTRATION.
        
        This method coordinates all components but rarely needs changes.
        
        Args:
            query: The user's query
            conversation_history: Optional conversation history
            conversation_id: Optional conversation ID for tracking
            
        Returns:
            Response dictionary with answer and metadata
        """
        # Refresh date for each query
        self.prompt_manager.refresh_date()
        current_date = self.prompt_manager.get_current_date()
        logger.info(f"Processing query with current date: {current_date['formatted_date']}")
        
        try:
            # Build messages using message builder
            messages = self.message_builder.build_messages(query, conversation_history)
            
            # Get function definitions
            functions = self.define_functions()
            
            # Log token estimate
            message_contents = [str(m.get("content", "")) for m in messages if m.get("content")]
            message_text = " ".join(message_contents)
            estimated_tokens = len(message_text) // 4
            logger.info(f"Estimated input tokens: {estimated_tokens}")
            
            # Call OpenAI API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                functions=functions,  # type: ignore
                function_call="auto",
                max_tokens=500
            )
            
            # Track token usage
            token_tracker.track_openai_usage(response)
            
            # Track API call for cost tracking
            if conversation_id and self.usage_tracker:
                try:
                    self.usage_tracker.track_api_call(
                        conversation_id=conversation_id,
                        model=self.model,
                        input_tokens=getattr(response.usage, 'prompt_tokens', 0) if response.usage else 0,
                        output_tokens=getattr(response.usage, 'completion_tokens', 0) if response.usage else 0,
                        agent_type="chatgpt"
                    )
                except Exception as e:
                    logger.warning(f"Failed to track API call: {e}")
            
            message = response.choices[0].message
            
            # Handle function calls
            if message.function_call:
                return self._handle_function_call(message, messages, functions, conversation_id)
            
            # Handle direct responses
            return self._handle_direct_response(message, response)
            
        except Exception as e:
            logger.error(f"Error processing query: {e}", exc_info=True)
            return {
                "error": str(e),
                "answer": f"I apologize, but I encountered an error while processing your query: {str(e)}"
            }

    def _handle_function_call(self, message, messages, functions, conversation_id):
        """Handle function call execution and response generation."""
        function_name = message.function_call.name
        function_args = json.loads(message.function_call.arguments)
        
        logger.info(f"Executing function: {function_name} with args: {function_args}")
        
        # Track tool usage for cost tracking
        if conversation_id and self.usage_tracker:
            try:
                self.usage_tracker.track_tool_usage(conversation_id, function_name)
            except Exception as e:
                logger.warning(f"Failed to track tool usage: {e}")
        
        # Execute the function using tool executor
        result = self.tool_executor.execute(function_name, function_args)
        
        # Store query metadata for follow-up context
        self.context_helpers.store_query_metadata(function_name, function_args, result)
        
        # Check if the function failed
        if isinstance(result, dict) and "error" in result:
            return {
                "answer": f"I encountered an error while trying to {function_name}: {result['error']}. Let me try to help you another way.",
                "response": message
            }
        
        # Format the result using response handler
        formatted_content = self.response_handler.format_function_result(result, function_name)
        
        # Check if result is already formatted as markdown table
        if "Here are the" in formatted_content and "|" in formatted_content:
            return {
                "answer": formatted_content,
                "response": message
            }
        
        # Add function result to messages and get final response
        messages.extend([
            {
                "role": "assistant",
                "function_call": {"name": function_name, "arguments": json.dumps(function_args)},
                "content": None
            },
            {
                "role": "function",
                "name": function_name,
                "content": formatted_content[:1000]  # Limit function result size
            }
        ])
        
        # Get final response
        final_response = self.client.chat.completions.create(
            model=self.model,
            messages=messages[-6:],  # Only send recent messages
            functions=functions,  # type: ignore
            function_call="none",  # Force it to respond without calling another function
            max_tokens=500
        )
        
        # Track token usage
        token_tracker.track_openai_usage(final_response)
        
        # Track final API call for cost tracking
        if conversation_id and self.usage_tracker:
            try:
                self.usage_tracker.track_api_call(
                    conversation_id=conversation_id,
                    model=self.model,
                    input_tokens=getattr(final_response.usage, 'prompt_tokens', 0) if final_response.usage else 0,
                    output_tokens=getattr(final_response.usage, 'completion_tokens', 0) if final_response.usage else 0,
                    agent_type="chatgpt"
                )
            except Exception as e:
                logger.warning(f"Failed to track final API call: {e}")
        
        final_message = final_response.choices[0].message
        
        # Check for goodbye or follow-up indicators in the response
        response_content = final_message.content or ""
        
        result = {
            "answer": response_content,
            "response": final_message
        }
        
        # Check if the response indicates a need for DeepSeek
        if self.session_detector.requires_deepseek(response_content):
            result["requires_deepseek"] = True
        
        # Check if this is a goodbye message
        if self.session_detector.is_goodbye_message(response_content):
            result["is_goodbye"] = True
        
        return result
    
    def _handle_direct_response(self, message, response):
        """Handle direct response from ChatGPT without function calls."""
        content = message.content or "I apologize, but I couldn't generate a response."
        
        result = {
            "answer": content,
            "response": message
        }
        
        # Check for special indicators in the response
        if self.session_detector.requires_deepseek(content):
            result["requires_deepseek"] = True
        
        if self.session_detector.is_goodbye_message(content):
            result["is_goodbye"] = True
        
        return result
