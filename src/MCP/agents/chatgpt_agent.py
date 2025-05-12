import os
import sys
import logging
import json
import time
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime
from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam
from openai.types.chat.chat_completion import ChatCompletion

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("chatgpt_agent")

# Import utilities
from MCP.agents.utils.date_utils import date_utils
from MCP.context_manager import ContextManager
from MCP.token_tracker import token_tracker
from MCP.response_formatter import MCPResponseFormatter

class ChatGPTAgent:
    """
    Agent that uses OpenAI's GPT models with function calling.
    
    This agent handles direct responses and tool execution for simpler queries,
    and can determine when a query should be handed off to DeepSeek.
    """
    
    def __init__(self, tool_registry=None, context_manager=None):
        """
        Initialize the ChatGPT agent.
        
        Args:
            tool_registry: Optional tool registry for tool execution
            context_manager: Optional context manager for conversation history
        """
        self.tool_registry = tool_registry
        self.context_manager = context_manager or ContextManager()
        self.response_formatter = MCPResponseFormatter(tool_registry=tool_registry)
        
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
            
        self.model = "gpt-4o-mini"  # Using the specified model
        
        # Load prompts with correct path
        self.system_prompt = self._load_system_prompt()
        
        logger.info(f"ChatGPT Agent initialized with model {self.model}")
    
    def _load_system_prompt(self) -> str:
        """Load the system prompt from the correct location."""
        try:
            # Try loading from the maintenance prompts directory
            prompt_path = "/Users/melville/Documents/Industrial_Engineering_Agent/src/agents/maintenance/prompts/system_prompt.txt"
            
            if os.path.exists(prompt_path):
                with open(prompt_path, 'r') as f:
                    prompt = f.read().strip()
                    logger.info(f"Loaded system prompt from {prompt_path}")
                    return prompt
            else:
                logger.warning(f"System prompt file not found at {prompt_path}")
                
        except Exception as e:
            logger.error(f"Error loading system prompt: {e}")
        
        # Fallback to enhanced minimal prompt
        logger.info("Using fallback enhanced system prompt")
        return """You are a maintenance management assistant. Your goal is to help users with:
1. Scheduling and tracking maintenance tasks
2. Querying maintenance data
3. Analyzing performance metrics

When answering questions:
- Consider the context of previous messages
- For follow-up questions, analyze the previous response to understand what the user is referring to
- Think about whether you need to query the database or can answer from context
- Be specific and helpful in your responses
- When maintenance tasks are past due, calculate and mention by how many days

For complex analysis or when you need to examine trends, indicate that deeper investigation is needed."""
    
    def define_functions(self) -> List[Dict[str, Any]]:
        """
        Define the functions that will be available to the GPT model.
        
        Returns:
            List of function definitions (minimal for token savings)
        """
        functions = []
        
        # Quick query function (new)
        functions.append({
            "name": "quick_query",
            "description": "Execute quick database queries for common requests",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language query about maintenance tasks or performance measurements"
                    }
                },
                "required": ["query"]
            }
        })
        
        # ScheduledMaintenance function (simplified)
        functions.append({
            "name": "run_scheduled_maintenance",
            "description": "Run scheduled maintenance workflow",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["run", "check", "preview"]
                    },
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"}
                },
                "required": ["action"]
            }
        })
        
        # QueryDatabase function (for complex queries)
        functions.append({
            "name": "query_database",
            "description": "Query database for complex requests requiring interpretation",
            "parameters": {
                "type": "object",
                "properties": {
                    "query_params": {
                        "type": "string",
                        "description": "Query parameters in format: table:columns;filters;limit"
                    }
                },
                "required": ["query_params"]
            }
        })
        
        return functions
    
    def process_query(self, query: str, conversation_history: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
        """
        Process a user query with reduced token usage and better context awareness.
        
        Args:
            query: The user's query
            conversation_history: Optional conversation history
            
        Returns:
            Response dictionary with answer and metadata
        """
        try:
            # First check if this might be a follow-up question
            is_follow_up = self._is_follow_up_query(query, conversation_history)
            
            # Prepare messages with minimal context
            messages: List[ChatCompletionMessageParam] = [
                {"role": "system", "content": self.system_prompt}
            ]
            
            # Add only recent conversation history (last 3 exchanges)
            if conversation_history:
                recent_history = conversation_history[-6:]  # Last 3 user-assistant pairs
                for msg in recent_history:
                    role = msg["role"]
                    content = msg["content"]
                    if role in ["user", "assistant"]:
                        messages.append({
                            "role": role,  # type: ignore
                            "content": content
                        })
            
            # Add the current query with context hint if it's a follow-up
            if is_follow_up and conversation_history:
                # Extract relevant context from the last assistant message
                last_assistant_msg = None
                for msg in reversed(conversation_history):
                    if msg["role"] == "assistant":
                        last_assistant_msg = msg["content"]
                        break
                
                if last_assistant_msg:
                    messages.append({
                        "role": "user", 
                        "content": f"{query}\n\n(Context: The previous response listed maintenance tasks with their due dates.)"
                    })
                else:
                    messages.append({"role": "user", "content": query})
            else:
                messages.append({"role": "user", "content": query})
            
            # Get function definitions
            functions = self.define_functions()
            
            # Log token estimate before sending
            message_contents = [str(m.get("content", "")) for m in messages if m.get("content")]
            message_text = " ".join(message_contents)
            estimated_tokens = len(message_text) // 4
            logger.info(f"Estimated input tokens: {estimated_tokens}")
            
            # First try without functions for follow-up questions
            if is_follow_up:
                # Try to answer directly without function calling
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    max_tokens=500
                )
                
                # Track token usage
                token_tracker.track_openai_usage(response)
                
                message = response.choices[0].message
                
                # If the response seems like it needs a function, then call with functions
                if message.content and self._needs_function_call(message.content):
                    response = self.client.chat.completions.create(
                        model=self.model,
                        messages=messages,
                        functions=functions,  # type: ignore
                        function_call="auto",
                        max_tokens=500
                    )
                    message = response.choices[0].message
            else:
                # Regular query - try with functions
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    functions=functions,  # type: ignore
                    function_call="auto",
                    max_tokens=500
                )
                message = response.choices[0].message
            
            # Handle function calls
            if message.function_call:
                function_name = message.function_call.name
                function_args = json.loads(message.function_call.arguments)
                
                logger.info(f"Executing function: {function_name}")
                
                # Execute the function
                result = self._execute_function(function_name, function_args)
                
                # Format the result using the response formatter
                formatted_result = self.response_formatter.format_tool_result(result, function_name)
                
                # Add function result to messages (minimal) - fixed type issue
                messages.append({
                    "role": "assistant",
                    "function_call": {
                        "name": function_name,
                        "arguments": json.dumps(function_args)
                    },
                    "content": None
                })
                messages.append({
                    "role": "function",
                    "name": function_name,
                    "content": formatted_result[:1000]  # Limit function result size
                })
                
                # Get final response with minimal context
                final_response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages[-4:],  # Only send recent messages
                    functions=functions,  # type: ignore
                    function_call="none",  # Force it to respond without calling another function
                    max_tokens=500
                )
                
                # Track token usage
                token_tracker.track_openai_usage(final_response)
                
                message = final_response.choices[0].message
            
            # Check if DeepSeek is needed
            if message.content and self._requires_deepseek(message.content):
                return {
                    "answer": message.content,
                    "requires_deepseek": True,
                    "response": response
                }
            
            # Make sure we have a proper response
            final_answer = message.content
            if not final_answer or final_answer.strip() == "":
                final_answer = "I apologize, but I couldn't analyze the data properly. Could you please rephrase your question?"
            
            return {
                "answer": final_answer,
                "response": response
            }
            
        except Exception as e:
            logger.error(f"Error processing query: {e}", exc_info=True)
            return {
                "error": str(e),
                "answer": f"I apologize, but I encountered an error while processing your query: {str(e)}"
            }
    
    def _is_follow_up_query(self, query: str, conversation_history: Optional[List[Dict[str, str]]]) -> bool:
        """Check if this query is likely a follow-up to a previous response."""
        if not conversation_history:
            return False
        
        # Keywords that indicate follow-up questions
        follow_up_indicators = [
            "them", "they", "these", "those", "it", "that",
            "any of", "which ones", "how many", "what about",
            "more details", "tell me more", "explain", "past due",
            "overdue", "late", "expired", "behind schedule"
        ]
        
        query_lower = query.lower()
        for indicator in follow_up_indicators:
            if indicator in query_lower:
                logger.info(f"Detected follow-up query due to keyword: {indicator}")
                return True
        
        return False
    
    def _needs_function_call(self, content: str) -> bool:
        """Check if a response suggests it needs to call a function."""
        indicators = [
            "I need to query",
            "Let me check",
            "I'll look up",
            "need to find out",
            "database query",
            "I need to see",
            "let me search"
        ]
        
        content_lower = content.lower()
        for indicator in indicators:
            if indicator in content_lower:
                return True
        
        return False
    
    def _requires_deepseek(self, message: Optional[str]) -> bool:
        """
        Check if a message indicates the need for DeepSeek.
        
        Args:
            message: Message to check
            
        Returns:
            Boolean indicating if DeepSeek is needed
        """
        if not message:
            return False
            
        # Check for indicator phrases
        indicators = [
            "I need to analyze this more deeply",
            "hand this over",
            "deep analysis",
            "complex analysis",
            "advanced analytics",
            "need deeper reasoning",
            "requires more extensive analysis",
            "this requires deeper investigation"
        ]
        
        for indicator in indicators:
            if indicator.lower() in message.lower():
                return True
                
        return False
    
    def _execute_function(self, function_name: str, function_args: Dict[str, Any]) -> Any:
        """
        Execute a function call.
        
        Args:
            function_name: Name of the function to call
            function_args: Arguments for the function
            
        Returns:
            Result of the function call
        """
        try:
            if function_name == "quick_query":
                # Execute quick query through the tool registry
                if self.tool_registry:
                    result = self.tool_registry.execute_tool("quick_query", function_args)
                    return result
                else:
                    return {"error": "Tool registry not available"}
                    
            elif function_name == "run_scheduled_maintenance":
                # Execute scheduled maintenance
                if self.tool_registry:
                    result = self.tool_registry.execute_tool("run_scheduled_maintenance", function_args)
                    return result
                else:
                    return {"error": "Tool registry not available"}
                    
            elif function_name == "query_database":
                # Execute complex database query
                if self.tool_registry:
                    result = self.tool_registry.execute_tool("query_database", function_args)
                    return result
                else:
                    return {"error": "Tool registry not available"}
                    
            else:
                logger.error(f"Unknown function: {function_name}")
                return {"error": f"Unknown function '{function_name}'"}
                
        except Exception as e:
            logger.error(f"Error executing function {function_name}: {e}")
            return {"error": f"Error executing {function_name}: {str(e)}"}