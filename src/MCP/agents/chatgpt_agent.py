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
        
        # Cache the current date at initialization
        self.current_date = self._get_current_date()
        
        # Load prompts with correct path
        self.system_prompt = self._load_system_prompt()
        
        logger.info(f"ChatGPT Agent initialized with model {self.model} and current date: {self.current_date['formatted_date']}")
    
    def _get_current_date(self) -> Dict[str, str]:
        """Get the current date in multiple formats."""
        now = datetime.now()
        return {
            "current_date": now.strftime("%Y-%m-%d"),
            "current_datetime": now.isoformat(),
            "formatted_date": now.strftime("%B %d, %Y"),
            "day_of_week": now.strftime("%A"),
            "time": now.strftime("%H:%M:%S")
        }
    
    def _load_system_prompt(self) -> str:
        """Load the system prompt without any hardcoded dates."""
        try:
            # Try loading from the maintenance prompts directory
            prompt_path = "/Users/melville/Documents/Industrial_Engineering_Agent/src/agents/maintenance/prompts/system_prompt.txt"
            
            if os.path.exists(prompt_path):
                with open(prompt_path, 'r') as f:
                    prompt = f.read().strip()
                
                # Remove any hardcoded dates or date references
                # Don't add the current date here - we'll inject it as a message
                logger.info(f"Loaded system prompt from {prompt_path}")
                return prompt
            else:
                logger.warning(f"System prompt file not found at {prompt_path}")
                
        except Exception as e:
            logger.error(f"Error loading system prompt: {e}")
        
        # Fallback to enhanced minimal prompt without dates
        logger.info("Using fallback enhanced system prompt")
        return """You are a maintenance management assistant. 

Your goal is to help users with:
1. Scheduling and tracking maintenance tasks
2. Querying maintenance data
3. Analyzing performance metrics

When answering questions:
- ALWAYS use the current date provided by the get_current_date function result
- The current date is provided at the start of each conversation
- Use this date for all calculations and comparisons
- Never guess or use a date from training data
- Consider the context of previous messages
- For follow-up questions, analyze the previous response
- Be specific and helpful in your responses
- When maintenance tasks are past due, calculate overdue days based on the current date

For complex analysis, indicate that deeper investigation is needed."""
    
    def define_functions(self) -> List[Dict[str, Any]]:
        """
        Define the functions that will be available to the GPT model.
        
        Returns:
            List of function definitions (minimal for token savings)
        """
        functions = []
        
        # Quick query function
        functions.append({
            "name": "quick_query",
            "description": "Execute database queries for maintenance tasks, watchlist items, and performance data",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language query about maintenance tasks, mechanics, or performance measurements"
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
        # Always refresh the current date for each query
        self.current_date = self._get_current_date()
        logger.info(f"Processing query with current date: {self.current_date['formatted_date']}")
        
        try:
            # First check if this might be a follow-up question
            is_follow_up = self._is_follow_up_query(query, conversation_history)
            
            # Prepare messages with minimal context
            messages: List[ChatCompletionMessageParam] = [
                {"role": "system", "content": self.system_prompt}
            ]
            
            # ALWAYS inject the current date as a function message right after system prompt
            date_message = {
                "role": "function",
                "name": "get_current_date",
                "content": json.dumps(self.current_date)
            }
            messages.append(date_message)  # type: ignore
            logger.debug(f"Injected date message: {date_message['content']}")
            
            # Add a brief assistant acknowledgment of the date
            messages.append({
                "role": "assistant",
                "content": f"I understand that today is {self.current_date['formatted_date']} ({self.current_date['day_of_week']}). I'll use this date for all calculations."
            })
            
            def build_message(role: str, content: str) -> ChatCompletionMessageParam:
                return {"role": role, "content": content}  # type: ignore

            if conversation_history:
                recent_history = conversation_history[-6:]  # Last 3 user-assistant pairs
                for msg in recent_history:
                    role = msg["role"]
                    content = msg["content"]
                    if role in ["user", "assistant", "system", "function"] and content is not None:
                        messages.append(build_message(role, str(content)))
            
            # Add the current query
            messages.append(build_message("user", query))
            
            # Get function definitions
            functions = self.define_functions()
            
            # Log token estimate before sending
            message_contents = [str(m.get("content", "")) for m in messages if m.get("content")]
            message_text = " ".join(message_contents)
            estimated_tokens = len(message_text) // 4
            logger.info(f"Estimated input tokens: {estimated_tokens}")
            
            # Always try with functions available
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                functions=functions,  # type: ignore
                function_call="auto",
                max_tokens=500
            )
            
            # Track token usage
            token_tracker.track_openai_usage(response)
            
            message = response.choices[0].message
            
            # Handle function calls
            if message.function_call:
                function_name = message.function_call.name
                function_args = json.loads(message.function_call.arguments)
                
                logger.info(f"Executing function: {function_name} with args: {function_args}")
                
                # Execute the function
                result = self._execute_function(function_name, function_args)
                
                # Log the result
                logger.info(f"Function {function_name} result: {json.dumps(result)[:200]}...")
                
                # Check if the function failed
                if isinstance(result, dict) and "error" in result:
                    # If function failed, return a helpful message
                    return {
                        "answer": f"I encountered an error while trying to {function_name}: {result['error']}. Let me try to help you another way.",
                        "response": response
                    }
                
                # Format the result using the response formatter
                formatted_result = self.response_formatter.format_tool_result(result, function_name)
                
                # Add function result to messages
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
                    messages=messages[-6:],  # Only send recent messages
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
    
    def refresh_date(self):
        """Refresh the cached date - useful for long-running sessions that span midnight."""
        self.current_date = self._get_current_date()
        logger.info(f"Refreshed current date to: {self.current_date['formatted_date']}")
    
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
        Execute a function call with better error handling.
        
        Args:
            function_name: Name of the function to call
            function_args: Arguments for the function
            
        Returns:
            Result of the function call
        """
        try:
            logger.info(f"Executing {function_name} with args: {json.dumps(function_args)}")
            
            if function_name == "quick_query":
                # Extract the query string from function_args
                query_string = function_args.get("query", "")
                logger.info(f"Quick query string: {query_string}")
                
                if self.tool_registry:
                    # The query parameter should be passed as "query" not inside another dict
                    result = self.tool_registry.execute_tool("quick_query", {"query": query_string})
                    logger.info(f"Quick query execution result: {json.dumps(result)[:200]}...")
                    return result
                else:
                    logger.error("Tool registry not available for quick_query")
                    return {"error": "Tool registry not available"}
                    
            elif function_name == "run_scheduled_maintenance":
                if self.tool_registry:
                    result = self.tool_registry.execute_tool("run_scheduled_maintenance", function_args)
                    logger.info(f"Scheduled maintenance result: {json.dumps(result)[:200]}...")
                    return result
                else:
                    logger.error("Tool registry not available for run_scheduled_maintenance")
                    return {"error": "Tool registry not available"}
                    
            elif function_name == "query_database":
                if self.tool_registry:
                    result = self.tool_registry.execute_tool("query_database", function_args)
                    logger.info(f"Database query result: {json.dumps(result)[:200]}...")
                    return result
                else:
                    logger.error("Tool registry not available for query_database")
                    return {"error": "Tool registry not available"}
                    
            else:
                logger.error(f"Unknown function: {function_name}")
                return {"error": f"Unknown function '{function_name}'"}
                
        except Exception as e:
            logger.error(f"Error executing function {function_name}: {e}", exc_info=True)
            return {"error": f"Error executing {function_name}: {str(e)}"}